#![doc = include_str!("../README.md")]

pub mod args;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use cache_loader_async::{
    backing::{LruCacheBacking, TtlCacheBacking, TtlMeta},
    cache_api::{CacheEntry, LoadingCache, WithMeta},
};
use log::debug;
use metrics::increment_counter;
use reqwest::{Certificate, Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Instant};

use streambed::{
    delayer::Delayer,
    secret_store::{
        AppRoleAuthReply, Error, GetSecretReply, SecretData, SecretStore, UserPassAuthReply,
    },
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AppRoleAuthRequest {
    pub role_id: String,
    pub secret_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UserPassAuthRequest {
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct UserPassCreateUpdateRequest {
    pub username: String,
    pub password: String,
}

type TtlCache = LoadingCache<
    String,
    Option<GetSecretReply>,
    Error,
    TtlCacheBacking<
        String,
        CacheEntry<Option<GetSecretReply>, Error>,
        LruCacheBacking<String, (CacheEntry<Option<GetSecretReply>, Error>, Instant)>,
    >,
>;

/// A client interface that uses the Hashicorp Vault HTTP API.
#[derive(Clone)]
pub struct VaultSecretStore {
    cache: TtlCache,
    client: Client,
    client_token: Arc<Mutex<Option<String>>>,
    max_secrets_cached: usize,
    server: Url,
    ttl_field: Option<String>,
    unauthorized_timeout: Duration,
}

const APPROLE_AUTH_LABEL: &str = "approle_auth";
const SECRET_PATH_LABEL: &str = "secret_path";
const USERPASS_AUTH_LABEL: &str = "userpass_auth";
const USERPASS_CREATE_UPDATE_LABEL: &str = "userpass_create_update";

impl VaultSecretStore {
    /// Establish a new client to Hashicorp Vault. In the case where TLS is required,
    /// a root certificate may be provided e.g. when using self-signed certificates. TLS
    /// connections are encouraged.
    /// An unauthorized_timeout determines how long the server should wait before being
    /// requested again.
    /// A max_secrets_cached arg limits the number of secrets that can be held at any time.
    ///
    /// Avoid creating many new Vault secret stores and clone them instead so that HTTP
    /// connection pools can be shared.
    pub fn new(
        server: Url,
        server_cert: Option<Certificate>,
        tls_insecure: bool,
        unauthorized_timeout: Duration,
        max_secrets_cached: usize,
        ttl_field: Option<&str>,
    ) -> Self {
        let client = Client::builder().danger_accept_invalid_certs(tls_insecure);
        let client = if let Some(cert) = server_cert {
            client.add_root_certificate(cert)
        } else {
            client
        };

        Self::with_new_cache(
            client.build().unwrap(),
            server,
            unauthorized_timeout,
            max_secrets_cached,
            ttl_field.map(|s| s.to_string()),
        )
    }

    fn with_new_cache(
        client: Client,
        server: Url,
        unauthorized_timeout: Duration,
        max_secrets_cached: usize,
        ttl_field: Option<String>,
    ) -> Self {
        let client_token: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let retained_client_token = Arc::clone(&client_token);

        let retained_client = client.clone();
        let retained_server = server.clone();
        let retained_ttl_field = ttl_field.clone();
        let retained_unauthorized_timeout = unauthorized_timeout;

        let cache: TtlCache = LoadingCache::with_meta_loader(
            TtlCacheBacking::with_backing(
                unauthorized_timeout,
                LruCacheBacking::new(max_secrets_cached),
            ),
            move |secret_path| {
                let task_client_token = Arc::clone(&client_token);

                let task_client = client.clone();
                let task_server = server.clone();
                let task_ttl_field = ttl_field.clone();

                async move {
                    let mut delayer = Delayer::default();
                    loop {
                        increment_counter!("ss_get_secret_requests", SECRET_PATH_LABEL => secret_path.clone());

                        let mut builder = task_client.get(
                            task_server
                                .join(&format!("{}v1/secret/data/{}", task_server, secret_path))
                                .unwrap(),
                        );
                        if let Some(client_token) = task_client_token.lock().await.as_deref() {
                            builder = builder.header("X-Vault-Token", client_token)
                        }

                        let result = builder.send().await;
                        match result {
                            Ok(response) => {
                                if response.status() == StatusCode::FORBIDDEN {
                                    increment_counter!("ss_unauthorized", SECRET_PATH_LABEL => secret_path.clone());
                                    break Err(Error::Unauthorized);
                                } else {
                                    let secret_reply = if response.status().is_success() {
                                        response.json::<GetSecretReply>().await.ok()
                                    } else {
                                        debug!(
                                            "Secret store failure status while getting secret: {:?}",
                                            response.status()
                                        );
                                        increment_counter!("ss_other_reply_failures");
                                        None
                                    };
                                    let lease_duration = secret_reply.as_ref().map(|sr| {
                                        let mut lease_duration = None;
                                        if let Some(ttl_field) = task_ttl_field.as_ref() {
                                            if let Some(ttl) = sr.data.data.get(ttl_field) {
                                                if let Ok(ttl_duration) =
                                                    ttl.parse::<humantime::Duration>()
                                                {
                                                    lease_duration = Some(ttl_duration.into());
                                                }
                                            }
                                        }
                                        lease_duration.unwrap_or_else(|| {
                                            Duration::from_secs(sr.lease_duration)
                                        })
                                    });
                                    break Ok(secret_reply)
                                        .with_meta(lease_duration.map(TtlMeta::from));
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Secret store is unavailable while getting secret. Error: {:?}",
                                    e
                                );
                                increment_counter!("ss_unavailables");
                            }
                        }
                        delayer.delay().await;
                    }
                }
            },
        );

        Self {
            cache,
            client: retained_client,
            client_token: retained_client_token,
            max_secrets_cached,
            ttl_field: retained_ttl_field,
            server: retained_server,
            unauthorized_timeout: retained_unauthorized_timeout,
        }
    }

    pub fn with_new_auth_prepared(ss: &Self) -> Self {
        Self::with_new_cache(
            ss.client.clone(),
            ss.server.clone(),
            ss.unauthorized_timeout,
            ss.max_secrets_cached,
            ss.ttl_field.clone(),
        )
    }
}

#[async_trait]
impl SecretStore for VaultSecretStore {
    async fn approle_auth(
        &self,
        role_id: &str,
        secret_id: &str,
    ) -> Result<AppRoleAuthReply, Error> {
        loop {
            let role_id = role_id.to_string();

            increment_counter!("ss_approle_auth_requests", APPROLE_AUTH_LABEL => role_id.clone());

            let task_client_token = Arc::clone(&self.client_token);
            let result = self
                .client
                .post(
                    self.server
                        .join(&format!("{}v1/auth/approle/login", self.server))
                        .unwrap(),
                )
                .json(&AppRoleAuthRequest {
                    role_id: role_id.to_string(),
                    secret_id: secret_id.to_string(),
                })
                .send()
                .await;
            match result {
                Ok(response) => {
                    if response.status() == StatusCode::FORBIDDEN {
                        increment_counter!("ss_unauthorized", APPROLE_AUTH_LABEL => role_id.clone());
                        break Err(Error::Unauthorized);
                    } else {
                        let secret_reply = if response.status().is_success() {
                            let approle_auth_reply = response
                                .json::<AppRoleAuthReply>()
                                .await
                                .map_err(|_| Error::Unauthorized);
                            if let Ok(r) = &approle_auth_reply {
                                let mut client_token = task_client_token.lock().await;
                                *client_token = Some(r.auth.client_token.clone());
                            }
                            approle_auth_reply
                        } else {
                            debug!(
                                "Secret store failure status while authenticating: {:?}",
                                response.status()
                            );
                            increment_counter!("ss_other_reply_failures", APPROLE_AUTH_LABEL => role_id.clone());
                            Err(Error::Unauthorized)
                        };
                        break secret_reply;
                    }
                }
                Err(e) => {
                    debug!(
                        "Secret store is unavailable while authenticating. Error: {:?}",
                        e
                    );
                    increment_counter!("ss_unavailables");
                }
            }
        }
    }

    async fn create_secret(
        &self,
        _secret_path: &str,
        _secret_data: SecretData,
    ) -> Result<(), Error> {
        todo!()
    }

    async fn get_secret(&self, secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
        self.cache
            .get(secret_path.to_owned())
            .await
            .map_err(|e| e.as_loading_error().unwrap().clone()) // Unsure how we can deal with caching issues
    }

    async fn userpass_auth(
        &self,
        username: &str,
        password: &str,
    ) -> Result<UserPassAuthReply, Error> {
        let username = username.to_string();

        increment_counter!("ss_userpass_auth_requests", USERPASS_AUTH_LABEL => username.clone());

        let task_client_token = Arc::clone(&self.client_token);
        let result = self
            .client
            .post(
                self.server
                    .join(&format!(
                        "{}v1/auth/userpass/login/{}",
                        self.server, username
                    ))
                    .unwrap(),
            )
            .json(&UserPassAuthRequest {
                password: password.to_string(),
            })
            .send()
            .await;
        match result {
            Ok(response) => {
                if response.status() == StatusCode::FORBIDDEN {
                    increment_counter!("ss_unauthorized", USERPASS_AUTH_LABEL => username.clone());
                    Err(Error::Unauthorized)
                } else {
                    let secret_reply = if response.status().is_success() {
                        let userpass_auth_reply = response
                            .json::<UserPassAuthReply>()
                            .await
                            .map_err(|_| Error::Unauthorized);
                        if let Ok(r) = &userpass_auth_reply {
                            let mut client_token = task_client_token.lock().await;
                            *client_token = Some(r.auth.client_token.clone());
                        }
                        userpass_auth_reply
                    } else {
                        debug!(
                            "Secret store failure status while authenticating: {:?}",
                            response.status()
                        );
                        increment_counter!("ss_other_reply_failures", USERPASS_AUTH_LABEL => username.clone());
                        Err(Error::Unauthorized)
                    };
                    secret_reply
                }
            }
            Err(e) => {
                debug!(
                    "Secret store is unavailable while authenticating. Error: {:?}",
                    e
                );
                increment_counter!("ss_unavailables");
                Err(Error::Unauthorized)
            }
        }
    }

    async fn token_auth(&self, _token: &str) -> Result<(), Error> {
        todo!()
    }

    async fn userpass_create_update_user(
        &self,
        current_username: &str,
        username: &str,
        password: &str,
    ) -> Result<(), Error> {
        let username = username.to_string();

        increment_counter!("ss_userpass_create_updates", USERPASS_CREATE_UPDATE_LABEL => username.clone());

        let mut builder = self
            .client
            .post(
                self.server
                    .join(&format!(
                        "{}v1/auth/userpass/users/{}",
                        self.server, current_username
                    ))
                    .unwrap(),
            )
            .json(&UserPassCreateUpdateRequest {
                username: username.to_string(),
                password: password.to_string(),
            });
        if let Some(client_token) = self.client_token.lock().await.as_deref() {
            builder = builder.header("X-Vault-Token", client_token)
        }

        let result = builder.send().await;
        match result {
            Ok(response) => {
                if response.status() == StatusCode::FORBIDDEN {
                    increment_counter!("ss_unauthorized", USERPASS_CREATE_UPDATE_LABEL => username.clone());
                    Err(Error::Unauthorized)
                } else if response.status().is_success() {
                    let _ = response.json::<()>().await.map_err(|_| Error::Unauthorized);
                    Ok(())
                } else {
                    debug!(
                        "Secret store failure status while creating/updating userpass: {:?}",
                        response.status()
                    );
                    increment_counter!("ss_other_reply_failures", USERPASS_CREATE_UPDATE_LABEL => username.clone());
                    Err(Error::Unauthorized)
                }
            }
            Err(e) => {
                debug!(
                    "Secret store is unavailable while creating/updating userpass. Error: {:?}",
                    e
                );
                increment_counter!("ss_unavailables");
                Err(Error::Unauthorized)
            }
        }
    }
}
