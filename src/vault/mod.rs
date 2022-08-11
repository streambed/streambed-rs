//! Provides an implementation of the secret store to be used with the
//! [Hashicorp Vault API](https://www.vaultproject.io/api-docs).

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

use crate::{
    delayer::Delayer,
    secret_store::{AppRoleAuthReply, Error, GetSecretReply, SecretStore},
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AppRoleAuthRequest {
    pub role_id: String,
    pub secret_id: String,
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
    server: Url,
}

const APPROLE_AUTH_LABEL: &str = "approle_auth";
const SECRET_PATH_LABEL: &str = "secret_path";

impl VaultSecretStore {
    /// Establish a new client to Hashicorp Vault. In the case where TLS is required,
    /// a root certificate may be provided e.g. when using self-signed certificates. TLS
    /// connections are encouraged.
    /// An unauthorized_timeout determines how long the server should wait before being
    /// requested again.
    /// A max_secrets_cached arg limits the number of secrets that can be held at any time.
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

        let client_token: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let retained_client_token = Arc::clone(&client_token);

        let client = client.build().unwrap();
        let ttl_field = ttl_field.map(|s| s.to_string());

        let retained_client = client.clone();
        let retained_server = server.clone();

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
                    let mut delayer = Delayer::new();
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
            server: retained_server,
        }
    }
}

#[async_trait]
impl SecretStore for VaultSecretStore {
    /// Perform an app authentication given a role and secret. If successful, then the
    /// secret store will be updated with a client token thereby permitting subsequent
    /// operations including getting secrets.
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
                .put(
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

    /// Attempt to access a secret. An optional value of None in reply means that
    /// the client is unauthorized to obtain it - either due to authorization
    /// or it may just not exist.
    async fn get_secret(&self, secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
        self.cache
            .get(secret_path.to_owned())
            .await
            .map_err(|e| e.as_loading_error().unwrap().clone()) // Unsure how we can deal with caching issues
    }
}
