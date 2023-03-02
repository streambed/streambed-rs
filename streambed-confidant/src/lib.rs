#![doc = include_str!("../README.md")]

pub mod args;

use async_trait::async_trait;
use cache_loader_async::{
    backing::{LruCacheBacking, TtlCacheBacking, TtlMeta},
    cache_api::{CacheEntry, LoadingCache, WithMeta},
};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::prelude::{MetadataExt, PermissionsExt};
use std::{
    collections::HashMap,
    io::ErrorKind,
    path::PathBuf,
    time::{Duration, SystemTime},
};
use streambed::{
    crypto::{self, KEY_SIZE, SALT_SIZE},
    secret_store::{
        AppRoleAuthReply, AuthToken, Error, GetSecretReply, SecretData, SecretStore,
        UserPassAuthReply,
    },
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

const AUTHORIZED_SECRET_TTL: Duration = Duration::from_secs(60 * 5);
const USERPASS_LEASE_TIME: Duration = Duration::from_secs(86400 * 7);

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

#[derive(Deserialize, Serialize)]
struct StorableSecretData {
    version: u32,
    secret_data: SecretData,
}

#[derive(Debug, Deserialize, Serialize)]
struct TokenData {
    username: String,
    expires: u128,
}

#[derive(Debug, Deserialize)]
struct ClientToken {
    data: TokenData,
    signature: String,
}

/// A secret store implementation that uses the file system as its
/// backing store.
/// An unauthorized_timeout determines how long the server should wait before being
/// requested again.
/// A max_secrets_cached arg limits the number of secrets that can be held at any time.

#[derive(Clone)]
pub struct FileSecretStore {
    cache: TtlCache,
    max_secrets_cached: usize,
    root_path: PathBuf,
    root_secret: [u8; KEY_SIZE],
    ttl_field: Option<String>,
    unauthorized_timeout: Duration,
}

impl FileSecretStore {
    pub fn new<P: Into<PathBuf>>(
        root_path: P,
        root_secret: &[u8; KEY_SIZE],
        unauthorized_timeout: Duration,
        max_secrets_cached: usize,
        ttl_field: Option<&str>,
    ) -> Self {
        let root_path = root_path.into();
        Self::with_new_cache(
            root_path,
            root_secret,
            unauthorized_timeout,
            max_secrets_cached,
            ttl_field.map(|s| s.to_string()),
        )
    }

    fn with_new_cache(
        root_path: PathBuf,
        root_secret: &[u8; KEY_SIZE],
        unauthorized_timeout: Duration,
        max_secrets_cached: usize,
        ttl_field: Option<String>,
    ) -> Self {
        let retained_root_path = root_path.clone();
        let retained_root_secret = root_secret;
        let retained_ttl_field = ttl_field.clone();

        let root_secret = *root_secret;

        let cache: TtlCache = LoadingCache::with_meta_loader(
            TtlCacheBacking::with_backing(
                unauthorized_timeout,
                LruCacheBacking::new(max_secrets_cached),
            ),
            move |secret_path| {
                let task_root_path = root_path.clone();
                let task_ttl_field = ttl_field.clone();

                async move {
                    let mut result = Err(Error::Unauthorized);
                    match fs::File::open(task_root_path.join(secret_path)).await {
                        Ok(mut file) => {
                            let mut buf = Vec::new();
                            if file.read_to_end(&mut buf).await.is_ok()
                                && buf.len() >= crypto::SALT_SIZE
                            {
                                let (salt, bytes) = buf.split_at_mut(crypto::SALT_SIZE);
                                if let Ok(salt) = salt.try_into() {
                                    crypto::decrypt(bytes, &root_secret, &salt);
                                    if let Ok(stored) =
                                        postcard::from_bytes::<StorableSecretData>(bytes)
                                    {
                                        let secret_data = stored.secret_data;
                                        let mut lease_duration = None;
                                        if let Some(ttl_field) = task_ttl_field {
                                            if let Some(ttl) = secret_data.data.get(&ttl_field) {
                                                if let Ok(ttl_duration) =
                                                    ttl.parse::<humantime::Duration>()
                                                {
                                                    lease_duration = Some(ttl_duration.into());
                                                }
                                            }
                                        }

                                        result = Ok(Some(GetSecretReply {
                                            lease_duration: lease_duration
                                                .unwrap_or(AUTHORIZED_SECRET_TTL)
                                                .as_secs(),
                                            data: secret_data,
                                        }))
                                        .with_meta(lease_duration.map(TtlMeta::from))
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::NotFound => {
                            result = Ok(None).with_meta(None);
                        }
                        Err(_) => (),
                    }
                    result
                }
            },
        );

        Self {
            cache,
            root_path: retained_root_path,
            root_secret: *retained_root_secret,
            max_secrets_cached,
            ttl_field: retained_ttl_field,
            unauthorized_timeout,
        }
    }

    pub fn with_new_auth_prepared(ss: &Self) -> Self {
        Self::with_new_cache(
            ss.root_path.clone(),
            &ss.root_secret,
            ss.unauthorized_timeout,
            ss.max_secrets_cached,
            ss.ttl_field.clone(),
        )
    }

    fn hashed(password: &str, salt: &[u8; SALT_SIZE]) -> Vec<u8> {
        crypto::hash(password.as_bytes(), salt)
    }
}

#[async_trait]
impl SecretStore for FileSecretStore {
    /// Authentication is a noop for this secret store, and it will always succeed.
    /// Authentication is essentially implied given the user a host process is
    /// assigned to.
    async fn approle_auth(
        &self,
        _role_id: &str,
        _secret_id: &str,
    ) -> Result<AppRoleAuthReply, Error> {
        Ok(AppRoleAuthReply {
            auth: AuthToken {
                client_token: "some-token".to_string(),
                lease_duration: u64::MAX,
            },
        })
    }

    async fn create_secret(&self, secret_path: &str, secret_data: SecretData) -> Result<(), Error> {
        match fs::metadata(&self.root_path).await {
            #[cfg(unix)]
            Ok(attrs) if attrs.permissions().mode() & 0o077 != 0 => Err(Error::Unauthorized),
            Ok(attrs) => {
                let mut result = Err(Error::Unauthorized);

                let path = self.root_path.join(secret_path);
                if let Some(parent) = path.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }

                let mut file_options = fs::OpenOptions::new();
                let mut open_options = file_options.create(true).write(true);
                #[cfg(unix)]
                {
                    open_options = open_options.mode(attrs.mode());
                }

                if let Ok(mut file) = open_options.open(path).await {
                    let stored = StorableSecretData {
                        version: 0,
                        secret_data,
                    };
                    if let Ok(mut bytes) = postcard::to_stdvec(&stored) {
                        let salt = {
                            let mut rng = ThreadRng::default();
                            crypto::salt(&mut rng)
                        };
                        crypto::encrypt(&mut bytes, &self.root_secret, &salt);
                        let mut buf = Vec::with_capacity(SALT_SIZE + bytes.len());
                        buf.extend(salt);
                        buf.extend(bytes);

                        if file.write_all(&buf).await.is_ok() && file.sync_all().await.is_ok() {
                            result = Ok(());
                            // We should be able to read our writes
                            let _ = self.cache.remove(secret_path.to_string()).await;
                        }
                    }
                }
                result
            }
            Err(_) => Err(Error::Unauthorized),
        }
    }

    async fn get_secret(&self, secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
        match fs::metadata(&self.root_path).await {
            #[cfg(unix)]
            Ok(attrs) if attrs.permissions().mode() & 0o077 != 0 => Err(Error::Unauthorized),
            Ok(_) => self
                .cache
                .get(secret_path.to_string())
                .await
                .map_err(|e| e.as_loading_error().unwrap().clone()), // Unsure how we can deal with caching issues
            Err(_) => Err(Error::Unauthorized),
        }
    }

    async fn userpass_auth(
        &self,
        username: &str,
        password: &str,
    ) -> Result<UserPassAuthReply, Error> {
        if let Ok(Some(data)) = self
            .get_secret(&format!("auth/userpass/users/{username}"))
            .await
        {
            if let Some(data_password) = data.data.data.get("password") {
                if let Ok(data_password) = hex::decode(data_password) {
                    let (salt, _) = data_password.split_at(SALT_SIZE);
                    if let Ok(salt) = salt.try_into() {
                        let password = Self::hashed(password, &salt);
                        if password == data_password {
                            let now = SystemTime::now();
                            let expires = now
                                .checked_add(USERPASS_LEASE_TIME)
                                .unwrap_or(now)
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .map(|t| t.as_millis())
                                .unwrap_or(0);
                            let data =
                                format!(r#"{{"username":"{username}","expires":{expires}}}"#);
                            let signature =
                                hex::encode(crypto::sign(data.as_bytes(), &self.root_secret));
                            return Ok(UserPassAuthReply {
                                auth: AuthToken {
                                    client_token: base64::encode(format!(
                                        r#"{{"data":{data},"signature":"{signature}"}}"#
                                    )),
                                    lease_duration: USERPASS_LEASE_TIME.as_secs(),
                                },
                            });
                        }
                    }
                }
            }
        }
        Err(Error::Unauthorized)
    }

    async fn token_auth(&self, token: &str) -> Result<(), Error> {
        if let Ok(token) = base64::decode(token) {
            if let Ok(client_token) = serde_json::from_slice::<ClientToken>(&token) {
                let data = serde_json::to_string(&client_token.data).unwrap();
                if let Ok(signature) = hex::decode(&client_token.signature) {
                    if crypto::verify(data.as_bytes(), &self.root_secret, &signature) {
                        let now = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|t| t.as_millis())
                            .unwrap_or(0);
                        if now <= client_token.data.expires {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Err(Error::Unauthorized)
    }

    async fn userpass_create_update_user(
        &self,
        _current_username: &str,
        username: &str,
        password: &str,
    ) -> Result<(), Error> {
        let salt = {
            let mut rng = ThreadRng::default();
            crypto::salt(&mut rng)
        };
        let password = Self::hashed(password, &salt);
        let mut data = HashMap::new();
        data.insert("password".to_string(), hex::encode(password));
        let data = SecretData { data };
        self.create_secret(&format!("auth/userpass/users/{username}"), data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env};

    use streambed::crypto;
    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    #[cfg(unix)]
    async fn test_set_get_secret() {
        let confidant_dir = env::temp_dir().join("test_set_get_secret");
        let _ = fs::remove_dir_all(&confidant_dir).await;
        let _ = fs::create_dir_all(&confidant_dir).await;
        println!("Writing to {}", confidant_dir.to_string_lossy());

        let ss = FileSecretStore::new(
            confidant_dir.clone(),
            &[0; crypto::KEY_SIZE],
            Duration::from_secs(1),
            1,
            Some("ttl"),
        );

        // Establish a secret store for our service as a whole
        ss.approle_auth("role_id", "secret_id").await.unwrap();

        let mut data = HashMap::new();
        data.insert("key".to_string(), "value".to_string());
        data.insert("ttl".to_string(), "60m".to_string());
        let data = SecretData { data };

        // Set up the permissions to cause secret creation failure
        fs::set_permissions(&confidant_dir, PermissionsExt::from_mode(0o755))
            .await
            .unwrap();

        // This should fail as we don't have the correct file permissions.
        // We are looking for the confidant dir to have owner permissions
        // only + the ability for the owner to write.
        assert!(ss.create_secret("some.secret", data.clone()).await.is_err());

        // Let's now set up the correct permissions
        fs::set_permissions(&confidant_dir, PermissionsExt::from_mode(0o700))
            .await
            .unwrap();

        assert!(ss.create_secret("some.secret", data.clone()).await.is_ok());

        // Try reading a secret that doesn't exist. It should fail by returning
        // None.
        assert!(ss.get_secret("some.other.secret").await.unwrap().is_none());

        // Now read the secret we wrote before - all should be well.
        assert_eq!(
            ss.get_secret("some.secret").await,
            Ok(Some(GetSecretReply {
                lease_duration: 3600,
                data
            }))
        );

        // Create a user
        assert!(ss
            .userpass_create_update_user("mitchellh", "mitchellh", "foo")
            .await
            .is_ok());

        // Login as that user
        let user_ss = FileSecretStore::with_new_auth_prepared(&ss);
        let userpass_auth = user_ss.userpass_auth("mitchellh", "foo").await.unwrap();

        // Login as that user with the wrong password
        let bad_user_ss = FileSecretStore::with_new_auth_prepared(&ss);
        assert!(bad_user_ss
            .userpass_auth("mitchellh", "foo2")
            .await
            .is_err());

        // Auth login from the previous valid userpass token
        let token_ss = FileSecretStore::with_new_auth_prepared(&ss);
        token_ss
            .token_auth(&userpass_auth.auth.client_token)
            .await
            .unwrap();
    }
}
