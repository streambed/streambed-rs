#![doc = include_str!("README.md")]

use crate::secret_store::{
    AppRoleAuthReply, AuthToken, Error, GetSecretReply, SecretData, SecretStore,
};
use async_trait::async_trait;
use cache_loader_async::{
    backing::{LruCacheBacking, TtlCacheBacking, TtlMeta},
    cache_api::{CacheEntry, LoadingCache, WithMeta},
};
use serde::{Deserialize, Serialize};
use std::{
    io::ErrorKind,
    os::unix::prelude::{MetadataExt, PermissionsExt},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

const AUTHORIZED_SECRET_TTL: Duration = Duration::from_secs(60 * 5);

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

/// A secret store implementation that uses the file system as its
/// backing store.
/// An unauthorized_timeout determines how long the server should wait before being
/// requested again.
/// A max_secrets_cached arg limits the number of secrets that can be held at any time.
#[derive(Clone)]
pub struct FileSecretStore {
    cache: TtlCache,
    root_path: PathBuf,
}

impl FileSecretStore {
    pub fn new(
        root_path: PathBuf,
        unauthorized_timeout: Duration,
        max_secrets_cached: usize,
        ttl_field: Option<&str>,
    ) -> Self {
        let retained_root_path = root_path.clone();
        let ttl_field = ttl_field.map(|s| s.to_string());
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
                            if file.read_to_end(&mut buf).await.is_ok() {
                                if let Ok(stored) = postcard::from_bytes::<StorableSecretData>(&buf)
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
        }
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
            Ok(attrs) if attrs.permissions().mode() & 0o077 != 0 => Err(Error::Unauthorized),
            Ok(attrs) => {
                let mut result = Err(Error::Unauthorized);
                if let Ok(mut file) = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .mode(attrs.mode())
                    .open(self.root_path.join(secret_path))
                    .await
                {
                    let stored = StorableSecretData {
                        version: 0,
                        secret_data,
                    };
                    if let Ok(buf) = postcard::to_stdvec(&stored) {
                        if file.write_all(&buf).await.is_ok() {
                            result = Ok(());
                            let _ = self.cache.remove(secret_path.to_string()); // We should be able to read our writes
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
            Ok(attrs) if attrs.permissions().mode() & 0o077 != 0 => Err(Error::Unauthorized),
            Ok(_) => self
                .cache
                .get(secret_path.to_string())
                .await
                .map_err(|e| e.as_loading_error().unwrap().clone()), // Unsure how we can deal with caching issues
            Err(_) => Err(Error::Unauthorized),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env};

    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    async fn test_set_get_secret() {
        let confidant_dir = env::temp_dir().join("test_set_get_secret");
        let _ = fs::remove_dir_all(&confidant_dir).await;
        let _ = fs::create_dir_all(&confidant_dir).await;
        println!("Writing to {}", confidant_dir.to_string_lossy());

        let ss = FileSecretStore::new(
            confidant_dir.clone(),
            Duration::from_secs(1),
            1,
            Some("ttl"),
        );

        // We don't need to auth, but doing so is a noop.
        ss.approle_auth("role_id", "secret_id").await.unwrap();

        let mut data = HashMap::new();
        data.insert("key".to_string(), "value".to_string());
        data.insert("ttl".to_string(), "60m".to_string());
        let data = SecretData { data };

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
    }
}
