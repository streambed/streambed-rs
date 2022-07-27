use std::time::Duration;
use std::{error::Error, path::Path};

use log::{info, warn};
use rand::RngCore;
use reqwest::Certificate;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::time;
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader},
};

pub mod base64_serde;
pub mod commit_log;
pub mod crypto;
pub mod kafka;
pub mod secret_store;
pub mod vault;

/// Read the secret store's secret id associated with a service's role.
/// We read the secret from stdin to avoid requiring secrets being
/// persisted elsewhere.
pub async fn secret_id_from_stdin() -> Result<String, io::Error> {
    let mut ss_secret_id = String::new();
    let mut reader = BufReader::new(io::stdin());
    reader.read_line(&mut ss_secret_id).await?;
    let len = ss_secret_id.trim_end_matches(&['\r', '\n'][..]).len();
    ss_secret_id.truncate(len);
    Ok(ss_secret_id)
}

/// Read a file as a pem file and return its corresponding Reqwest certificate.
pub async fn pem_from_file(path: &Path) -> Result<Certificate, Box<dyn Error>> {
    let mut f = fs::File::open(path).await?;
    let mut buf = vec![];
    f.read_to_end(&mut buf).await?;
    Certificate::from_pem(&buf).map_err(|e| e.into())
}

/// Performs an initial authentication with the secret store and also spawns a
/// task to re-authenticate on token expiry. A timeout is provided to cause the
/// re-authentication to sleep between non-successful authentication attempts.
pub async fn authenticate_secret_store(
    ss: impl secret_store::SecretStore + Sync + Send + Clone + 'static,
    role_id: &str,
    secret_id: &str,
    unauthenticated_timeout: &Duration,
) {
    let mut approle_auth_reply = ss.approle_auth(role_id, secret_id).await;
    let auth_role_id = role_id.to_string();
    let auth_secret_id = secret_id.to_string();
    let auth_unauthenticated_timeout = *unauthenticated_timeout;
    tokio::spawn(async move {
        let mut never_reported_info = true;
        let mut never_reported_warn = true;
        loop {
            match &approle_auth_reply {
                Ok(approle_auth) => {
                    if never_reported_info {
                        info!("Initially authenticated with the secret store");
                        never_reported_info = false;
                    }
                    time::sleep(Duration::from_secs(approle_auth.auth.lease_duration)).await;
                    approle_auth_reply = ss.approle_auth(&auth_role_id, &auth_secret_id).await;
                }
                Err(e) => {
                    if never_reported_warn {
                        warn!(
                            "Unable to initially authenticate with the secret store. Error: {:?}",
                            e
                        );
                        never_reported_warn = false;
                    }
                    time::sleep(auth_unauthenticated_timeout).await
                }
            }
        }
    });
}

/// Given a secret store, a path to a secret, get a secret.
/// The secret is expected to reside in a data field named "value".
pub async fn get_secret_value(
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
) -> Option<String> {
    let result = ss.get_secret(secret_path).await;
    if let Ok(Some(secret_reply)) = result {
        secret_reply.data.data.get("value").cloned()
    } else {
        None
    }
}

/// Given a secret store, a path to a secret, and a byte buffer to be decrypted,
/// decrypt it in place. Returns a JSON decoded structure if decryption
/// was successful.
/// The secret is expected to reside in a data field named "value" and
/// is encoded as a hex string of 32 characters (16 bytes)
/// The buffer is expected to contain both the salt and the bytes to be decrypted.
pub async fn decrypt_buf<'a, T>(
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    buf: &'a mut [u8],
) -> Option<T>
where
    T: Deserialize<'a>,
{
    if buf.len() >= crypto::SALT_SIZE {
        if let Some(secret_value) = get_secret_value(ss, secret_path).await {
            if let Ok(s) = hex::decode(secret_value) {
                let (salt, bytes) = buf.split_at_mut(crypto::SALT_SIZE);
                crypto::decrypt(bytes, &s, salt);
                return serde_json::from_slice(bytes).ok().flatten();
            }
        }
    }
    None
}

/// Given a secret store, a path to a secret, and a type to be encrypted,
/// serialize to JSON and then encrypt it.
/// Returns an encrypted buffer prefixed with a random salt if successful.
/// The secret is expected to reside in a data field named "value" and
/// is encoded as a hex string of 32 characters (16 bytes)
/// is encoded as a hex string. Any non alpha-numeric characters are
/// also filtered out.
pub async fn encrypt_struct<T, U>(
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    rng: &mut U,
    t: &T,
) -> Option<Vec<u8>>
where
    T: Serialize,
    U: RngCore,
{
    if let Some(secret_value) = get_secret_value(ss, secret_path).await {
        if let Ok(s) = hex::decode(secret_value) {
            if let Ok(mut bytes) = serde_json::to_vec(t) {
                let mut salt = crypto::salt(rng);
                crypto::encrypt(&mut bytes, &s, &salt);
                salt.extend(bytes);
                return Some(salt);
            }
        }
    }
    None
}

/// Loads and deserializes a structure described by T, if the file is present.
/// If there are IO issues outside of the file not being there, they will be returned
/// as an error. Beyond IO, state is attempted to be decrypted and deserialized when present.
/// Any issues there cause the default representation of the structure to be returned. The default
/// structure is also returned where there is no file present in the first place.
pub async fn load_struct<T>(
    state_storage_path: &Path,
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
) -> Result<T, Box<dyn Error>>
where
    T: Default + DeserializeOwned,
{
    if let Ok(mut f) = fs::File::open(state_storage_path).await {
        let mut buf = vec![];
        f.read_to_end(&mut buf).await?;
        Ok(decrypt_buf(ss, secret_path, &mut buf)
            .await
            .unwrap_or_default())
    } else {
        Ok(T::default())
    }
}

/// Saves an encrypted structure described by T. Any IO errors are returned.
pub async fn save_struct<T, U>(
    state_storage_path: &Path,
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    rng: &mut U,
    state: &T,
) -> Result<(), Box<dyn Error>>
where
    T: Serialize,
    U: RngCore,
{
    let mut f = fs::File::create(state_storage_path).await?;
    if let Some(buf) = encrypt_struct(ss, secret_path, rng, state).await {
        f.write_all(&buf).await?;
    }
    Ok(())
}
