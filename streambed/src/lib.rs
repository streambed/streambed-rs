#![doc = include_str!("../../README.md")]

use std::sync::Arc;
use std::time::Duration;
use std::{error::Error, path::Path};

use crypto::SALT_SIZE;
use log::{debug, info, warn};
use rand::RngCore;
use reqwest::Certificate;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::{
    fs,
    io::{self, AsyncReadExt, BufReader},
};

pub mod commit_log;
pub mod crypto;
pub mod delayer;
pub mod secret_store;

/// Read a line from stdin.
pub async fn read_line_from_stdin() -> Result<String, io::Error> {
    let mut line = String::new();
    let mut reader = BufReader::new(io::stdin());
    reader.read_line(&mut line).await?;
    let len = line.trim_end_matches(&['\r', '\n'][..]).len();
    line.truncate(len);
    Ok(line)
}

/// Read a file as a pem file and return its corresponding Reqwest certificate.
pub async fn pem_from_file(path: &Path) -> Result<Certificate, Box<dyn Error>> {
    let mut f = fs::File::open(path).await?;
    let mut buf = vec![];
    f.read_to_end(&mut buf).await?;
    Certificate::from_pem(&buf).map_err(|e| e.into())
}

/// A handle to the task created by `authenticate_secret_store` that
/// can be used to subsequently cancel it.
pub struct AuthenticationTask {
    join_handle: Option<JoinHandle<()>>,
    termination: Arc<Notify>,
}

/// Performs an initial authentication with the secret store and also spawns a
/// task to re-authenticate on token expiry. A timeout is provided to cause the
/// re-authentication to sleep between non-successful authentication attempts.
pub async fn reauthenticate_secret_store(
    ss: impl secret_store::SecretStore + Sync + Send + Clone + 'static,
    role_id: &str,
    secret_id: &str,
    unauthenticated_timeout: Duration,
    max_lease_duration: Duration,
) -> AuthenticationTask {
    let mut approle_auth_reply = ss.approle_auth(role_id, secret_id).await;
    let auth_role_id = role_id.to_string();
    let auth_secret_id = secret_id.to_string();
    let auth_unauthenticated_timeout = unauthenticated_timeout;
    let termination = Arc::new(Notify::new());
    let task_termination = termination.clone();
    let join_handle = tokio::spawn(async move {
        let mut never_reported_info = true;
        let mut never_reported_warn = true;
        loop {
            match &approle_auth_reply {
                Ok(approle_auth) => {
                    if never_reported_info {
                        info!("Initially authenticated with the secret store");
                        never_reported_info = false;
                    }
                    tokio::select! {
                        _ = task_termination.notified() => break,
                        _ = time::sleep(Duration::from_secs(approle_auth.auth.lease_duration).min(max_lease_duration)) => {
                            approle_auth_reply = ss.approle_auth(&auth_role_id, &auth_secret_id).await;
                        }
                    }
                }
                Err(e) => {
                    if never_reported_warn {
                        warn!(
                            "Unable to initially authenticate with the secret store. Error: {:?}",
                            e
                        );
                        never_reported_warn = false;
                    }
                    tokio::select! {
                        _ = task_termination.notified() => break,
                        _ = time::sleep(auth_unauthenticated_timeout) => (),
                    }
                }
            }
        }
    });
    AuthenticationTask {
        join_handle: Some(join_handle),
        termination,
    }
}

impl AuthenticationTask {
    /// Cancels a previous authentication task and waits for it to
    /// finish. The method may be called multiple times, although
    /// it is effective only on the first call.
    pub async fn cancel(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            debug!("Cancelling the original secret store authentication");
            self.termination.notify_one();
            let _ = join_handle.await;
        }
    }
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
/// decrypt it in place. Returns a decoded structure if decryption
/// was successful.
/// The secret is expected to reside in a data field named "value" and
/// is encoded as a hex string of 32 characters (16 bytes)
/// The buffer is expected to contain both the salt and the bytes to be decrypted.
pub async fn decrypt_buf<'a, T, D, DE>(
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    buf: &'a mut [u8],
    deserialize: D,
) -> Option<T>
where
    T: Deserialize<'a>,
    D: FnOnce(&'a [u8]) -> Result<T, DE>,
{
    if buf.len() >= crypto::SALT_SIZE {
        if let Some(secret_value) = get_secret_value(ss, secret_path).await {
            if let Ok(s) = hex::decode(secret_value) {
                let (salt, bytes) = buf.split_at_mut(crypto::SALT_SIZE);
                crypto::decrypt(bytes, &s.try_into().ok()?, &salt.try_into().ok()?);
                return deserialize(bytes).ok();
            }
        }
    }
    None
}

/// Given a secret store, a path to a secret, and a type to be encrypted,
/// serialize and then encrypt it.
/// Returns an encrypted buffer prefixed with a random salt if successful.
/// The secret is expected to reside in a data field named "value" and
/// is encoded as a hex string of 32 characters (16 bytes)
/// is encoded as a hex string. Any non alpha-numeric characters are
/// also filtered out.
pub async fn encrypt_struct<T, U, F, S, SE>(
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    serialize: S,
    rng: F,
    t: &T,
) -> Option<Vec<u8>>
where
    T: Serialize,
    S: FnOnce(&T) -> Result<Vec<u8>, SE>,
    F: FnOnce() -> U,
    U: RngCore,
{
    if let Some(secret_value) = get_secret_value(ss, secret_path).await {
        if let Ok(s) = hex::decode(secret_value) {
            if let Ok(mut bytes) = serialize(t) {
                let salt = crypto::salt(&mut (rng)());
                crypto::encrypt(&mut bytes, &s.try_into().ok()?, &salt);
                let mut buf = Vec::with_capacity(SALT_SIZE + bytes.len());
                buf.extend(salt);
                buf.extend(bytes);
                return Some(buf);
            }
        }
    }
    None
}
