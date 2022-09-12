//! streambed-rs - Event-driven services toolkit
//!
//! Event-driven services
//!
//! Event driven microservices promote responsiveness allowing decisions to be made faster. Services also become more resilient to failure as they can quickly rebuild their state by replaying events.
//!
//! Efficient
//!
//! Streambed based applications are designed to run at the edge on embedded computers as well as in the cloud and so efficient CPU and memory usage are of primary concern.
//!
//! Secure
//!
//! Security is another primary consideration throughout the design of Streambed. For example, in the world of the Internet of Things, if an individual sensor becomes compromised then its effects can be minimized.
//!
//! Built for integration
//!
//! Streambed is a toolkit that promotes the consented sharing of data between many third-party applications. No more silos of data. Improved data availability leads to better decision making, which leads to better business.
//!
//! Standing on the shoulders of giants, leveraging existing communities
//!
//! Streambed is an assemblage of proven approaches and technologies that already have strong communities. Should you have a problem there are many people and resources you can call on.
//!
//! Open source and open standards
//!
//! Streambed is entirely open source providing cost benefits, fast-time-to-market, the avoidance of vendor lock-in, improved security and more.
//!
//! Rust
//!
//! Streambed builds on Rust's traits of writing fast and efficient software correctly.

use std::sync::Arc;
use std::time::Duration;
use std::{error::Error, path::Path};

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

pub mod base64_serde;
pub mod commit_log;
pub mod confidant;
pub mod crypto;
pub mod kafka;
pub mod logged;
pub mod secret_store;
pub mod state_storage;
pub mod vault;

mod delayer;

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

/// A handle to the task created by `authenticate_secret_store` that
/// can be used to subsequently cancel it.
pub struct AuthenticationTask {
    join_handle: Option<JoinHandle<()>>,
    termination: Arc<Notify>,
}

/// Performs an initial authentication with the secret store and also spawns a
/// task to re-authenticate on token expiry. A timeout is provided to cause the
/// re-authentication to sleep between non-successful authentication attempts.
pub async fn authenticate_secret_store(
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
