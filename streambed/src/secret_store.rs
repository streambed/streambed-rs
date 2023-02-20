//! Secret store behavior modelled off of the Hashicorp Vault HTTP API,
//! but that which facilitates various backend implementations (including
//! those that do not use HTTP).

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The reply to a approle authentication
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppRoleAuthReply {
    pub auth: AuthToken,
}

/// An authentication token
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthToken {
    pub client_token: String,
    pub lease_duration: u64,
}

/// The reply to a get secret request - includes the actual secret data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GetSecretReply {
    pub lease_duration: u64,
    pub data: SecretData,
}

/// Secrets and metadata
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecretData {
    pub data: HashMap<String, String>,
}

/// The reply to a userpass authentication
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UserPassAuthReply {
    pub auth: AuthToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    /// The secret store is not authenticated.
    Unauthorized,
}

/// Describes a secret store modelled on the Hashicorp Vault API,
/// but one that can be backended with other implementations.
/// Connections are managed and retried if they cannot be established.
#[async_trait]
pub trait SecretStore: Clone + Send + Sync {
    /// Perform an app authentication given a role and secret. If successful, then the
    /// secret store will be updated with a client token thereby permitting subsequent
    /// operations including getting secrets.
    async fn approle_auth(&self, role_id: &str, secret_id: &str)
        -> Result<AppRoleAuthReply, Error>;

    /// Attempt to create/update a secret.
    async fn create_secret(&self, secret_path: &str, secret_data: SecretData) -> Result<(), Error>;

    /// Attempt to access a secret. An optional value of None in reply means that
    /// the client is unauthorized to obtain it - either due to authorization
    /// or it may just not exist.
    async fn get_secret(&self, secret_path: &str) -> Result<Option<GetSecretReply>, Error>;

    /// Given a token, authenticate the secret store.
    async fn token_auth(&self, token: &str) -> Result<(), Error>;

    /// Perform an app authentication given a username and password.
    async fn userpass_auth(
        &self,
        username: &str,
        password: &str,
    ) -> Result<UserPassAuthReply, Error>;

    /// Updates a username and password.
    async fn userpass_create_update_user(
        &self,
        current_username: &str,
        username: &str,
        password: &str,
    ) -> Result<(), Error>;
}
