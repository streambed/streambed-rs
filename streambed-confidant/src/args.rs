//! Provides command line arguments that are typically used for all services using this module.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct SsArgs {
    /// The max number of Vault Secret Store secrets to retain by our cache at any time.
    /// Least Recently Used (LRU) secrets will be evicted from our cache once this value
    /// is exceeded.
    #[clap(env, long, default_value_t = 10_000)]
    pub ss_max_secrets: usize,

    /// A namespace to use when communicating with the Vault Secret Store
    #[clap(env, long, default_value = "default")]
    pub ss_ns: String,

    /// The Secret Store role_id to use for approle authentication.
    #[clap(env, long)]
    pub ss_role_id: String,

    /// The location of all secrets belonging to confidant. The recommendation is to
    /// create a user for confidant and a requirement is to remove group and world permissions.
    /// Then, use ACLs to express further access conditions.
    #[clap(env, long, default_value = "/var/lib/confidant")]
    pub ss_root_path: PathBuf,

    /// A data field to used in place of Vault's lease_duration field. Time
    /// will be interpreted as a humantime string e.g. "1m", "1s" etc. Note
    /// that v2 of the Vault server does not appear to populate the lease_duration
    /// field for the KV secret store any longer. Instead, we can use a "ttl" field
    /// from the data.
    #[clap(env, long)]
    pub ss_ttl_field: Option<String>,

    /// How long we wait until re-requesting the Vault Secret Store server for an
    /// unauthorized secret again.
    #[clap(env, long, default_value = "1m")]
    pub ss_unauthorized_timeout: humantime::Duration,
}
