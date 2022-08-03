//! Provides command line arguments that are typically used for all services using this module.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct StateStorageArgs {
    /// A file system path to where state may be stored
    #[clap(env, long, default_value = "/var/run/farmo-integrator")]
    pub state_storage_path: PathBuf,

    /// The interval between connecting to the commit-log-server and storing our state.
    #[clap(env, long, default_value = "1m")]
    pub state_store_interval: humantime::Duration,
}
