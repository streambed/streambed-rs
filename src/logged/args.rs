//! Provides command line arguments that are typically used for all services using this module.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct CommitLogArgs {
    /// The amount of time to indicate that no more events are immediately
    /// available from the Commit Log endpoint
    #[clap(env, long, default_value = "100ms")]
    pub cl_idle_timeout: humantime::Duration,

    /// A namespace to connect to
    #[clap(env, long)]
    pub cl_namespace: Option<String>,

    /// A namespace to use when communicating with the Commit Log
    #[clap(env, long, default_value = "default")]
    pub cl_ns: String,

    /// The location of all topics in the Commit Log
    #[clap(env, long, default_value = "/var/lib/logged")]
    pub cl_root_path: PathBuf,
}
