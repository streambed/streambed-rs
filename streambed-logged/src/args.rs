//! Provides command line arguments that are typically used for all services using this module.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
pub struct CommitLogArgs {
    /// A namespace to use when communicating with the Commit Log
    #[clap(env, long, default_value = "default")]
    pub cl_ns: String,

    /// The location of all topics in the Commit Log
    #[clap(env, long, default_value = "/var/lib/logged")]
    pub cl_root_path: PathBuf,
}
