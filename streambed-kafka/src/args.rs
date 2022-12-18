//! Provides command line arguments that are typically used for all services using this module.

use std::path::PathBuf;

use clap::Parser;
use reqwest::Url;

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

    /// A URL of the Commit Log RESTful Kafka server to communicate with
    #[clap(env, long, default_value = "http://localhost:8082")]
    pub cl_server: Url,

    /// A path to a TLS cert pem file to be used for connecting with the Commit Log server.
    #[clap(env, long)]
    pub cl_server_cert_path: Option<PathBuf>,

    /// Insecurely trust the Commit Log's TLS certificate
    #[clap(env, long)]
    pub cl_server_insecure: bool,
}
