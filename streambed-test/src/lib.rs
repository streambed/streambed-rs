/// Taken from https://github.com/seanmonstar/reqwest/blob/master/tests/support/mod.rs
pub mod server;

#[allow(unused)]
pub static DEFAULT_USER_AGENT: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
