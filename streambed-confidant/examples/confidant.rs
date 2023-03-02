use std::error::Error;

use clap::Parser;
use git_version::git_version;
use log::info;
use streambed::secret_store::SecretStore;
use streambed_confidant::{args::SsArgs, FileSecretStore};

/// Service CLI that brings in the args we need for configuring access to a secret store
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version=git_version!())]
struct Args {
    #[clap(flatten)]
    ss_args: SsArgs,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::init();
    info!("Confidant authenticator started");

    // Setup and authenticate our service with the secret store

    let line = streambed::read_line(std::io::stdin()).unwrap();
    assert!(!line.is_empty(), "Cannot source a line from stdin");
    let (root_secret, ss_secret_id) = line.split_at(32);
    let root_secret = hex::decode(root_secret).unwrap();
    assert!(
        !ss_secret_id.is_empty(),
        "Cannot source a secret id from stdin"
    );

    let ss = FileSecretStore::new(
        args.ss_args.ss_root_path,
        &root_secret.try_into().unwrap(),
        args.ss_args.ss_unauthorized_timeout.into(),
        args.ss_args.ss_max_secrets,
        args.ss_args.ss_ttl_field.as_deref(),
    );

    ss.approle_auth(&args.ss_args.ss_role_id, ss_secret_id)
        .await
        .unwrap();

    // We now create a user so that we can authenticate with it later

    if ss
        .userpass_create_update_user("mitchellh", "mitchellh", "foo")
        .await
        .is_ok()
    {
        info!("User created");

        // Now authenticate as a human i.e. with a username and password. These credentials
        // would normally come from the outside e.g. via an http login.
        // To authenticate, we base a new store on our existing one and authenticate again.
        // The idea is that we create and drop secret store instances in association with
        // user sessions. A secret store instance will share resources with the other
        // instances it has been associated with, and have its own cache of secrets.

        let user_ss = FileSecretStore::with_new_auth_prepared(&ss);
        let auth_token = user_ss.userpass_auth("mitchellh", "foo").await.unwrap();
        info!("User pass auth token: {:?}", auth_token);

        // Now suppose that we have a token received from the outside world.  We establish
        // a new token store and then authenticate the token against it. If we choose to
        // store secret stores in relation to the same user id, then we could also do that
        // and share secret key caching. For now though, we'll just create a new from from
        // our service's secret store.

        let user_ss = FileSecretStore::with_new_auth_prepared(&ss);
        let result = user_ss.token_auth(&auth_token.auth.client_token).await;
        info!("Token auth result: {:?}", result);
    }

    Ok(())
}
