use std::error::Error;

use clap::Parser;
use git_version::git_version;
use log::info;
use streambed::secret_store::SecretStore;
use streambed_vault::{args::SsArgs, VaultSecretStore};

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
    info!("Vault authenticator started");

    // Setup and authenticate our service with the secret store

    let ss_secret_id = streambed::read_line_from_stdin().await.unwrap();
    assert!(
        !ss_secret_id.is_empty(),
        "Cannot source a secret id from stdin"
    );

    let ss_server_cert = if let Some(path) = args.ss_args.ss_server_cert_path {
        Some(
            streambed::pem_from_file(&path)
                .await
                .unwrap_or_else(|_| panic!("Cannot find ss_server_cert_path at {:?}", path)),
        )
    } else {
        None
    };

    let ss = VaultSecretStore::new(
        args.ss_args.ss_server,
        ss_server_cert,
        args.ss_args.ss_server_insecure,
        args.ss_args.ss_unauthorized_timeout.into(),
        args.ss_args.ss_max_secrets,
        args.ss_args.ss_ttl_field.as_deref(),
    );

    streambed::reauthenticate_secret_store(
        ss.clone(),
        &args.ss_args.ss_role_id,
        &ss_secret_id,
        args.ss_args.ss_unauthenticated_timeout.into(),
        args.ss_args.ss_max_lease_duration.into(),
    )
    .await;

    // At this point, the secret store may not be available to us (it may not be online).
    // This is a normal condition - the above `streambed::authenticate_secret_store`
    // call will spawn a task to retry connecting and then authentication, plus
    // re-authenticate when a token has expired.
    //
    // We are going to assume that the secret store is available though for the purposes of this example.
    // If it isn't then you'll see errors.

    // Let's now read a secret that our role should permit reading

    let secret_path = format!("{}/secrets.some-secret", args.ss_args.ss_ns);

    if let Some(value) = streambed::get_secret_value(&ss, &secret_path).await {
        info!("Read secrets.some-secret: {:?}", value);
    }

    // Now authenticate as a human i.e. with a username and password. These credentials
    // would normally come from the outside e.g. via an http login.
    // To authenticate, we base a new store on our existing one and authenticate again.
    // The idea is that we create and drop secret store instances in association with
    // user sessions. A secret store instance will share HTTP resources with the other
    // instances it has been associated with, and its own cache of secrets.

    let user_ss = VaultSecretStore::with_new_auth_prepared(&ss);
    let auth_token = user_ss.userpass_auth("mitchellh", "foo").await;
    info!("User pass auth token: {:?}", auth_token);

    // Given our new identity, we should no longer be able to access that secret we
    // accessed before.

    if streambed::get_secret_value(&user_ss, &secret_path)
        .await
        .is_none()
    {
        info!("Correctly disallows access to the secret given the user access");
    }

    // For fun, let's update our credentials to the same values. Note how we do
    // this from our service's identity, not the user one. For our example, our user
    // actually doesn't have the role assigned such that it is able to.
    if ss
        .userpass_create_update_user("mitchellh", "mitchellh", "foo")
        .await
        .is_ok()
    {
        info!("User pass updated");
    }

    Ok(())
}
