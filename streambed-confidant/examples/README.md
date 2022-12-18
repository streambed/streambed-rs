# confidant authenticator

This example illustrates how confidant can be used to set up a service, create a user
and then authenticate against that user.

To run, first create a directory that confidant can use to store its encrypted secrets:

```
mkdir /tmp/confidant
chmod 700 /tmp/confidant
```

The service can then be invoked:

```
echo -n "01234567890123456789012345678912some-secret-id" | \
RUST_LOG=debug cargo run --example main -- \
  --ss-role-id="my-great-service" \
  --ss-root-path="/tmp/confidant"
```

You should then receive an output like:

```
[2022-12-20T14:27:48Z INFO  main] Confidant authenticator started
[2022-12-20T14:27:48Z INFO  main] User created
[2022-12-20T14:27:48Z INFO  main] User pass auth token: UserPassAuthReply { auth: AuthToken { client_token: "eyJkYXRhIjp7InVzZXJuYW1lIjoibWl0Y2hlbGxoIiwiZXhwaXJlcyI6MTY3MjE1MTI2ODc3Nn0sInNpZ25hdHVyZSI6ImJjYTdmZTY4ZWI0NDcwN2QwMGI1MmUzZjgwZDE4MmFlMTcxYjE5MDZkYzdjYTJjZTUwN2EwNTk2OTY0MDQ4MGMifQ==", lease_duration: 604800 } }
[2022-12-20T14:27:48Z INFO  main] Token auth result: Ok(())
```