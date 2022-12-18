# vault authenticator

This example illustrates the essential setup of a Vault secret store and authenticates a service.
It then goes on to authenticate a username/password.

> Note that you'll need both [`vault`](https://www.vaultproject.io/) and [`jq`](https://stedolan.github.io/jq/).

To begin with, you must start Vault in development mode:

```
vault server -dev
```

In another terminal, enter the following commands to enable app role authentication, configure policies, 
and the obtain a role_id and secret_id for our example service:

```
export VAULT_ADDR='http://127.0.0.1:8200'

vault kv put secret/default/secrets.some-secret value=some-secret-value

vault auth enable approle

vault policy write my-example-policy -<<EOF
path "auth/userpass/users/*" {
  capabilities = [ "create", "read", "update", "patch", "delete", "list" ]
}
path "secret/data/default/secrets.some-secret" {
  capabilities = [ "read" ]
}
EOF

vault write auth/approle/role/my-role token_policies="my-example-policy" \
    token_ttl=20m \
    token_max_ttl=30m
    
export ROLE_ID=$(vault read auth/approle/role/my-role/role-id -format=json|jq -r ".data.role_id")

export SECRET_ID=$(vault write -f auth/approle/role/my-role/secret-id -format=json|jq -r ".data.secret_id")

vault auth enable userpass

vault write auth/userpass/users/mitchellh \
    password=foo \
    policies=admins

```

The service can then be invoked:

```
echo -n $SECRET_ID | \
RUST_LOG=debug cargo run --example main -- \
  --ss-server="http://localhost:8200" \
  --ss-ttl-field="ttl" \
  --ss-role-id=$ROLE_ID
```

You should see that secret store has authenticated our service e.g.:

```
[2022-12-20T00:07:03Z INFO  main] Vault authenticator started
[2022-12-20T00:07:03Z DEBUG reqwest::connect] starting new connection: http://localhost:8200/
[2022-12-20T00:07:03Z INFO  streambed] Initially authenticated with the secret store
[2022-12-20T00:07:03Z INFO  main] Read secrets.some-secret: "some-secret-value"
[2022-12-20T00:07:03Z INFO  main] User pass auth token: Ok(UserPassAuthReply { auth: AuthToken { client_token: "hvs.CAESIBtdwc1t0ilTs9UU6g0bOZViNOKcakDteLkPbBp8a6XDGh4KHGh2cy5CVGUyWlY2bnFCNFdrMlRDY1YwWW95WVU", lease_duration: 2764800 } })
[2022-12-20T00:07:03Z INFO  main] Correctly disallows access to the secret given the user access
[2022-12-20T00:07:04Z INFO  main] User pass updated
```
