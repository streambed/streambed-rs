# confidant

Confidant is a small library that implements a file-system-based 
secret store function for autonomous systems that often live
at the edge of a wider network.

Confidant implements the Streambed secret store API.

## An quick introduction

Nothing beats code for a quick introduction! Here is an example of
writing a secret and retrieving it. Please refer to the various tests 
for more complete examples.

```rs
// Let's set up the correct permissions for where all secrets will live
fs::set_permissions(&confidant_dir, PermissionsExt::from_mode(0o600))
    .await
    .unwrap();

let ss = FileSecretStore::new(
    confidant_dir.clone(),
    Duration::from_secs(1), // Timeout for unauthorized secrets before we try again
    10, // The number of secrets we cache
    None, // A data field to be used to indicate a TTL - if any
);

let mut data = HashMap::new();
data.insert("key".to_string(), "value".to_string());
let data = SecretData { data };

// Write the secret out.
assert!(ss.create_secret("some.secret", data.clone()).await.is_ok());

// Read the secret
assert!(ss.get_secret("some.secret").await.unwrap().is_some());
```

## Why confidant?

The primary functional use-cases of confidant are:

* retrieve and store a user's secrets (user meaning operating system
user); and
* share secrets with other users.

The primary operational use-cases of confidant are:

* to be hosted by resource-constrained devices, typically with less
than 128MiB memory and 8GB of storage.

## What is confidant?

### No networking

Confidant has no notion of what a network is and relies on the
file system along with operating system permissions

### No authentication or authorization

...over and above what the operating system provides.

### The Vault model

Confidant is modelled with the same concepts as Hashicorp Vault. 
Services using confidant may therefore lend themselves to portability 
toward Vault.

## How is confidant implemented?

Confidant is implemented as a library avoiding the need for a server process.
Please note that we had no requirements to serve Windows and wanted to leverage
Unix file permissions explicitly. When a secret is written is to written with
the same mode as the directory given to Confidant when instantiated. This then
ensures that the same permissions, including ACLs, are passed to each secret
file.

The file system is used to store secrets and the host operating system
permissions, including users, groups and ACLs, are leverage.
[Tokio](https://tokio.rs/) is used for file read/write operations so that any stalled
operations permit other tasks to continue running. [Postcard](https://docs.rs/postcard/latest/postcard/)
is used for serialization as it is able to conveniently represent in-memory structures
and is optimized for resource-constrained targets.

A TTL cache is maintained so that IO is minimized for those secrets that are often retrieved.

## When should you not use confidant

When you have Hashicorp Vault.