confidant
===

The `confidant` command provides a utility for conveniently operating on file-based secret store. It is
often used in conjunction `logged` to encrypt payloads as a pre-stage, or decrypt as a post-stage. No
assumption is made regarding the nature of a payload beyond it being encrypted using streambed crypto
functions.

Running with an example encrypt followed by a decrypt
---

First get the executable:

```
cargo install streambed-confidant-cli
```

...or build it from this repo:

```
cargo build --bin confidant --release
```

...and make a build available on the PATH (only for `cargo build` and just once per shell session):

```
export PATH="$PWD/target/release":$PATH
```

Before you can use `confidant`, you must provide it with a root secret and a "secret id" (password)
to authenticate the session. Here's an example with some dummy data:

```
echo "1800af9b273e4b9ea71ec723426933a4" > /tmp/root-secret
echo "unused-id" > /tmp/ss-secret-id
```

We also need to create a directory for `confidant` to read and write its secrets. A security feature
of the `confidant` library is that the directory must have a permission of `600` for the owner user.
ACLs should then be used to control access for individual processes. Here's how the directory can be
created:

```
mkdir /tmp/confidant
chmod 700 /tmp/confidant
```

You would normally source the above secrets from your production system, preferably without
them leaving your production system.

Given the root secret, encrypt some data :

```
echo '{"value":"SGkgdGhlcmU="}' | \
  confidant --root-path=/tmp/confidant encrypt --file - --path="default/my-secret-path"
```

...which would output:

```
{"value":"EZy4HLnFC4c/W63Qtp288WWFj8U="}
```

That value is now encrypted with a salt.

We can also decrypt in a similar fashion:

```
echo '{"value":"EZy4HLnFC4c/W63Qtp288WWFj8U="}' | \
  confidant --root-path=/tmp/confidant decrypt --file - --path="default/my-secret-path"
```

...which will yield the original BASE64 value that we encrypted.

Use `--help` to discover all of the options.
