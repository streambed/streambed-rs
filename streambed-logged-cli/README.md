logged
===

The `logged` command provides a utility for conveniently operating on file-based commit logs. Functions such as the ability to consume a JSON file of records, or produce them, are available. No assumptions are made regarding the structure of a record's value (payload), or whether it is encrypted or not. The expectation is that a separate tool for that concern is used in a pipeline if required.

Running with an example write followed by a read
---

First build the executable:

```
cargo build --bin logged --release
```

...make it available on the PATH:

```
export PATH="$PWD/target/release":$PATH
```

...then write some data to a topic named `my-topic`:

```
echo '{"topic":"my-topic","headers":[],"key":0,"value":"SGkgdGhlcmU=","partition":0}' | \
  logged --root-path=/tmp produce --file -
```

...then read it back:

```
logged --root-path=/tmp subscribe --subscription my-topic --idle-timeout=100ms
```

Use `--help` to discover all of the options.
