logged
===

The `logged` command provides a utility for conveniently operating on file-based commit logs. Functions such as the ability to consume a JSON file of records, or produce them, are available. No assumptions are made regarding the structure of a record's value (payload), or whether it is encrypted or not. The expectation is that a separate tool for that concern is used in a pipeline.

Running with debug
---

```
cargo run --bin logged -- \
  subscribe --offset=my-topic,0,1000 --subscription=my-topic
```

Use `--help` to discover all of the options.

Running with a releasable binary
---

To build and run the binary directory:

```
cargo build --bin logged --release
target/release/logged \
  subscribe --offset=my-topic,0,1000 --subscription=my-topic
```
