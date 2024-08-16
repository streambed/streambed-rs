logged
===

The `logged` command provides a utility for conveniently operating on file-based commit logs. Functions such as the ability to consume a JSON file of records, or produce them, are available. No assumptions are made regarding the structure of a record's value (payload), or whether it is encrypted or not. The expectation is that a separate tool for that concern is used in a pipeline if required.

Running with an example write followed by a read
---

First get the executable:

```
cargo install streambed-logged-cli
```

...or build it from this repo:

```
cargo build --bin logged --release
```

...and make a build available on the PATH (only for `cargo build` and just once per shell session):

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
logged --root-path=/tmp subscribe --subscription my-topic --idle-timeout=0ms
```

...which would output:

```
{"topic":"my-topic","headers":[],"timestamp":null,"key":0,"value":"SGkgdGhlcmU=","partition":0,"offset":0}
```

If you're using [nushell](https://www.nushell.sh/) then you can do nice things like base64 decode payload
values along the way:

```
logged --root-path=/tmp subscribe --subscription my-topic --idle-timeout=0m | from json --objects | update value {decode base64}
```

...which would output:

```
╭────┬──────────┬────────────────┬───────────┬─────┬──────────┬───────────┬────────╮
│  # │  topic   │    headers     │ timestamp │ key │  value   │ partition │ offset │
├────┼──────────┼────────────────┼───────────┼─────┼──────────┼───────────┼────────┤
│  0 │ my-topic │ [list 0 items] │           │   0 │ Hi there │         0 │      0 │
╰────┴──────────┴────────────────┴───────────┴─────┴──────────┴───────────┴────────╯
```

Use `--help` to discover all of the options.
