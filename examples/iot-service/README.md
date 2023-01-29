IoT service example
===

This is an example that illustrates the use of streambed that writes telemetry
to an append log. A simple HTTP API is provided to query and scan the commit log.

The service is not a complete example given that encryption has been left out so
that the example remains simple. Encryption should normally be applied to data
at rest (persisted by the commit log) and in flight (http and UDP).

Running
---

To run via cargo, first `cd` into this directory and then:

```
mkdir -p /tmp/configurator/var/lib/logged
RUST_LOG=debug cargo run -- \
  --cl-root-path=/tmp/configurator/var/lib/logged
```

You should now be able to query for database events:

```
curl -v "localhost:8080/api/database/events?id=1"
```

You should also be able to post database events to the UDP socket. Note that
we're using Postcard to deserialize binary data. Postcard uses variable length
integers where the top bit, when set, indicates that the next byte also contains
data. See [Postcard](https://docs.rs/postcard/latest/postcard/) for more details.

```
echo -n "\x01\x02" | nc -w0 localhost -u 8081
```

You should see a `DEBUG` log indicating that the post has been received. You should
also be able to query the database again with the id that was sent (`1`).

You should also be able to see events being written to the log file store itself:

```
hexdump /tmp/configurator/var/lib/logged/device-events
```