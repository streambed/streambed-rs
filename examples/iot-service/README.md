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
mkdir -p /tmp/iot-service/var/lib/logged
RUST_LOG=debug cargo run -- \
  --cl-root-path=/tmp/iot-service/var/lib/logged
```

You should now be able to query for database events:

```
curl -v "127.0.0.1:8080/api/database/events?id=1"
```

You should also be able to post database events to the UDP socket. Note that
we're using Postcard to deserialize binary data. Postcard uses variable length
integers where the top bit, when set, indicates that the next byte also contains
data. See [Postcard](https://docs.rs/postcard/latest/postcard/) for more details.

```
echo -n "\x01\x02" | nc -w0 127.0.0.1 -u 8081
```

You should see a `DEBUG` log indicating that the post has been received. You should
also be able to query the database again with the id that was sent (`1`).

You should also be able to see events being written to the log file store itself:

```
hexdump /tmp/iot-service/var/lib/logged/device-events
```

Compaction
----

If you would like to see compaction at work then you can drive UDP traffic with
a script such as the following:

```bash
#!/bin/bash
for i in {0..2000}
do
  printf "\x01\x02" | nc -w0 127.0.0.1 -u 8081
done
```

The above will send 2,000 UDP messages to the service. Toward the end, if you watch
the log of the service, you will see various messages from the compactor at work.
When it has finished, you can observe that they are two log files for our topic e.g.:

```
ls -al /tmp/iot-service/var/lib/logged

drwxr-xr-x  4 huntc  wheel  128 14 Feb 14:31 .
drwxr-xr-x  3 huntc  wheel   96 14 Feb 10:49 ..
-rw-r--r--  1 huntc  wheel  495 14 Feb 14:31 device-events
-rw-r--r--  1 huntc  wheel  330 14 Feb 14:31 device-events.history
```

The `history` file contains the compacted log. As each record is 33 bytes, this means
that compaction retained the last 10 records (330 bytes). The active file, or `device-events`,
contains 15 additional records that continued to be written while compaction was
in progress. The compactor is designed to avoid back-pressuring the production of 
records. That said, if the production of events overwhelms compaction then
it will back-pressure on the producer. It is up to you, as the application developer,
to decide whether to always await the completion of producing a record. In some
real-time scenarios, waiting on confirmation that an event is written may be 
undesirable. However, with the correct dimensioning of the commit log in terms of
its buffers and how the compaction strategies are composed, compaction back-pressure
can also be avoided.