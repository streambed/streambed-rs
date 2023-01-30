# patterns

Patterns for working with streambed.

## Ask Pattern

The ask pattern is based on the concept of the same name from [Akka](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/ask.html).
It encodes async request-reply semantics which can be used, for example, to perform request-reply operations across a `tokio::sync::mpsc::channel` or
similar. The provided implementation sends a message using a `tokio::sync::mpsc::Sender<_>` and uses a `tokio::sync::oneshot` channel internally to convey the
response.