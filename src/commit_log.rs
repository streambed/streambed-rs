//! Commit log functionality that is modelled on Apache Kafka's
//! API, but can be implemented with multiple types of backend
//! e.g. one that uses the Kafka HTTP REST API.

use std::{pin::Pin, time::Duration};

use super::base64_serde;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;

/// A topic to subscribe to or has been subscribed to. Topics
/// may be namespaced by prefixing with characters followed by
/// a `:`. For example, "my-ns:my-topic". In the absence of
/// a namespace, the server will assume a default namespace.
pub type Topic = String;

/// A header provides a means of augmenting a record with
/// meta-data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Header {
    key: String,
    #[serde(with = "base64_serde")]
    value: Vec<u8>,
}

/// A declaration of an offset to be committed to a topic.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Offset {
    pub topic: Topic,
    pub partition: u32,
    pub offset: u64,
}

/// A declaration of a topic to subscribe to
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Subscription {
    pub topic: Topic,
}

/// A declaration of a consumer group session to connect with.
/// In the case that offsets are supplied, these offsets are
/// associated with their respective topics such that any
/// subsequent subscription will source from the offset.
/// In the case where subscriptions are supplied, the consumer
/// instance will subscribe and reply with a stream of records
/// ending only when the connection to the topic is severed.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Consumer {
    pub offsets: Option<Vec<Offset>>,
    pub subscriptions: Option<Vec<Subscription>>,
}

/// A declaration of a record produced by a subscription
#[derive(Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct ConsumerRecord {
    pub topic: Topic,
    pub headers: Vec<Header>,
    pub timestamp: Option<DateTime<Utc>>,
    pub key: u64,
    #[serde(with = "base64_serde")]
    pub value: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

/// A declaration of a record produced by a subscription
#[derive(Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct ProducerRecord {
    pub topic: Topic,
    pub headers: Vec<Header>,
    pub timestamp: Option<DateTime<Utc>>,
    pub key: u64,
    #[serde(with = "base64_serde")]
    pub value: Vec<u8>,
    pub partition: u32,
}

/// The reply to a publish request
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProducedOffset {
    pub offset: u64,
}

/// There was a problem producing a record
#[derive(Clone, Debug, PartialEq)]
pub enum ProducerError {
    /// The commit log received the request but was unable to process it.
    CannotProduce,
    /// The commit log is unavailable at this time. Try later.
    Unavailable,
}

/// A commit log holds topics and can be appended to and tailed.
#[async_trait]
pub trait CommitLog {
    /// Subscribe to one or more topics for a given consumer group
    /// having committed zero or more topics. Connections are
    /// retried if they cannot be established, or become lost.
    /// Once a connection is established then records are streamed
    /// back indefinitely unless an idle timeout argument is provided.
    /// In the case of an idle timeout, if no record is received
    /// within that period, None is returned to end the stream.
    // NOTE: It'd be great to use the async_trait macro here, but
    // that only supports futures, not streams. Even better,
    // it'd be great to have `impl Stream` available here and
    // in other places where we are using the async_trait macro.
    // Rust ain't there yet though.
    fn scoped_subscribe<'a>(
        &'a self,
        consumer_group_name: &str,
        offsets: Option<&[Offset]>,
        subscriptions: Option<&[Subscription]>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + 'a>>;

    /// Publish a record and return the offset that was assigned.
    async fn produce(&self, record: &ProducerRecord) -> Result<ProducedOffset, ProducerError>;
}
