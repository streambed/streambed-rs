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
pub struct ConsumerOffset {
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
    pub offsets: Option<Vec<ConsumerOffset>>,
    pub subscriptions: Vec<Subscription>,
}

/// A declaration of a record produced by a subscription
#[derive(Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct ConsumerRecord {
    pub topic: Topic,
    pub headers: Option<Vec<Header>>,
    pub timestamp: Option<DateTime<Utc>>,
    pub key: u64,
    #[serde(with = "base64_serde")]
    pub value: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

/// The reply to an offsets request
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PartitionOffsets {
    pub beginning_offset: u64,
    pub end_offset: u64,
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
}

/// A commit log holds topics and can be appended to and tailed.
/// Connections are managed and retried if they cannot be established.
#[async_trait]
pub trait CommitLog {
    /// Retrieve the current offsets of a topic if they are present.
    async fn offsets(&self, topic: Topic, partition: u32) -> Option<PartitionOffsets>;

    /// Publish a record and return the offset that was assigned.
    async fn produce(&self, record: ProducerRecord) -> Result<ProducedOffset, ProducerError>;

    /// Subscribe to one or more topics for a given consumer group
    /// having committed zero or more topics. The records are streamed
    /// back indefinitely unless an idle timeout argument is provided.
    /// In the case of an idle timeout, if no record is received
    /// within that period, None is returned to end the stream.
    fn scoped_subscribe<'a>(
        &'a self,
        consumer_group_name: &str,
        offsets: Option<Vec<ConsumerOffset>>,
        subscriptions: Vec<Subscription>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + 'a>>;
}
