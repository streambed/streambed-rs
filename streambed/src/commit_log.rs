//! [A commit log is an append-only data structure that can be used
//! in a variety of use-cases, such as tracking sequences of events,
//! transactions or replicated state machines](https://docs.rs/commitlog/latest/commitlog/).
//!
//! Commit log functionality that is modelled on [Apache Kafka's](https://kafka.apache.org/)
//! API, and can be implemented with multiple types of backend
//! e.g. one that uses the Kafka HTTP REST API.

use std::{pin::Pin, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{base64::Base64, serde_as};
use tokio_stream::Stream;

/// An offset into a commit log. Offsets are used to address
/// records and can be relied on to have an ascending order.
pub type Offset = u64;

/// Each record in a commit log has a key. How the key is formed
/// is an application concern. By way of an example, keys can be
/// used to associate to an entity.
pub type Key = u64;

/// Topics can be distributed into partitions which, in turn,
/// enable scaling.
pub type Partition = u32;

/// A topic to subscribe to or has been subscribed to. Topics
/// may be namespaced by prefixing with characters followed by
/// a `:`. For example, "my-ns:my-topic". In the absence of
/// a namespace, the server will assume a default namespace.
pub type Topic = String;

/// A ref to a topic
pub type TopicRef<'a> = &'a str;

/// A header provides a means of augmenting a record with
/// meta-data.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Header {
    pub key: String,
    #[serde_as(as = "Base64")]
    pub value: Vec<u8>,
}

/// A declaration of an offset to be committed to a topic.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsumerOffset {
    pub topic: Topic,
    pub partition: Partition,
    pub offset: Offset,
}

/// A declaration of a topic to subscribe to
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Consumer {
    #[serde(deserialize_with = "nullable_vec")]
    pub offsets: Vec<ConsumerOffset>,
    pub subscriptions: Vec<Subscription>,
}

/// A declaration of a record produced by a subscription
#[serde_as]
#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
pub struct ConsumerRecord {
    pub topic: Topic,
    #[serde(deserialize_with = "nullable_vec")]
    pub headers: Vec<Header>,
    pub timestamp: Option<DateTime<Utc>>,
    pub key: Key,
    #[serde_as(as = "Base64")]
    pub value: Vec<u8>,
    pub partition: Partition,
    pub offset: Offset,
}

/// The reply to an offsets request
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionOffsets {
    pub beginning_offset: Offset,
    pub end_offset: Offset,
}

/// A declaration of a record to be produced to a topic
#[serde_as]
#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
pub struct ProducerRecord {
    pub topic: Topic,
    #[serde(deserialize_with = "nullable_vec")]
    pub headers: Vec<Header>,
    pub timestamp: Option<DateTime<Utc>>,
    pub key: Key,
    #[serde_as(as = "Base64")]
    pub value: Vec<u8>,
    pub partition: Partition,
}

/// The reply to a publish request
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProducedOffset {
    pub offset: Offset,
}

/// There was a problem producing a record
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProducerError {
    /// The commit log received the request but was unable to process it.
    CannotProduce,
}

/// A commit log holds topics and can be appended to and tailed.
/// Connections are managed and retried if they cannot be established.
#[async_trait]
pub trait CommitLog: Clone + Send + Sync {
    /// Retrieve the current offsets of a topic if they are present.
    async fn offsets(&self, topic: Topic, partition: Partition) -> Option<PartitionOffsets>;

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
        offsets: Vec<ConsumerOffset>,
        subscriptions: Vec<Subscription>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'a>>;
}

fn nullable_vec<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(d).map(|x: Option<_>| x.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nullable_vec_handles_null() {
        let json = r#"
        {
            "offsets": null,
            "subscriptions": []
        }
        "#;
        assert_eq!(
            serde_json::from_str::<Consumer>(json).unwrap(),
            Consumer {
                offsets: vec![],
                subscriptions: vec![]
            }
        );
    }

    #[test]
    fn test_nullable_vec_handles_empty_vec() {
        let json = r#"
        {
            "offsets": [],
            "subscriptions": []
        }
        "#;
        assert_eq!(
            serde_json::from_str::<Consumer>(json).unwrap(),
            Consumer {
                offsets: vec![],
                subscriptions: vec![]
            }
        );
    }

    #[test]
    fn test_nullable_vec_handles_vec() {
        let json = r#"
        {
            "offsets": [{"topic": "topic", "partition": 0, "offset": 0}],
            "subscriptions": []
        }
        "#;
        assert_eq!(
            serde_json::from_str::<Consumer>(json).unwrap(),
            Consumer {
                offsets: vec![ConsumerOffset {
                    topic: "topic".to_string(),
                    partition: 0,
                    offset: 0
                }],
                subscriptions: vec![]
            }
        );
    }
}
