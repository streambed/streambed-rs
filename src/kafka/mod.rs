//! Provides commit log functionality to connect with the Kafka HTTP API

pub mod args;

use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use super::commit_log::CommitLog;
use super::commit_log::Consumer;
use super::commit_log::ConsumerRecord;
use super::commit_log::Subscription;
use super::delayer::Delayer;
use crate::commit_log::ConsumerOffset;
use crate::commit_log::PartitionOffsets;
use crate::commit_log::ProducedOffset;
use crate::commit_log::ProducerError;
use crate::commit_log::ProducerRecord;
use crate::commit_log::Topic;
use async_stream::stream;
use async_trait::async_trait;
use log::{debug, trace};
use metrics::increment_counter;
use reqwest::Certificate;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde::Serialize;
use tokio::time;
use tokio_stream::Stream;

/// A commit log holds topics and can be appended to and tailed.
pub struct KafkaRestCommitLog {
    client: Client,
    server: Url,
}

const CONSUMER_GROUP_NAME_LABEL: &str = "consumer_group_name";
const TOPIC_LABEL: &str = "topic";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ProduceRequest {
    pub records: Vec<ProducerRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ProduceReply {
    pub offsets: Vec<ProducedOffset>,
}

type OffsetMap = HashMap<(Topic, u32), u64>;

impl KafkaRestCommitLog {
    /// Establish a new commit log session.
    pub fn new(server: Url, server_cert: Option<Certificate>, tls_insecure: bool) -> Self {
        let client = Client::builder().danger_accept_invalid_certs(tls_insecure);

        let client = if let Some(cert) = server_cert {
            client.add_root_certificate(cert)
        } else {
            client
        };

        Self {
            client: client.build().unwrap(),
            server,
        }
    }
}

#[async_trait]
impl CommitLog for KafkaRestCommitLog {
    async fn offsets(&self, topic: Topic, partition: u32) -> Option<PartitionOffsets> {
        let mut delayer = Delayer::new();
        loop {
            match self
                .client
                .get(
                    self.server
                        .join(&format!(
                            "/topics/{}/partitions/{}/offsets",
                            topic, partition
                        ))
                        .unwrap(),
                )
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        trace!("Retrieved offsets for: {} {}", topic, partition);
                        increment_counter!("offset_replies",  TOPIC_LABEL => topic);
                        break response.json::<PartitionOffsets>().await.ok();
                    } else {
                        debug!(
                            "Commit log failure status while retrieving offsets: {:?}",
                            response.status()
                        );
                        increment_counter!("offsets_other_reply_failures");
                        break None;
                    }
                }
                Err(e) => {
                    debug!(
                        "Commit log is unavailable while retrieving offsets. Error: {:?}",
                        e
                    );
                    increment_counter!("offsets_unavailables");
                }
            }
            delayer.delay().await;
        }
    }

    async fn produce(&self, record: ProducerRecord) -> Result<ProducedOffset, ProducerError> {
        let mut delayer = Delayer::new();
        loop {
            match self
                .client
                .post(
                    self.server
                        .join(&format!("/topics/{}", record.topic))
                        .unwrap(),
                )
                .json(&ProduceRequest {
                    records: vec![record.clone()],
                })
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        trace!("Produced record: {:?}", record);
                        increment_counter!("producer_replies",  TOPIC_LABEL => record.topic.to_owned());
                        break response
                            .json::<ProduceReply>()
                            .await
                            .map_err(|_| ProducerError::CannotProduce)
                            .and_then(|r| {
                                r.offsets.first().map(|o| o.to_owned()).ok_or_else(|| {
                                    debug!(
                                        "Commit log failure reply with no offset while producing"
                                    );
                                    increment_counter!("producer_other_reply_failures");
                                    ProducerError::CannotProduce
                                })
                            });
                    } else {
                        debug!(
                            "Commit log failure status while producing: {:?}",
                            response.status()
                        );
                        increment_counter!("producer_other_reply_failures");
                        break Err(ProducerError::CannotProduce);
                    }
                }
                Err(e) => {
                    debug!("Commit log is unavailable while producing. Error: {:?}", e);
                    increment_counter!("producer_unavailables");
                }
            }
            delayer.delay().await;
        }
    }

    /// Subscribe to one or more topics for a given consumer group
    /// having committed zero or more topics. Connections are
    /// retried if they cannot be established, or become lost.
    /// Once a connection is established then records are streamed
    /// back indefinitely unless an idle timeout argument is provided.
    /// In the case of an idle timeout, if no record is received
    /// within that period, None is returned to end the stream.
    fn scoped_subscribe<'a>(
        &'a self,
        consumer_group_name: &str,
        offsets: Vec<ConsumerOffset>,
        subscriptions: Vec<Subscription>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'a>> {
        let consumer_group_name = consumer_group_name.to_string();
        let mut offsets: OffsetMap = offsets
            .iter()
            .map(|e| ((e.topic.to_owned(), e.partition), e.offset))
            .collect::<OffsetMap>();
        let subscriptions: Vec<Subscription> = subscriptions.iter().map(|e| e.to_owned()).collect();
        Box::pin(stream!({
            let mut delayer = Delayer::new();
            'stream_loop: loop {
                increment_counter!("consumer_group_requests", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string());
                let consumer = Consumer {
                    offsets: offsets
                        .clone()
                        .iter()
                        .map(|((topic, partition), offset)| ConsumerOffset {
                            offset: *offset,
                            partition: *partition,
                            topic: topic.to_string(),
                        })
                        .collect(),
                    subscriptions: subscriptions.clone(),
                };
                let response = self
                    .client
                    .post(
                        self.server
                            .join(&format!("/consumers/{}", consumer_group_name))
                            .unwrap(),
                    )
                    .json(&consumer)
                    .send()
                    .await;
                match response {
                    Ok(mut r) => loop {
                        let chunk = if let Some(it) = idle_timeout {
                            match time::timeout(it, r.chunk()).await {
                                Ok(c) => c,
                                Err(_) => break 'stream_loop,
                            }
                        } else {
                            r.chunk().await
                        };
                        match chunk {
                            Ok(Some(c)) => {
                                if let Ok(record) = serde_json::from_slice::<ConsumerRecord>(&c) {
                                    trace!("Received record: {:?}", record);
                                    let topic = record.topic.to_owned();
                                    let partition = record.partition;
                                    let record_offset = record.offset;
                                    increment_counter!("consumer_group_replies", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string(), TOPIC_LABEL => topic.clone());
                                    yield record;

                                    let _ = offsets.insert((topic, partition), record_offset);
                                } else {
                                    debug!("Unable to decode record");
                                }
                            }
                            Ok(None) => {
                                debug!("Unable to receive chunk");
                                delayer = Delayer::new();
                                continue 'stream_loop;
                            }
                            Err(e) => debug!("Error receiving chunk {:?}", e),
                        }
                    },
                    Err(e) => {
                        debug!(
                            "Commit log is unavailable while subscribing. Error: {:?}",
                            e
                        );
                        increment_counter!("consumer_group_request_failures", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string());
                    }
                }
                delayer.delay().await;
            }
        }))
    }
}
