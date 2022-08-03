//! Provides commit log functionality to connect with the Kafka HTTP API

pub mod args;

use std::pin::Pin;
use std::sync::Once;
use std::time::Duration;

use async_stream::stream;
use async_trait::async_trait;
use log::{debug, trace};
use metrics::describe_counter;
use metrics::increment_counter;
use reqwest::Certificate;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde::Serialize;
use tokio::time;
use tokio_stream::Stream;

use crate::commit_log::ConsumerOffset;
use crate::commit_log::ProducedOffset;
use crate::commit_log::ProducerError;
use crate::commit_log::ProducerRecord;

use super::commit_log::CommitLog;
use super::commit_log::Consumer;
use super::commit_log::ConsumerRecord;
use super::commit_log::Subscription;

static INIT: Once = Once::new();

/// A commit log holds topics and can be appended to and tailed.
pub struct KafkaRestCommitLog {
    client: Client,
    server: Url,
}

const CONSUMER_GROUP_NAME_LABEL: &str = "consumer_group_name";
const TOPIC_LABEL: &str = "topic";
const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ProduceRequest {
    pub records: Vec<ProducerRecord>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ProduceReply {
    pub offsets: Vec<ProducedOffset>,
}

impl KafkaRestCommitLog {
    /// Establish a new commit log session.
    pub fn new(server: &Url, server_cert: Option<&Certificate>, tls_insecure: bool) -> Self {
        INIT.call_once(|| {
            describe_counter!(
                "consumer_group_requests",
                "number of consumer group requests"
            );
            describe_counter!(
                "consumer_group_request_failures",
                "number of consumer group request failures"
            );
            describe_counter!("producer_replies", "number of successful producer replies");
            describe_counter!(
                "producer_other_reply_failures",
                "number of producer request failures"
            );
            describe_counter!(
                "producer_unavailables",
                "number of times the producer is unavailable/offline"
            );
        });

        let client = Client::builder().danger_accept_invalid_certs(tls_insecure);

        let client = if let Some(cert) = server_cert {
            client.add_root_certificate(cert.clone())
        } else {
            client
        };

        Self {
            client: client.build().unwrap(),
            server: server.clone(),
        }
    }
}

#[async_trait]
impl CommitLog for KafkaRestCommitLog {
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
        offsets: Option<&[ConsumerOffset]>,
        subscriptions: Option<&[Subscription]>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + 'a>> {
        let consumer = Consumer {
            offsets: offsets.map(|e| e.iter().map(|e| e.to_owned()).collect()),
            subscriptions: subscriptions.map(|e| e.iter().map(|e| e.to_owned()).collect()),
        };
        let consumer_group_name = consumer_group_name.to_string();
        let request = self
            .client
            .post(
                self.server
                    .join(&format!("/consumers/{}", consumer_group_name))
                    .unwrap(),
            )
            .json(&consumer);

        Box::pin(stream! {
            'stream_loop: loop {
                increment_counter!("consumer_group_requests", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string());
                let response = request.try_clone().unwrap().send().await;
                match response {
                    Ok(mut r) => {
                        loop {
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
                                    if let Ok(record) = serde_json::from_slice(&c) {
                                        trace!("Received record: {:?}", record);
                                        let topic = (&record as &ConsumerRecord).topic.to_owned();
                                        increment_counter!("consumer_group_replies", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string(), TOPIC_LABEL => topic);
                                        yield record;
                                    } else {
                                        debug!("Unable to decode record");
                                    }
                                }
                                Ok(None) => {
                                    debug!("Unable to receive chunk");
                                    break 'stream_loop;
                                }
                                Err(e) => debug!("Error receiving chunk {:?}", e),
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Commit log is unavailable while subscribing. Error: {:?}",
                            e
                        );
                        increment_counter!("consumer_group_request_failures", CONSUMER_GROUP_NAME_LABEL => consumer_group_name.to_string());
                    }
                }
                time::sleep(RETRY_DELAY).await;
            }
        })
    }

    async fn produce(&self, record: &ProducerRecord) -> Result<ProducedOffset, ProducerError> {
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
                    response
                        .json::<ProduceReply>()
                        .await
                        .map_err(|_| ProducerError::CannotProduce)
                        .and_then(|r| {
                            r.offsets.first().map(|o| o.to_owned()).ok_or_else(|| {
                                debug!("Commit log failure reply with no offset while producing");
                                increment_counter!("producer_other_reply_failures");
                                ProducerError::CannotProduce
                            })
                        })
                } else {
                    debug!(
                        "Commit log failure status while producing: {:?}",
                        response.status()
                    );
                    increment_counter!("producer_other_reply_failures");
                    Err(ProducerError::CannotProduce)
                }
            }
            Err(e) => {
                debug!("Commit log is unavailable while producing. Error: {:?}", e);
                increment_counter!("producer_unavailables");
                Err(ProducerError::Unavailable)
            }
        }
    }
}
