/// Provides commit log functionality to connect with the Kafka HTTP API
use std::pin::Pin;
use std::sync::Once;
use std::time::Duration;

use async_stream::stream;
use log::{debug, trace};
use metrics::describe_counter;
use metrics::increment_counter;
use reqwest::Certificate;
use reqwest::{Client, Url};
use tokio::time;
use tokio_stream::Stream;

use super::commit_log::CommitLog;
use super::commit_log::Consumer;
use super::commit_log::Offset;
use super::commit_log::Record;
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

impl KafkaRestCommitLog {
    /// Establish a new commit log session.
    pub fn new(server: &Url, server_cert: Option<&Certificate>, tls_insecure: bool) -> Self {
        INIT.call_once(|| {
            describe_counter!(
                "consumer_group_requests",
                "number of consumer group requests".into()
            );
            describe_counter!(
                "consumer_group_request_failures",
                "number of consumer group request failures".into()
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
        offsets: Option<&[Offset]>,
        subscriptions: Option<&[Subscription]>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = Record> + 'a>> {
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
                                        let topic = (&record as &Record).topic.to_owned();
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
}
