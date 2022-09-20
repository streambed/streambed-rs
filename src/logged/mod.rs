#![doc = include_str!("README.md")]

pub mod args;

use super::base64_serde;
use crate::commit_log::{
    CommitLog, ConsumerOffset, ConsumerRecord, Header, PartitionOffsets, ProducedOffset,
    ProducerError, ProducerRecord, Subscription, Topic,
};
use async_stream::stream;
use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use chrono::{DateTime, Utc};
use log::{trace, warn};
use metrics::increment_counter;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
    time,
};
use tokio_stream::Stream;
use tokio_util::codec::Decoder;

const CONSUMER_QUEUE_SIZE: usize = 10;
const PRODUCER_QUEUE_SIZE: usize = 10;
const TOPIC_FILE_CONSUMER_POLL: Duration = Duration::from_secs(1);
const TOPIC_LABEL: &str = "topic";

/// A commit log implementation that uses the file system as its
/// backing store.
///
/// Notes:
/// 1. Partition values cannot be non-zero.
/// 2. The number of subscriptions of a topic will translate to
///    the number of tasks that are spawned, along with their
///    associated resources.
#[derive(Clone)]
pub struct FileLog {
    producer_tx: mpsc::Sender<(
        ProducerRecord,
        oneshot::Sender<Result<ProducedOffset, ProducerError>>,
    )>,
    root_path: PathBuf,
}

#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
struct StorableRecord {
    version: u32,
    headers: Vec<Header>,
    timestamp: Option<DateTime<Utc>>,
    key: u64,
    #[serde(with = "base64_serde")]
    value: Vec<u8>,
    offset: u64,
}

impl FileLog {
    pub fn new<P: Into<PathBuf>>(root_path: P) -> Self {
        let root_path = root_path.into();
        let retained_root_path = root_path.clone();

        let (producer_tx, mut producer_rx) = mpsc::channel::<(
            ProducerRecord,
            oneshot::Sender<Result<ProducedOffset, ProducerError>>,
        )>(PRODUCER_QUEUE_SIZE);

        tokio::spawn({
            async move {
                let mut topic_files: HashMap<Topic, File> = HashMap::new();
                while let Some((record, reply_to)) = producer_rx.recv().await {
                    let mut result = Err(ProducerError::CannotProduce);

                    if let Ok(offsets) = find_offset(&root_path, &record.topic).await {
                        let next_offset = if let Some(offsets) = offsets {
                            offsets.end_offset.wrapping_add(1)
                        } else {
                            0
                        };

                        let topic_file: Option<&mut File> = if let Some(topic_file) =
                            topic_files.get_mut(&record.topic)
                        {
                            Some(topic_file)
                        } else {
                            let topic_file = fs::OpenOptions::new()
                                .append(true)
                                .create(true)
                                .open(root_path.join(&record.topic))
                                .await;
                            if let Ok(topic_file) = topic_file {
                                topic_files.insert(record.topic.clone(), topic_file);
                                topic_files.get_mut(&record.topic)
                            } else {
                                warn!("Error when trying to append to a file: {:?}", topic_file);
                                increment_counter!("producer_append_failures");
                                None
                            }
                        };

                        if let Some(topic_file) = topic_file {
                            let storable_record = StorableRecord {
                                version: 0,
                                headers: record.headers,
                                timestamp: record.timestamp,
                                key: record.key,
                                value: record.value,
                                offset: next_offset,
                            };
                            if let Ok(buf) = postcard::to_stdvec(&storable_record) {
                                if topic_file.write_all(&buf).await.is_ok() {
                                    trace!("Produced record: {:?}", storable_record);
                                    increment_counter!("produced_records",  TOPIC_LABEL => record.topic.to_owned());
                                    result = Ok(ProducedOffset {
                                        offset: next_offset,
                                    });
                                }
                            }
                        }
                    };

                    let _ = reply_to.send(result);
                }
            }
        });

        Self {
            producer_tx,
            root_path: retained_root_path,
        }
    }
}

async fn find_offset(
    root_path: &Path,
    topic: &str,
) -> Result<Option<PartitionOffsets>, std::io::Error> {
    let topic_file = File::open(root_path.join(topic)).await;
    match topic_file {
        Ok(mut topic_file) => {
            let mut buf = BytesMut::new();
            let mut decoder = StorableRecordDecoder;
            let mut beginning_offset = None;
            let mut end_offset = None;
            while let Ok(len) = topic_file.read_buf(&mut buf).await {
                let decode_fn = if len == 0 {
                    StorableRecordDecoder::decode_eof
                } else {
                    StorableRecordDecoder::decode
                };
                match decode_fn(&mut decoder, &mut buf)? {
                    Some(record) if beginning_offset.is_none() => {
                        beginning_offset = Some(record.offset);
                        end_offset = Some(record.offset);
                    }
                    Some(record) => {
                        end_offset = Some(record.offset);
                    }
                    None if len == 0 => break,
                    None => (),
                }
            }
            Ok(Some(PartitionOffsets {
                beginning_offset: beginning_offset.unwrap_or(0),
                end_offset: end_offset.unwrap_or(0),
            }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[async_trait]
impl CommitLog for FileLog {
    async fn offsets(&self, topic: Topic, _partition: u32) -> Option<PartitionOffsets> {
        find_offset(&self.root_path, topic.as_str())
            .await
            .ok()
            .flatten()
    }

    async fn produce(&self, record: ProducerRecord) -> Result<ProducedOffset, ProducerError> {
        let (reply_tx, reply_rx) = oneshot::channel::<Result<ProducedOffset, ProducerError>>();
        if self.producer_tx.send((record, reply_tx)).await.is_ok() {
            if let Ok(reply) = reply_rx.await {
                reply
            } else {
                Err(ProducerError::CannotProduce)
            }
        } else {
            Err(ProducerError::CannotProduce)
        }
    }

    fn scoped_subscribe<'a>(
        &'a self,
        _consumer_group_name: &str,
        offsets: Vec<ConsumerOffset>,
        subscriptions: Vec<Subscription>,
        idle_timeout: Option<Duration>,
    ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'a>> {
        let offsets = offsets
            .iter()
            .map(|e| {
                assert_eq!(e.partition, 0);
                (e.topic.to_owned(), e.offset)
            })
            .collect::<HashMap<Topic, u64>>();

        let (tx, mut rx) = mpsc::channel(CONSUMER_QUEUE_SIZE);

        for s in subscriptions {
            let task_topic_file = self.root_path.join(&s.topic);
            let task_topic = s.topic.clone();
            let mut task_offset = offsets.get(&s.topic).copied();
            let task_tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = BytesMut::new();
                let mut decoder = StorableRecordDecoder;
                'outer: loop {
                    buf.clear();
                    match File::open(&task_topic_file).await {
                        Ok(mut topic_file) => {
                            while let Ok(len) = topic_file.read_buf(&mut buf).await {
                                let decode_fn = if len == 0 {
                                    StorableRecordDecoder::decode_eof
                                } else {
                                    StorableRecordDecoder::decode
                                };
                                match decode_fn(&mut decoder, &mut buf) {
                                    Ok(Some(record)) => {
                                        if task_offset.is_none()
                                            || Some(record.offset) > task_offset
                                        {
                                            let consumer_record = ConsumerRecord {
                                                topic: task_topic.clone(),
                                                headers: record.headers,
                                                timestamp: record.timestamp,
                                                key: record.key,
                                                value: record.value,
                                                partition: 0,
                                                offset: record.offset,
                                            };

                                            trace!("Consumed record: {:?}", consumer_record);
                                            increment_counter!("consumed_records",  TOPIC_LABEL => task_topic.clone());

                                            if task_tx.send(consumer_record).await.is_err() {
                                                break 'outer;
                                            }

                                            task_offset = Some(record.offset)
                                        }
                                    }
                                    Ok(None) if len == 0 => {
                                        time::sleep(TOPIC_FILE_CONSUMER_POLL).await;
                                        continue 'outer;
                                    }
                                    Ok(None) => (),
                                    Err(e) => {
                                        warn!("Error consuming topic file: {e} - aborting subscription for {task_topic}");
                                        increment_counter!("subscription_failures", TOPIC_LABEL => task_topic.clone());
                                        break 'outer;
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            time::sleep(TOPIC_FILE_CONSUMER_POLL).await
                        }
                        Err(e) => {
                            warn!("Error reading topic file: {e} - aborting subscription");
                            increment_counter!("subscription_failures", TOPIC_LABEL => task_topic.clone());
                        }
                    }
                }
            });
        }

        Box::pin(stream!({
            if let Some(it) = idle_timeout {
                while let Some(record) = time::timeout(it, rx.recv()).await.ok().flatten() {
                    yield record;
                }
            } else {
                while let Some(record) = rx.recv().await {
                    yield record;
                }
            }
            trace!("Ending subscriptions");
        }))
    }
}

struct StorableRecordDecoder;

impl Decoder for StorableRecordDecoder {
    type Item = StorableRecord;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let new_src = src.clone();
        let result = postcard::take_from_bytes::<StorableRecord>(&new_src);
        match result {
            Ok((record, remaining)) => {
                src.advance(src.len() - remaining.len());
                Ok(Some(record))
            }
            Err(e) if e == postcard::Error::DeserializeUnexpectedEnd => Ok(None),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use test_log::test;
    use tokio::{fs, sync::Notify};
    use tokio_stream::StreamExt;

    use super::*;

    #[test(tokio::test)]
    async fn test_produce_consume() {
        let logged_dir = env::temp_dir().join("test_produce_consume");
        // Deliberately converting to a String to test Into<PathBuf>
        let logged_dir = logged_dir.to_string_lossy().to_string();
        let _ = fs::remove_dir_all(&logged_dir).await;
        let _ = fs::create_dir_all(&logged_dir).await;
        println!("Writing to {}", logged_dir);

        let cl = FileLog::new(logged_dir);
        let task_cl = cl.clone();

        let topic = "my-topic";

        assert!(cl.offsets(topic.to_string(), 0).await.is_none());

        tokio::spawn(async move {
            task_cl
                .produce(ProducerRecord {
                    topic: topic.to_string(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"some-value-0".to_vec(),
                    partition: 0,
                })
                .await
                .unwrap();
            task_cl
                .produce(ProducerRecord {
                    topic: topic.to_string(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"some-value-1".to_vec(),
                    partition: 0,
                })
                .await
                .unwrap();
            task_cl
                .produce(ProducerRecord {
                    topic: topic.to_string(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"some-value-2".to_vec(),
                    partition: 0,
                })
                .await
                .unwrap();

            let offsets = task_cl.offsets(topic.to_string(), 0).await.unwrap();
            assert_eq!(
                offsets,
                PartitionOffsets {
                    beginning_offset: 0,
                    end_offset: 2
                }
            );
        });

        let offsets = vec![ConsumerOffset {
            topic: topic.to_string(),
            partition: 0,
            offset: 1,
        }];
        let subscriptions = vec![Subscription {
            topic: topic.to_string(),
        }];
        let mut records = cl.scoped_subscribe("some-consumer", offsets, subscriptions, None);

        assert_eq!(
            records.next().await,
            Some(ConsumerRecord {
                topic: topic.to_string(),
                headers: vec![],
                timestamp: None,
                key: 0,
                value: b"some-value-2".to_vec(),
                partition: 0,
                offset: 2
            })
        );
    }

    #[test(tokio::test)]
    async fn test_consume_wait_for_append() {
        let logged_dir = env::temp_dir().join("test_consume_wait_for_append");
        let _ = fs::remove_dir_all(&logged_dir).await;
        let _ = fs::create_dir_all(&logged_dir).await;
        println!("Writing to {}", logged_dir.to_string_lossy());

        let cl = FileLog::new(logged_dir);
        let task_cl = cl.clone();

        let topic = "my-topic";

        let subscribing = Arc::new(Notify::new());
        let task_subscribing = subscribing.clone();

        let produced = Arc::new(Notify::new());
        let task_produced = produced.clone();

        tokio::spawn(async move {
            let subscriptions = vec![Subscription {
                topic: topic.to_string(),
            }];
            let mut records =
                task_cl.scoped_subscribe("some-consumer", vec![], subscriptions, None);
            task_subscribing.notify_one();

            while records.next().await.is_some() {
                task_produced.notify_one();
            }
        });

        subscribing.notified().await;
        time::sleep(TOPIC_FILE_CONSUMER_POLL + Duration::from_millis(500)).await;

        cl.produce(ProducerRecord {
            topic: topic.to_string(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-0".to_vec(),
            partition: 0,
        })
        .await
        .unwrap();

        produced.notified().await;
    }

    #[test(tokio::test)]
    async fn test_consume_with_idle() {
        let logged_dir = env::temp_dir().join("test_consume_with_idle");
        let _ = fs::remove_dir_all(&logged_dir).await;
        let _ = fs::create_dir_all(&logged_dir).await;

        let cl = FileLog::new(logged_dir);

        let topic = "my-topic";

        let offsets = vec![ConsumerOffset {
            topic: topic.to_string(),
            partition: 0,
            offset: 1,
        }];
        let subscriptions = vec![Subscription {
            topic: topic.to_string(),
        }];
        let mut records = cl.scoped_subscribe(
            "some-consumer",
            offsets,
            subscriptions,
            Some(Duration::from_millis(100)),
        );
        assert!(records.next().await.is_none());

        cl.produce(ProducerRecord {
            topic: topic.to_string(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-0".to_vec(),
            partition: 0,
        })
        .await
        .unwrap();

        let subscriptions = vec![Subscription {
            topic: topic.to_string(),
        }];
        let mut records = cl.scoped_subscribe(
            "some-consumer",
            vec![],
            subscriptions,
            Some(TOPIC_FILE_CONSUMER_POLL + Duration::from_millis(500)),
        );
        assert!(records.next().await.is_some());
        assert!(records.next().await.is_none());
    }
}
