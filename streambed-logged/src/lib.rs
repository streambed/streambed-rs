#![doc = include_str!("../README.md")]

pub mod args;
pub mod compaction;
mod topic_file_op;

use async_stream::stream;
use async_trait::async_trait;
use bytes::BufMut;
use bytes::{Buf, BytesMut};
use chrono::{DateTime, Utc};
use compaction::{CompactionStrategy, Compactor, ScopedTopicSubscriber, TopicStorageOps};
use log::{trace, warn};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TimestampSecondsWithFrac};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::slice;
use std::sync::{Arc, Mutex};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};
use streambed::commit_log::{
    CommitLog, ConsumerOffset, ConsumerRecord, Header, Offset, PartitionOffsets, ProducedOffset,
    ProducerError, ProducerRecord, Subscription, Topic, TopicRef,
};
use streambed::commit_log::{Key, Partition};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tokio_stream::Stream;
use tokio_util::codec::Decoder;
use topic_file_op::TopicFileOp;

use crate::topic_file_op::TopicFileOpError;

const COMPACTOR_QUEUE_SIZE: usize = 10;
const COMPACTOR_WRITE_POLL: Duration = Duration::from_millis(10);
const CONSUMER_QUEUE_SIZE: usize = 10;
const PRODUCER_QUEUE_SIZE: usize = 10;
const TOPIC_FILE_CONSUMER_POLL: Duration = Duration::from_secs(1);
const TOPIC_FILE_PRODUCER_FLUSH: Duration = Duration::from_millis(10);

type ProduceReply = Result<ProducedOffset, ProducerError>;
type ProduceRequest = (ProducerRecord, oneshot::Sender<ProduceReply>);
type ShareableTopicMap<T> = Arc<Mutex<HashMap<Topic, T>>>;

/// A commit log implementation that uses the file system as its
/// backing store.
///
/// Considerations:
///
/// 1. Partition values cannot be non-zero.
/// 2. The number of subscriptions of a topic will translate to
///    the number of tasks that are spawned, along with their
///    associated resources.
/// 3. Only one process can produce to a specific topic. There
///    is no process-wide locking considered. Multiple processes
///    can read a topic though.
#[derive(Clone)]
pub struct FileLog {
    root_path: PathBuf,
    compaction_threshold_size: u64,
    read_buffer_size: usize,
    compaction_write_buffer_size: usize,
    write_buffer_size: usize,
    compactor_txs: ShareableTopicMap<mpsc::Sender<u64>>,
    producer_txs: ShareableTopicMap<mpsc::Sender<ProduceRequest>>,
    pub(crate) topic_file_ops: ShareableTopicMap<TopicFileOp>,
}

#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
pub struct StorableHeader {
    key: String,
    value: Vec<u8>,
}

#[serde_as]
#[derive(Clone, Deserialize, Debug, Eq, PartialEq, Serialize)]
struct StorableRecord {
    version: u32,
    headers: Vec<StorableHeader>,
    #[serde_as(as = "Option<TimestampSecondsWithFrac>")]
    timestamp: Option<DateTime<Utc>>,
    key: u64,
    value: Vec<u8>,
    offset: u64,
}

/// Some unrecoverable issue when attempting to register compaction.
#[derive(Debug)]
pub struct CompactionRegistrationError;

impl FileLog {
    /// Construct a new file log that will also spawn a task for each
    /// topic being produced.
    pub fn new<P>(root_path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self::with_config(root_path, 64 * 1024, 8192, 64 * 1024, 8192)
    }

    /// Construct a new file log that will also spawn a task for each
    /// topic being produced. The compaction_threshold_size is the size of the
    /// active file that the compactor looks at before deciding to perform a
    /// compaction (in bytes). This typically equates to the blocksize on disk
    /// i.e. 64KB for flash based storage. 64KB is still small enough that scans
    /// over a topic are relatively fast, working on the principle of having roughly
    /// 2,000 records. We also require a read and write buffer sizes to reduce
    /// system calls. When writing, either the buffer reaches capacity or a
    /// flush of the buffer occurs in the absence of another write to perform.
    pub fn with_config<P>(
        root_path: P,
        compaction_threshold_size: u64,
        read_buffer_size: usize,
        compaction_write_buffer_size: usize,
        write_buffer_size: usize,
    ) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            root_path: root_path.into(),
            compaction_threshold_size,
            read_buffer_size,
            compaction_write_buffer_size,
            write_buffer_size,
            compactor_txs: Arc::new(Mutex::new(HashMap::new())),
            producer_txs: Arc::new(Mutex::new(HashMap::new())),
            topic_file_ops: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Frees resources associated with a topic, but not any associated compaction.
    /// Invoking the method is benign in that if consuming or producing occurs
    /// on this post closing, resources will be re-established.
    pub fn close_topic(&mut self, topic: TopicRef) {
        if let Ok(mut locked_producer_txs) = self.producer_txs.lock() {
            locked_producer_txs.remove(topic);
        }
        if let Ok(mut locked_topic_file_ops) = self.topic_file_ops.lock() {
            locked_topic_file_ops.remove(topic);
        }
    }

    /// Register compaction for a given topic. Any previously registered compaction
    /// is replaced. A new task for compaction will be created in the background.
    ///
    /// Compaction's memory can be controlled somewhat through `compaction_write_buffer_size`
    /// when creating this file commit log. This buffer size is selected to minimize
    /// writing to flash and will be allocated once per topic compaction registered here.
    pub async fn register_compaction<CS>(
        &mut self,
        topic: Topic,
        compaction_strategy: CS,
    ) -> Result<(), CompactionRegistrationError>
    where
        CS: CompactionStrategy + Send + Sync + 'static,
    {
        let topic_file_op = {
            let Ok(mut locked_topic_file_ops) = self.topic_file_ops.lock() else {return Err(CompactionRegistrationError)};
            acquire_topic_file_ops(&self.root_path, &topic, &mut locked_topic_file_ops)
        };

        let mut age_active_file_topic_file_op = topic_file_op.clone();
        let age_active_file_read_buffer_size = self.read_buffer_size;
        let new_work_file_topic_file_op = topic_file_op.clone();
        let replace_history_files_topic_file_op = topic_file_op;

        let compaction_write_buffer_size = self.compaction_write_buffer_size;

        let mut compactor = Compactor::new(
            compaction_strategy,
            self.compaction_threshold_size,
            ScopedTopicSubscriber::new(self.clone(), topic.clone()),
            TopicStorageOps::new(
                move || {
                    age_active_file_topic_file_op.age_active_file()?;
                    find_offset(
                        &age_active_file_topic_file_op,
                        age_active_file_read_buffer_size,
                        true,
                    )
                    .map(|o| o.map(|o| o.end_offset))
                    .map_err(TopicFileOpError::IoError)
                },
                move || new_work_file_topic_file_op.new_work_file(compaction_write_buffer_size),
                move || replace_history_files_topic_file_op.replace_history_files(),
            ),
        );

        let (compactor_tx, mut compactor_rx) = mpsc::channel::<u64>(COMPACTOR_QUEUE_SIZE);

        tokio::spawn(async move {
            let mut recv = compactor_rx.recv().await;
            while let Some(active_file_size) = recv {
                compactor.step(active_file_size).await;
                if compactor.is_idle() {
                    recv = compactor_rx.recv().await;
                } else if let Ok(r) = time::timeout(COMPACTOR_WRITE_POLL, compactor_rx.recv()).await
                {
                    recv = r;
                }
            }
        });

        if let Ok(mut compactors) = self.compactor_txs.lock() {
            compactors.insert(topic, compactor_tx);
        }

        Ok(())
    }

    /// Unregister compaction for a given topic
    pub fn unregister_compaction(&mut self, topic: TopicRef) {
        if let Ok(mut compactors) = self.compactor_txs.lock() {
            compactors.remove(topic);
        }
    }
}

#[async_trait]
impl CommitLog for FileLog {
    async fn offsets(&self, topic: Topic, _partition: Partition) -> Option<PartitionOffsets> {
        let Ok(mut locked_topic_file_ops) = self.topic_file_ops.lock() else {return None};
        let topic_file_op =
            acquire_topic_file_ops(&self.root_path, &topic, &mut locked_topic_file_ops);
        drop(locked_topic_file_ops);

        find_offset(&topic_file_op, self.read_buffer_size, false)
            .ok()
            .flatten()
    }

    async fn produce(&self, record: ProducerRecord) -> ProduceReply {
        let topic_producer = {
            let Ok(mut locked_producer_map) = self.producer_txs.lock() else {
                return Err(ProducerError::CannotProduce)
            };
            if let Some(topic_producer) = locked_producer_map.get(&record.topic) {
                let producer_tx = topic_producer.clone();
                drop(locked_producer_map);
                producer_tx
            } else {
                let (producer_tx, mut producer_rx) =
                    mpsc::channel::<ProduceRequest>(PRODUCER_QUEUE_SIZE);
                locked_producer_map.insert(record.topic.clone(), producer_tx.clone());
                drop(locked_producer_map); // drop early so we don't double-lock with the next thing

                let Ok(mut locked_topic_file_ops) = self.topic_file_ops.lock() else {return Err(ProducerError::CannotProduce)};
                let mut topic_file_op = acquire_topic_file_ops(
                    &self.root_path,
                    &record.topic,
                    &mut locked_topic_file_ops,
                );
                drop(locked_topic_file_ops);

                let mut next_offset = find_offset(&topic_file_op, self.read_buffer_size, false)
                    .ok()
                    .flatten()
                    .map_or(0, |offsets| offsets.end_offset.wrapping_add(1));

                let task_root_path = self.root_path.clone();
                let task_compactor_txs = self.compactor_txs.clone();
                let task_topic_file_ops = self.topic_file_ops.clone();
                let task_write_buffer_size = self.write_buffer_size;

                let mut open_options = fs::OpenOptions::new();
                open_options.append(true).create(true);

                let mut file_size = topic_file_op
                    .active_file_size(&open_options, task_write_buffer_size)
                    .unwrap_or_default();

                tokio::spawn({
                    async move {
                        let mut recv = producer_rx.recv().await;
                        while let Some((record, reply_to)) = recv {
                            let topic_file_op = {
                                if let Ok(mut locked_topic_file_ops) = task_topic_file_ops.lock() {
                                    Some(acquire_topic_file_ops(
                                        &task_root_path,
                                        &record.topic,
                                        &mut locked_topic_file_ops,
                                    ))
                                } else {
                                    None
                                }
                            };
                            if let Some(mut topic_file_op) = topic_file_op {
                                let r = topic_file_op.with_active_file(
                                    &open_options,
                                    task_write_buffer_size,
                                    |file| {
                                        let storable_record = StorableRecord {
                                            version: 0,
                                            headers: record
                                                .headers
                                                .into_iter()
                                                .map(|h| StorableHeader {
                                                    key: h.key,
                                                    value: h.value,
                                                })
                                                .collect(),
                                            timestamp: record.timestamp,
                                            key: record.key,
                                            value: record.value,
                                            offset: next_offset,
                                        };

                                        trace!("Producing record: {:?}", storable_record);

                                        if let Ok(buf) = postcard::to_stdvec(&storable_record) {
                                            file.write_all(&buf)
                                                .map_err(TopicFileOpError::IoError)
                                                .map(|_| buf.len())
                                        } else {
                                            Err(TopicFileOpError::CannotSerialize)
                                        }
                                    },
                                );

                                if let Ok((bytes_written, is_new_active_file)) = r {
                                    let _ = reply_to.send(Ok(ProducedOffset {
                                        offset: next_offset,
                                    }));

                                    next_offset = next_offset.wrapping_add(1);

                                    if is_new_active_file {
                                        file_size = 0;
                                    }
                                    file_size = file_size.wrapping_add(bytes_written as u64);

                                    let compactor_tx = {
                                        if let Ok(locked_task_compactor_txs) =
                                            task_compactor_txs.lock()
                                        {
                                            locked_task_compactor_txs.get(&record.topic).cloned()
                                        } else {
                                            None
                                        }
                                    };
                                    if let Some(compactor_tx) = compactor_tx {
                                        let _ = compactor_tx.send(file_size).await;
                                    }

                                    match time::timeout(
                                        TOPIC_FILE_PRODUCER_FLUSH,
                                        producer_rx.recv(),
                                    )
                                    .await
                                    {
                                        Ok(r) => recv = r,
                                        Err(_) => {
                                            let _ = topic_file_op.flush_active_file();
                                            recv = producer_rx.recv().await;
                                        }
                                    }

                                    continue;
                                }
                            }

                            let _ = reply_to.send(Err(ProducerError::CannotProduce));
                            break;
                        }
                    }
                });

                producer_tx
            }
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        if topic_producer.send((record, reply_tx)).await.is_ok() {
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

        let mut open_options = OpenOptions::new();
        open_options.read(true);

        for s in subscriptions {
            let task_root_path = self.root_path.clone();
            let task_topic = s.topic.clone();
            let mut task_offset = offsets.get(&s.topic).copied();
            let task_tx = tx.clone();
            let task_read_buffer_size = self.read_buffer_size;
            let task_topic_file_ops = self.topic_file_ops.clone();
            let task_open_options = open_options.clone();
            tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(task_read_buffer_size);
                let mut decoder = StorableRecordDecoder;
                'outer: loop {
                    buf.clear();

                    let topic_file_op = {
                        let Ok(mut locked_topic_file_ops) = task_topic_file_ops.lock() else {
                            break
                        };
                        let topic_file_op = acquire_topic_file_ops(
                            &task_root_path,
                            &task_topic,
                            &mut locked_topic_file_ops,
                        );
                        drop(locked_topic_file_ops);
                        topic_file_op
                    };

                    let mut topic_files = topic_file_op
                        .open_files(task_open_options.clone(), false)
                        .into_iter();
                    match topic_files.next() {
                        Some(Ok(mut topic_file)) => loop {
                            let Ok(len) = read_buf(&mut topic_file, &mut buf) else {
                                break;
                            };

                            let decode_fn = if len == 0 {
                                StorableRecordDecoder::decode_eof
                            } else {
                                StorableRecordDecoder::decode
                            };
                            match decode_fn(&mut decoder, &mut buf) {
                                Ok(Some(record)) => {
                                    if task_offset.is_none() || Some(record.offset) > task_offset {
                                        let consumer_record = ConsumerRecord {
                                            topic: task_topic.clone(),
                                            headers: record
                                                .headers
                                                .into_iter()
                                                .map(|h| Header {
                                                    key: h.key,
                                                    value: h.value,
                                                })
                                                .collect(),
                                            timestamp: record.timestamp,
                                            key: record.key,
                                            value: record.value,
                                            partition: 0,
                                            offset: record.offset,
                                        };

                                        trace!("Consumed record: {:?}", consumer_record);

                                        if task_tx.send(consumer_record).await.is_err() {
                                            break 'outer;
                                        }

                                        task_offset = Some(record.offset)
                                    }
                                }
                                Ok(None) if len == 0 => match topic_files.next() {
                                    Some(Ok(tf)) => topic_file = tf,
                                    Some(Err(e)) => {
                                        warn!("Error consuming topic file: {e} - aborting subscription for {task_topic}");
                                        break 'outer;
                                    }
                                    None => {
                                        if task_tx.is_closed() {
                                            break 'outer;
                                        }
                                        time::sleep(TOPIC_FILE_CONSUMER_POLL).await;
                                        continue 'outer;
                                    }
                                },
                                Ok(None) => (),
                                Err(e) => {
                                    warn!("Error consuming topic file: {e} - aborting subscription for {task_topic}");
                                    break 'outer;
                                }
                            }
                        },
                        Some(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                            if task_tx.is_closed() {
                                break;
                            }
                            time::sleep(TOPIC_FILE_CONSUMER_POLL).await
                        }
                        Some(Err(e)) => {
                            warn!("Error reading topic file: {e} - aborting subscription");
                        }
                        None => {
                            if task_tx.is_closed() {
                                break;
                            }
                            time::sleep(TOPIC_FILE_CONSUMER_POLL).await
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

fn acquire_topic_file_ops(
    root_path: &Path,
    topic: TopicRef,
    topic_file_ops: &mut HashMap<Topic, TopicFileOp>,
) -> TopicFileOp {
    if let Some(topic_file_op) = topic_file_ops.get(topic) {
        topic_file_op.clone()
    } else {
        let topic = topic.to_string();
        let topic_file_op = TopicFileOp::new(root_path.to_path_buf(), topic.clone());
        topic_file_ops.insert(topic, topic_file_op.clone());
        topic_file_op
    }
}

fn find_offset(
    topic_file_op: &TopicFileOp,
    read_buffer_size: usize,
    exclude_active_file: bool,
) -> io::Result<Option<PartitionOffsets>> {
    let mut open_options = OpenOptions::new();
    open_options.read(true);
    let mut topic_files = topic_file_op
        .open_files(open_options, exclude_active_file)
        .into_iter();
    match topic_files.next() {
        Some(Ok(mut topic_file)) => {
            let mut buf = BytesMut::with_capacity(read_buffer_size);
            let mut decoder = StorableRecordDecoder;
            let mut beginning_offset = None;
            let mut end_offset = None;
            loop {
                let Ok(len) = read_buf(&mut topic_file, &mut buf) else {
                    break;
                };

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
                    None if len == 0 => match topic_files.next() {
                        Some(Ok(tf)) => topic_file = tf,
                        Some(Err(e)) => return Err(e),
                        None => break,
                    },
                    None => (),
                }
            }
            Ok(Some(PartitionOffsets {
                beginning_offset: beginning_offset.unwrap_or(0),
                end_offset: end_offset.unwrap_or(0),
            }))
        }
        Some(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

// Similar to Tokio's AsyncReadExt [`read_buf`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncReadExt.html#method.read_buf).
// Thanks to Alice Ryhl: https://discord.com/channels/500028886025895936/627696030334582784/1071037851980021761
fn read_buf<B>(file: &mut File, buf: &mut B) -> io::Result<usize>
where
    B: BufMut,
{
    let chunk = buf.chunk_mut();
    let len = chunk.len();
    let ptr = chunk.as_mut_ptr();
    let unused_buf = unsafe { slice::from_raw_parts_mut(ptr, len) };
    let result = file.read(unused_buf);
    if let Ok(len) = result {
        unsafe {
            buf.advance_mut(len);
        }
    }
    result
}

struct StorableRecordDecoder;

impl Decoder for StorableRecordDecoder {
    type Item = StorableRecord;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let result = postcard::take_from_bytes::<StorableRecord>(src);
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
        println!("Writing to {logged_dir}");

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

            time::sleep(TOPIC_FILE_PRODUCER_FLUSH * 2).await;
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
    async fn test_produce_consume_with_split() {
        let logged_dir = env::temp_dir().join("test_produce_consume_with_split");
        // Deliberately converting to a String to test Into<PathBuf>
        let _ = fs::remove_dir_all(&logged_dir).await;
        let _ = fs::create_dir_all(&logged_dir).await;
        println!("Writing to {}", logged_dir.to_string_lossy());

        let mut cl = FileLog::new(logged_dir.clone());
        let mut task_cl = cl.clone();

        let topic = "my-topic";

        cl.register_compaction(topic.to_string(), compaction::KeyBasedRetention::new(1))
            .await
            .unwrap();

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

            // At this point we're going to pretend we've performed a compaction
            // that would result in the commit log file being split into a .history
            // file.

            let mut topic_file_op = {
                let locked_topic_file_ops = task_cl.topic_file_ops.lock().unwrap();
                locked_topic_file_ops.get(topic).unwrap().clone()
            };
            topic_file_op.age_active_file().unwrap();

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

            time::sleep(TOPIC_FILE_PRODUCER_FLUSH * 2).await;
            let offsets = task_cl.offsets(topic.to_string(), 0).await.unwrap();
            assert_eq!(
                offsets,
                PartitionOffsets {
                    beginning_offset: 0,
                    end_offset: 2
                }
            );

            let topic_file_path = logged_dir.join(topic);
            assert!(topic_file_path.exists());
            assert!(topic_file_path
                .with_extension(topic_file_op::HISTORY_FILE_EXTENSION)
                .exists());

            task_cl.close_topic(topic);
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
