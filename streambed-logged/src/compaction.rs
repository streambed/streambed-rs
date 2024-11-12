use std::{error::Error, fmt::Debug};

use super::*;

use log::{debug, error};
use smol_str::SmolStr;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

const ACTIVE_FILE_CONSUMER_IDLE_TIMEOUT: Duration = Duration::from_millis(10);

/// A map of keys to offsets
pub type CompactionMap = HashMap<Key, Offset>;

/// Returned by a compaction reducer function when there is no capacity
/// to process any more keys. A compactor will use this information to
/// determine whether another compaction pass is required.
#[derive(Debug, PartialEq, Eq)]
pub struct MaxKeysReached(pub bool);

/// A compactor strategy's role is to be fed consumer records for a single topic
/// and ultimately determine, for each record key, what the earliest offset is
/// that may be retained. Upon the consumer completing, logged will then proceed
/// to remove unwanted records from the commit log.
#[async_trait]
pub trait CompactionStrategy {
    /// The state to manage throughout a compaction run.
    type S: Debug + Send;

    /// The type of state required to manage keys. It may be that no state is
    /// required i.e. if the key field from the record is used.
    type KS: Debug + Send + Clone;

    /// Produce the initial key state for the compaction.
    fn key_init(&self) -> Self::KS;

    /// The key function computes a key that will be used by the compactor for
    /// subsequent use. In simple scenarios, this key can be the key field from
    /// the topic's record itself, which is the default assumption here.
    /// Returning None will result in this record being excluded from compaction.
    fn key(key_state: &mut Self::KS, r: &ConsumerRecord) -> Option<Key>;

    /// Produce the initial state for the reducer function.
    async fn init(&self) -> Self::S;

    /// The reducer function receives a mutable state reference, a key and a consumer
    /// record, and returns a bool indicating whether the function has
    /// reached its maximum number of distinct keys. If it has then
    /// compaction may occur again once the current consumption of records
    /// has finished. The goal is to avoid needing to process every type of
    /// topic/partition/key in one go i.e. a subset can be processed, and then another
    /// subset etc. This strategy helps to manage memory.
    fn reduce(state: &mut Self::S, key: Key, record: ConsumerRecord) -> MaxKeysReached;

    /// The collect function is responsible for mapping the state into
    /// a map of keys and their minimum offsets. The compactor will filter out
    /// the first n keys in a subsequent run where n is the number of keys
    /// found on the first run. This permits memory to be controlled given
    /// a large number of distinct keys. The cost of the re-run strategy is that
    /// additional compaction scans will be required, resulting in more I/O.
    fn collect(state: Self::S) -> CompactionMap;
}

/// A qualified id is a string that identifies a type of a record and some key derived from the record e.g.
/// a an entity and a primary key of the entity.
pub type Qid = SmolStr;

/// The goal of key-based retention is to keep the latest record for a given
/// key within a topic. This is the same as Kafka's key-based retention and
/// effectively makes the commit log a key value store.
///
/// KeyBasedRetention is guaranteed to return a key that is the record's key.
pub struct KeyBasedRetention {
    max_compaction_keys: usize,
    qid_from_record: Option<fn(&ConsumerRecord) -> Option<Qid>>,
}

impl KeyBasedRetention {
    /// A max_compaction_keys parameter is used to limit the number of distinct topic/partition/keys
    /// processed in a single run of the compactor.
    pub fn new(max_compaction_keys: usize) -> Self {
        Self {
            qid_from_record: None,
            max_compaction_keys,
        }
    }

    /// Similar to the above, but the qualified id of a record will be determined by a function.
    /// A max_compaction_keys parameter is used to limit the number of distinct topic/partition/keys
    /// processed in a single run of the compactor.
    pub fn with_qids(
        qid_from_record: fn(&ConsumerRecord) -> Option<Qid>,
        max_compaction_keys: usize,
    ) -> Self {
        Self {
            qid_from_record: Some(qid_from_record),
            max_compaction_keys,
        }
    }
}

/// The state associated with key based retention.
pub type KeyBasedRetentionState = (CompactionMap, usize);

#[async_trait]
impl CompactionStrategy for KeyBasedRetention {
    type S = KeyBasedRetentionState;
    type KS = Option<(HashMap<Qid, Key>, fn(&ConsumerRecord) -> Option<Qid>)>;

    fn key_init(&self) -> Self::KS {
        self.qid_from_record.map(|id_from_record| {
            (
                HashMap::with_capacity(self.max_compaction_keys),
                id_from_record,
            )
        })
    }

    fn key(key_state: &mut Self::KS, r: &ConsumerRecord) -> Option<Key> {
        if let Some((key_map, id_from_record)) = key_state {
            let id = (id_from_record)(r)?;
            if let Some(key) = key_map.get(id.as_str()) {
                Some(*key)
            } else {
                let key = key_map.len() as Key;
                let _ = key_map.insert(id, key);
                Some(key)
            }
        } else {
            Some(r.key)
        }
    }

    async fn init(&self) -> Self::S {
        (
            CompactionMap::with_capacity(self.max_compaction_keys),
            self.max_compaction_keys,
        )
    }

    fn reduce(state: &mut Self::S, key: Key, record: ConsumerRecord) -> MaxKeysReached {
        let (compaction_map, max_keys) = state;

        let l = compaction_map.len();
        match compaction_map.entry(key) {
            Entry::Occupied(mut e) => {
                *e.get_mut() = record.offset;
                MaxKeysReached(false)
            }
            Entry::Vacant(e) if l < *max_keys => {
                e.insert(record.offset);
                MaxKeysReached(false)
            }
            Entry::Vacant(_) => MaxKeysReached(true),
        }
    }

    fn collect(state: Self::S) -> CompactionMap {
        let (compaction_map, _) = state;
        compaction_map
    }
}

/// Similar to [KeyBasedRetention], but instead of retaining the latest offset for a key. this strategy retains
/// the oldest nth offset associated with a key.
pub struct NthKeyBasedRetention {
    max_compaction_keys: usize,
    max_records_per_key: usize,
    qid_from_record: Option<fn(&ConsumerRecord) -> Option<Qid>>,
}

impl NthKeyBasedRetention {
    /// A max_compaction_keys parameter is used to limit the number of distinct topic/partition/keys
    /// processed in a single run of the compactor. The max_records_per_key is used to retain the
    /// nth oldest key.
    pub fn new(max_compaction_keys: usize, max_records_per_key: usize) -> Self {
        Self {
            max_compaction_keys,
            max_records_per_key,
            qid_from_record: None,
        }
    }

    /// Similar to the above, but the qualified id of a record will be determined by a function.
    /// A max_compaction_keys parameter is used to limit the number of distinct topic/partition/keys
    /// processed in a single run of the compactor. The max_records_per_key is used to retain the
    /// nth oldest key.
    pub fn with_qids(
        max_compaction_keys: usize,
        max_records_per_key: usize,
        qid_from_record: fn(&ConsumerRecord) -> Option<Qid>,
    ) -> Self {
        Self {
            qid_from_record: Some(qid_from_record),
            max_compaction_keys,
            max_records_per_key,
        }
    }
}

/// The state associated with nth key based retention.
pub type NthKeyBasedRetentionState = (HashMap<Key, VecDeque<Offset>>, usize, usize);

#[async_trait]
impl CompactionStrategy for NthKeyBasedRetention {
    type S = NthKeyBasedRetentionState;
    type KS = Option<(HashMap<Qid, Key>, fn(&ConsumerRecord) -> Option<Qid>)>;

    fn key_init(&self) -> Self::KS {
        self.qid_from_record.map(|id_from_record| {
            (
                HashMap::with_capacity(self.max_compaction_keys),
                id_from_record,
            )
        })
    }

    fn key(key_state: &mut Self::KS, r: &ConsumerRecord) -> Option<Key> {
        if let Some((key_map, id_from_record)) = key_state {
            let id = (id_from_record)(r)?;
            if let Some(key) = key_map.get(id.as_str()) {
                Some(*key)
            } else {
                let key = key_map.len() as Key;
                let _ = key_map.insert(id, key);
                Some(key)
            }
        } else {
            Some(r.key)
        }
    }

    async fn init(&self) -> Self::S {
        (
            HashMap::with_capacity(self.max_compaction_keys),
            self.max_compaction_keys,
            self.max_records_per_key,
        )
    }

    fn reduce(state: &mut Self::S, key: Key, record: ConsumerRecord) -> MaxKeysReached {
        let (compaction_map, max_keys, max_records_per_key) = state;

        let l = compaction_map.len();
        match compaction_map.entry(key) {
            Entry::Occupied(mut e) => {
                let offsets = e.get_mut();
                if offsets.len() == *max_records_per_key {
                    offsets.pop_front();
                }
                offsets.push_back(record.offset);
                MaxKeysReached(false)
            }
            Entry::Vacant(e) if l < *max_keys => {
                let mut offsets = VecDeque::with_capacity(*max_records_per_key);
                offsets.push_back(record.offset);
                e.insert(offsets);
                MaxKeysReached(false)
            }
            Entry::Vacant(_) => MaxKeysReached(true),
        }
    }

    fn collect(state: Self::S) -> CompactionMap {
        let (compaction_map, _, _) = state;
        compaction_map
            .into_iter()
            .flat_map(|(k, mut v)| v.pop_front().map(|v| (k, v)))
            .collect::<CompactionMap>()
    }
}

/// Responsible for interacting with a commit log for the purposes of
/// subscribing to a provided topic.
#[derive(Clone)]
pub(crate) struct ScopedTopicSubscriber<CL>
where
    CL: CommitLog,
{
    commit_log: CL,
    subscriptions: Vec<Subscription>,
}

impl<CL> ScopedTopicSubscriber<CL>
where
    CL: CommitLog,
{
    pub fn new(commit_log: CL, topic: Topic) -> Self {
        Self {
            commit_log,
            subscriptions: vec![Subscription { topic }],
        }
    }

    pub fn subscribe<'a>(&'a self) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'a>> {
        self.commit_log.scoped_subscribe(
            "compactor",
            vec![],
            self.subscriptions.clone(),
            Some(ACTIVE_FILE_CONSUMER_IDLE_TIMEOUT),
        )
    }
}

/// Responsible for performing operations on topic storage.
pub(crate) struct TopicStorageOps<E, W>
where
    E: Error,
    W: Write,
{
    age_active: Box<dyn FnMut() -> Result<Option<Offset>, E> + Send>,
    new_work_writer: Box<dyn FnMut() -> Result<W, E> + Send>,
    replace_history_files: Box<dyn FnMut() -> Result<(), E> + Send>,
}

impl<E, W> TopicStorageOps<E, W>
where
    E: Error,
    W: Write,
{
    pub fn new<AA, NWF, RCHF, RPHF>(
        age_active: AA,
        new_work_file: NWF,
        mut recover_history_files: RCHF,
        replace_history_files: RPHF,
    ) -> Self
    where
        AA: FnMut() -> Result<Option<Offset>, E> + Send + 'static,
        NWF: FnMut() -> Result<W, E> + Send + 'static,
        RCHF: FnMut() -> Result<(), E> + Send + 'static,
        RPHF: FnMut() -> Result<(), E> + Send + 'static,
    {
        let _ = recover_history_files();

        Self {
            age_active: Box::new(age_active),
            new_work_writer: Box::new(new_work_file),
            replace_history_files: Box::new(replace_history_files),
        }
    }

    pub fn age_active(&mut self) -> Result<Option<Offset>, E> {
        (self.age_active)()
    }

    pub fn new_work_writer(&mut self) -> Result<W, E> {
        (self.new_work_writer)()
    }

    pub fn replace_history_files(&mut self) -> Result<(), E> {
        (self.replace_history_files)()
    }
}

/// A compactor actually performs the work of compaction. It is a state
/// machine with the following major transitions:
///
/// Idle -> Analyzing -> Compacting -> Idle
///
/// An async function named `step` is provided that will step through
/// the state machine. Its present stratgey is to back-pressure by not
/// returning when the size of the active file has reached the threshold
/// for compaction when compaction is already in progress. If a producer
/// is sensitive to back-pressure (this should be rare given the correct
/// dimensioning of the compactor's configuration) then awaiting on
/// producing messages can be avoided. The primary aim of the compactor is to
/// manage storage space. Exhausting storage can perhaps create a
/// similar number of problems upstream as back-pressuring and awaiting
/// a reply to producing a message. It is left to the application developer
/// on which strategy should be adopted and it will depend on the real-time
/// consequences of being back-pressured.
///
/// While idle, we are notified with the file size of the active portion
/// of the commit log. If the active size exceeds a provided threshold of
/// bytes, e.g. the erase size of a flash drive, then we move to the
/// Analysing stage.
///
/// During analysis, we call upon the compaction strategy to collect a
/// map of offsets. These offsets are then supplied to the Compacting stage.
///
/// The Compacting stage will perform the work of producing a new history
/// file given the map of offsets.
///
/// If the analysis stage did not finish entirely then analysis is run
/// again until it is. Otherwise, back to idle.
///
/// A note on running the strategy again: It is not scalable to store a
/// set of all of the keys we have previously encountered so as to avoid
/// using them again on a subsequent run. Instead, we note the record
/// offset of the first record where the compaction strategy detects
/// that the max number of compaction keys has been reached. We then
/// begin our subsequent scan from that offset. We are therefore
/// guaranteed to encounter a key that was not able to be processed on
/// the run so far. The worst case scenario is that we would re-run
/// compaction having only ever discovered one new key to process given
/// a heavy presence of prior-run keys being detected again before other
/// keys become apparent. Detecting only one new key at a time would
/// slow down compaction overall, and back-pressure would ultimately
/// occur on producing to the log. Compaction should eventually finish
/// though. We also expect the worst-case scenario to be avoidable
/// given consideration by an application developer in terms of the
/// number of keys that can be processed by a strategy in one run. Application
/// developers should at least strive to dimension their compaction
/// strategies with a number of keys that are sufficient to require
/// only a single compaction pass.
pub(crate) struct Compactor<E, W, CL, CS>
where
    E: Error,
    W: Write,
    CL: CommitLog,
    CS: CompactionStrategy + Send + 'static,
{
    compaction_strategy: CS,
    compaction_threshold: u64,
    scoped_topic_subscriber: ScopedTopicSubscriber<CL>,
    topic_storage_ops: TopicStorageOps<E, W>,

    state: State<CS>,
}

#[derive(Debug)]
enum CompactionError {
    CannotSerialize,
    #[allow(dead_code)]
    IoError(io::Error),
}

enum State<CS>
where
    CS: CompactionStrategy,
{
    Idle,
    PreparingAnalyze(Option<Offset>),
    Analyzing(JoinHandle<(CS::KS, CS::S, Option<Offset>)>, Offset),
    PreparingCompaction(CS::KS, CompactionMap, Offset, Option<Offset>),
    Compacting(JoinHandle<Result<(), CompactionError>>, Option<Offset>),
}

impl<CS> Debug for State<CS>
where
    CS: CompactionStrategy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::PreparingAnalyze(arg0) => f.debug_tuple("PreparingAnalyze").field(arg0).finish(),
            Self::Analyzing(arg0, arg1) => {
                f.debug_tuple("Analyzing").field(arg0).field(arg1).finish()
            }
            Self::PreparingCompaction(arg0, arg1, arg2, arg3) => f
                .debug_tuple("PreparingCompaction")
                .field(arg0)
                .field(arg1)
                .field(arg2)
                .field(arg3)
                .finish(),
            Self::Compacting(arg0, arg1) => {
                f.debug_tuple("Compacting").field(arg0).field(arg1).finish()
            }
        }
    }
}

impl<E, W, CL, CS> Compactor<E, W, CL, CS>
where
    E: Error + Send + 'static,
    W: Write + Send + 'static,
    CL: CommitLog + Clone + Send + 'static,
    CS: CompactionStrategy + Send + 'static,
{
    pub fn new(
        compaction_strategy: CS,
        compaction_threshold: u64,
        scoped_topic_subscriber: ScopedTopicSubscriber<CL>,
        topic_storage_ops: TopicStorageOps<E, W>,
    ) -> Self {
        Self {
            compaction_strategy,
            compaction_threshold,
            scoped_topic_subscriber,
            topic_storage_ops,
            state: State::Idle,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.state, State::Idle)
    }

    pub async fn step(&mut self, mut active_file_size: u64) {
        loop {
            let mut step_again = false;
            let next_state = match &mut self.state {
                State::Idle if active_file_size < self.compaction_threshold => None,
                State::Idle => {
                    step_again = true;
                    Some(State::PreparingAnalyze(None))
                }
                State::PreparingAnalyze(mut next_start_offset) => {
                    let r = self.topic_storage_ops.age_active();
                    if let Ok(Some(end_offset)) = r {
                        let task_scoped_topic_subscriber = self.scoped_topic_subscriber.clone();
                        let task_init = self.compaction_strategy.init().await;
                        let task_key_state_init = self.compaction_strategy.key_init();
                        let h = tokio::spawn(async move {
                            let mut strategy_state = task_init;
                            let mut key_state = task_key_state_init;
                            let mut records = task_scoped_topic_subscriber.subscribe();
                            let start_offset = next_start_offset;
                            next_start_offset = None;
                            while let Some(record) = records.next().await {
                                let record_offset = record.offset;
                                if record_offset > end_offset {
                                    break;
                                }
                                if Some(record_offset) >= start_offset {
                                    if let Some(key) = CS::key(&mut key_state, &record) {
                                        if matches!(
                                            CS::reduce(&mut strategy_state, key, record),
                                            MaxKeysReached(true)
                                        ) && next_start_offset.is_none()
                                        {
                                            next_start_offset = Some(record_offset);
                                        }
                                    }
                                }
                            }
                            (key_state, strategy_state, next_start_offset)
                        });
                        step_again = true;
                        Some(State::Analyzing(h, end_offset))
                    } else {
                        error!("Could not age the active file/locate end offset. Aborting compaction. {r:?}");
                        Some(State::Idle)
                    }
                }
                State::Analyzing(h, end_offset) => {
                    step_again = active_file_size >= self.compaction_threshold;
                    if step_again || h.is_finished() {
                        let r = h.await;
                        let s = if let Ok((key_state, strategy_state, next_start_offset)) = r {
                            let compaction_map = CS::collect(strategy_state);
                            State::PreparingCompaction(
                                key_state,
                                compaction_map,
                                *end_offset,
                                next_start_offset,
                            )
                        } else {
                            error!("Some error analysing compaction: {r:?}");
                            State::Idle
                        };
                        Some(s)
                    } else {
                        None
                    }
                }
                State::PreparingCompaction(
                    key_state,
                    compaction_map,
                    end_offset,
                    next_start_offset,
                ) => {
                    let r = self.topic_storage_ops.new_work_writer();
                    if let Ok(mut writer) = r {
                        let mut task_key_state = key_state.clone();
                        let task_compaction_map = compaction_map.clone();
                        let task_end_offset = *end_offset;
                        let task_scoped_topic_subscriber = self.scoped_topic_subscriber.clone();
                        let h = tokio::spawn(async move {
                            let mut records = task_scoped_topic_subscriber.subscribe();
                            while let Some(record) = records.next().await {
                                if record.offset > task_end_offset {
                                    break;
                                }
                                let key = CS::key(&mut task_key_state, &record);
                                let copy = if let Some(key) = key {
                                    task_compaction_map
                                        .get(&key)
                                        .map(|min_offset| record.offset >= *min_offset)
                                        .unwrap_or(true)
                                } else {
                                    false
                                };
                                if copy {
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
                                        offset: record.offset,
                                    };

                                    let Ok(buf) =
                                        postcard::to_stdvec_crc32(&storable_record, CRC.digest())
                                    else {
                                        return Err(CompactionError::CannotSerialize);
                                    };
                                    writer.write_all(&buf).map_err(CompactionError::IoError)?;
                                }
                            }
                            writer.flush().map_err(CompactionError::IoError)
                        });
                        step_again = true;
                        Some(State::Compacting(h, *next_start_offset))
                    } else {
                        error!("Could not create the new temp file. Aborting compaction.");
                        Some(State::Idle)
                    }
                }
                State::Compacting(h, next_start_offset) => {
                    step_again = active_file_size >= self.compaction_threshold;
                    if step_again || h.is_finished() {
                        let r = h.await;
                        let s = if r.is_ok() {
                            let r = self.topic_storage_ops.replace_history_files();
                            if r.is_ok() {
                                if next_start_offset.is_some() {
                                    warn!("Subsequent logging pass required from offset {next_start_offset:?}");
                                    State::PreparingAnalyze(*next_start_offset)
                                } else {
                                    State::Idle
                                }
                            } else {
                                error!("Some error during compaction: {r:?}");
                                State::Idle
                            }
                        } else {
                            error!(
                                "Some error replacing the history file during compaction: {r:?}"
                            );
                            State::Idle
                        };
                        Some(s)
                    } else {
                        None
                    }
                }
            };
            if let Some(next_state) = next_state {
                debug!("Compaction moving to {next_state:?}");
                self.state = next_state;
            }
            if !step_again {
                break;
            }
            active_file_size = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        sync::atomic::{AtomicU32, Ordering},
    };

    use super::*;

    #[tokio::test]
    async fn test_key_based_retention() {
        let topic = Topic::from("my-topic");

        let r0 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 0,
        };

        let r1 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 1,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 1,
        };

        let r2 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 2,
        };

        let mut expected_compactor_result = HashMap::new();
        expected_compactor_result.insert(0, 2);

        let compaction = KeyBasedRetention::new(1);

        let mut key_state = compaction.key_init();
        let mut state = compaction.init().await;

        assert_eq!(
            KeyBasedRetention::reduce(
                &mut state,
                KeyBasedRetention::key(&mut key_state, &r0).unwrap(),
                r0
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            KeyBasedRetention::reduce(
                &mut state,
                KeyBasedRetention::key(&mut key_state, &r1).unwrap(),
                r1
            ),
            MaxKeysReached(true),
        );

        assert_eq!(
            KeyBasedRetention::reduce(
                &mut state,
                KeyBasedRetention::key(&mut key_state, &r2).unwrap(),
                r2
            ),
            MaxKeysReached(false)
        );

        assert_eq!(KeyBasedRetention::collect(state), expected_compactor_result);
    }

    #[tokio::test]
    async fn test_nth_key_based_retention() {
        let topic = Topic::from("my-topic");

        let r0 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 0,
        };

        let r1 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 1,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 1,
        };

        let r2 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 2,
        };

        let r3 = ConsumerRecord {
            topic: topic.clone(),
            headers: vec![],
            timestamp: None,
            key: 0,
            value: b"some-value-2".to_vec(),
            partition: 0,
            offset: 3,
        };

        let mut expected_compactor_result = HashMap::new();
        expected_compactor_result.insert(0, 2);

        let compaction = NthKeyBasedRetention::new(1, 2);

        let mut key_state = compaction.key_init();
        let mut state = compaction.init().await;

        assert_eq!(
            NthKeyBasedRetention::reduce(
                &mut state,
                NthKeyBasedRetention::key(&mut key_state, &r0).unwrap(),
                r0
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            NthKeyBasedRetention::reduce(
                &mut state,
                NthKeyBasedRetention::key(&mut key_state, &r1).unwrap(),
                r1
            ),
            MaxKeysReached(true),
        );

        assert_eq!(
            NthKeyBasedRetention::reduce(
                &mut state,
                NthKeyBasedRetention::key(&mut key_state, &r2).unwrap(),
                r2
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            NthKeyBasedRetention::reduce(
                &mut state,
                NthKeyBasedRetention::key(&mut key_state, &r3).unwrap(),
                r3
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            NthKeyBasedRetention::collect(state),
            expected_compactor_result
        );
    }

    // Test the ability to put compaction strategies together that retains
    // the last ten copies of a specific type of key for a topic, but the rest of the
    // keys should leverage key based retention.

    // We will have events where the battery level and name change events will use
    // key based retention, but we keep ten copies of the temperature sensed events via
    // nth key based retention.

    // We start off with our modelling of events.

    type TemperatureSensorId = u32;

    #[derive(Deserialize, Serialize)]
    enum TemperatureSensorEvent {
        BatteryLevelSensed(TemperatureSensorId, u32),
        NameChanged(TemperatureSensorId, String),
        TemperatureSensed(TemperatureSensorId, u32),
    }

    // Our event keys will occupy the top 12 bits of the key, meaning
    // that we can have 4K types of record. We use the bottom 32
    // bits as the sensor id.
    const EVENT_TYPE_BIT_SHIFT: usize = 52;

    // Convert from events into keys - this is a one-way process.
    impl From<TemperatureSensorEvent> for Key {
        fn from(val: TemperatureSensorEvent) -> Self {
            let event_key = match val {
                TemperatureSensorEvent::BatteryLevelSensed(id, _) => {
                    TemperatureSensorEventKey::BatteryLevelSensed(id)
                }
                TemperatureSensorEvent::NameChanged(id, _) => {
                    TemperatureSensorEventKey::NameChanged(id)
                }
                TemperatureSensorEvent::TemperatureSensed(id, _) => {
                    TemperatureSensorEventKey::TemperatureSensed(id)
                }
            };
            let (event_type, id) = match event_key {
                TemperatureSensorEventKey::BatteryLevelSensed(id) => (0u64, id),
                TemperatureSensorEventKey::NameChanged(id) => (1u64, id),
                TemperatureSensorEventKey::TemperatureSensed(id) => (2u64, id),
            };
            event_type << EVENT_TYPE_BIT_SHIFT | (id as u64)
        }
    }

    // Introduce a type that represents just the key components of our
    // event model object. This is so that we can conveniently coearce
    // keys into something readable in the code.

    enum TemperatureSensorEventKey {
        BatteryLevelSensed(TemperatureSensorId),
        NameChanged(TemperatureSensorId),
        TemperatureSensed(TemperatureSensorId),
    }

    struct TemperatureSensorEventKeyParseError;

    impl TryFrom<Key> for TemperatureSensorEventKey {
        type Error = TemperatureSensorEventKeyParseError;

        fn try_from(value: Key) -> Result<Self, Self::Error> {
            let id = (value & 0x0000_0000_FFFF_FFFF) as u32;
            match value >> EVENT_TYPE_BIT_SHIFT {
                0 => Ok(TemperatureSensorEventKey::BatteryLevelSensed(id)),
                1 => Ok(TemperatureSensorEventKey::NameChanged(id)),
                2 => Ok(TemperatureSensorEventKey::TemperatureSensed(id)),
                _ => Err(TemperatureSensorEventKeyParseError),
            }
        }
    }

    // We introduce a type here that captures behavior associated
    // with our specific topic, including the ability to be registered
    // for compaction.

    struct TemperatureSensorTopic;

    impl TemperatureSensorTopic {
        fn name() -> Topic {
            Topic::from("temp-sensor-events")
        }
    }

    // This is the state object that will be used during compaction.
    // We are using a hybrid of retention strategies, and of course,
    // you can have your own.

    #[derive(Debug)]
    struct TemperatureSensorCompactionState {
        temperature_events: NthKeyBasedRetentionState,
        remaining_events: KeyBasedRetentionState,
    }

    const MAX_TEMPERATURE_SENSOR_IDS_PER_COMPACTION: usize = 10;
    const MAX_TEMPERATURE_SENSOR_TEMPS_PER_ID: usize = 10;

    #[async_trait]
    impl CompactionStrategy for TemperatureSensorTopic {
        type S = TemperatureSensorCompactionState;
        type KS = ();

        fn key_init(&self) -> Self::KS {}

        fn key(_key_state: &mut Self::KS, r: &ConsumerRecord) -> Option<Key> {
            Some(r.key)
        }

        async fn init(&self) -> TemperatureSensorCompactionState {
            TemperatureSensorCompactionState {
                temperature_events: NthKeyBasedRetention::new(
                    MAX_TEMPERATURE_SENSOR_IDS_PER_COMPACTION,
                    MAX_TEMPERATURE_SENSOR_TEMPS_PER_ID,
                )
                .init()
                .await,
                // We only have two types of event that we wish to use
                // with key based retention: battery level and name changes.
                remaining_events: KeyBasedRetention::new(
                    2 * MAX_TEMPERATURE_SENSOR_IDS_PER_COMPACTION,
                )
                .init()
                .await,
            }
        }

        fn reduce(
            state: &mut TemperatureSensorCompactionState,
            key: Key,
            record: ConsumerRecord,
        ) -> MaxKeysReached {
            let Ok(event_type) = TemperatureSensorEventKey::try_from(key) else {
                return MaxKeysReached(false);
            };

            if matches!(event_type, TemperatureSensorEventKey::TemperatureSensed(_)) {
                NthKeyBasedRetention::reduce(&mut state.temperature_events, key, record)
            } else {
                KeyBasedRetention::reduce(&mut state.remaining_events, key, record)
            }
        }

        fn collect(state: TemperatureSensorCompactionState) -> CompactionMap {
            let mut compaction_map = NthKeyBasedRetention::collect(state.temperature_events);
            compaction_map.extend(KeyBasedRetention::collect(state.remaining_events));
            compaction_map
        }
    }

    // Now let's test all of that out!

    #[tokio::test]
    async fn test_both_retention_types() {
        let e0 = TemperatureSensorEvent::BatteryLevelSensed(0, 10);
        let v0 = postcard::to_stdvec(&e0).unwrap();
        let r0 = ConsumerRecord {
            topic: TemperatureSensorTopic::name(),
            headers: vec![],
            timestamp: None,
            key: e0.into(),
            value: v0,
            partition: 0,
            offset: 0,
        };

        let e1 = TemperatureSensorEvent::BatteryLevelSensed(0, 8);
        let v1 = postcard::to_stdvec(&e1).unwrap();
        let r1 = ConsumerRecord {
            topic: TemperatureSensorTopic::name(),
            headers: vec![],
            timestamp: None,
            key: e1.into(),
            value: v1,
            partition: 0,
            offset: 1,
        };

        let e2 = TemperatureSensorEvent::TemperatureSensed(0, 30);
        let v2 = postcard::to_stdvec(&e2).unwrap();
        let r2 = ConsumerRecord {
            topic: TemperatureSensorTopic::name(),
            headers: vec![],
            timestamp: None,
            key: e2.into(),
            value: v2,
            partition: 0,
            offset: 2,
        };

        let e3 = TemperatureSensorEvent::TemperatureSensed(0, 31);
        let v3 = postcard::to_stdvec(&e3).unwrap();
        let r3 = ConsumerRecord {
            topic: TemperatureSensorTopic::name(),
            headers: vec![],
            timestamp: None,
            key: e3.into(),
            value: v3,
            partition: 0,
            offset: 3,
        };

        let mut expected_compactor_result = HashMap::new();

        expected_compactor_result.insert(r1.key, 1);

        expected_compactor_result.insert(r2.key, 2);

        let compaction = TemperatureSensorTopic;

        let mut state = compaction.init().await;

        assert_eq!(
            TemperatureSensorTopic::reduce(
                &mut state,
                TemperatureSensorTopic::key(&mut (), &r0).unwrap(),
                r0
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            TemperatureSensorTopic::reduce(
                &mut state,
                TemperatureSensorTopic::key(&mut (), &r1).unwrap(),
                r1
            ),
            MaxKeysReached(false),
        );

        assert_eq!(
            TemperatureSensorTopic::reduce(
                &mut state,
                TemperatureSensorTopic::key(&mut (), &r2).unwrap(),
                r2
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            TemperatureSensorTopic::reduce(
                &mut state,
                TemperatureSensorTopic::key(&mut (), &r3).unwrap(),
                r3
            ),
            MaxKeysReached(false)
        );

        assert_eq!(
            TemperatureSensorTopic::collect(state),
            expected_compactor_result
        );
    }

    #[tokio::test]
    async fn test_key_state() {
        let compaction = KeyBasedRetention::with_qids(
            |r| std::str::from_utf8(&r.value).map(|s| s.into()).ok(),
            2,
        );

        let mut key_state = compaction.key_init();

        assert_eq!(
            KeyBasedRetention::key(
                &mut key_state,
                &ConsumerRecord {
                    topic: "some-topic".into(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: "some-key".as_bytes().to_vec(),
                    partition: 0,
                    offset: 10,
                },
            ),
            Some(0)
        );

        assert_eq!(
            KeyBasedRetention::key(
                &mut key_state,
                &ConsumerRecord {
                    topic: "some-topic".into(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: "some-key".as_bytes().to_vec(),
                    partition: 0,
                    offset: 10,
                },
            ),
            Some(0)
        );

        assert_eq!(
            KeyBasedRetention::key(
                &mut key_state,
                &ConsumerRecord {
                    topic: "some-topic".into(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: "some-other-key".as_bytes().to_vec(),
                    partition: 0,
                    offset: 10,
                },
            ),
            Some(1)
        );

        assert_eq!(
            key_state.unwrap().0,
            HashMap::from([("some-key".into(), 0), ("some-other-key".into(), 1)])
        );
    }

    #[derive(Clone)]
    struct TestCommitLog;

    #[async_trait]
    impl CommitLog for TestCommitLog {
        async fn offsets(&self, _topic: Topic, _partition: Partition) -> Option<PartitionOffsets> {
            todo!()
        }

        async fn produce(&self, _record: ProducerRecord) -> ProduceReply {
            todo!()
        }

        fn scoped_subscribe<'a>(
            &'a self,
            _consumer_group_name: &str,
            _offsets: Vec<ConsumerOffset>,
            _subscriptions: Vec<Subscription>,
            _idle_timeout: Option<Duration>,
        ) -> Pin<Box<dyn Stream<Item = ConsumerRecord> + Send + 'a>> {
            Box::pin(stream!({
                yield ConsumerRecord {
                    topic: Topic::from(""),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"".to_vec(),
                    partition: 0,
                    offset: 0,
                };
                yield ConsumerRecord {
                    topic: Topic::from(""),
                    headers: vec![],
                    timestamp: None,
                    key: 1,
                    value: b"".to_vec(),
                    partition: 0,
                    offset: 1,
                };
            }))
        }
    }

    struct TestCompactionStrategy;

    #[async_trait]
    impl CompactionStrategy for TestCompactionStrategy {
        type S = CompactionMap;
        type KS = ();

        fn key_init(&self) -> Self::KS {}

        fn key(_key_state: &mut Self::KS, r: &ConsumerRecord) -> Option<Key> {
            Some(r.key)
        }

        async fn init(&self) -> Self::S {
            CompactionMap::new()
        }

        fn reduce(state: &mut Self::S, key: Key, record: ConsumerRecord) -> MaxKeysReached {
            if state.is_empty() {
                state.insert(key, record.offset);
                MaxKeysReached(false)
            } else {
                MaxKeysReached(true)
            }
        }

        fn collect(state: Self::S) -> CompactionMap {
            state
        }
    }

    #[tokio::test]
    async fn test_compactor_end_to_end() {
        let topic = Topic::from("my-topic");

        let compaction_dir = env::temp_dir().join("test_compactor_end_to_end");
        let _ = fs::remove_dir_all(&compaction_dir);
        let _ = fs::create_dir_all(&compaction_dir);
        println!("Writing to {compaction_dir:?}");

        let cl = TestCommitLog;
        let cs = TestCompactionStrategy;
        let sts = ScopedTopicSubscriber::new(cl, topic);

        let num_ages = Arc::new(AtomicU32::new(0));
        let tso_num_ages = num_ages.clone();
        let num_new_work_writers = Arc::new(AtomicU32::new(0));
        let tso_num_new_work_writers = num_new_work_writers.clone();
        let num_recover_histories = Arc::new(AtomicU32::new(0));
        let tso_num_recover_histories = num_recover_histories.clone();
        let num_rename_histories = Arc::new(AtomicU32::new(0));
        let tso_num_rename_histories = num_rename_histories.clone();
        let work_file = compaction_dir.join("work_file");
        let tso_work_file = work_file.clone();

        let tso = TopicStorageOps::new(
            move || {
                tso_num_ages.clone().fetch_add(1, Ordering::Relaxed);
                Ok(Some(1))
            },
            move || {
                tso_num_new_work_writers
                    .clone()
                    .fetch_add(1, Ordering::Relaxed);
                File::create(tso_work_file.clone())
            },
            move || {
                tso_num_recover_histories
                    .clone()
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            move || {
                tso_num_rename_histories
                    .clone()
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        );

        let mut c = Compactor::new(cs, 1, sts, tso);

        let mut steps = 1u32;
        c.step(1).await;
        while steps < 10 && !c.is_idle() {
            c.step(1).await;
            steps = steps.wrapping_add(1);
        }

        assert!(c.is_idle());

        assert_eq!(num_ages.load(Ordering::Relaxed), 2);
        assert_eq!(num_new_work_writers.load(Ordering::Relaxed), 2);
        assert_eq!(num_recover_histories.load(Ordering::Relaxed), 1);
        assert_eq!(num_rename_histories.load(Ordering::Relaxed), 2);

        let mut f = File::open(work_file).unwrap();
        let mut buf = vec![];
        let _ = f.read_to_end(&mut buf).unwrap();

        // Two records should have been written back out.
        assert_eq!(
            buf,
            [0, 0, 0, 0, 0, 0, 138, 124, 42, 87, 0, 0, 0, 1, 0, 1, 247, 109, 0, 0]
        );
    }
}
