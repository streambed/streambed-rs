use std::{collections::VecDeque, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use streambed::commit_log::{CommitLog, ProducerRecord, Subscription, Topic};
use streambed_logged::{compaction::NthKeyBasedRetention, FileLog};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

const DEVICE_EVENTS_TOPIC: &str = "device-events";
const IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_EVENTS_TO_REPLY: usize = 10;
// Size the following to the typical number of devices we expect to have in the system.
// Note though that it will impact memory, so there is a trade-off. Let's suppose this
// was some LoRaWAN system and that our gateway cannot handle more than 1,000 devices
// being connected. We can work out that 1,000 is therefore a reasonable limit. We can
// have less or more. The overhead is small, but should be calculated and measured for
// a production app.
const MAX_TOPIC_COMPACTION_KEYS: usize = 1_000;

pub type GetReplyToFnOnce = dyn FnOnce(VecDeque<(DateTime<Utc>, Event)>) + Send;

pub enum Command {
    Get(u64, Box<GetReplyToFnOnce>),
    Post(u64, Event),
}

#[derive(Deserialize, Serialize)]
pub enum Event {
    TemperatureRead(u32),
}

/// Manage the database. We use CBOR as the serialization type for the data
/// we persist to the log. We do this as it has the benefits of JSON in terms
/// of schema evolution, but is faster to serialize and represents itself as
/// smaller on disk.
pub async fn task(mut cl: FileLog, mut database_command_rx: mpsc::Receiver<Command>) {
    // We register a compaction strategy for our topic such that when we use up
    // 64KB of disk space (the default), we will run compaction so that unwanted
    // events are removed. In our scenario, unwanted events can be removed when
    // the exceed MAX_EVENTS_TO_REPLY as we do not have a requirement to ever
    // return more than that.
    cl.register_compaction(
        Topic::from(DEVICE_EVENTS_TOPIC),
        NthKeyBasedRetention::new(MAX_TOPIC_COMPACTION_KEYS, MAX_EVENTS_TO_REPLY),
    )
    .await
    .unwrap();

    while let Some(c) = database_command_rx.recv().await {
        match c {
            Command::Get(id, reply_to) => {
                let offsets = vec![];

                let subscriptions = vec![Subscription {
                    topic: Topic::from(DEVICE_EVENTS_TOPIC),
                }];

                let mut records =
                    cl.scoped_subscribe("iot-service", offsets, subscriptions, Some(IDLE_TIMEOUT));

                let mut events = VecDeque::with_capacity(MAX_EVENTS_TO_REPLY);
                while let Some(record) = records.next().await {
                    if record.key == id {
                        let Some(timestamp) = record.timestamp else {
                            continue;
                        };
                        let Ok(database_event) = ciborium::de::from_reader::<Event, _>(&*record.value) else {
                            continue;
                        };
                        if events.len() == MAX_EVENTS_TO_REPLY {
                            events.pop_front();
                        }
                        events.push_back((timestamp, database_event));

                        // We perform an additional check to return if the
                        // event is recent so that we don't get stuck consuming
                        // when appending is happening simultaneously at a very
                        // high frequency.
                        if (timestamp - Utc::now())
                            .to_std()
                            .map(|r| r < IDLE_TIMEOUT)
                            .unwrap_or_default()
                        {
                            break;
                        }
                    }
                }

                reply_to(events);
            }
            Command::Post(id, event) => {
                let mut buf = Vec::new();
                if ciborium::ser::into_writer(&event, &mut buf).is_ok() {
                    let record = ProducerRecord {
                        topic: Topic::from(DEVICE_EVENTS_TOPIC),
                        headers: vec![],
                        timestamp: Some(Utc::now()),
                        key: id,
                        value: buf,
                        partition: 0,
                    };
                    let _ = cl.produce(record).await;
                }
            }
        }
    }
}
