use std::{collections::VecDeque, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use streambed::commit_log::{CommitLog, ProducerRecord, Subscription};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

const DEVICE_EVENTS_TOPIC: &str = "device-events";
const IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_EVENTS_TO_REPLY: usize = 10;

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
pub async fn task(cl: impl CommitLog, mut database_command_rx: mpsc::Receiver<Command>) {
    while let Some(c) = database_command_rx.recv().await {
        match c {
            Command::Get(id, reply_to) => {
                let offsets = vec![];

                let subscriptions = vec![Subscription {
                    topic: DEVICE_EVENTS_TOPIC.to_string(),
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
                        topic: DEVICE_EVENTS_TOPIC.to_string(),
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
