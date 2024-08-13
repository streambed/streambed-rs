use std::{io::Write, time::Duration};

use streambed::commit_log::{CommitLog, ConsumerOffset, Subscription};
use tokio::time;
use tokio_stream::StreamExt;

use crate::errors::Errors;

const FLUSH_DELAY: Duration = Duration::from_millis(100);

pub async fn subscribe(
    cl: impl CommitLog,
    idle_timeout: Option<Duration>,
    offsets: Vec<ConsumerOffset>,
    mut output: impl Write,
    subscriptions: Vec<Subscription>,
) -> Result<(), Errors> {
    let mut records =
        cl.scoped_subscribe("streambed-logged-cli", offsets, subscriptions, idle_timeout);
    let mut timeout = FLUSH_DELAY;
    loop {
        tokio::select! {
            record = records.next() => {
                if let Some(record) = record {
                    let buf = serde_json::to_string(&record).map_err(|e| Errors::Io(e.into()))?;
                    output.write_all(buf.as_bytes())?;
                    output.write_all(b"\n")?;
                    timeout = FLUSH_DELAY;
                } else {
                    break;
                }
            }
            _ = time::sleep(timeout) => {
                output.flush()?;
                timeout = Duration::MAX;
            }
        }
    }
    Ok(())
}
