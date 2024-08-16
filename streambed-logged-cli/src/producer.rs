use std::io::{self};

use streambed::commit_log::{CommitLog, ProducerRecord};

use crate::errors::Errors;

pub async fn produce(
    cl: impl CommitLog,
    mut line_reader: impl FnMut() -> Result<Option<String>, io::Error>,
) -> Result<(), Errors> {
    while let Some(line) = line_reader()? {
        let record = serde_json::from_str::<ProducerRecord>(&line)?;
        let record = ProducerRecord {
            topic: record.topic,
            headers: record.headers,
            timestamp: record.timestamp,
            key: record.key,
            value: record.value,
            partition: record.partition,
        };
        cl.produce(record).await.map_err(Errors::Producer)?;
    }
    Ok(())
}
