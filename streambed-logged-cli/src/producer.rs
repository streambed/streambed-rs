use std::io::Read;

use streambed::commit_log::{CommitLog, ProducerRecord};

use crate::errors::Errors;

pub async fn produce(cl: impl CommitLog, input: impl Read) -> Result<(), Errors> {
    let deserialiser = serde_json::from_reader::<_, ProducerRecord>(input);
    for record in deserialiser.into_iter() {
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
