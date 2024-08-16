use std::{
    collections::HashMap,
    io::{self, Write},
    time::Duration,
};

use rand::RngCore;
use serde_json::Value;
use streambed::{
    crypto::{self, SALT_SIZE},
    get_secret_value,
    secret_store::{SecretData, SecretStore},
};
use tokio::{sync::mpsc::channel, time};

use crate::errors::Errors;

const FLUSH_DELAY: Duration = Duration::from_millis(100);
const OUTPUT_QUEUE_SIZE: usize = 10;

pub async fn process_records(
    ss: impl SecretStore,
    mut line_reader: impl FnMut() -> Result<Option<String>, io::Error>,
    mut output: impl Write,
    path: &str,
    process: fn(Vec<u8>, Vec<u8>) -> Option<Vec<u8>>,
    select: &str,
) -> Result<(), Errors> {
    let (output_tx, mut output_rx) = channel(OUTPUT_QUEUE_SIZE);

    let processor = async move {
        while let Some(line) = line_reader()? {
            let mut record = serde_json::from_str::<Value>(&line).map_err(Errors::from)?;
            let Some(value) = record.get_mut(select) else {
                return Err(Errors::CannotSelectValue);
            };
            let Some(str_value) = value.as_str() else {
                return Err(Errors::CannotGetValue);
            };
            let Ok(bytes) = base64::decode(str_value) else {
                return Err(Errors::CannotDecodeValue);
            };
            let decrypted_bytes = get_secret_value(&ss, path)
                .await
                .and_then(|secret_value| {
                    let key = hex::decode(secret_value).ok()?;
                    process(key, bytes)
                })
                .ok_or(Errors::CannotDecryptValue)?;
            let encoded_decrypted_str = base64::encode(decrypted_bytes);
            *value = Value::String(encoded_decrypted_str);
            let _ = output_tx.send(record).await;
        }

        Ok(())
    };

    let outputter = async move {
        let mut timeout = FLUSH_DELAY;
        loop {
            tokio::select! {
                record = output_rx.recv() => {
                    if let Some(record) = record {
                        let buf = serde_json::to_string(&record)?;
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
    };

    tokio::try_join!(processor, outputter).map(|_| ())
}

pub async fn decrypt(
    ss: impl SecretStore,
    line_reader: impl FnMut() -> Result<Option<String>, io::Error>,
    output: impl Write,
    path: &str,
    select: &str,
) -> Result<(), Errors> {
    fn process(key: Vec<u8>, mut bytes: Vec<u8>) -> Option<Vec<u8>> {
        let (salt, data_bytes) = bytes.split_at_mut(crypto::SALT_SIZE);
        crypto::decrypt(data_bytes, &key.try_into().ok()?, &salt.try_into().ok()?);
        Some(data_bytes.to_vec())
    }
    process_records(ss, line_reader, output, path, process, select).await
}

pub async fn encrypt(
    ss: impl SecretStore,
    line_reader: impl FnMut() -> Result<Option<String>, io::Error>,
    output: impl Write,
    path: &str,
    select: &str,
) -> Result<(), Errors> {
    // As a convenience, we create the secret when encrypting if there
    // isn't one.
    if get_secret_value(&ss, path).await.is_none() {
        let mut key = vec![0; 16];
        rand::thread_rng().fill_bytes(&mut key);
        let data = HashMap::from([("value".to_string(), hex::encode(key))]);
        ss.create_secret(path, SecretData { data })
            .await
            .map_err(Errors::SecretStore)?;
    }

    fn process(key: Vec<u8>, mut data_bytes: Vec<u8>) -> Option<Vec<u8>> {
        let salt = crypto::salt(&mut rand::thread_rng());
        crypto::encrypt(&mut data_bytes, &key.try_into().ok()?, &salt);
        let mut buf = Vec::with_capacity(SALT_SIZE + data_bytes.len());
        buf.extend(salt);
        buf.extend(data_bytes);
        Some(buf)
    }
    process_records(ss, line_reader, output, path, process, select).await
}
