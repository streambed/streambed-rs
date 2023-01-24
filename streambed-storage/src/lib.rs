#![doc = include_str!("../README.md")]

pub mod args;

use rand::RngCore;
use serde::{de::DeserializeOwned, Serialize};
use std::{error::Error, path::Path};
use streambed::{decrypt_buf, encrypt_struct, secret_store};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

/// Loads and deserializes a structure described by T, if the file is present.
/// If there are IO issues outside of the file not being there, they will be returned
/// as an error. Beyond IO, state is attempted to be decrypted and deserialized when present.
/// Any issues there cause the default representation of the structure to be returned. The default
/// structure is also returned where there is no file present in the first place.
pub async fn load_struct<T>(
    state_storage_path: &Path,
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
) -> Result<T, Box<dyn Error>>
where
    T: Default + DeserializeOwned,
{
    if let Ok(mut f) = fs::File::open(state_storage_path).await {
        let mut buf = vec![];
        f.read_to_end(&mut buf).await?;
        Ok(decrypt_buf(ss, secret_path, &mut buf)
            .await
            .unwrap_or_default())
    } else {
        Ok(T::default())
    }
}

/// Saves an encrypted structure described by T. Any IO errors are returned.
pub async fn save_struct<T, U>(
    state_storage_path: &Path,
    ss: &impl secret_store::SecretStore,
    secret_path: &str,
    rng: &mut U,
    state: &T,
) -> Result<(), Box<dyn Error>>
where
    T: Serialize,
    U: RngCore,
{
    if let Some(buf) = encrypt_struct(ss, secret_path, rng, state).await {
        let mut f = fs::File::create(state_storage_path).await?;
        f.write_all(&buf).await?;
    }
    Ok(())
}
