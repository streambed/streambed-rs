use std::{
    error::Error,
    fmt::{self, Debug},
    io,
};

use streambed::secret_store;

#[derive(Debug)]
pub enum Errors {
    CannotDecodeRootSecretAsHex,
    CannotDecodeValue,
    CannotDecryptValue,
    CannotEncryptValue,
    CannotGetValue,
    CannotSelectValue,
    EmptyRootSecretFile,
    EmptySecretIdFile,
    InvalidRootSecret,
    InvalidSaltLen,
    Io(io::Error),
    RootSecretFileIo(io::Error),
    SecretIdFileIo(io::Error),
    SecretStore(secret_store::Error),
    Serde(serde_json::Error),
}

impl From<io::Error> for Errors {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for Errors {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CannotDecodeRootSecretAsHex => {
                f.write_str("Cannot decode the root-secret as hex")
            }
            Self::CannotDecodeValue => {
                f.write_str("Cannot decode the selected value of the JSON object")
            }
            Self::CannotDecryptValue => {
                f.write_str("Cannot decrypt the selected value of the JSON object")
            }
            Self::CannotEncryptValue => {
                f.write_str("Cannot encrypt the selected value of the JSON object")
            }
            Self::CannotGetValue => {
                f.write_str("Cannot get the selected value of the selected field")
            }
            Self::CannotSelectValue => {
                f.write_str("Cannot select the value within the JSON object")
            }
            Self::EmptyRootSecretFile => f.write_str("Empty root-secret file"),
            Self::EmptySecretIdFile => f.write_str("Empty secret-id file"),
            Self::InvalidRootSecret => {
                f.write_str("Invalid root secret - must be a hex string of 32 characters")
            }
            Self::InvalidSaltLen => f.write_str("Invalid salt length - must be 12 bytes"),
            Self::Io(e) => fmt::Display::fmt(&e, f),
            Self::SecretStore(_) => f.write_str("Unauthorised access"),
            Self::RootSecretFileIo(_) => f.write_str("root-secret file problem"),
            Self::SecretIdFileIo(_) => f.write_str("secret-id file problem"),
            Self::Serde(e) => fmt::Display::fmt(&e, f),
        }
    }
}

impl Error for Errors {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CannotDecodeValue
            | Self::CannotDecodeRootSecretAsHex
            | Self::CannotDecryptValue
            | Self::CannotEncryptValue
            | Self::CannotGetValue
            | Self::CannotSelectValue
            | Self::EmptyRootSecretFile
            | Self::EmptySecretIdFile
            | Self::InvalidRootSecret
            | Self::InvalidSaltLen
            | Self::SecretStore(_) => None,
            Self::Io(e) => e.source(),
            Self::RootSecretFileIo(e) => e.source(),
            Self::SecretIdFileIo(e) => e.source(),
            Self::Serde(e) => e.source(),
        }
    }
}
