use std::{error::Error, fmt, io};

use streambed::commit_log::ProducerError;

#[derive(Debug)]
pub enum Errors {
    Io(io::Error),
    Producer(ProducerError),
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
            Self::Io(e) => e.fmt(f),
            Self::Producer(_) => f.write_str("CannotProduce"),
            Self::Serde(e) => e.fmt(f),
        }
    }
}

impl Error for Errors {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(e) => e.source(),
            Self::Producer(_) => None,
            Self::Serde(e) => e.source(),
        }
    }
}
