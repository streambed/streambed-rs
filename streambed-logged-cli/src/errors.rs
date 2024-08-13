use std::{error::Error, fmt, io};

use streambed::commit_log::ProducerError;

#[derive(Debug)]
pub enum Errors {
    Io(io::Error),
    Producer(ProducerError),
}

impl From<io::Error> for Errors {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Producer(_) => f.write_str("CannotProduce"),
        }
    }
}

impl Error for Errors {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(e) => e.source(),
            Self::Producer(_) => None,
        }
    }
}
