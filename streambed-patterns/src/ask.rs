//! This is an implementation of the ask pattern that sends a request to a [tokio::sync::mpsc::Sender<_>]
//! and uses a [tokio::sync::oneshot] channel internally to convey the reply.

use async_trait::async_trait;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum AskError {
    SendError,
    ReceiveError,
}

impl Display for AskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AskError::SendError => write!(f, "SendError"),
            AskError::ReceiveError => write!(f, "ReceiveError"),
        }
    }
}

impl Error for AskError {}

#[async_trait]
pub trait Ask<A> {
    /// The ask pattern is a way to send a request and get a reply back.
    async fn ask<F, R>(&self, f: F) -> Result<R, AskError>
    where
        F: FnOnce(Box<dyn FnOnce(R) + Send>) -> A + Send,
        R: Send + 'static;
}

#[async_trait]
impl<A> Ask<A> for mpsc::Sender<A>
where
    A: Send,
{
    async fn ask<F, R>(&self, f: F) -> Result<R, AskError>
    where
        F: FnOnce(Box<dyn FnOnce(R) + Send>) -> A + Send,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let reply_to = Box::new(|r| {
            let _ = tx.send(r);
        });

        let request = f(reply_to);
        self.send(request).await.map_err(|_| AskError::SendError)?;

        rx.await.map_err(|_| AskError::ReceiveError)
    }
}
