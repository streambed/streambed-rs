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
    /// The ask pattern is a way to send a message and get a response back.
    async fn ask<R>(
        &self,
        f: impl FnOnce(Box<dyn FnOnce(R) + Send>) -> A + Send,
    ) -> Result<R, AskError>
    where
        R: Send + 'static;
}

#[async_trait]
impl<A> Ask<A> for mpsc::Sender<A>
where
    A: Send,
{
    /// This implementation of the ask pattern sends a message to a [tokio::sync::mpsc::Sender<_>]
    /// and uses a [tokio::sync::oneshot] channel internally to convey the response.
    async fn ask<R>(
        &self,
        f: impl FnOnce(Box<dyn FnOnce(R) + Send>) -> A + Send,
    ) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        let (reply_to, receiver) = oneshot::channel();
        let reply_to = Box::new(move |r| {
            let _ = reply_to.send(r);
        });

        self.send(f(reply_to))
            .await
            .map_err(|_| AskError::SendError)?;

        receiver.await.map_err(|_| AskError::ReceiveError)
    }
}
