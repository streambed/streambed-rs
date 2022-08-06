// Utility for delaying with exponential backoff. Once retries have
// been attained then we start again. There are a hard set of values
// here as it is private within Streambed.

use std::time::Duration;

use exponential_backoff::Backoff;
use tokio::time;

pub(crate) struct Delayer {
    backoff: Backoff,
    retry_attempt: u32,
}

impl Delayer {
    pub fn new() -> Self {
        let backoff = Backoff::new(8, Duration::from_millis(100), Duration::from_secs(10));
        Self {
            backoff,
            retry_attempt: 0,
        }
    }

    pub async fn delay(&mut self) {
        let delay = if let Some(d) = self.backoff.next(self.retry_attempt) {
            d
        } else {
            self.retry_attempt = 0;
            self.backoff.next(self.retry_attempt).unwrap()
        };
        time::sleep(delay).await;
        self.retry_attempt = self.retry_attempt.wrapping_add(1);
    }
}
