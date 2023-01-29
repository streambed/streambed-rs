use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::database::{self, Command};

const MAX_DATAGRAM_SIZE: usize = 12;

#[derive(Debug, Deserialize, Serialize)]
struct TemperatureUpdated(u64, u32);

pub async fn task(socket: UdpSocket, database_command_tx: mpsc::Sender<Command>) {
    let mut recv_buf = [0; MAX_DATAGRAM_SIZE];
    while let Ok((len, _remote_addr)) = socket.recv_from(&mut recv_buf).await {
        if let Ok(event) =
            postcard::from_bytes::<TemperatureUpdated>(&recv_buf[..len.min(MAX_DATAGRAM_SIZE)])
        {
            debug!("Posting : {:?}", event);

            let _ = database_command_tx
                .send(Command::Post(
                    event.0,
                    database::Event::TemperatureRead(event.1),
                ))
                .await;
        }
    }
}
