use crate::ExampleCommand::{GetState, SetState};
use std::error::Error;
use streambed_patterns::ask::Ask;

enum ExampleCommand {
    GetState {
        reply_to: Box<dyn FnOnce(String) + Send>,
    },
    SetState {
        new_state: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ExampleCommand>(1);

    tokio::spawn(async move {
        let mut state = "initial state".to_string();

        while let Some(msg) = rx.recv().await {
            match msg {
                GetState { reply_to } => {
                    reply_to(state.clone());
                }
                SetState { new_state } => {
                    state = new_state;
                }
            }
        }
    });

    // Use the ask pattern to get the state
    let state = tx.ask(|reply_to| GetState { reply_to }).await.unwrap();
    println!("state: {state}");

    // Set the state to something different
    let _ = tx
        .send(SetState {
            new_state: "new state".to_string(),
        })
        .await;
    println!("state updated");

    // Use the ask pattern to get the state again
    let state = tx.ask(|reply_to| GetState { reply_to }).await.unwrap();
    println!("state: {state}");

    Ok(())
}
