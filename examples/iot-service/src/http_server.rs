//! Handle http serving concerns
//!
use crate::database;
use std::collections::HashMap;
use streambed_patterns::ask::Ask;
use tokio::sync::mpsc;
use warp::{hyper::StatusCode, Filter, Rejection, Reply};

/// Declares routes to serve our HTTP interface.
pub fn routes(
    database_command_tx: mpsc::Sender<database::Command>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let get_database_route = {
        warp::get()
            .and(warp::path("events"))
            .and(warp::path::end())
            .and(warp::query())
            .then(move |query: HashMap<String, String>| {
                let task_database_command_tx = database_command_tx.clone();
                async move {
                    let Some(id) = query.get("id") else {
                        return warp::reply::with_status(
                            warp::reply::json(&"An id is required"),
                            StatusCode::BAD_REQUEST,
                        )
                    };

                    let Ok(id) = id.parse() else {
                        return warp::reply::with_status(
                            warp::reply::json(&"Invalid id - must be a number"),
                            StatusCode::BAD_REQUEST,
                        )
                    };

                    let Ok(events) = task_database_command_tx
                        .ask(|reply_to| database::Command::Get(id, reply_to))
                        .await else {
                            return warp::reply::with_status(
                                warp::reply::json(&"Service unavailable"),
                                StatusCode::SERVICE_UNAVAILABLE,
                            )
                         };

                    warp::reply::with_status(warp::reply::json(&events), StatusCode::OK)
                }
            })
    };

    warp::path("api").and(warp::path("database").and(get_database_route))
}
