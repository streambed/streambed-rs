#[path = "support/mod.rs"]
mod support;
use std::{convert::Infallible, time::Duration};

use support::*;

use reqwest::Url;
use streambed::{commit_log::*, kafka::KafkaRestCommitLog};
use tokio_stream::StreamExt;

#[tokio::test]
async fn kafka_rest_scoped_subscribe() {
    let server = server::http(move |mut req| async move {
        assert_eq!(req.uri(), "/consumers/farmo-integrator");

        let mut req_body: Vec<u8> = Vec::new();
        while let Some(item) = req.body_mut().next().await {
            req_body.extend(&*item.unwrap());
        }

        let req_body: Consumer = serde_json::from_slice(&req_body).unwrap();
        assert_eq!(
            req_body,
            Consumer {
                offsets: Some(vec!(Offset {
                    topic: "default:end-device-events".to_string(),
                    partition: 0,
                    offset: 0,
                })),
                subscriptions: Some(vec!(Subscription {
                    topic: "default:end-device-events".to_string()
                })),
            }
        );

        let chunks = vec![
            Record {
                topic: "default:end-device-events".to_string(),
                key: 0,
                value: b"hi there".to_vec(),
                partition: 0,
                offset: 1,
            },
            Record {
                topic: "default:end-device-events".to_string(),
                key: 0,
                value: b"hi there again".to_vec(),
                partition: 0,
                offset: 2,
            },
        ];

        let stream = tokio_stream::iter(
            chunks
                .into_iter()
                .map(|e| Result::<_, Infallible>::Ok(serde_json::to_vec(&e).unwrap())),
        );

        let body = hyper::Body::wrap_stream(stream);

        http::Response::new(body)
    });

    let server_addr = server.addr();

    let cl = KafkaRestCommitLog::new(
        &Url::parse(&format!(
            "http://{}:{}",
            server_addr.ip(),
            server_addr.port()
        ))
        .unwrap(),
        None,
        false,
    );

    let events = cl.scoped_subscribe(
        "farmo-integrator",
        Some(&[Offset {
            topic: "default:end-device-events".to_string(),
            partition: 0,
            offset: 0,
        }]),
        Some(&[Subscription {
            topic: "default:end-device-events".to_string(),
        }]),
        Some(Duration::from_millis(100)),
    );
    tokio::pin!(events);

    assert_eq!(
        events.next().await,
        Some(Record {
            topic: "default:end-device-events".to_string(),
            key: 0,
            value: b"hi there".to_vec(),
            partition: 0,
            offset: 1,
        })
    );
    assert_eq!(
        events.next().await,
        Some(Record {
            topic: "default:end-device-events".to_string(),
            key: 0,
            value: b"hi there again".to_vec(),
            partition: 0,
            offset: 2,
        })
    );
    assert_eq!(events.next().await, None);
}
