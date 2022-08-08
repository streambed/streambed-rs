#[path = "support/mod.rs"]
mod support;
use std::{convert::Infallible, time::Duration};

use serde::Deserialize;
use support::*;

use async_stream::stream;
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
                offsets: Some(vec!(ConsumerOffset {
                    topic: "default:end-device-events".to_string(),
                    partition: 0,
                    offset: 0,
                })),
                subscriptions: vec!(Subscription {
                    topic: "default:end-device-events".to_string()
                }),
            }
        );

        let stream = stream! {
            yield Result::<_, Infallible>::Ok(serde_json::to_vec(&ConsumerRecord {
                topic: "default:end-device-events".to_string(),
                headers: None,
                timestamp: None,
                key: 0,
                value: b"hi there".to_vec(),
                partition: 0,
                offset: 1,
            }).unwrap());

            yield Result::<_, Infallible>::Ok(serde_json::to_vec(&ConsumerRecord {
                topic: "default:end-device-events".to_string(),
                headers: None,
                timestamp: None,
                key: 0,
                value: b"hi there again".to_vec(),
                partition: 0,
                offset: 2,
            }).unwrap());

            tokio::time::sleep(Duration::from_millis(500)).await;
        };

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
        Some(&[ConsumerOffset {
            topic: "default:end-device-events".to_string(),
            partition: 0,
            offset: 0,
        }]),
        &[Subscription {
            topic: "default:end-device-events".to_string(),
        }],
        Some(Duration::from_millis(100)),
    );
    tokio::pin!(events);

    assert_eq!(
        events.next().await,
        Some(ConsumerRecord {
            topic: "default:end-device-events".to_string(),
            headers: None,
            timestamp: None,
            key: 0,
            value: b"hi there".to_vec(),
            partition: 0,
            offset: 1,
        })
    );
    assert_eq!(
        events.next().await,
        Some(ConsumerRecord {
            topic: "default:end-device-events".to_string(),
            headers: None,
            timestamp: None,
            key: 0,
            value: b"hi there again".to_vec(),
            partition: 0,
            offset: 2,
        })
    );
    assert_eq!(events.next().await, None);
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct ProduceRequest {
    pub records: Vec<ProducerRecord>,
}

#[tokio::test]
async fn kafka_publish() {
    let server = server::http(move |mut req| async move {
        assert_eq!(req.uri(), "/topics/default:end-device-events");

        let mut req_body: Vec<u8> = Vec::new();
        while let Some(item) = req.body_mut().next().await {
            req_body.extend(&*item.unwrap());
        }

        let req_body: ProduceRequest = serde_json::from_slice(&req_body).unwrap();
        assert_eq!(
            req_body,
            ProduceRequest {
                records: vec![ProducerRecord {
                    topic: "default:end-device-events".to_string(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"hi there".to_vec(),
                    partition: 0
                }]
            }
        );

        let body = r#"
        {
            "key_schema_id": null,
            "value_schema_id": null,
            "offsets": [
              {
                "partition": 2,
                "offset": 100
              },
              {
                "partition": 1,
                "offset": 101
              },
              {
                "partition": 2,
                "offset": 102
              }
            ]
        }          
        "#;

        http::Response::new(body.into())
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

    let record = ProducerRecord {
        topic: "default:end-device-events".to_string(),
        headers: vec![],
        timestamp: None,
        key: 0,
        value: b"hi there".to_vec(),
        partition: 0,
    };
    let result = cl.produce(&record).await.unwrap();
    assert_eq!(result.offset, 100);
}

#[tokio::test]
async fn kafka_offsets() {
    let server = server::http(move |req| async move {
        assert_eq!(
            req.uri(),
            "/topics/default:end-device-events/partitions/0/offsets"
        );

        let body = r#"
        {
            "beginning_offset": 0,
            "end_offset": 1
        }          
        "#;

        http::Response::new(body.into())
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

    let result = cl
        .offsets(&"default:end-device-events".to_string(), 0)
        .await
        .unwrap();
    assert_eq!(
        result,
        PartitionOffsets {
            beginning_offset: 0,
            end_offset: 1,
        }
    );
}
