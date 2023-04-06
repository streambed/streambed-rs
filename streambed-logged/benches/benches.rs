use std::thread;
use std::time::Duration;
use std::{env, fs};

use criterion::{criterion_group, criterion_main, Criterion};
use streambed::commit_log::CommitLog;
use streambed::commit_log::ProducerRecord;
use streambed::commit_log::Subscription;
use streambed_logged::FileLog;
use tokio_stream::StreamExt;

const SAMPLE_SIZE: usize = 30_000;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("produce records", move |b| {
        let logged_dir = env::temp_dir().join("benches");
        let _ = fs::remove_dir_all(&logged_dir);
        let _ = fs::create_dir_all(&logged_dir);

        let cl = FileLog::new(logged_dir);

        let topic = "my-topic";

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            cl.produce(ProducerRecord {
                topic: topic.to_string(),
                headers: vec![],
                timestamp: None,
                key: 0,
                value: b"some-value".to_vec(),
                partition: 0,
            })
            .await
            .unwrap();
        });

        b.to_async(&rt).iter(|| {
            let task_cl = cl.clone();
            async move {
                tokio::spawn(async move {
                    for _ in 1..SAMPLE_SIZE {
                        task_cl
                            .produce(ProducerRecord {
                                topic: topic.to_string(),
                                headers: vec![],
                                timestamp: None,
                                key: 0,
                                value: b"some-value".to_vec(),
                                partition: 0,
                            })
                            .await
                            .unwrap();
                    }
                })
                .await
            }
        })
    });

    c.bench_function("consume records", move |b| {
        let logged_dir = env::temp_dir().join("benches");
        let _ = fs::remove_dir_all(&logged_dir);
        let _ = fs::create_dir_all(&logged_dir);

        let cl = FileLog::new(logged_dir);

        let topic = "my-topic";

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            for _ in 0..SAMPLE_SIZE {
                cl.produce(ProducerRecord {
                    topic: topic.to_string(),
                    headers: vec![],
                    timestamp: None,
                    key: 0,
                    value: b"some-value".to_vec(),
                    partition: 0,
                })
                .await
                .unwrap();
            }
        });

        // Provide time for the writes to be flushed
        thread::sleep(Duration::from_secs(1));

        b.to_async(&rt).iter(|| {
            let task_cl = cl.clone();
            async move {
                tokio::spawn(async move {
                    let offsets = vec![];
                    let subscriptions = vec![Subscription {
                        topic: topic.to_string(),
                    }];
                    let mut records =
                        task_cl.scoped_subscribe("some-consumer", offsets, subscriptions, None);

                    for _ in 0..SAMPLE_SIZE {
                        let _ = records.next().await;
                    }
                })
                .await
            }
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
