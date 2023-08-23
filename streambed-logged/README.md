# logged

Logged is a small library that implements a file-system-based 
commit log function for autonomous systems that often live
at the edge of a wider network.

Logged implements the Streambed commit log API which, in turn, models
itself on the Kafka API.

## An quick introduction

Nothing beats code for a quick introduction! Here is an example of
establishing the commit log, producing a record and then consuming
it. Please refer to the various tests for more complete examples.

```rs
  let cl = FileLog::new(logged_dir);

  let topic = Topic::from("my-topic"_;

  cl.produce(ProducerRecord {
      topic: topic.clone(),
      headers: None,
      timestamp: None,
      key: 0,
      value: b"some-value".to_vec(),
      partition: 0,
  })
  .await
  .unwrap();

  let subscriptions = vec![Subscription {
      topic: topic.clone(),
  }];
  let mut records = cl.scoped_subscribe("some-consumer", None, subscriptions, None);

  assert_eq!(
      records.next().await,
      Some(ConsumerRecord {
          topic,
          headers: None,
          timestamp: None,
          key: 0,
          value: b"some-value".to_vec(),
          partition: 0,
          offset: 0
      })
  );
```

## Why logged?

The primary functional use-cases of logged are:

* event sourcing - to re-constitute state from events; and
* observing - to communicate events reliably for one or more consumers.

The primary operational use-cases of logged are:

* to be hosted by resource-constrained devices, typically with less
than 128MiB memory and 8GB of storage.

## What is logged?

### No networking

Logged has no notion of what a network is. Any replication of the
data managed by logged is to be handled outside of it.

### Memory vs speed

Logged is optimized for small memory usage over speed and also
recognizes that storage is limited.

### Single writer/multiple reader

Logged deliberately constrains the appending of a commit log to
one process. This is known as the [single writer principle](https://mechanical-sympathy.blogspot.com/2011/09/single-writer-principle.html), and
greatly simplifies the design of logged.

### Multiple reader processes

Readers can exist in processes other than the one that writes to a
commit log topic. There is no broker process involved
as both readers and writers read the same files on the file system.
No locking is required for writing due to the single
writer principle.

### No encryption, compression or authentication

Encryption and compression are an application concern, with encryption
being encouraged. Compression may be less important, depending on the 
volume of data that an application needs to retain.

Authentication is bypassed given the assumption of encryption. If a reader is unable
to decrypt a message then this is seen to be as good as not gaining
access in the first place. Avoiding authentication yields an additional
side-effect of keeping logged free of complexity by not having to manage
identity.

### Data retention is an application concern

Logged provides the tools to the application so that an application 
may perform compaction. Our experience with applying commit logs
has shown that data retention strategies are an application concern
often subject to the contents of the data.

### The Kafka model

Logged is modelled with the same concepts as Apache Kafka including
topics, partitions, keys, records, offsets, timestamps and headers. 
Services using logged may therefore lend themselves to portability 
toward Kafka and others.

### Data integrity

A CRC32C checksum is used to ensure data integrity against the harsh reality
of a host being powered off abruptly. Any errors detected, including
incomplete writes or compactions, will be automatically recovered.

## How is logged implemented?

Logged is implemented as a library avoiding the need for a broker process. This
is similar to [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) on the JVM.

The file system is used to store records in relation to a topic and a single
partition. [Tokio](https://tokio.rs/) is used for file read/write operations so that any stalled
operations permit other tasks to continue running. [Postcard](https://docs.rs/postcard/latest/postcard/)
is used for serialization as it is able to conveniently represent in-memory structures
and is optimized for resource-constrained targets.

When seeking an offset into a topic, logged will read through records sequentially
as there is no indexing. This simple approach relies on the ability
for processors to scan memory fast (which they do), and also for the application-level
compaction to be effective. By "effective" we mean that scanning can avoid traversing
thousands of records.

The compaction of topics is an application concern and as it is able
to consider the contents of a record. Logged provides functions to atomically "split" 
an existing topic log at its head and yield a new topic to compact to. The 
application-based compactor can then consume the existing topic, retain the records 
it needs in memory. Having consumed the topic, the application-based compactor will 
append the retained records to the topic to compact to. Once appended, logged can be 
instructed to replace the compacted log for the old one. Meanwhile, any new appends post 
splitting the log will be written to another new log for the topic. As a result,
there can be two files representing the topic, and sometimes three during a compaction
activity. Logged takes care of interpreting the file system and various files used
when an application operates with a topic.

## When should you not use logged

When you have Kafka.