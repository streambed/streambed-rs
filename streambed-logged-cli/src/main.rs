use std::{error::Error, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use git_version::git_version;

/// A utility for conveniently operating on file-based commit logs.
/// Functions such as the ability to consume a JSON file of records,
/// or produce them, are available.
/// No assumptions are made regarding the structure of a record's
/// value (payload), or whether it is encrypted or not. The expectation
/// is that a separate tool for that concern is used in a pipeline.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version = git_version ! ())]
struct ProgramArgs {
    /// A namespace to use when communicating with the Commit Log
    #[clap(env, long, default_value = "default")]
    pub ns: String,

    /// The location of all topics in the Commit Log
    #[clap(env, long, default_value = "/var/lib/logged")]
    pub root_path: PathBuf,

    #[command(subcommand)]
    pub commands: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Produce(ProduceCommand),
    Subscribe(SubscribeCommand),
}

/// Consume JSON records from a stream until EOF and append them to the log.
#[derive(Args, Debug)]
struct ProduceCommand {
    /// The file to consume records from, or `-` to indicate STDIN.
    #[clap(env, short, long)]
    pub file: PathBuf,
}

/// Subscribe to topics and consume from them producing JSON records to a stream.
#[derive(Args, Debug)]
struct SubscribeCommand {
    /// The amount of time to indicate that no more events are immediately
    /// available from the Commit Log endpoint. If unspecified then the
    /// CLI will wait indefinitely for records to appear.
    #[clap(env, long)]
    pub idle_timeout: Option<humantime::Duration>,

    /// In the case that an offset is supplied, it is
    /// associated with their respective topics such that any
    /// subsequent subscription will source from the offset.
    /// The fields are topic name, partition and offset which
    /// are separated by commas with no spaces e.g. "offset=mytopic,0,0".
    #[clap(env, long)]
    #[arg(value_parser = parse_offset)]
    pub offset: Vec<Offset>,

    /// By default, records of the topic are consumed and output to STDOUT.
    /// This option can be used to write to a file. Records are output as JSON.
    #[clap(env, short, long)]
    pub output: Option<PathBuf>,

    /// In the case where a subscription topic names are supplied, the consumer
    /// instance will subscribe and reply with a stream of records
    /// ending only when the connection to the topic is severed.
    #[clap(env, long)]
    pub subscription: Vec<String>,
}

#[derive(Clone, Debug)]
struct Offset {
    pub _topic: String,
    pub _partition: usize,
    pub _offset: u64,
}

#[derive(Debug)]
enum OffsetParseError {
    MissingTopic,
    MissingPartition,
    InvalidPartition,
    MissingOffset,
    InvalidOffset,
}

impl std::fmt::Display for OffsetParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetParseError::MissingTopic => {
                f.write_str("Missing the topic as the first part of the argument")
            }
            OffsetParseError::MissingPartition => {
                f.write_str("Missing the partition number as the second part to the argument")
            }
            OffsetParseError::InvalidPartition => {
                f.write_str("An invalid partition number was provided")
            }
            OffsetParseError::MissingOffset => {
                f.write_str("Missing the offset as the third part to the argument")
            }
            OffsetParseError::InvalidOffset => f.write_str("An invalid offset number was provided"),
        }
    }
}

impl Error for OffsetParseError {}

fn parse_offset(arg: &str) -> Result<Offset, OffsetParseError> {
    let mut iter = arg.split(',');
    let Some(topic) = iter.next().map(|s| s.to_string()) else {
        return Err(OffsetParseError::MissingTopic);
    };
    let Some(partition) = iter.next() else {
        return Err(OffsetParseError::MissingPartition);
    };
    let Ok(partition) = partition.parse() else {
        return Err(OffsetParseError::InvalidPartition);
    };
    let Some(offset) = iter.next() else {
        return Err(OffsetParseError::MissingOffset);
    };
    let Ok(offset) = offset.parse() else {
        return Err(OffsetParseError::InvalidOffset);
    };
    Ok(Offset {
        _topic: topic,
        _partition: partition,
        _offset: offset,
    })
}

fn main() {
    let args = ProgramArgs::parse();

    env_logger::builder().format_timestamp_millis().init();

    println!("args: {args:?}");
}
