use std::{
    error::Error,
    fmt, fs,
    io::{self, BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand};
use errors::Errors;
use git_version::git_version;
use streambed::commit_log::{ConsumerOffset, Subscription};
use streambed_logged::FileLog;

pub mod errors;
pub mod producer;
pub mod subscriber;

/// A utility for conveniently operating on file-based commit logs.
/// Functions such as the ability to consume a JSON file of records,
/// or produce them, are available.
/// No assumptions are made regarding the structure of a record's
/// value (payload), or whether it is encrypted or not. The expectation
/// is that a separate tool for that concern is used in a pipeline.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version = git_version ! ())]
struct ProgramArgs {
    /// The location of all topics in the Commit Log
    #[clap(env, long, default_value = "/var/lib/logged")]
    pub root_path: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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
    /// are separated by commas with no spaces e.g. "--offset=my-topic,0,1000".
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
    /// Topics may be namespaced by prefixing with characters followed by
    /// a `:`. For example, "my-ns:my-topic".
    #[clap(env, long, required = true)]
    pub subscription: Vec<String>,
}

#[derive(Clone, Debug)]
struct Offset {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

impl From<Offset> for ConsumerOffset {
    fn from(value: Offset) -> Self {
        ConsumerOffset {
            topic: value.topic.into(),
            partition: value.partition,
            offset: value.offset,
        }
    }
}

#[derive(Debug)]
enum OffsetParseError {
    MissingTopic,
    MissingPartition,
    InvalidPartition,
    MissingOffset,
    InvalidOffset,
}

impl fmt::Display for OffsetParseError {
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
        topic,
        partition,
        offset,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ProgramArgs::parse();

    env_logger::builder().format_timestamp_millis().init();

    let cl = FileLog::new(args.root_path);

    let task = tokio::spawn(async move {
        match args.command {
            Command::Produce(command) => {
                let input: Box<dyn Read + Send> = if command.file.as_os_str() == "-" {
                    Box::new(io::stdin())
                } else {
                    Box::new(BufReader::new(
                        fs::File::open(command.file).map_err(Errors::from)?,
                    ))
                };
                producer::produce(cl, input).await
            }
            Command::Subscribe(command) => {
                let output: Box<dyn Write + Send> = if let Some(output) = command.output {
                    Box::new(BufWriter::new(
                        fs::File::create(output).map_err(Errors::from)?,
                    ))
                } else {
                    Box::new(io::stdout())
                };
                subscriber::subscribe(
                    cl,
                    command.idle_timeout.map(|d| d.into()),
                    command.offset.into_iter().map(|o| o.into()).collect(),
                    output,
                    command
                        .subscription
                        .into_iter()
                        .map(|s| Subscription { topic: s.into() })
                        .collect(),
                )
                .await
            }
        }
    });

    task.await
        .map_err(|e| e.into())
        .and_then(|r: Result<(), Errors>| r.map_err(|e| e.into()))
}