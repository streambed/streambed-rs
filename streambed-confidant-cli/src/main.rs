use std::{
    error::Error,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use errors::Errors;
use streambed::secret_store::SecretStore;
use streambed_confidant::FileSecretStore;

pub mod cryptor;
pub mod errors;

/// The `confidant` command provides a utility for conveniently operating on file-based secret store. It is
/// often used in conjunction `logged` to encrypt payloads as a pre-stage, or decrypt as a post-stage. JSON objects
/// followed by a newline are expected to be streamed in and out. The value to be encrypted/decypted is defaulted to a
/// field named "value", which can be altered via the `select` option. This value is expected to be encoded as BASE64 and
/// will output as encoded BASE64. No assumption is made regarding the nature of a value passed in beyond it to be
/// encrypted/decrypted using streambed crypto functions.
#[derive(Parser, Debug)]
#[clap(author, about, long_about = None, version)]
struct ProgramArgs {
    /// In order to initialise the secret store, a root secret is also required. A credentials-directory path can be provided
    /// where a `root-secret`` file is expected. This argument corresponds conveniently with systemd's CREDENTIALS_DIRECTORY
    /// environment variable and is used by various services we have written.
    /// Also associated with this argument is the `secret_id` file` for role-based authentication with the secret store.
    /// This secret is expected to be found in a ss-secret-id file of the directory.
    #[clap(env, long, default_value = "/tmp")]
    pub credentials_directory: PathBuf,

    /// The max number of Vault Secret Store secrets to retain by our cache at any time.
    /// Least Recently Used (LRU) secrets will be evicted from our cache once this value
    /// is exceeded.
    #[clap(env, long, default_value_t = 10_000)]
    pub max_secrets: usize,

    /// The Secret Store role_id to use for approle authentication.
    #[clap(env, long, default_value = "streambed-confidant-cli")]
    pub role_id: String,

    /// The location of all secrets belonging to confidant. The recommendation is to
    /// create a user for confidant and a requirement is to remove group and world permissions.
    /// Then, use ACLs to express further access conditions.
    #[clap(env, long, default_value = "/var/lib/confidant")]
    pub root_path: PathBuf,

    /// A data field to used in place of Vault's lease_duration field. Time
    /// will be interpreted as a humantime string e.g. "1m", "1s" etc. Note
    /// that v2 of the Vault server does not appear to populate the lease_duration
    /// field for the KV secret store any longer. Instead, we can use a "ttl" field
    /// from the data.
    #[clap(env, long)]
    pub ttl_field: Option<String>,

    /// How long we wait until re-requesting the Secret Store for an
    /// unauthorized secret again.
    #[clap(env, long, default_value = "1m")]
    pub unauthorized_timeout: humantime::Duration,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Decrypt(DecryptCommand),
    Encrypt(EncryptCommand),
}

/// Consume a JSON object followed by a newline character, from a stream until EOF and decrypt it.
#[derive(Args, Debug)]
struct DecryptCommand {
    /// The file to consume JSON objects with newlines from, or `-` to indicate STDIN.
    #[clap(short, long)]
    pub file: PathBuf,

    /// The max number utf-8 characters to read from the file.
    #[clap(long, default_value_t = 8 * 1024)]
    pub max_line_len: u64,

    /// By default, records are output to STDOUT as JSON followed newlines.
    /// This option can be used to write to a file.
    #[clap(short, long)]
    pub output: Option<PathBuf>,

    /// The JSON field to select that holds the BASE64 value for decryption.
    /// The first 12 bytes of a decoded BASE64 value is expected to be a salt
    /// value.
    #[clap(long, default_value = "value")]
    pub select: String,

    /// The path to the secret e.g. "default/secrets.configurator-events.key"
    #[clap(long)]
    pub path: String,
}

/// Consume a JSON object followed by a newline character, from a stream until EOF and encrypt it.
#[derive(Args, Debug)]
struct EncryptCommand {
    /// The file to consume JSON objects with newlines from, or `-` to indicate STDIN.
    #[clap(short, long)]
    pub file: PathBuf,

    /// The max number utf-8 characters to read from the file.
    #[clap(long, default_value_t = 8 * 1024)]
    pub max_line_len: u64,

    /// By default, records are output to STDOUT as a JSON followed newlines.
    /// This option can be used to write to a file.
    #[clap(short, long)]
    pub output: Option<PathBuf>,

    /// The JSON field to select that holds the BASE64 value for encryption.
    /// The first 12 bytes of a decoded BASE64 value is expected to be a salt
    /// value.
    #[clap(long, default_value = "value")]
    pub select: String,

    /// The path to the secret e.g. "default/secrets.configurator-events.key".
    /// NOTE: as a convenience, in the case where the there is no secret at
    /// this path, then one will be attempted to be created.
    #[clap(long)]
    pub path: String,
}

async fn secret_store(
    credentials_directory: PathBuf,
    max_secrets: usize,
    root_path: PathBuf,
    role_id: String,
    ttl_field: Option<&str>,
    unauthorized_timeout: Duration,
) -> Result<impl SecretStore, Errors> {
    let ss = {
        let (root_secret, ss_secret_id) = {
            let f = File::open(credentials_directory.join("root-secret"))
                .map_err(Errors::RootSecretFileIo)?;
            let f = BufReader::new(f);
            let root_secret = f
                .lines()
                .next()
                .ok_or(Errors::EmptyRootSecretFile)?
                .map_err(Errors::RootSecretFileIo)?;

            let f = File::open(credentials_directory.join("ss-secret-id"))
                .map_err(Errors::SecretIdFileIo)?;
            let f = BufReader::new(f);
            let ss_secret_id = f
                .lines()
                .next()
                .ok_or(Errors::EmptyRootSecretFile)?
                .map_err(Errors::SecretIdFileIo)?;

            (root_secret, ss_secret_id)
        };

        let root_secret =
            hex::decode(root_secret).map_err(|_| Errors::CannotDecodeRootSecretAsHex)?;

        let ss = FileSecretStore::new(
            root_path,
            &root_secret
                .try_into()
                .map_err(|_| Errors::InvalidRootSecret)?,
            unauthorized_timeout,
            max_secrets,
            ttl_field,
        );

        ss.approle_auth(&role_id, &ss_secret_id)
            .await
            .map_err(Errors::SecretStore)?;

        ss
    };
    Ok(ss)
}

type LineReaderFn = Box<dyn FnMut() -> Result<Option<String>, io::Error> + Send>;
type OutputFn = Box<dyn Write + Send>;

fn pipeline(
    file: PathBuf,
    output: Option<PathBuf>,
    max_line_len: u64,
) -> Result<(LineReaderFn, OutputFn), Errors> {
    // A line reader function is provided rather than something based on the
    // Read trait as reading a line from stdin involves locking it via its
    // own read_line function. A Read trait will cause a lock to occur for
    // each access of the Read methods, and we cannot be guaranteed which of those will
    // be used - serde, for example, may only read a byte at a time. This would cause
    // many locks.
    let line_reader: LineReaderFn = if file.as_os_str() == "-" {
        let stdin = io::stdin();
        let mut line = String::new();
        Box::new(move || {
            line.clear();
            if stdin.lock().take(max_line_len).read_line(&mut line)? > 0 {
                Ok(Some(line.clone()))
            } else {
                Ok(None)
            }
        })
    } else {
        let mut buf =
            BufReader::new(fs::File::open(file).map_err(Errors::from)?).take(max_line_len);
        let mut line = String::new();
        Box::new(move || {
            line.clear();
            if buf.read_line(&mut line)? > 0 {
                Ok(Some(line.clone()))
            } else {
                Ok(None)
            }
        })
    };
    let output: OutputFn = if let Some(output) = output {
        Box::new(BufWriter::new(
            fs::File::create(output).map_err(Errors::from)?,
        ))
    } else {
        Box::new(io::stdout())
    };
    Ok((line_reader, output))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ProgramArgs::parse();

    env_logger::builder().format_timestamp_millis().init();

    let ss = secret_store(
        args.credentials_directory,
        args.max_secrets,
        args.root_path,
        args.role_id,
        args.ttl_field.as_deref(),
        args.unauthorized_timeout.into(),
    )
    .await?;

    let task = tokio::spawn(async move {
        match args.command {
            Command::Decrypt(command) => {
                let (line_reader, output) =
                    pipeline(command.file, command.output, command.max_line_len)?;
                cryptor::decrypt(ss, line_reader, output, &command.path, &command.select).await
            }
            Command::Encrypt(command) => {
                let (line_reader, output) =
                    pipeline(command.file, command.output, command.max_line_len)?;
                cryptor::encrypt(ss, line_reader, output, &command.path, &command.select).await
            }
        }
    });

    task.await
        .map_err(|e| e.into())
        .and_then(|r: Result<(), Errors>| r.map_err(|e| e.into()))
}
