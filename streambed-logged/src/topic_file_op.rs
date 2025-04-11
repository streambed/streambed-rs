//! Provides atomic operations on the files associated with a specific topic of a commit log.
//! We can have zero or more files representing the entire commit log for a topic.
//! There are history files plus an "active" file. The active file is the one that
//! can be appended to. History files are immutable.
//! Two types of history file are available - history and ancient history. A history
//! file signifies that compaction has taken or is taking place. Ancient history represents
//! that compaction is taking place and is removed once compaction is done. Both can exist.
//! The first elements of the returned vec, sans the last one, will represent file handles
//! to history files. The last element will always represent the present commit log.
//! Note that these operations return file handles so that their operations may
//! succeed post subsequent file renames, deletions and so forth.

use std::{
    error::Error,
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use streambed::commit_log::Topic;

pub(crate) const ANCIENT_HISTORY_FILE_EXTENSION: &str = "ancient_history";
pub(crate) const HISTORY_FILE_EXTENSION: &str = "history";
pub(crate) const WORK_FILE_EXTENSION: &str = "work";

#[derive(Debug)]
pub(crate) enum TopicFileOpError {
    CannotLock,
    CannotSerialize,
    #[allow(dead_code)]
    IoError(io::Error),
}
impl Display for TopicFileOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicFileOpError::CannotLock => write!(f, "CannotLock"),
            TopicFileOpError::CannotSerialize => write!(f, "CannotSerialize"),
            e @ TopicFileOpError::IoError(_) => e.fmt(f),
        }
    }
}
impl Error for TopicFileOpError {}

#[derive(Clone)]
pub(crate) struct TopicFileOp {
    root_path: PathBuf,
    topic: Topic,
    write_handle: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl TopicFileOp {
    pub fn new(root_path: PathBuf, topic: Topic) -> Self {
        Self {
            root_path,
            topic,
            write_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn active_file_size(
        &mut self,
        open_options: &OpenOptions,
        write_buffer_size: usize,
    ) -> Result<u64, TopicFileOpError> {
        // Ensure the active file is opened for appending first.
        let _ = self.with_active_file(open_options, write_buffer_size, |_| Ok(0))?;

        let Ok(mut locked_write_handle) = self.write_handle.lock() else {
            return Err(TopicFileOpError::CannotLock);
        };
        let r = if let Some(write_handle) = locked_write_handle.as_mut() {
            write_handle.get_ref().metadata().map(|m| m.len())
        } else {
            Ok(0)
        };
        r.map_err(TopicFileOpError::IoError)
    }

    pub fn age_active_file(&mut self) -> Result<(), TopicFileOpError> {
        let Ok(mut locked_write_handle) = self.write_handle.lock() else {
            return Err(TopicFileOpError::CannotLock);
        };
        let present_path = self.root_path.join(self.topic.as_str());
        let ancient_history_path = present_path.with_extension(ANCIENT_HISTORY_FILE_EXTENSION);
        let history_path = present_path.with_extension(HISTORY_FILE_EXTENSION);

        if let Some(write_handle) = locked_write_handle.as_mut() {
            write_handle.flush().map_err(TopicFileOpError::IoError)?;
        }

        if !ancient_history_path.exists() {
            let r = fs::rename(&history_path, ancient_history_path);
            if let Err(e) = r {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(TopicFileOpError::IoError(e));
                }
            }
            let r = fs::rename(present_path, history_path);
            *locked_write_handle = None;
            if let Err(e) = r {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(TopicFileOpError::IoError(e));
                }
            }
        }
        Ok(())
    }

    pub fn flush_active_file(&self) -> Result<(), TopicFileOpError> {
        let Ok(mut locked_write_handle) = self.write_handle.lock() else {
            return Err(TopicFileOpError::CannotLock);
        };
        let r = if let Some(write_handle) = locked_write_handle.as_mut() {
            write_handle.flush()
        } else {
            Ok(())
        };
        r.map(|_| ()).map_err(TopicFileOpError::IoError)
    }

    pub fn open_active_file(&self, open_options: OpenOptions) -> Result<File, TopicFileOpError> {
        let Ok(locked_write_handle) = self.write_handle.lock() else {
            return Err(TopicFileOpError::CannotLock);
        };
        let present_path = self.root_path.join(self.topic.as_str());
        let r = open_options.open(present_path);
        drop(locked_write_handle);
        r.map_err(TopicFileOpError::IoError)
    }

    pub fn modification_time(&self) -> Option<SystemTime> {
        let locked_write_handle = self.write_handle.lock().ok()?;

        let present_path = self.root_path.join(self.topic.as_str());
        let modification_time = fs::metadata(&present_path)
            .ok()
            .and_then(|m| m.modified().ok())
            .or_else(|| {
                let history_path = present_path.with_extension(HISTORY_FILE_EXTENSION);
                fs::metadata(&history_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
            })
            .or_else(|| {
                let ancient_history_path =
                    present_path.with_extension(ANCIENT_HISTORY_FILE_EXTENSION);
                fs::metadata(&ancient_history_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
            });

        drop(locked_write_handle);
        modification_time
    }

    pub fn open_files(
        &self,
        open_options: OpenOptions,
        exclude_active_file: bool,
    ) -> Vec<io::Result<File>> {
        let Ok(locked_write_handle) = self.write_handle.lock() else {
            return vec![];
        };

        let mut files = Vec::with_capacity(3);
        let present_path = self.root_path.join(self.topic.as_str());
        let ancient_history_path = present_path.with_extension(ANCIENT_HISTORY_FILE_EXTENSION);
        let history_path = present_path.with_extension(HISTORY_FILE_EXTENSION);
        match open_options.open(ancient_history_path) {
            Ok(f) => files.push(Ok(f)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => files.push(Err(e)),
        }
        match open_options.open(history_path) {
            Ok(f) => files.push(Ok(f)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => files.push(Err(e)),
        }
        if !exclude_active_file {
            match open_options.open(present_path) {
                Ok(f) => files.push(Ok(f)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                e @ Err(_) => files.push(e),
            }
        }
        drop(locked_write_handle);
        files
    }

    pub fn new_work_file(
        &self,
        write_buffer_size: usize,
    ) -> Result<BufWriter<File>, TopicFileOpError> {
        let present_path = self.root_path.join(self.topic.as_str());
        let work_path = present_path.with_extension(WORK_FILE_EXTENSION);
        let r = File::create(work_path);
        r.map(|write_handle| BufWriter::with_capacity(write_buffer_size, write_handle))
            .map_err(TopicFileOpError::IoError)
    }

    pub fn recover_history_files(&self) -> Result<(), TopicFileOpError> {
        let present_path = self.root_path.join(self.topic.as_str());
        let work_path = present_path.with_extension(WORK_FILE_EXTENSION);
        let ancient_history_path = present_path.with_extension(ANCIENT_HISTORY_FILE_EXTENSION);
        let history_path = present_path.with_extension(HISTORY_FILE_EXTENSION);

        let _ = fs::rename(ancient_history_path, history_path);
        let _ = fs::remove_file(work_path);
        Ok(())
    }

    pub fn replace_history_files(&self) -> Result<(), TopicFileOpError> {
        let present_path = self.root_path.join(self.topic.as_str());
        let work_path = present_path.with_extension(WORK_FILE_EXTENSION);
        let ancient_history_path = present_path.with_extension(ANCIENT_HISTORY_FILE_EXTENSION);
        let history_path = present_path.with_extension(HISTORY_FILE_EXTENSION);

        let r = fs::rename(work_path, history_path);
        if let Err(e) = r {
            return Err(TopicFileOpError::IoError(e));
        }
        let _ = fs::remove_file(ancient_history_path);
        Ok(())
    }

    pub fn with_active_file<F>(
        &mut self,
        open_options: &OpenOptions,
        write_buffer_size: usize,
        f: F,
    ) -> Result<(usize, bool), TopicFileOpError>
    where
        F: FnOnce(&mut BufWriter<File>) -> Result<usize, TopicFileOpError>,
    {
        let Ok(mut locked_write_handle) = self.write_handle.lock() else {
            return Err(TopicFileOpError::CannotLock);
        };
        if let Some(write_handle) = locked_write_handle.as_mut() {
            f(write_handle).map(|size| (size, false))
        } else {
            let present_path = self.root_path.join(self.topic.as_str());
            let r = open_options.clone().open(present_path);
            if let Ok(write_handle) = r {
                let mut write_handle = BufWriter::with_capacity(write_buffer_size, write_handle);
                let r = f(&mut write_handle);
                *locked_write_handle = Some(write_handle);
                r.map(|size| (size, true))
            } else {
                r.map(|_| (0, true)).map_err(TopicFileOpError::IoError)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use test_log::test;
    use tokio::fs;

    use super::*;

    #[test(tokio::test)]
    async fn test_open_topic_files() {
        let root_path = env::temp_dir().join("test_open_topic_files");
        let _ = fs::remove_dir_all(&root_path).await;
        let _ = fs::create_dir_all(&root_path).await;

        let current_path = root_path.join("topic");
        assert!(fs::File::create(&current_path).await.is_ok());

        let topic = Topic::from("topic");

        let file_ops = TopicFileOp::new(root_path, topic);

        let mut open_options = OpenOptions::new();
        open_options.read(true);

        assert_eq!(
            file_ops
                .open_files(open_options.clone(), false)
                .into_iter()
                .map(|f| f.is_ok())
                .collect::<Vec<bool>>(),
            [true]
        );

        let history_path = current_path.with_extension(HISTORY_FILE_EXTENSION);
        println!("{history_path:?}");
        assert!(fs::File::create(&history_path).await.is_ok());
        assert_eq!(
            file_ops
                .open_files(open_options.clone(), false)
                .into_iter()
                .map(|f| f.is_ok())
                .collect::<Vec<bool>>(),
            [true, true]
        );
        assert_eq!(
            file_ops
                .open_files(open_options.clone(), true)
                .into_iter()
                .map(|f| f.is_ok())
                .collect::<Vec<bool>>(),
            [true]
        );
    }
}
