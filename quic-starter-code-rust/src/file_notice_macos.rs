use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::time::sleep;

#[derive(Debug, thiserror::Error)]
pub enum FileWaitError {
    #[error("file already exists")]
    AlreadyExists,
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct FileMarker {
    path: PathBuf,
}

impl FileMarker {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, [])?;
        Ok(Self { path })
    }
}

impl Drop for FileMarker {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub struct AsyncFileWaiter {
    path: PathBuf,
}

impl AsyncFileWaiter {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, FileWaitError> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            return Err(FileWaitError::AlreadyExists);
        }
        Ok(Self { path })
    }

    pub async fn wait_until_file_marker(&mut self) -> Result<(), FileWaitError> {
        loop {
            if self.path.exists() {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}
