// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod entry;
mod file;
use async_std::{
    fs,
    path::{Path, PathBuf},
    prelude::*,
};
pub use entry::Entry;
pub use file::WalFile;
use std::{ffi::OsStr, fmt::Display, io};

#[cfg(test)]
macro_rules! trace {
    ($s:expr $(, $opt:expr)*) => {
        eprintln!(concat!("[{}:{}] ", $s), file!(), line!(), $($opt),*)
    };
}

#[cfg(not(test))]
macro_rules! trace {
    ($s:expr $(, $opt:expr)*) => {
        concat!("[{}:{}] ", $s);
    };
}

#[derive(Debug)]
/// Error type
pub enum Error {
    /// System IO error
    Io(io::Error),
    /// The provided Path is not a directory
    NotADirectory,
    /// The provided Path is not a file
    NotAFile,
    /// An Ack Entry in the WAL is corrupted
    InvalidAck,
    /// An Data Entry in the WAL is corrupted
    InvalidEntry,
    /// A WAL file is corrupted
    InvalidFile,
    /// The WAL is exceeding it's limits and can not be written to
    SizeExceeded,
    /// Invalid ACK id is provided, it has to be between the last `ack` and the current `read`
    InvalidAckId,
    /// An invalid seek index has been given, it has to be after the last `ack` and before `write`
    InvalidIndex,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::NotADirectory => write!(f, "Not a directory"),
            Error::NotAFile => write!(f, "Not a file"),
            Error::InvalidAck => write!(f, "Invalid WAL entry (ACK)"),
            Error::InvalidEntry => write!(f, "Invalid WAL entry (Entry)"),
            Error::InvalidFile => write!(f, "Invalid WAL File"),
            Error::SizeExceeded => write!(f, "WAL Size Exceeded"),
            Error::InvalidAckId => write!(f, "Invalid Ack Index"),
            Error::InvalidIndex => write!(f, "Invalid Index"),
        }
    }
}
impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
/// Normalize errors in this crate to std::io::Result<T>
pub type Result<T> = std::result::Result<T, Error>;

/// A sequential Write-Ahead Log that iterates over multiple files and acts as a queue.
///
/// The size is limitable by two options:
///
/// * `chunk_size` - soft limits of bytes each chunk can take
/// * `max_chunks` - soft limit of chunks that are allowed to exist at one time
///
/// Those limits are soft limits, so they are the guaranteed sizes with the option to exceed them
/// once to keep this guarantee. In other words:
/// - if a chunk is `chunk_size - 1` bytes you will still be allowed to write one entry, after that
///   a new chunk is started.
/// - if `max_chunks` exist you can still create one more chunk before the WAL raises a out of space
///   error.
///
///  In result the maximum worst case size can be calculated as:
/// `(chunk_size + size of 1 entry)*(max_chunks + 1)
///
/// It features 4 main operations:
///
/// 1) `push` - adds a new entry to the back of the WAL
/// 2) `pop` - reads an entry from the front of the queue
/// 3) `ack` - acknowledges an entry and all entries before it to be completed and the space
///    reclaimable
/// 4) `revert` - fails back to the last acked message and re-produces the entries from there
///
/// Graphically it can be visualized as following:
///
/// ```text
///           .ack                                     .write
/// |--------------------------------------------------|
/// 'start                    'read                    
/// ```
///
/// - Entries between `start` and `ack` are acknowledged and will be recycled evnetually, they can
///   not be read any more.
/// - Entriesb etwen `ack` and `read` are read already but not yet acknowledged, `pop` will not read
///   them. However on a call to `revert`, the `read` pointer is moved to the `ack` pointer so those
///   entries would be repated
/// - Entries between `read` and `write` are written but not read, `pop` will progress through them
///   one entry at a time.
pub struct Wal {
    /// The path to an instance of a write-ahead-log
    dir: PathBuf,
    /// Dictionary of indexes and their associated index files
    files: Vec<(u64, PathBuf)>,
    read_file: Option<WalFile>,
    write_file: WalFile,
    chunk_size: u64,
    max_chunks: usize,
}

impl Wal {
    /// Open or creates a Write-Ahead Log given a path to a directory containing the Write-Ahead Log
    ///
    /// - `path` is the directory the WAL is residing it
    /// - `chunk_size` is the soft limit of bytes per chunks. They will be cycled once a entry is
    ///   appended and the chunk now exeeds the lmit.
    /// - `max_chunks` is the soft limit of maximum numbers of chunks to be usable. As chunks will
    ///   only be reclaimed once all elements in them are recycled `max_chunks + 1`  chunks might
    ///   exist at one time to guarantee at least `max_chunks` full chunks can be written.
    ///
    /// ## Errors
    /// Errors if `path` isn't an existing directory, it has content that isn't a valid Wal.
    pub async fn open<P>(path: P, chunk_size: u64, max_chunks: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let dir = path.to_path_buf();
        let m = fs::metadata(path).await?;

        if !m.is_dir() {
            return Err(Error::NotADirectory);
        }
        let mut files = Vec::new();
        let mut rd = fs::read_dir(path).await?;
        while let Some(file) = rd.next().await {
            let file = file?.path();
            if fs::metadata(&file).await?.is_file() {
                let first_idx: u64 = file
                    .file_name()
                    .and_then(OsStr::to_str)
                    .and_then(|s| s.parse().ok())
                    .ok_or(Error::NotAFile)?;

                files.push((first_idx, file))
            }
        }
        files.sort();

        if let Some((_, last_file)) = files.last() {
            let write_file = WalFile::open(&last_file).await?;
            trace!("Opening WRITE file: {:?}", write_file);
            let next_idx_to_read = write_file.next_idx_to_read;
            let mut wal = Self {
                dir,
                files,
                read_file: None,
                write_file,
                chunk_size,
                max_chunks,
            };
            wal.seek_to(next_idx_to_read).await?;
            Ok(wal)
        } else {
            let mut file = dir.clone();
            file.push(Self::format_file_name(0));
            let write_file = WalFile::open(&file).await?;
            files.push((0, file));
            Ok(Self {
                dir,
                files,
                read_file: None,
                write_file,
                chunk_size,
                max_chunks,
            })
        }
    }

    /// Push a new entry into the write-ahead-log
    ///
    /// ## Errors
    /// On IO Errors or if the entry is exceed the WAL's capacity
    pub async fn push<E>(&mut self, data: E) -> Result<u64>
    where
        E: Entry,
    {
        let idx = self.write_file.push(data).await?;
        if self.write_file.size() > self.chunk_size {
            trace!(
                "Current file exceeds max size with {} > {}",
                self.write_file.size(),
                self.chunk_size
            );
            if self.files.len() > self.max_chunks {
                return Err(Error::SizeExceeded);
            }
            let mut path = self.dir.clone();
            path.push(Self::format_file_name(self.write_file.next_idx_to_write));
            self.files
                .push((self.write_file.next_idx_to_write, path.clone()));
            let mut next_wal = WalFile::open(path).await?;
            next_wal.next_idx_to_read = self.write_file.next_idx_to_read;
            next_wal.next_idx_to_write = self.write_file.next_idx_to_write;
            next_wal.ack_idx = self.write_file.ack_idx;
            next_wal.ack_written = self.write_file.ack_written;
            std::mem::swap(&mut next_wal, &mut self.write_file);

            self.write_file.preserve_ack().await?;
            if self.read_file.is_none() {
                self.read_file = Some(next_wal)
            }
        }
        Ok(idx)
    }

    /// Pop an existing entry from the write-ahead-log, returs `None` if no new entry exists
    ///
    /// ## Errors
    /// Erros on IO Errors or invalid WAL files
    pub async fn pop<E>(&mut self) -> Result<Option<(u64, E::Output)>>
    where
        E: Entry,
    {
        'outer: loop {
            if let Some(read) = self.read_file.as_mut() {
                trace!("Read file exists: {:?}", read);
                if let Some(r) = read.pop::<E>().await? {
                    trace!("  We found an entry: {}", r.0);
                    return Ok(Some(r));
                }
                trace!("  We are exhausted.");
                if let Some((_, files)) = self.files.split_last() {
                    for (idx, path) in files {
                        trace!(
                            "  testing next file with {} >= {}",
                            *idx,
                            read.next_idx_to_read
                        );
                        if *idx >= read.next_idx_to_read {
                            *read = WalFile::open(path).await?;
                            continue 'outer;
                        }
                    }
                }
            }
            break;
        }
        self.read_file = None;
        self.write_file.pop::<E>().await
    }

    /// Acknowledges an entry as completely processed allowing it to be reclaimed.
    ///
    /// Note acknowledgements aren't persistet imidiately. The guarantee we provide is an
    /// `at least once` delivery. Acks are persistet as part of the next push or when the WAL
    /// is closed.
    ///
    /// Reclimation does not happen imideately either, we never delete a singular entry we always
    /// reclaim an entire chunk once every entry in it is acknowledged
    ///
    /// ## Errors
    ///  - if the id to ack is larger then the read id - we can not acknowlege something that has
    ///    not been read.
    /// - if the id to ack is smaller then the currently acknowledged id - we can not undo acks
    /// - on IO Errors if reclemation of files fails
    pub async fn ack(&mut self, id: u64) -> Result<()> {
        trace!("ACKing {}", id);

        if self.read_idx() <= id || self.write_file.ack_idx > id {
            return Err(Error::InvalidAckId);
        }

        self.write_file.ack(id);

        let mut files = self.files.iter();
        let mut to_delete = None;
        let mut cnt = 0;
        if let Some(mut this) = files.next() {
            trace!("  First file to check: {}", this.0);
            loop {
                let last = this;
                if let Some(next) = files.next() {
                    this = next;
                    if this.0 > id {
                        break;
                    };
                } else {
                    break;
                }
                cnt += 1;
                to_delete = Some(last.0);
            }
        }
        if let Some(to_delete) = to_delete {
            trace!("  Deleting Wal File up to id: {}", to_delete);
            let mut files = Vec::with_capacity(self.files.len() - cnt);
            std::mem::swap(&mut self.files, &mut files);
            let files = files.into_iter();
            for (id, f) in files {
                if id <= to_delete {
                    trace!("  Deleting Wal File@{} {:?}", id, f.to_string_lossy());
                    fs::remove_file(f).await?;
                } else {
                    self.files.push((id, f))
                }
            }
        }

        Ok(())
    }

    /// Reverts the read index back to the next item after the last acknowledged index.
    ///
    /// ## Errors
    /// on IO Errors or invalid WAL files
    pub async fn revert(&mut self) -> Result<()> {
        trace!("Reverting to {}", self.write_file.ack_idx + 1);
        self.seek_to(self.write_file.ack_idx + 1).await
    }

    /// Cleanly closes a WAL file and persists it's ack index if needed.
    ///
    /// For for an operating WAL this call isn't needed, but if not used the chance of duplicate
    /// messages increases as items acknowledged between the last `push` and dropping the `Wal`
    /// will be read again.
    pub async fn close(mut self) -> Result<()> {
        self.write_file.close().await?;
        if let Some(f) = self.read_file.take() {
            f.close().await?;
        }
        Ok(())
    }

    /// Name formating for WAL files
    fn format_file_name(idx: u64) -> String {
        format!("{:020}", idx)
    }

    /// Seeks to a given index in the Wal files
    async fn seek_to(&mut self, idx: u64) -> Result<()> {
        trace!("Seeking to: {} in {:?}", idx, self.files);

        // Check if we can seek in the write file
        if let Some((write_idx, _)) = self.files.last() {
            if idx >= *write_idx {
                trace!("Seeking in write file: {:?}", self.write_file);
                // we clear the read file and seek to the desired index on the write file
                self.read_file = None;
                return self.write_file.seek_to(idx).await;
            }
        };

        let mut i = self.files.iter().rev().skip_while(|(i, _)| {
            trace!("  testing if {} > {} == {}", *i, idx, *i > idx);
            *i > idx
        });

        if let Some((_, f)) = i.next() {
            trace!("Seek picked: {} {:?}", idx, f);
            // We open a new read file and seek to it
            let mut read_file = WalFile::open(f).await?;
            trace!("Seeking in write read file: {:?}", read_file);
            read_file.seek_to(idx).await?;
            self.read_file = Some(read_file);
            Ok(())
        } else {
            Err(Error::InvalidIndex)
        }
    }

    /// Current read index
    fn read_idx(&self) -> u64 {
        if let Some(read_file) = &self.read_file {
            read_file.next_idx_to_read
        } else {
            self.write_file.next_idx_to_read
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use tempfile::Builder as TempDirBuilder;

    #[async_std::test]
    async fn wal() -> Result<()> {
        let temp_dir = TempDirBuilder::new().prefix("tremor-wal").tempdir()?;

        let path = temp_dir.path().to_path_buf();

        {
            let mut w = Wal::open(&path, 50, 10).await?;
            assert_eq!(w.push(b"1".to_vec()).await?, 0);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((0, b"1".to_vec())));
            w.ack(0).await?;
            assert_eq!(w.push(b"22".to_vec()).await?, 1);

            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"22".to_vec())));
            w.close().await?;
        }
        {
            let mut w = Wal::open(&path, 50, 10).await?;
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"22".to_vec())));

            w.revert().await?;

            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"22".to_vec())));

            w.ack(1).await?;

            assert_eq!(w.push(b"333".to_vec()).await?, 2);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((2, b"333".to_vec())));
            w.ack(2).await?;

            w.close().await?;
        }
        let mut w = Wal::open(&path, 50, 10).await?;
        assert_eq!(w.pop::<Vec<u8>>().await?, None);
        temp_dir.close()?;
        Ok(())
    }
}
