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

mod file;
use file::WalFile;
use std::{
    ffi::OsStr,
    io::{Error, ErrorKind},
};

#[cfg(test)]
macro_rules! trace {
    ($s:expr $(, $opt:expr)*) => {
        eprintln!(concat!("[{}:{}] ", $s), file!(), line!(), $($opt),*)
    };
}

#[cfg(not(test))]
macro_rules! trace {
    ($s:expr $(, $opt:expr)*) => {
        ()
    };
}

use async_std::{
    fs,
    path::{Path, PathBuf},
    prelude::*,
};

pub type Result<T> = std::io::Result<T>;

pub trait Entry {
    fn serialize(self) -> Vec<u8>;
    fn deserialize(data: Vec<u8>) -> Self;
}

impl Entry for Vec<u8> {
    fn serialize(self) -> Vec<u8> {
        self
    }

    fn deserialize(data: Vec<u8>) -> Self {
        data
    }
}

pub struct Wal {
    dir: PathBuf,
    files: Vec<(u64, PathBuf)>,
    read_file: Option<WalFile>,
    write_file: WalFile,
    chunk_size: u64,
    max_chunks: usize,
}

impl Wal {
    fn format_file_name(idx: u64) -> String {
        format!("{:020}", idx)
    }

    pub async fn close(mut self) -> Result<()> {
        self.write_file.close().await?;
        if let Some(f) = self.read_file.take() {
            f.close().await?;
        }
        Ok(())
    }
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
                return Err(Error::new(ErrorKind::Other, "Exceeded maximum size"));
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

    pub async fn pop<E>(&mut self) -> Result<Option<(u64, E)>>
    where
        E: Entry,
    {
        'outer: loop {
            if let Some(read) = self.read_file.as_mut() {
                trace!("Read file exists: {:?}", read);
                if let Some(r) = read.pop().await? {
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
        self.write_file.pop().await
    }

    pub async fn open<P>(path: P, chunk_size: u64, max_chunks: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let dir = path.to_path_buf();
        let m = fs::metadata(path).await?;

        if !m.is_dir() {
            return Err(Error::new(ErrorKind::Other, "not a directory"));
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
                    .ok_or(Error::new(ErrorKind::Other, "not a file"))?;

                files.push((first_idx, file))
            }
        }
        files.sort();
        if let Some(((first_idx, first_file), (last_idx, last_file))) =
            files.first().zip(files.last())
        {
            trace!("Opening files between: {} and {}", first_idx, last_idx);
            let write_file = WalFile::open(&last_file).await?;
            trace!("Opening WRITE file: {:?}", write_file);
            if first_idx == last_idx {
                Ok(Self {
                    dir,
                    files: files,
                    read_file: None,
                    write_file,
                    chunk_size,
                    max_chunks,
                })
            } else {
                let read_file = {
                    let mut read_file = WalFile::open(&first_file).await?;
                    read_file.ack_idx = write_file.ack_idx;
                    read_file.ack_written = write_file.ack_written;
                    read_file.seek_to(write_file.next_idx_to_read).await?;
                    trace!("Opening READ file: {:?}", read_file);
                    Some(read_file)
                };

                Ok(Self {
                    dir,
                    files: files,
                    read_file,
                    write_file,
                    chunk_size,
                    max_chunks,
                })
            }
        } else {
            let mut file = dir.clone();
            file.push(Self::format_file_name(0));
            let write_file = WalFile::open(&file).await?;
            files.push((0, file));
            Ok(Self {
                dir,
                files: files,
                read_file: None,
                write_file,
                chunk_size,
                max_chunks,
            })
        }
    }

    pub async fn ack(&mut self, id: u64) -> Result<()> {
        trace!("ACKing {}", id);
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
                    trace!("  Deleting Wal File@{} {:?}", id, f.to_str().unwrap());
                    fs::remove_file(f).await?;
                } else {
                    self.files.push((id, f))
                }
            }
        }

        Ok(())
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
            dbg!(0);
            let mut w = Wal::open(&path, 50, 10).await?;
            assert_eq!(w.push(b"1".to_vec()).await?, 0);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((0, b"1".to_vec())));
            dbg!();
            w.ack(0).await?;
            dbg!();
            assert_eq!(w.push(b"22".to_vec()).await?, 1);

            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"22".to_vec())));
            w.close().await?;
        }
        {
            dbg!(1);
            let mut w = Wal::open(&path, 50, 10).await?;
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"22".to_vec())));
            w.ack(1).await?;

            assert_eq!(w.push(b"333".to_vec()).await?, 2);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((2, b"333".to_vec())));
            w.ack(2).await?;

            w.close().await?;
        }
        dbg!(2);
        let mut w = Wal::open(&path, 50, 10).await?;
        assert_eq!(w.pop::<Vec<u8>>().await?, None);
        temp_dir.close()?;
        Ok(())
    }
}
