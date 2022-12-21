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

use crate::match_error;

use super::{Entry, Error, Result};

use std::{io::SeekFrom, mem::size_of};

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
#[cfg(feature = "async-std")]
use async_std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    path::Path,
};
use byteorder::{BigEndian, ByteOrder};
#[cfg(feature = "tokio")]
use std::path::Path;
#[cfg(feature = "tokio")]
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

/// Represents entries in a write-ahead-log index data file
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum WalData {
    // Entry is a data record
    Data {
        idx: u64,
        ack_idx: u64,
        data: Vec<u8>,
    },
    /// Entry is an acknowledgement record
    Ack { idx: u64, ack_idx: u64 },
}

//
//  format:

impl WalData {
    const OFFSET_LEN: usize = 0;
    const OFFSET_IDX: usize = Self::OFFSET_LEN + size_of::<u64>();
    const OFFSET_ACK: usize = Self::OFFSET_IDX + size_of::<u64>();
    const OFFSET_DATA: usize = Self::OFFSET_ACK + size_of::<u64>();
    const OFFSET_TAILING_LEN: usize = Self::OFFSET_ACK + size_of::<u64>();

    async fn read(f: &mut File) -> Result<Option<Self>> {
        let mut buf = vec![0u8; size_of::<u64>() * 3];

        // read size
        if f.read_exact(&mut buf).await.is_err() {
            return Ok(None);
        }
        let len = BigEndian::read_u64(&buf[Self::OFFSET_LEN..]);

        // read id
        let idx = BigEndian::read_u64(&buf[Self::OFFSET_IDX..]);
        // read ack_id
        let ack_idx = BigEndian::read_u64(&buf[Self::OFFSET_ACK..]);
        if len == u64::MAX {
            let mut buf = vec![0u8; size_of::<u64>()];
            // THIS is a ack token
            f.read_exact(&mut buf).await?;
            let len2 = BigEndian::read_u64(&buf);
            if len2 != 0 {
                Err(Error::InvalidAck)
            } else {
                Ok(Some(Self::Ack { ack_idx, idx }))
            }
        } else {
            // read data
            let mut data = vec![0u8; len as usize];
            // We set the len since we know the len and then
            unsafe { data.set_len(len as usize) };
            f.read_exact(&mut data).await?;

            let mut buf = vec![0u8; size_of::<u64>()];
            f.read_exact(&mut buf).await?;
            let len2 = BigEndian::read_u64(&buf);
            if len2 != len {
                Err(Error::InvalidEntry)
            } else {
                Ok(Some(Self::Data { idx, ack_idx, data }))
            }
        }
    }

    fn size_on_disk_from_len(len: u64) -> u64 {
        let len64 = size_of::<u64>() as u64;
        if len == u64::MAX {
            len64 * 4
        } else {
            len + (len64 * 4)
        }
    }

    fn size_on_disk(&self) -> u64 {
        match self {
            WalData::Data { data, .. } => WalData::size_on_disk_from_len(data.len() as u64),
            WalData::Ack { .. } => WalData::size_on_disk_from_len(0),
        }
    }

    #[allow(clippy::uninit_vec)]
    async fn write(&self, w: &mut File) -> Result<u64> {
        let size_on_disk = self.size_on_disk() as usize;
        let mut buf: Vec<u8> = vec![0; size_on_disk];
        match self {
            WalData::Data { idx, ack_idx, data } => {
                // id + ack_id + len + len (tailing len)
                let len = data.len();
                BigEndian::write_u64(&mut buf[Self::OFFSET_LEN..], len as u64);
                BigEndian::write_u64(&mut buf[Self::OFFSET_IDX..], *idx);
                BigEndian::write_u64(&mut buf[Self::OFFSET_ACK..], *ack_idx);
                buf[Self::OFFSET_DATA..(Self::OFFSET_DATA + len)].clone_from_slice(data);
                BigEndian::write_u64(&mut buf[(Self::OFFSET_DATA + len)..], len as u64); // len2
                w.write_all(&buf).await?;
            }
            WalData::Ack { ack_idx, idx } => {
                BigEndian::write_u64(&mut buf[Self::OFFSET_LEN..], u64::MAX);
                BigEndian::write_u64(&mut buf[Self::OFFSET_IDX..], *idx);
                BigEndian::write_u64(&mut buf[Self::OFFSET_ACK..], *ack_idx);
                BigEndian::write_u64(&mut buf[Self::OFFSET_TAILING_LEN..], 0);
                w.write_all(&buf).await?;
            }
        }
        Ok(self.size_on_disk())
    }

    /// Fetches the index of the most recent acknowledgement
    pub fn ack_idx(&self) -> u64 {
        match self {
            WalData::Data { ack_idx, .. } | WalData::Ack { ack_idx, .. } => *ack_idx,
        }
    }

    /// Fetches the curent index
    pub fn idx(&self) -> u64 {
        match self {
            WalData::Data { idx, .. } | WalData::Ack { idx, .. } => *idx,
        }
    }
}

/// Represents an data file in a write-ahead-log-structure
#[derive(Debug)]
pub struct WalFile {
    /// Reference to the data file
    pub(crate) file: File,
    /// The next index to be written
    pub(crate) next_idx_to_write: u64,
    /// The write offset
    pub(crate) write_offset: u64,
    /// The next index to be read
    pub(crate) next_idx_to_read: u64,
    /// The read offset
    pub(crate) read_pointer: u64,
    /// The index for acknowledged entries
    pub(crate) ack_idx: u64,
    /// The most recent acknowledgement offset
    pub(crate) ack_written: u64,
}

impl WalFile {
    async fn sync(&self) -> Result<()> {
        self.file.sync_all().await.map_err(Error::Io)
    }

    /// Persists an acknowledgement at the end of the data file at the after the last committed write operation
    pub(crate) async fn preserve_ack(&mut self) -> Result<()> {
        trace!("Appending ack index {} to {:?}", self.ack_idx, self.file);

        let ack_idx = self.ack_idx;
        self.ack_written = ack_idx;

        let data = WalData::Ack {
            // we remove this since we usually ack with the previos index and this is no real data
            idx: self.next_idx_to_write - 1,
            ack_idx,
        };
        self.file.seek(SeekFrom::Start(self.write_offset)).await?;
        self.write_offset += data.write(&mut self.file).await?;
        self.sync().await
    }

    /// Closes this write-ahead-log data file
    pub async fn close(mut self) -> Result<()> {
        trace!("Closing WAL file {:?}", self);
        if self.ack_written != self.ack_idx {
            self.preserve_ack().await?;
        }
        Ok(())
    }

    /// Push an entry into the write-ahead-log data file
    pub async fn push<E>(&mut self, data: E) -> std::result::Result<u64, Error<E::Error>>
    where
        E: Entry,
    {
        let idx = self.next_idx_to_write;
        self.next_idx_to_write += 1;

        let ack_idx = self.ack_idx;
        self.ack_written = ack_idx;
        let data = WalData::Data {
            idx,
            ack_idx,
            data: data.serialize().map_err(Error::Entry)?,
        };
        self.file.seek(SeekFrom::Start(self.write_offset)).await?;
        self.write_offset += data.write(&mut self.file).await.map_err(match_error)?;
        self.sync().await.map_err(match_error)?;
        Ok(idx)
    }

    /// Pop an entry from the write-ahead-log data file
    pub async fn pop<E>(&mut self) -> std::result::Result<Option<(u64, E::Output)>, Error<E::Error>>
    where
        E: Entry,
    {
        self.file.seek(SeekFrom::Start(self.read_pointer)).await?;
        loop {
            let data = WalData::read(&mut self.file).await.map_err(match_error)?;
            let advance_by = data.as_ref().map(WalData::size_on_disk).unwrap_or_default();
            trace!("Advance read pointer by: {}", advance_by);
            self.read_pointer += advance_by;
            match data {
                None => return Ok(None),
                Some(WalData::Data { idx, data, .. }) => {
                    self.next_idx_to_read = idx + 1;
                    return Ok(Some((idx, E::deserialize(data).map_err(Error::Entry)?)));
                }
                Some(WalData::Ack { .. }) => {}
            }
        }
    }

    /// Convenience for debugging
    pub async fn inspect<P, E>(path: P) -> Result<()>
    where
        P: AsRef<Path>,
        E: Entry,
    {
        let mut o = OpenOptions::new();
        o.create(false);
        o.write(false);
        o.read(true);
        let mut file = o.open(&path).await?;
        let mut offset = 0;
        while let Some(data) = WalData::read(&mut file).await? {
            println!("{offset:9}: {:?}", data);
            offset = file.seek(SeekFrom::Current(0)).await?
        }
        Ok(())
    }

    /// Open a write-ahead-log data file
    pub async fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let p: &Path = path.as_ref();
        let mut o = OpenOptions::new();
        trace!("Opening existing WAL file: {:?}", p.to_string_lossy());
        if exists(p).await {
            trace!("  Opening...");
            o.create(false);
            o.write(true);
            o.read(true);

            let mut file = o.open(&path).await?;

            file.seek(SeekFrom::End(-8)).await?;

            let mut len = vec![0u8; 8];
            file.read_exact(&mut len).await?;
            let len = BigEndian::read_u64(&len);
            let read_offset = file
                .seek(SeekFrom::End(-(WalData::size_on_disk_from_len(len) as i64)))
                .await?;

            let data = WalData::read(&mut file).await?.ok_or(Error::InvalidFile)?;
            let write_offset = file.seek(SeekFrom::Current(0)).await?;

            let next_idx_to_read = data.ack_idx() + 1;
            let mut wal = WalFile {
                file,
                next_idx_to_write: data.idx() + 1,
                write_offset,
                next_idx_to_read,
                read_pointer: read_offset,
                ack_idx: data.ack_idx(),
                ack_written: data.ack_idx(),
            };

            if data.idx() != wal.next_idx_to_read {
                wal.seek_to(wal.next_idx_to_read).await?
            }
            trace!("Wal opened: {:?}", wal);
            Ok(wal)
        } else {
            trace!("  Creating...");
            let mut o = OpenOptions::new();
            o.create(true);
            o.read(true);
            o.write(true);
            Ok(WalFile {
                file: o.open(path).await?,
                next_idx_to_write: 1,
                write_offset: 0,
                next_idx_to_read: 1,
                read_pointer: 0,
                ack_idx: 0,
                ack_written: 0,
            })
        }
    }

    /// Retrieve the write offset for this data file
    pub fn size(&self) -> u64 {
        self.write_offset
    }

    // Seek to a specified index for the next read operation
    pub async fn seek_to(&mut self, next_idx_to_read: u64) -> Result<()> {
        trace!("Seeking to {} in {:?}", next_idx_to_read, self.file);
        self.file.seek(SeekFrom::Start(0)).await?;
        match WalData::read(&mut self.file).await? {
            // This would mean we want to seek infront of the file, in this case
            // just stick with the first element
            Some(data) if data.idx() > next_idx_to_read => {
                trace!("First index {} > {}", data.idx(), next_idx_to_read);
                self.read_pointer = 0;
                self.next_idx_to_read = data.idx();
            }
            // This is the correct element, we set the offset to zero and read from here on
            Some(data) if data.idx() == next_idx_to_read => {
                // since the currently read data is the data we wanted to seek to
                // we have to move the read pointer one back
                let read_offset = 0;
                trace!(
                    "First index {} == {} => read_offset: {}",
                    data.idx(),
                    next_idx_to_read,
                    read_offset
                );
                self.read_pointer = read_offset;
                self.next_idx_to_read = next_idx_to_read;
            }
            Some(_) => loop {
                let read_offset = self.pos().await?;
                if let Some(data) = WalData::read(&mut self.file).await? {
                    trace!(
                        "Testing {} > {} @ {}",
                        data.idx(),
                        next_idx_to_read,
                        read_offset
                    );
                    if data.idx() >= next_idx_to_read {
                        self.read_pointer = read_offset;
                        self.next_idx_to_read = next_idx_to_read;
                        break;
                    }
                } else {
                    trace!(
                        "EOF Reached next read: {} @ {}",
                        next_idx_to_read,
                        read_offset
                    );
                    self.read_pointer = read_offset;
                    self.next_idx_to_read = next_idx_to_read;
                    break;
                }
            },
            None => {
                trace!("No entries found setting read_idx and read_offset to 0");
                self.read_pointer = 0;
                self.next_idx_to_read = 0;
            }
        }
        Ok(())
    }

    async fn pos(&mut self) -> Result<u64> {
        self.file
            .seek(SeekFrom::Current(0))
            .await
            .map_err(Error::Io)
    }

    // Mark up to the specified index as acknowledged
    pub fn ack(&mut self, idx: u64) {
        trace!("Marking ack as {} in {:?}", idx, self.file);
        self.ack_idx = idx;
    }
}

#[cfg(feature = "async-std")]
async fn exists(p: &Path) -> bool {
    p.exists().await
}

#[cfg(feature = "tokio")]
async fn exists(p: &Path) -> bool {
    p.exists()
}

#[cfg(test)]
mod test {

    use super::*;
    use tempfile::Builder as TempDirBuilder;

    #[cfg_attr(feature = "async-std", async_std::test)]
    #[cfg_attr(feature = "tokio", tokio::test)]
    async fn file() -> Result<()> {
        let temp_dir = TempDirBuilder::new().prefix("tremor-wal").tempdir()?;
        let mut path = temp_dir.path().to_path_buf();
        path.push("wal.file");

        {
            let mut w = WalFile::open(&path).await?;

            assert_eq!(w.push(b"1".to_vec()).await?, 1);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"1".to_vec())));

            w.close().await?;
        }
        {
            let mut w = WalFile::open(&path).await?;

            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"1".to_vec())));
            w.ack(1);

            assert_eq!(w.push(b"22".to_vec()).await?, 2);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((2, b"22".to_vec())));
            w.close().await?;
        }
        {
            let mut w = WalFile::open(&path).await?;
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((2, b"22".to_vec())));
            w.ack(2);

            assert_eq!(w.push(b"33".to_vec()).await?, 3);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((3, b"33".to_vec())));
            w.ack(3);

            w.close().await?;
        }
        let mut w = WalFile::open(&path).await?;
        assert_eq!(w.pop::<Vec<u8>>().await?, None);

        Ok(())
    }
}
