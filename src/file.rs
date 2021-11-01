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

use super::{Entry, Result};

use std::{
    io::{Error, ErrorKind, SeekFrom},
    mem::size_of,
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
    fs::{File, OpenOptions},
    io::prelude::*,
    path::Path,
};
use byteorder::{BigEndian, ByteOrder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum WalData {
    Data {
        idx: u64,
        ack_idx: u64,
        data: Vec<u8>,
    },
    Ack {
        idx: u64,
        ack_idx: u64,
    },
}

impl WalData {
    async fn read(f: &mut File) -> Result<Option<Self>> {
        let mut buf = vec![0u8; 8];

        // read size
        if f.read_exact(&mut buf).await.is_err() {
            return Ok(None);
        }
        let len = BigEndian::read_u64(&buf);

        // read id
        f.read_exact(&mut buf).await?;
        let idx = BigEndian::read_u64(&buf);
        // read ack_id
        f.read_exact(&mut buf).await?;
        let ack_idx = BigEndian::read_u64(&buf);

        if len == u64::MAX {
            // THIS is a ack token
            f.read_exact(&mut buf).await?;
            let len2 = BigEndian::read_u64(&buf);
            if len2 != 0 {
                Err(Error::new(ErrorKind::Other, "Invalid WAL entry (ACK)"))
            } else {
                Ok(Some(Self::Ack { ack_idx, idx }))
            }
        } else {
            // read data
            let mut data = Vec::with_capacity(len as usize);
            // We set the len since we know the len and then
            unsafe { data.set_len(len as usize) };
            f.read_exact(&mut data).await?;

            f.read_exact(&mut buf).await?;
            let len2 = BigEndian::read_u64(&buf);
            if len2 != len {
                Err(Error::new(ErrorKind::Other, "Invalid WAL entry (Data)"))
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
            len + len64 * 4
        }
    }

    fn size_on_disk(&self) -> u64 {
        match self {
            WalData::Data { data, .. } => WalData::size_on_disk_from_len(data.len() as u64),
            WalData::Ack { .. } => WalData::size_on_disk_from_len(0),
        }
    }

    async fn write(&self, w: &mut File) -> Result<u64> {
        let size_on_disk = self.size_on_disk() as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(size_on_disk);
        unsafe { buf.set_len(size_on_disk) }
        match self {
            WalData::Data { idx, ack_idx, data } => {
                // id + ack_id + len + len (tailing len)
                let len = data.len();
                BigEndian::write_u64(&mut buf[0..], len as u64); //len
                BigEndian::write_u64(&mut buf[8..], *idx); // idx
                BigEndian::write_u64(&mut buf[16..], *ack_idx); // ack
                buf[24..(size_on_disk - 8)].clone_from_slice(&data); // data
                BigEndian::write_u64(&mut buf[(24 + len)..], len as u64); // len2
                w.write_all(&buf).await?;
            }
            WalData::Ack { ack_idx, idx } => {
                BigEndian::write_u64(&mut buf[0..], u64::MAX); // len
                BigEndian::write_u64(&mut buf[8..], *idx); // idx
                BigEndian::write_u64(&mut buf[16..], *ack_idx); // ack
                BigEndian::write_u64(&mut buf[24..], 0); // len 2
                BigEndian::write_u64(&mut buf[..], 0);
                w.write_all(&buf).await?;
            }
        }
        Ok(self.size_on_disk())
    }
    pub fn ack_idx(&self) -> u64 {
        match self {
            WalData::Data { ack_idx, .. } | WalData::Ack { ack_idx, .. } => *ack_idx,
        }
    }
    pub fn idx(&self) -> u64 {
        match self {
            WalData::Data { idx, .. } | WalData::Ack { idx, .. } => *idx,
        }
    }
}

#[derive(Debug)]
pub struct WalFile {
    pub(crate) file: File,
    pub(crate) next_idx_to_write: u64,
    pub(crate) write_offset: u64,
    pub(crate) next_idx_to_read: u64,
    pub(crate) read_offset: u64,
    pub(crate) ack_idx: u64,
    pub(crate) ack_written: u64,
}

impl WalFile {
    async fn sync(&self) -> Result<()> {
        self.file.sync_all().await
    }
    pub(crate) async fn preserve_ack(&mut self) -> Result<()> {
        trace!("Appending ack index {} to {:?}", self.ack_idx, self.file);

        let data = WalData::Ack {
            // we remove this since we usually ack with the previos index and this is no real data
            idx: self.next_idx_to_write - 1,
            ack_idx: self.ack_idx,
        };
        self.file.seek(SeekFrom::Start(self.write_offset)).await?;
        data.write(&mut self.file).await?;
        self.sync().await
    }
    pub async fn close(mut self) -> Result<()> {
        trace!("Closing WAL file {:?}", self);
        if self.ack_written != self.ack_idx {
            self.preserve_ack().await?;
        }
        Ok(())
    }
    pub async fn push<E>(&mut self, data: E) -> Result<u64>
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
            data: data.serialize(),
        };
        self.file.seek(SeekFrom::Start(self.write_offset)).await?;
        self.write_offset += data.write(&mut self.file).await?;
        self.sync().await?;
        Ok(idx)
    }

    pub async fn pop<E>(&mut self) -> Result<Option<(u64, E::Output)>>
    where
        E: Entry,
    {
        self.file.seek(SeekFrom::Start(self.read_offset)).await?;
        loop {
            let data = WalData::read(&mut self.file).await?;
            self.read_offset += data.as_ref().map(WalData::size_on_disk).unwrap_or_default();
            match data {
                None => return Ok(None),
                Some(WalData::Data { idx, data, .. }) => {
                    self.next_idx_to_read = idx + 1;
                    return Ok(Some((idx, E::deserialize(data))));
                }
                Some(WalData::Ack { .. }) => {}
            }
        }
    }

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
        while let Ok(Some(data)) = WalData::read(&mut file).await {
            println!("{:?}", data);
        }
        Ok(())
    }

    pub async fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let p: &Path = path.as_ref();
        if p.exists().await {
            trace!("Opening existing WAL file: {:?}", p.to_str().unwrap());

            let mut o = OpenOptions::new();
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

            let data = WalData::read(&mut file)
                .await?
                .ok_or(Error::new(ErrorKind::Other, "Invalid WAL file"))?;
            let write_offset = file.seek(SeekFrom::Current(0)).await?;

            let next_idx_to_read = data.ack_idx() + 1;
            let mut wal = WalFile {
                file,
                next_idx_to_write: data.idx() + 1,
                write_offset,
                next_idx_to_read,
                read_offset,
                ack_idx: data.ack_idx(),
                ack_written: data.ack_idx(),
            };

            if data.idx() != wal.next_idx_to_read {
                wal.seek_to(wal.next_idx_to_read).await?
            }
            trace!("Wal opened: {:?}", wal);
            Ok(wal)
        } else {
            trace!("Creating new WAL file: {:?}", p.to_str().unwrap());
            let mut o = OpenOptions::new();
            o.create(true);
            o.read(true);
            o.write(true);
            Ok(WalFile {
                file: o.open(path).await?,
                next_idx_to_write: 0,
                write_offset: 0,
                next_idx_to_read: 0,
                read_offset: 0,
                ack_idx: 0,
                ack_written: 0,
            })
        }
    }
    pub fn size(&self) -> u64 {
        self.write_offset
    }

    pub async fn seek_to(&mut self, next_idx_to_read: u64) -> Result<()> {
        trace!("Seeking to {} in {:?}", next_idx_to_read, self.file);
        self.file.seek(SeekFrom::Start(0)).await?;
        if let Some(data) = WalData::read(&mut self.file).await? {
            let read_offset = self.pos().await?;
            if data.idx() > next_idx_to_read {
                trace!("First index {} > {}", data.idx(), next_idx_to_read);
                self.read_offset = 0;
                self.next_idx_to_read = data.idx();
                return Ok(());
            } else if data.idx() == next_idx_to_read {
                // since the currently read data is the data we wanted to seek to
                // we have to move the read pointer one back
                let read_offset = read_offset - data.size_on_disk();
                trace!(
                    "First index {} == {} => read_offset: {}",
                    data.idx(),
                    next_idx_to_read,
                    read_offset
                );
                self.read_offset = read_offset;
                self.next_idx_to_read = next_idx_to_read;
                return Ok(());
            }
        } else {
            trace!("No entries found setting read_idx and read_offset to 0");
            self.read_offset = 0;
            self.next_idx_to_read = 0;
            return Ok(());
        }
        loop {
            let read_offset = self.pos().await?;
            if let Some(data) = WalData::read(&mut self.file).await? {
                trace!(
                    "Testing {} > {} @ {}",
                    data.idx(),
                    next_idx_to_read,
                    read_offset
                );
                if data.idx() >= next_idx_to_read {
                    self.read_offset = read_offset;
                    self.next_idx_to_read = next_idx_to_read;
                    break;
                }
            } else {
                trace!(
                    "EOF Reached next read: {} @ {}",
                    next_idx_to_read,
                    read_offset
                );
                self.read_offset = read_offset;
                self.next_idx_to_read = next_idx_to_read;
                break;
            }
        }
        Ok(())
    }
    async fn pos(&mut self) -> Result<u64> {
        self.file.seek(SeekFrom::Current(0)).await
    }

    pub fn ack(&mut self, idx: u64) {
        trace!("Marking ack as {} in {:?}", idx, self.file);
        self.ack_idx = idx;
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use tempfile::Builder as TempDirBuilder;

    #[async_std::test]
    async fn file() -> Result<()> {
        let temp_dir = TempDirBuilder::new().prefix("tremor-wal").tempdir()?;
        let mut path = temp_dir.path().to_path_buf();
        path.push("wal.file");

        {
            let mut w = WalFile::open(&path).await?;

            assert_eq!(w.push(b"snot".to_vec()).await?, 0);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((0, b"snot".to_vec())));

            w.ack(0);

            assert_eq!(w.push(b"badger".to_vec()).await?, 1);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"badger".to_vec())));
            w.close().await?;
        }
        {
            let mut w = WalFile::open(&path).await?;
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((1, b"badger".to_vec())));
            w.ack(1);

            assert_eq!(w.push(b"boo".to_vec()).await?, 2);
            assert_eq!(w.pop::<Vec<u8>>().await?, Some((2, b"boo".to_vec())));
            w.ack(2);

            w.close().await?;
        }
        let mut w = WalFile::open(&path).await?;
        assert_eq!(w.pop::<Vec<u8>>().await?, None);

        Ok(())
    }
}
