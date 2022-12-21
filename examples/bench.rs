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

use qwal::{Result, Wal};
use std::time;

use tempfile::Builder as TempDirBuilder;
async fn bench_read_write(n: u32, size: usize) -> Result<()> {
    let temp_dir = TempDirBuilder::new().prefix("tremor-wal").tempdir()?;

    let mut wal = Wal::open(temp_dir.path(), 2048, 100).await?;

    let data = vec![0u8; size];
    for _ in 0..n {
        let id = wal.push(data.as_slice()).await?;
        let read = wal.pop::<Vec<u8>>().await?.unwrap();
        assert_eq!(read.0, id);
        assert_eq!(read.1, data);
        wal.ack(id).await?;
    }
    Ok(())
}

const RUNS: u32 = 10000;
#[cfg_attr(feature = "async-std", async_std::main)]
#[cfg_attr(feature = "tokio", tokio::main)]
async fn main() {
    let sizes = vec![0, 128, 1024, 1024 * 10, 1024 * 100];
    for size in sizes {
        let start = time::Instant::now();
        bench_read_write(RUNS, size)
            .await
            .expect("bench_read_write");
        let e = start.elapsed();
        println!(
            "bench_read_write@{:7}bytes took {:?} ({:?} per entry)",
            size,
            e,
            e / RUNS
        );
    }
}
