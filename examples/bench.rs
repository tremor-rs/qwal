use byteorder::{BigEndian, ByteOrder};
use qwal::{Result, Wal};
use std::time;

use tempfile::Builder as TempDirBuilder;
async fn bench_read_write(n: u32, size: usize) -> Result<()> {
    let temp_dir = TempDirBuilder::new().prefix("tremor-wal").tempdir()?;

    let mut wal = Wal::open(temp_dir.path(), 2048, 100).await?;

    let mut data = vec![0u8; size];
    for _ in 0..n {
        BigEndian::write_u64(&mut data[0..], size as u64);
        BigEndian::write_u32(&mut data[8..], n);
        let id = wal.push(data.as_slice()).await?;
        let read = wal.pop::<Vec<u8>>().await?.unwrap();
        assert_eq!(read.0, id);
        assert_eq!(read.1, data);
        wal.ack(id).await?;
    }
    Ok(())
}

const RUNS: u32 = 10000;
#[async_std::main]
async fn main() {
    let sizes = vec![128, 1024, 1024 * 10, 1024 * 100];
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
