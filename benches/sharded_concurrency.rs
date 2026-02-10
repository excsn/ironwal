//! Concurrent operations benchmarks

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn setup_sharded(shard_count: u16) -> (ShardedWal, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = SyncMode::BatchOnly;
  let wal = ShardedWal::new(opts, shard_count).unwrap();

  // Warmup: Ensure all shards are initialized to avoid lazy-init fsync costs during benchmark
  let mut covered = HashSet::new();
  let mut i = 0;
  while covered.len() < shard_count as usize {
    let key = format!("warmup_{}", i);
    let (shard_id, _) = wal.append(key.as_bytes(), b"warmup").unwrap();
    covered.insert(shard_id);
    i += 1;
  }

  (wal, dir)
}

fn bench_concurrent_writes(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Concurrency/Writes");
  group.sample_size(20); // Reduce from default 100 since threads are expensive

  let payload = vec![0u8; 128];

  for thread_count in [1, 4, 8, 16].iter() {
    group.throughput(Throughput::Elements(*thread_count as u64 * 100));

    group.bench_with_input(
      BenchmarkId::new("threads", thread_count),
      thread_count,
      |b, &tc| {
        b.iter_batched(
          || setup_sharded(16),
          |(wal, _dir)| {
            let wal = Arc::new(wal);
            let barrier = Arc::new(Barrier::new(tc));
            let mut handles = vec![];

            for thread_id in 0..tc {
              let wal_clone = wal.clone();
              let barrier_clone = barrier.clone();
              let payload_clone = payload.clone();

              handles.push(thread::spawn(move || {
                barrier_clone.wait();
                for i in 0..100 {
                  let key = format!("t{}_k{}", thread_id, i);
                  wal_clone.append(key.as_bytes(), &payload_clone).unwrap();
                }
              }));
            }

            for h in handles {
              h.join().unwrap();
            }
          },
          criterion::BatchSize::SmallInput,
        )
      },
    );
  }

  group.finish();
}

fn bench_concurrent_checkpoint_creation(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Concurrency/Checkpoint");
  group.sample_size(20); // Reduce samples

  group.bench_function("write_and_checkpoint", |b| {
    b.iter_batched(
      || setup_sharded(16),
      |(wal, _dir)| {
        let wal = Arc::new(wal);

        // Writer thread
        let wal_writer = wal.clone();
        let write_handle = thread::spawn(move || {
          for i in 0..500 {
            let key = format!("key_{}", i);
            wal_writer.append(key.as_bytes(), b"data").unwrap();
          }
        });

        // Checkpoint thread
        let wal_checkpoint = wal.clone();
        let checkpoint_handle = thread::spawn(move || {
          for i in 0..10 {
            thread::sleep(std::time::Duration::from_micros(100));
            let id = format!("ckpt_{}", i);
            wal_checkpoint.create_checkpoint(id.as_bytes()).unwrap();
          }
        });

        write_handle.join().unwrap();
        checkpoint_handle.join().unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.finish();
}

criterion_group!(
  benches,
  bench_concurrent_writes,
  bench_concurrent_checkpoint_creation
);
criterion_main!(benches);
