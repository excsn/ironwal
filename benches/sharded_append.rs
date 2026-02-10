//! Sharded WAL append benchmarks

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::collections::HashSet;
use std::hint::black_box;
use tempfile::TempDir;

fn setup_sharded(shard_count: u16, sync_mode: SyncMode) -> (ShardedWal, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = sync_mode;
  let wal = ShardedWal::new(opts, shard_count).unwrap();

  // Warmup: Ensure all shards are initialized to avoid lazy-init fsync costs during benchmark
  let mut covered = HashSet::new();
  let mut i = 0;
  // Append to unique keys until all shards have been touched at least once
  while covered.len() < shard_count as usize {
    let key = format!("warmup_key_{}", i);
    let (shard_id, _) = wal.append(key.as_bytes(), b"warmup_data").unwrap();
    covered.insert(shard_id);
    i += 1;
  }
  (wal, dir)
}

fn bench_sharded_append(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Append");
  let payload = vec![0u8; 256];

  // Single append across different shard counts
  for shard_count in [4, 16, 64].iter() {
    group.bench_with_input(
      BenchmarkId::new("single_async", shard_count),
      shard_count,
      |b, &sc| {
        let (wal, _dir) = setup_sharded(sc, SyncMode::Async);
        b.iter(|| {
          let key = b"benchmark_key";
          wal.append(black_box(key), black_box(&payload)).unwrap();
        })
      },
    );
  }

  group.finish();
}

fn bench_sharded_batch(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Batch");
  group.throughput(Throughput::Elements(100));

  let payload = vec![0u8; 256];
  let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
    .map(|i| (format!("key_{}", i).into_bytes(), payload.clone()))
    .collect();

  for shard_count in [4, 16, 64].iter() {
    group.bench_with_input(
      BenchmarkId::new("batch_100", shard_count),
      shard_count,
      |b, &sc| {
        let (wal, _dir) = setup_sharded(sc, SyncMode::Async);
        b.iter(|| {
          wal.append_batch(black_box(&batch)).unwrap();
        })
      },
    );
  }

  group.finish();
}

fn bench_sharded_routing_overhead(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Routing");

  let payload = vec![0u8; 256];

  group.bench_function("core_wal_baseline", |b| {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = WalOptions::default();
    opts.root_path = dir.path().to_path_buf();
    opts.sync_mode = SyncMode::Async;
    let wal = ironwal::Wal::new(opts).unwrap();

    b.iter(|| {
      wal.append("stream", black_box(&payload)).unwrap();
    })
  });

  group.bench_function("sharded_16", |b| {
    let (wal, _dir) = setup_sharded(16, SyncMode::Async);

    b.iter(|| {
      wal.append(b"key", black_box(&payload)).unwrap();
    })
  });

  group.finish();
}

criterion_group!(
  benches,
  bench_sharded_append,
  bench_sharded_batch,
  bench_sharded_routing_overhead
);
criterion_main!(benches);
