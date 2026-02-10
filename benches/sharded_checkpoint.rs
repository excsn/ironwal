//! Checkpoint operation benchmarks

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use std::hint::black_box;
use tempfile::TempDir;

fn setup_with_data(shard_count: u16, entry_count: usize) -> (ShardedWal, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = SyncMode::BatchOnly;
  let wal = ShardedWal::new(opts, shard_count).unwrap();

  // Pre-populate with data
  let payload = vec![0u8; 128];
  for i in 0..entry_count {
    let key = format!("key_{}", i);
    wal.append(key.as_bytes(), &payload).unwrap();
  }

  (wal, dir)
}

fn bench_checkpoint_create(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Checkpoint/Create");

  for shard_count in [4, 16, 64].iter() {
    group.bench_with_input(
      BenchmarkId::new("create", shard_count),
      shard_count,
      |b, &sc| {
        // Setup once per benchmark run, outside the timed loop
        let (wal, _dir) = setup_with_data(sc, 1000); // Wal is already populated
        let mut counter = 0; // Use a counter for unique checkpoint IDs

        b.iter_batched(
          || {
          let id = format!("ckpt_{}", counter);
          counter += 1;
          id // Return the unique ID for this iteration
          },
          |id| {
            wal.create_checkpoint(black_box(id.as_bytes())).unwrap();
          },
          criterion::BatchSize::SmallInput,
        )
      },
    );
  }

  group.finish();
}

fn bench_checkpoint_load(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Checkpoint/Load");

  for shard_count in [4, 16, 64].iter() {
    group.bench_with_input(
      BenchmarkId::new("load", shard_count),
      shard_count,
      |b, &sc| {
        // Setup once per benchmark run, outside the timed loop
        let (wal, _dir) = setup_with_data(sc, 1000); // Wal is already populated
        wal.create_checkpoint(b"bench").unwrap(); // Create the checkpoint once

        b.iter(|| { // Only measure the load operation
          wal.load_checkpoint(black_box(b"bench")).unwrap();
        });
      },
    );
  }

  group.finish();
}

fn bench_checkpoint_with_many_checkpoints(c: &mut Criterion) {
  let mut group = c.benchmark_group("ShardedWal/Checkpoint/Scale");

  // Reduce sample count for expensive benchmarks
  group.sample_size(10);

  // Test load performance with many checkpoints in index
  for checkpoint_count in [10, 100, 500].iter() {
    // Changed: 1000 -> 500
    group.bench_with_input(
      BenchmarkId::new("load_with_count", checkpoint_count),
      checkpoint_count,
      |b, &count| {
        // Setup once and reuse
        let (wal, _dir) = setup_with_data(16, 100);

        // Create many checkpoints once (not per iteration)
        for i in 0..count {
          let id = format!("checkpoint_{}", i);
          wal.create_checkpoint(id.as_bytes()).unwrap();
        }

        b.iter(|| {
          // Only benchmark the load operation
          let id = format!("checkpoint_{}", count / 2);
          wal.load_checkpoint(black_box(id.as_bytes())).unwrap();
        });
      },
    );
  }

  group.finish();
}

criterion_group!(
  benches,
  bench_checkpoint_create,
  bench_checkpoint_load,
  bench_checkpoint_with_many_checkpoints
);
criterion_main!(benches);
