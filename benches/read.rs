use std::hint::black_box;

use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use ironwal::{ReadStrategy, SyncMode, Wal, WalOptions};
use rand::{Rng, rng};
use tempfile::TempDir;

const NUM_RECORDS: u64 = 10_000;

// Helper to set up and pre-populate a WAL with data for reading.
fn setup_populated_wal(strategy: ReadStrategy, cache_size: Option<u64>) -> (Wal, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.read_strategy = strategy;
  opts.block_cache_size = cache_size;
  opts.sync_mode = SyncMode::Async; // Writes should be fast for setup

  let wal = Wal::new(opts).unwrap();

  // Populate with data
  let payload = vec![0u8; 128];
  for _ in 0..NUM_RECORDS {
    wal.append("read_bench", &payload).unwrap();
  }

  (wal, dir)
}

fn bench_reads(c: &mut Criterion) {
  let mut group = c.benchmark_group("Read Operations");

  // --- Random Read (get) Benchmarks ---

  group.bench_function("read_random_get_std_io", |b: &mut Bencher| {
    b.iter_batched(
      || setup_populated_wal(ReadStrategy::StandardIo, None),
      |(wal, _dir)| {
        let id = rng().random_range(0..NUM_RECORDS);
        wal.get("read_bench", black_box(id)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.bench_function("read_random_get_mmap", |b: &mut Bencher| {
    b.iter_batched(
      || setup_populated_wal(ReadStrategy::Mmap, None),
      |(wal, _dir)| {
        let id = rng().random_range(0..NUM_RECORDS);
        wal.get("read_bench", black_box(id)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.bench_function("read_random_get_mmap_cached", |b: &mut Bencher| {
    b.iter_batched(
      || setup_populated_wal(ReadStrategy::Mmap, Some(10 * 1024 * 1024)), // 10MB cache
      |(wal, _dir)| {
        let id = rng().random_range(0..NUM_RECORDS);
        wal.get("read_bench", black_box(id)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  // --- Sequential Read (iter) Benchmarks ---

  group.bench_function("read_sequential_iter_std_io", |b: &mut Bencher| {
    b.iter_batched(
      || setup_populated_wal(ReadStrategy::StandardIo, None),
      |(wal, _dir)| {
        let iter = wal.iter("read_bench", 0).unwrap();
        // Consume the iterator
        for item in iter {
          black_box(item.unwrap());
        }
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.bench_function("read_sequential_iter_mmap", |b: &mut Bencher| {
    b.iter_batched(
      || setup_populated_wal(ReadStrategy::Mmap, None),
      |(wal, _dir)| {
        let iter = wal.iter("read_bench", 0).unwrap();
        for item in iter {
          black_box(item.unwrap());
        }
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.finish();
}

criterion_group!(benches, bench_reads);
criterion_main!(benches);
