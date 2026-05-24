use std::hint::black_box;

use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use ironwal::{SyncMode, Wal, WalOptions};
use tempfile::TempDir;

// Helper to create a WAL in a temporary directory for isolated benchmark runs.
fn setup_wal(sync_mode: SyncMode) -> (Wal, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let mut opts = WalOptions::default();
  opts.root_path = dir.path().to_path_buf();
  opts.sync_mode = sync_mode;
  let wal = Wal::new(opts).unwrap();
  (wal, dir)
}

fn bench_appends(c: &mut Criterion) {
  let mut group = c.benchmark_group("Append Operations");
  let payload = vec![0u8; 256]; // A realistic 256-byte payload

  // --- Single Append Benchmarks ---

  group.bench_function("append_single_strict", |b: &mut Bencher| {
    b.iter_batched(
      || setup_wal(SyncMode::Strict),
      |(wal, _dir)| {
        wal.append("bench_stream", black_box(&payload)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.bench_function("append_single_async", |b: &mut Bencher| {
    b.iter_batched(
      || setup_wal(SyncMode::Async),
      |(wal, _dir)| {
        wal.append("bench_stream", black_box(&payload)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  // --- Batch Append Benchmarks ---

  let batch: Vec<Vec<u8>> = (0..100).map(|_| payload.clone()).collect();
  let batch_refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();

  group.bench_function("append_batch_100_strict", |b: &mut Bencher| {
    b.iter_batched(
      || setup_wal(SyncMode::Strict),
      |(wal, _dir)| {
        wal.append_batch("bench_stream", black_box(&batch_refs)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.bench_function("append_batch_100_async", |b: &mut Bencher| {
    b.iter_batched(
      || setup_wal(SyncMode::Async),
      |(wal, _dir)| {
        wal.append_batch("bench_stream", black_box(&batch_refs)).unwrap();
      },
      criterion::BatchSize::SmallInput,
    )
  });

  group.finish();
}

criterion_group!(benches, bench_appends);
criterion_main!(benches);
