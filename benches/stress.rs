use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ironwal::{SyncMode, Wal, WalOptions};
use std::sync::{Arc, Barrier};
use std::thread;

fn bench_stress(c: &mut Criterion) {
  // --- Scenario 1: Cache Thrashing ---
  // Write to 100 different streams, but only allow 10 open files.
  // This forces the WAL to constantly evict (flush + close) and reopen files.
  // This measures the overhead of the resource management system.
  let mut group = c.benchmark_group("Stress/CacheThrashing");

  group.bench_function("write_round_robin_thrashing", |b| {
    b.iter_batched(
      || {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = WalOptions::default();
        opts.root_path = dir.path().to_path_buf();
        opts.sync_mode = SyncMode::BatchOnly;
        opts.max_open_segments = 10; // Constrained!
        let wal = Wal::new(opts).unwrap();
        (wal, dir)
      },
      |(wal, _dir)| {
        // Round-robin write to 100 streams
        // With size 10, this guarantees 90% miss rate on the write cache.
        for i in 0..100 {
          let stream = format!("stream_{}", i);
          wal.append(&stream, b"payload").unwrap();
        }
      },
      criterion::BatchSize::SmallInput,
    )
  });
  group.finish();

  // --- Scenario 2: Concurrent Contention ---
  // Measure total throughput with increasing thread counts.
  // This reveals lock contention on the global Mutex.
  let mut group = c.benchmark_group("Stress/Concurrency");
  let payload = vec![0u8; 128];

  for threads in [1, 4, 8, 16].iter() {
    group.throughput(Throughput::Elements(*threads as u64 * 100)); // 100 ops per thread
    group.bench_with_input(BenchmarkId::from_parameter(threads), threads, |b, &t_count| {
      b.iter_batched(
        || {
          let dir = tempfile::tempdir().unwrap();
          let mut opts = WalOptions::default();
          opts.root_path = dir.path().to_path_buf();
          opts.sync_mode = SyncMode::BatchOnly;
          let wal = Wal::new(opts).unwrap();
          (wal, dir)
        },
        |(wal, _dir)| {
          let barrier = Arc::new(Barrier::new(t_count));
          let mut handles = Vec::new();

          for i in 0..t_count {
            let w = wal.clone();
            let bar = barrier.clone();
            let p = payload.clone();
            let stream = format!("worker_{}", i);
            handles.push(thread::spawn(move || {
              bar.wait();
              for _ in 0..100 {
                w.append(&stream, &p).unwrap();
              }
            }));
          }

          for h in handles {
            h.join().unwrap();
          }
        },
        criterion::BatchSize::SmallInput,
      );
    });
  }
  group.finish();
}

// --- Scenario 3: Recovery Speed ---
// Measure how long it takes to open a WAL with many existing segments.
fn bench_recovery(c: &mut Criterion) {
  let mut group = c.benchmark_group("Stress/Recovery");

  // Setup a heavy WAL *once*
  let dir = tempfile::tempdir().unwrap();
  let root = dir.path().to_path_buf();

  // Create 1,000 segments (by forcing rotation)
  {
    let mut opts = WalOptions::default();
    opts.root_path = root.clone();
    opts.max_entries_per_segment = 10;
    opts.sync_mode = SyncMode::BatchOnly;
    let wal = Wal::new(opts).unwrap();
    // Write 10,000 entries -> 1,000 segments
    let batch = vec![0u8; 10];
    for _ in 0..10_000 {
      wal.append("recovery_stream", &batch).unwrap();
    }
  } // wal dropped here

  group.bench_function("recover_10k_entries_1k_segments", |b| {
    b.iter(|| {
      // We only measure the time to `new()`
      // We rely on the OS page cache to keep the files hot,
      // so this tests CPU/Parsing overhead, not disk seek.
      let opts = WalOptions::new(&root);
      let _ = Wal::new(opts).unwrap();
    })
  });

  group.finish();
  // Keep dir alive until bench ends
  drop(dir);
}

criterion_group!(benches, bench_stress, bench_recovery);
criterion_main!(benches);
