use ironwal::{Wal, WalOptions};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

const STREAM: &str = "test_stream";

fn make_opts(dir: &TempDir, max_entries: u64, max_read_descriptors: usize) -> WalOptions {
  let mut opts = WalOptions::new(dir.path());
  opts.max_entries_per_segment = max_entries;
  opts.max_segment_size = 512; // tiny — force rotation on size too
  opts.max_open_read_segments = max_read_descriptors;
  opts
}

/// Write 50 entries (5 per segment → 10 rotated segments), restart,
/// then read every entry twice to verify the cache doesn't corrupt on re-use.
#[test]
fn test_rotated_segment_reads_are_correct() {
  let dir = TempDir::new().unwrap();
  let opts = make_opts(&dir, 5, 128);

  let entries: Vec<Vec<u8>> = (0u64..50).map(|i| format!("entry-{i:04}").into_bytes()).collect();

  {
    let mut wal = Wal::new(opts.clone()).unwrap();
    for (i, e) in entries.iter().enumerate() {
      let id = wal.append(STREAM, e).unwrap();
      assert_eq!(id, i as u64);
    }
  }

  // After restart all segments are rotated (none is active).
  let wal = Wal::new(opts).unwrap();

  for pass in 0..2 {
    for (i, expected) in entries.iter().enumerate() {
      let val = wal.get(STREAM, i as u64).unwrap();
      assert_eq!(
        val.as_deref(),
        Some(expected.as_slice()),
        "pass {pass}: entry {i} mismatch"
      );
    }
  }
}

/// Write 200 entries across many segments, restart, then spawn 8 threads
/// that each read all 200 entries concurrently via the shared descriptor cache.
#[test]
fn test_concurrent_reads_from_rotated_segments() {
  let dir = TempDir::new().unwrap();
  let opts = make_opts(&dir, 5, 128);

  let entries: Vec<Vec<u8>> = (0u64..200).map(|i| format!("value-{i:06}").into_bytes()).collect();

  {
    let mut wal = Wal::new(opts.clone()).unwrap();
    for e in &entries {
      wal.append(STREAM, e).unwrap();
    }
  }

  let wal = Arc::new(Wal::new(opts).unwrap());
  let entries = Arc::new(entries);

  let handles: Vec<_> = (0..8)
    .map(|_| {
      let wal = Arc::clone(&wal);
      let entries = Arc::clone(&entries);
      thread::spawn(move || {
        for (i, expected) in entries.iter().enumerate() {
          let val = wal.get(STREAM, i as u64).unwrap();
          assert_eq!(
            val.as_deref(),
            Some(expected.as_slice()),
            "concurrent read: entry {i} mismatch"
          );
        }
      })
    })
    .collect();

  for h in handles {
    h.join().expect("reader thread panicked");
  }
}

/// Set max_open_read_segments = 2 with 10+ rotated segments.
/// LRU evictions must happen but all reads must still return the correct data.
#[test]
fn test_cache_eviction_under_pressure() {
  let dir = TempDir::new().unwrap();
  // Only 2 FDs in the cache, but we'll have ~20 rotated segments.
  let opts = make_opts(&dir, 1, 2);

  let entries: Vec<Vec<u8>> = (0u64..20).map(|i| format!("evict-{i:04}").into_bytes()).collect();

  {
    let mut wal = Wal::new(opts.clone()).unwrap();
    for e in &entries {
      wal.append(STREAM, e).unwrap();
    }
  }

  let wal = Wal::new(opts).unwrap();

  // Read forward and backward to guarantee many cache evictions.
  for i in 0..20u64 {
    let val = wal.get(STREAM, i).unwrap();
    assert_eq!(
      val.as_deref(),
      Some(entries[i as usize].as_slice()),
      "forward read: entry {i}"
    );
  }
  for i in (0..20u64).rev() {
    let val = wal.get(STREAM, i).unwrap();
    assert_eq!(
      val.as_deref(),
      Some(entries[i as usize].as_slice()),
      "reverse read: entry {i}"
    );
  }
}
