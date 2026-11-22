use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use ironwal::{CompressionType, SyncMode, Wal, WalOptions};

struct Counting;

thread_local! {
  static COUNTING: Cell<bool> = const { Cell::new(false) };
}
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);

fn note(size: usize) {
  LIVE.fetch_add(size, Relaxed);
  if COUNTING.with(Cell::get) {
    ALLOCATED.fetch_add(size, Relaxed);
    LARGEST.fetch_max(size, Relaxed);
  }
}

unsafe impl GlobalAlloc for Counting {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    let ptr = unsafe { System.alloc(layout) };
    if !ptr.is_null() {
      note(layout.size());
    }
    ptr
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) };
    LIVE.fetch_sub(layout.size(), Relaxed);
  }

  unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    let new = unsafe { System.realloc(ptr, layout, new_size) };
    if !new.is_null() {
      LIVE.fetch_sub(layout.size(), Relaxed);
      note(new_size);
    }
    new
  }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Bytes allocated on this thread while `work` runs, and the largest single allocation among them.
fn allocations(work: impl FnOnce()) -> (usize, usize) {
  ALLOCATED.store(0, Relaxed);
  LARGEST.store(0, Relaxed);
  COUNTING.with(|counting| counting.set(true));
  work();
  COUNTING.with(|counting| counting.set(false));
  (ALLOCATED.load(Relaxed), LARGEST.load(Relaxed))
}

const BATCH_BYTES: usize = 4_600_000;
const ENTRIES: usize = 100;

/// Text shaped like encoded trace rows: repeated names around ids and numbers that vary.
fn compressible(salt: u64) -> Vec<Vec<u8>> {
  let mut n = salt.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
  (0..ENTRIES)
    .map(|_| {
      let mut entry = Vec::with_capacity(BATCH_BYTES / ENTRIES + 256);
      while entry.len() < BATCH_BYTES / ENTRIES {
        n = n.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        write!(entry, "trace_id {:032x} root rpc.QueryTraces duration_us {} outcome ok span handle;", n as u128 * 7, n % 100_000).unwrap();
      }
      entry.truncate(BATCH_BYTES / ENTRIES);
      entry
    })
    .collect()
}

fn incompressible(salt: u64) -> Vec<Vec<u8>> {
  let mut n = salt.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
  (0..ENTRIES)
    .map(|_| {
      (0..BATCH_BYTES / ENTRIES)
        .map(|_| {
          n ^= n << 13;
          n ^= n >> 7;
          n ^= n << 17;
          n as u8
        })
        .collect()
    })
    .collect()
}

fn options(root: &std::path::Path, compression: CompressionType) -> WalOptions {
  WalOptions { root_path: root.to_path_buf(), sync_mode: SyncMode::Async, max_segment_size: u64::MAX / 4, compression, ..WalOptions::default() }
}

fn slices(batch: &[Vec<u8>]) -> Vec<&[u8]> {
  batch.iter().map(Vec::as_slice).collect()
}

/// Two warm appends, then the allocations of a third.
fn third_append(compression: CompressionType, batch: fn(u64) -> Vec<Vec<u8>>) -> (usize, usize) {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(options(dir.path(), compression)).unwrap();
  let batches: Vec<Vec<Vec<u8>>> = (0..3).map(batch).collect();
  wal.append_batch("s", &slices(&batches[0])).unwrap();
  wal.append_batch("s", &slices(&batches[1])).unwrap();
  let third = slices(&batches[2]);
  allocations(|| {
    wal.append_batch("s", &third).unwrap();
  })
}

#[test]
fn a_compressed_append_allocates_less_than_half_the_batch_it_writes() {
  let (allocated, largest) = third_append(CompressionType::Lz4, compressible);
  assert!(allocated < BATCH_BYTES / 2, "a {BATCH_BYTES}-byte compressed append allocated {allocated} bytes");
  assert!(largest < BATCH_BYTES / 4, "a {BATCH_BYTES}-byte compressed append allocated one block of {largest} bytes");
}

#[test]
fn a_raw_append_allocates_less_than_half_the_batch_it_writes() {
  let (allocated, largest) = third_append(CompressionType::None, compressible);
  assert!(allocated < BATCH_BYTES / 2, "a {BATCH_BYTES}-byte raw append allocated {allocated} bytes");
  assert!(largest < BATCH_BYTES / 4, "a {BATCH_BYTES}-byte raw append allocated one block of {largest} bytes");
}

#[test]
fn an_append_that_does_not_compress_allocates_less_than_twice_the_batch_it_writes() {
  let (allocated, _) = third_append(CompressionType::Lz4, incompressible);
  assert!(allocated < BATCH_BYTES * 2, "a {BATCH_BYTES}-byte incompressible append allocated {allocated} bytes");
}

#[test]
fn appends_of_large_batches_hold_no_more_than_one_frame_between_them() {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(options(dir.path(), CompressionType::Lz4)).unwrap();
  let batches: Vec<Vec<Vec<u8>>> = (0..4).map(compressible).collect();
  wal.append_batch("s", &slices(&batches[0])).unwrap();
  let before = LIVE.load(Relaxed);
  for round in 0..20 {
    wal.append_batch("s", &slices(&batches[round % 4])).unwrap();
  }
  let held = LIVE.load(Relaxed).saturating_sub(before);
  assert!(held < BATCH_BYTES, "20 appends of {BATCH_BYTES} bytes left {held} more bytes live");
}

fn warm_raw_append(entry_count: usize) -> usize {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(options(dir.path(), CompressionType::None)).unwrap();
  let entries: Vec<Vec<u8>> = (0..entry_count).map(|n| vec![n as u8; 256]).collect();
  let batch = slices(&entries);
  wal.append_batch("s", &batch).unwrap();
  wal.append_batch("s", &batch).unwrap();
  allocations(|| {
    wal.append_batch("s", &batch).unwrap();
  })
  .0
}

#[test]
fn a_small_raw_append_allocates_no_more_for_many_entries_than_for_one() {
  let one = warm_raw_append(1);
  let many = warm_raw_append(25);
  assert_eq!(many, one, "a warm raw append allocated {one} bytes for 1 entry and {many} for 25");
}

fn reads_back(compression: CompressionType, batch: fn(u64) -> Vec<Vec<u8>>) {
  let dir = tempfile::tempdir().unwrap();
  let batches: Vec<Vec<Vec<u8>>> = (0..3).map(batch).collect();
  {
    let wal = Wal::new(options(dir.path(), compression)).unwrap();
    for (index, batch) in batches.iter().enumerate() {
      let ids = wal.append_batch("s", &slices(batch)).unwrap();
      assert_eq!(ids, (index * ENTRIES) as u64..((index + 1) * ENTRIES) as u64);
    }
  }
  let wal = Wal::new(options(dir.path(), compression)).unwrap();
  let read: Vec<Vec<u8>> = wal.iter("s", 0).unwrap().map(Result::unwrap).collect();
  let written: Vec<Vec<u8>> = batches.into_iter().flatten().collect();
  assert_eq!(read.len(), written.len());
  assert!(read == written, "the entries read back differ from those written");
  assert_eq!(wal.get("s", (2 * ENTRIES + 7) as u64).unwrap().as_deref(), Some(written[2 * ENTRIES + 7].as_slice()));
}

#[test]
fn large_compressed_batches_read_back_entry_by_entry_after_a_reopen() {
  reads_back(CompressionType::Lz4, compressible);
}

#[test]
fn large_batches_that_do_not_compress_read_back_entry_by_entry_after_a_reopen() {
  reads_back(CompressionType::Lz4, incompressible);
}

#[test]
fn large_raw_batches_read_back_entry_by_entry_after_a_reopen() {
  reads_back(CompressionType::None, compressible);
}
