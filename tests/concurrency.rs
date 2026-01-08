mod common;
use common::TestEnv;
use ironwal::SyncMode;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_concurrent_appends() {
  // Setup environment
  let mut opts = ironwal::WalOptions::default();
  opts.sync_mode = SyncMode::BatchOnly; // Speed up test, rely on atomicity logic
  let env = TestEnv::new(opts);
  let stream = "concurrent_stream";

  let wal = env.wal.clone();
  let thread_count = 10;
  let items_per_thread = 100;

  let barrier = Arc::new(Barrier::new(thread_count));
  let mut handles = Vec::new();

  // Spawn N threads, each writing M items
  for t_id in 0..thread_count {
    let wal = wal.clone();
    let barrier = barrier.clone();
    let stream = stream.to_string();

    handles.push(thread::spawn(move || {
      barrier.wait(); // Synchronize start
      for i in 0..items_per_thread {
        let payload = format!("t{}_i{}", t_id, i).into_bytes();
        wal.append(&stream, &payload).unwrap();
      }
    }));
  }

  // Wait for all to finish
  for h in handles {
    h.join().unwrap();
  }

  // Verification
  // 1. Check total count
  // Since we don't have a "get_latest_id" API public yet, we iterate to count.
  let mut count = 0;
  let mut iter = env.wal.iter(stream, 0).unwrap();

  while let Some(res) = iter.next() {
    let _ = res.unwrap();
    count += 1;
  }

  assert_eq!(count, thread_count * items_per_thread, "Total items mismatch");
}

#[test]
fn test_concurrent_read_write() {
  let env = TestEnv::with_default();
  let stream = "rw_stream";
  let wal = env.wal.clone();

  // Seed data
  for i in 0..100 {
    wal.append(stream, &[i as u8]).unwrap();
  }

  let barrier = Arc::new(Barrier::new(2));

  // Reader Thread
  let reader_wal = wal.clone();
  let b_reader = barrier.clone();
  let reader_handle = thread::spawn(move || {
    b_reader.wait();
    // Constantly read random IDs
    for _ in 0..100 {
      // Just ensure no panics/crashes when reading while writing happens
      let _ = reader_wal.get("rw_stream", 50).unwrap();
    }
  });

  // Writer Thread
  let writer_handle = thread::spawn(move || {
    barrier.wait();
    for _ in 0..100 {
      wal.append("rw_stream", b"new").unwrap();
    }
  });

  reader_handle.join().unwrap();
  writer_handle.join().unwrap();

  // Validate survival
  let mut iter = env.wal.iter(stream, 0).unwrap();
  let mut count = 0;
  while iter.next().is_some() {
    count += 1;
  }

  assert_eq!(count, 200); // 100 seeded + 100 written
}
