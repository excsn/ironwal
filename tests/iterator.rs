mod common;
use common::TestEnv;
use ironwal::WalOptions;

#[test]
fn test_sequential_iteration() {
  let env = TestEnv::with_default();
  let stream = "iter_seq";

  // Write 100 entries
  for i in 0..100 {
    let payload = format!("val_{}", i).into_bytes();
    env.wal.append(stream, &payload).unwrap();
  }

  // Iterate from start
  let mut iter = env.wal.iter(stream, 0).unwrap();

  for i in 0..100 {
    let item = iter.next().expect("Iterator finished early").unwrap();
    let expected = format!("val_{}", i).into_bytes();
    assert_eq!(item, expected, "Mismatch at index {}", i);
  }

  assert!(iter.next().is_none(), "Iterator should be empty");
}

#[test]
fn test_iterator_segment_crossing() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = 500; // Force tiny segments
  let env = TestEnv::new(opts);
  let stream = "iter_cross";

  // Write enough data to span multiple segments
  // 20 entries of 100 bytes = 2000 bytes total.
  // Should create ~4 segments.
  for i in 0..20 {
    let payload = vec![i as u8; 100];
    env.wal.append(stream, &payload).unwrap();
  }

  // Start iterator from middle (ID 10)
  // This tests seeking to correct file AND crossing boundaries later.
  let mut iter = env.wal.iter(stream, 10).unwrap();

  for i in 10..20 {
    let item = iter.next().expect("Iterator stopped early").unwrap();
    assert_eq!(item, vec![i as u8; 100]);
  }

  assert!(iter.next().is_none());
}

#[test]
fn test_mid_batch_seek() {
  let env = TestEnv::with_default();
  let stream = "iter_mid_seek";

  // Write a BATCH of 10 items (ID 0-9)
  let batch: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8]).collect();
  let batch_refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();

  env.wal.append_batch(stream, &batch_refs).unwrap();

  // Write another BATCH of 10 items (ID 10-19)
  let batch2: Vec<Vec<u8>> = (10..20).map(|i| vec![i as u8]).collect();
  let batch2_refs: Vec<&[u8]> = batch2.iter().map(|v| v.as_slice()).collect();

  env.wal.append_batch(stream, &batch2_refs).unwrap();

  // Seek to ID 5 (Middle of first batch)
  let mut iter = env.wal.iter(stream, 5).unwrap();

  // Should verify we get 5, 6, 7, 8, 9, 10...
  for i in 5..20 {
    let item = iter.next().expect("Iterator early stop").unwrap();
    assert_eq!(item, vec![i as u8], "Mismatch at ID {}", i);
  }

  assert!(iter.next().is_none());

  // Seek to ID 15 (Middle of second batch)
  let mut iter2 = env.wal.iter(stream, 15).unwrap();
  let val = iter2.next().unwrap().unwrap();
  assert_eq!(val, vec![15u8]);
}

#[test]
fn test_iter_empty_stream() {
  let env = TestEnv::with_default();
  let mut iter = env.wal.iter("non_existent", 0).unwrap();
  assert!(iter.next().is_none());
}

#[test]
fn test_iter_with_ids_sequential() {
  let env = TestEnv::with_default();
  let stream = "iter_ids_seq";

  for i in 0..100 {
    let payload = format!("val_{}", i).into_bytes();
    env.wal.append(stream, &payload).unwrap();
  }

  let mut iter = env.wal.iter_with_ids(stream, 0).unwrap();

  for i in 0..100u64 {
    let (id, entry) = iter.next().expect("Iterator finished early").unwrap();
    assert_eq!(id, i);
    assert_eq!(entry, format!("val_{}", i).into_bytes());
  }

  assert!(iter.next().is_none());
}

#[test]
fn test_iter_with_ids_mid_batch_seek() {
  let env = TestEnv::with_default();
  let stream = "iter_ids_mid_seek";

  let batch: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8]).collect();
  let batch_refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
  env.wal.append_batch(stream, &batch_refs).unwrap();

  let mut iter = env.wal.iter_with_ids(stream, 5).unwrap();

  for i in 5..10u64 {
    let (id, entry) = iter.next().expect("Iterator early stop").unwrap();
    assert_eq!(id, i);
    assert_eq!(entry, vec![i as u8]);
  }

  assert!(iter.next().is_none());
}

#[test]
fn test_iter_with_ids_segment_crossing() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = 500;
  let env = TestEnv::new(opts);
  let stream = "iter_ids_cross";

  for i in 0..20 {
    env.wal.append(stream, &vec![i as u8; 100]).unwrap();
  }

  let mut iter = env.wal.iter_with_ids(stream, 10).unwrap();

  for i in 10..20u64 {
    let (id, entry) = iter.next().expect("Iterator stopped early").unwrap();
    assert_eq!(id, i);
    assert_eq!(entry, vec![i as u8; 100]);
  }

  assert!(iter.next().is_none());
}

#[test]
fn test_iter_with_ids_after_truncate() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;
  let env = TestEnv::new(opts);
  let stream = "iter_ids_truncate";

  for i in 0..30u8 {
    env.wal.append(stream, &[i]).unwrap();
  }

  env.wal.truncate(stream, 15).unwrap();

  // Segment 0 is gone; iterate from the first surviving segment.
  let mut iter = env.wal.iter_with_ids(stream, 10).unwrap();

  for i in 10..30u64 {
    let (id, entry) = iter.next().expect("Iterator stopped early").unwrap();
    assert_eq!(id, i);
    assert_eq!(entry, vec![i as u8]);
  }

  assert!(iter.next().is_none());
}

#[test]
fn test_iter_from_a_truncated_id_starts_at_the_first_retained_entry() {
  let mut opts = WalOptions::default();
  opts.max_segment_size = u64::MAX;
  opts.max_entries_per_segment = 10;
  let env = TestEnv::new(opts.clone());
  let stream = "iter_truncated_start";

  for i in 0..30u8 {
    env.wal.append(stream, &[i]).unwrap();
  }
  assert_eq!(env.wal.truncate(stream, 15).unwrap(), 1);

  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 0).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (10..30).collect::<Vec<_>>(), "a start id below the first retained segment begins at the first retained entry");

  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 5).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (10..30).collect::<Vec<_>>(), "a start id inside a freed segment behaves the same");

  let entries: Vec<Vec<u8>> = env.wal.iter(stream, 0).unwrap().map(|e| e.unwrap()).collect();
  assert_eq!(entries, (10..30u8).map(|i| vec![i]).collect::<Vec<_>>(), "the plain iterator agrees");

  // two segments freed; a fresh handle on the same files gives the same answer
  assert_eq!(env.wal.truncate(stream, 25).unwrap(), 1);
  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 0).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (20..30).collect::<Vec<_>>());
  let mut reopen = opts;
  reopen.root_path = env.root.clone();
  let reopened = ironwal::Wal::new(reopen).unwrap();
  let ids: Vec<u64> = reopened.iter_with_ids(stream, 3).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (20..30).collect::<Vec<_>>(), "a reopened wal resolves the start the same way");

  // an exact retained start and a start past the end are unchanged
  let ids: Vec<u64> = env.wal.iter_with_ids(stream, 20).unwrap().map(|e| e.unwrap().0).collect();
  assert_eq!(ids, (20..30).collect::<Vec<_>>());
  assert!(env.wal.iter_with_ids(stream, 30).unwrap().next().is_none());
  assert!(env.wal.get(stream, 5).unwrap().is_none(), "a freed id still reads as gone");
}

#[test]
fn test_iter_with_ids_empty_stream() {
  let env = TestEnv::with_default();
  let mut iter = env.wal.iter_with_ids("non_existent", 0).unwrap();
  assert!(iter.next().is_none());
}
