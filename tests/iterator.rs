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
