mod common;
use common::TestEnv;
use ironwal::WalOptions;

const SEGMENT_BYTES: u64 = 1024;

fn options() -> WalOptions {
  WalOptions::default().max_segment_size(SEGMENT_BYTES)
}

fn segment_sizes(env: &TestEnv, stream: &str) -> Vec<u64> {
  let mut sizes: Vec<(String, u64)> = std::fs::read_dir(env.root.join(stream))
    .unwrap()
    .flatten()
    .filter(|entry| entry.path().extension().and_then(|text| text.to_str()) == Some("wal"))
    .map(|entry| (entry.file_name().to_string_lossy().into_owned(), entry.metadata().unwrap().len()))
    .collect();
  sizes.sort();
  sizes.into_iter().map(|(_, size)| size).collect()
}

#[test]
fn single_entry_batches_never_grow_a_segment_past_its_limit() {
  let env = TestEnv::new(options());
  for _ in 0..50 {
    env.wal.append_batch("one", &[&[1u8; 100]]).unwrap();
  }
  let sizes = segment_sizes(&env, "one");
  assert!(sizes.len() > 1, "50 entries of 100 bytes stayed in one segment of {} bytes", sizes[0]);
  assert!(sizes.iter().all(|&size| size <= SEGMENT_BYTES), "segment sizes {sizes:?} for a limit of {SEGMENT_BYTES}");
  let read: Vec<Vec<u8>> = env.wal.iter("one", 0).unwrap().map(Result::unwrap).collect();
  assert_eq!(read, vec![vec![1u8; 100]; 50]);
}

#[test]
fn batches_of_several_entries_never_grow_a_segment_past_its_limit() {
  let env = TestEnv::new(options());
  for batch in 0..30u8 {
    let entries = [[batch; 100], [batch; 100], [batch; 100]];
    let refs: Vec<&[u8]> = entries.iter().map(|entry| entry.as_slice()).collect();
    env.wal.append_batch("three", &refs).unwrap();
  }
  let sizes = segment_sizes(&env, "three");
  assert!(sizes.iter().all(|&size| size <= SEGMENT_BYTES), "segment sizes {sizes:?} for a limit of {SEGMENT_BYTES}");
  let read: Vec<Vec<u8>> = env.wal.iter("three", 0).unwrap().map(Result::unwrap).collect();
  assert_eq!(read.len(), 90);
  assert_eq!(read[89], vec![29u8; 100]);
}

#[test]
fn truncate_frees_segments_written_by_single_entry_batches() {
  let env = TestEnv::new(options());
  for at in 0..50u8 {
    env.wal.append_batch("freed", &[&[at; 100]]).unwrap();
  }
  let freed = env.wal.truncate("freed", 40).unwrap();
  assert!(freed > 0, "truncate freed nothing");
  assert_eq!(env.wal.get("freed", 0).unwrap(), None);
  assert_eq!(env.wal.get("freed", 45).unwrap(), Some(vec![45u8; 100]));
}

#[test]
fn an_entry_larger_than_a_segment_lands_alone_and_the_next_one_starts_a_new_segment() {
  let env = TestEnv::new(options());
  env.wal.append_batch("large", &[&[1u8; 10]]).unwrap();
  env.wal.append_batch("large", &[&[2u8; 2000]]).unwrap();
  env.wal.append_batch("large", &[&[3u8; 10]]).unwrap();
  assert_eq!(env.wal.get("large", 1).unwrap(), Some(vec![2u8; 2000]));
  assert_eq!(env.wal.get("large", 2).unwrap(), Some(vec![3u8; 10]));
  let sizes = segment_sizes(&env, "large");
  assert_eq!(sizes.len(), 3, "segment sizes {sizes:?}");
  assert!(sizes[1] > SEGMENT_BYTES && sizes[0] <= SEGMENT_BYTES && sizes[2] <= SEGMENT_BYTES);
}
