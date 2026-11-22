mod common;
use common::TestEnv;
use ironwal::{SyncMode, WalOptions};
use std::sync::{Arc, Barrier};
use std::thread;

fn options(sync_mode: SyncMode) -> WalOptions {
  WalOptions::default().sync_mode(sync_mode)
}

#[test]
fn concurrent_batches_on_one_stream_share_fsyncs_and_all_land() {
  let env = TestEnv::new(options(SyncMode::BatchOnly));
  let threads = 8;
  let batches = 50;
  let barrier = Arc::new(Barrier::new(threads));
  let writers: Vec<_> = (0..threads)
    .map(|writer| {
      let (wal, barrier) = (env.wal.clone(), barrier.clone());
      thread::spawn(move || {
        barrier.wait();
        for batch in 0..batches {
          let entry = format!("{writer}-{batch}");
          wal.append_batch("shared", &[entry.as_bytes()]).unwrap();
        }
      })
    })
    .collect();
  for writer in writers {
    writer.join().unwrap();
  }
  let entries: Vec<Vec<u8>> = env.wal.iter("shared", 0).unwrap().map(Result::unwrap).collect();
  assert_eq!(entries.len(), threads * batches);
  let syncs = env.wal.syncs();
  assert!(syncs < (threads * batches) as u64, "{syncs} fsyncs for {} batches", threads * batches);
  assert!(syncs > 0);
}

#[test]
fn a_lone_writer_gets_one_fsync_per_batch() {
  let env = TestEnv::new(options(SyncMode::BatchOnly));
  env.wal.append_batch("alone", &[&[0]]).unwrap();
  let created = env.wal.syncs();
  for batch in 1..=10u8 {
    env.wal.append_batch("alone", &[&[batch]]).unwrap();
  }
  assert_eq!(env.wal.syncs() - created, 10);
}

#[test]
fn async_mode_issues_no_fsync() {
  let env = TestEnv::new(options(SyncMode::Async));
  for batch in 0..10u8 {
    env.wal.append_batch("relaxed", &[&[batch]]).unwrap();
  }
  assert_eq!(env.wal.syncs(), 0);
}

#[test]
fn a_batch_spanning_a_rotation_syncs_the_file_it_left() {
  let env = TestEnv::new(WalOptions::default().sync_mode(SyncMode::BatchOnly).max_segment_size(256));
  let entry = vec![7u8; 100];
  env.wal.append_batch("rotating", &[&entry, &entry, &entry, &entry]).unwrap();
  let entries: Vec<Vec<u8>> = env.wal.iter("rotating", 0).unwrap().map(Result::unwrap).collect();
  assert_eq!(entries.len(), 4);
  assert!(env.wal.syncs() >= 2, "the old segment and the new one are both synced: {}", env.wal.syncs());
}

#[test]
fn a_stream_written_again_after_deletion_still_waits_for_its_fsync() {
  let env = TestEnv::new(options(SyncMode::BatchOnly));
  for batch in 0..5u8 {
    env.wal.append_batch("again", &[&[batch]]).unwrap();
  }
  env.wal.delete_stream("again").unwrap();
  let before = env.wal.syncs();
  env.wal.append_batch("again", &[&[9]]).unwrap();
  assert_eq!(env.wal.syncs(), before + 2, "the recreated segment's directory and the batch are synced: ids restart at 0, so an old synced mark must not count");
}
