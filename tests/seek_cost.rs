use ironwal::{ReadStrategy, SyncMode, Wal, WalOptions};
use std::time::{Duration, Instant};

const FRAMES: u64 = 100_000;

fn options(root: &std::path::Path, read_strategy: ReadStrategy) -> WalOptions {
  WalOptions::default().root_path(root.to_path_buf()).sync_mode(SyncMode::Async).max_segment_size(u64::MAX / 4).read_strategy(read_strategy)
}

fn open_seek_and_get(root: &std::path::Path, read_strategy: ReadStrategy) -> Duration {
  let started = Instant::now();
  let wal = Wal::new(options(root, read_strategy)).unwrap();
  let mut iter = wal.iter_with_ids("tiny", FRAMES - 1).unwrap();
  assert_eq!(iter.next().unwrap().unwrap().0, FRAMES - 1);
  assert_eq!(wal.get("tiny", FRAMES - 1).unwrap(), Some(vec![9u8; 24]));
  started.elapsed()
}

#[test]
fn buffered_reads_walk_a_segment_of_many_small_frames_about_as_fast_as_mmap() {
  let dir = tempfile::tempdir().unwrap();
  {
    let wal = Wal::new(options(dir.path(), ReadStrategy::StandardIo)).unwrap();
    for _ in 0..FRAMES {
      wal.append("tiny", &[9u8; 24]).unwrap();
    }
  }
  let mapped = (0..3).map(|_| open_seek_and_get(dir.path(), ReadStrategy::Mmap)).min().unwrap();
  let buffered = (0..3).map(|_| open_seek_and_get(dir.path(), ReadStrategy::StandardIo)).min().unwrap();
  assert!(buffered < mapped * 5, "recovery, an iterator seek and a get over {FRAMES} frames took {buffered:?} buffered against {mapped:?} mapped");
}
