use ironwal::{ReadStrategy, SyncMode, Wal, WalOptions};
use std::path::Path;

const STREAM: &str = "s";
const ENTRY: [u8; 100] = [7u8; 100];

fn options(root: &Path, sync_mode: SyncMode, buffer_appends: bool) -> WalOptions {
  WalOptions { root_path: root.to_path_buf(), sync_mode, buffer_appends, ..WalOptions::default() }
}

fn bytes_on_disk(root: &Path, segment_start_id: u64) -> u64 {
  std::fs::metadata(root.join(STREAM).join(format!("{segment_start_id:020}.wal"))).map_or(0, |m| m.len())
}

fn entries(count: usize) -> Vec<Vec<u8>> {
  (0..count).map(|n| vec![n as u8; 100]).collect()
}

#[test]
fn a_buffered_append_reaches_the_file_only_when_the_write_buffer_fills() {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(options(dir.path(), SyncMode::Async, true)).unwrap();
  wal.append(STREAM, &ENTRY).unwrap();
  wal.append_batch(STREAM, &[&ENTRY, &ENTRY]).unwrap();
  assert_eq!(bytes_on_disk(dir.path(), 0), 0, "buffered appends were written before the buffer filled");

  let big = vec![1u8; WalOptions::default().write_buffer_size];
  wal.append(STREAM, &big).unwrap();
  assert!(bytes_on_disk(dir.path(), 0) > big.len() as u64, "a full buffer was not written");
}

#[test]
fn an_unbuffered_async_append_reaches_the_file_before_it_returns() {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(options(dir.path(), SyncMode::Async, false)).unwrap();
  wal.append(STREAM, &ENTRY).unwrap();
  let after_one = bytes_on_disk(dir.path(), 0);
  assert!(after_one > 0, "an unbuffered append was not written");
  wal.append_batch(STREAM, &[&ENTRY, &ENTRY]).unwrap();
  assert!(bytes_on_disk(dir.path(), 0) > after_one, "an unbuffered batch was not written");
}

#[test]
fn buffering_is_ignored_outside_async() {
  for sync_mode in [SyncMode::Strict, SyncMode::BatchOnly] {
    let dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(options(dir.path(), sync_mode, true)).unwrap();
    wal.append_batch(STREAM, &[&ENTRY, &ENTRY]).unwrap();
    assert!(bytes_on_disk(dir.path(), 0) > 0, "{sync_mode:?} left a batch in the buffer");
  }
}

fn reads_see_buffered_appends(read_strategy: ReadStrategy) {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(WalOptions { read_strategy, ..options(dir.path(), SyncMode::Async, true) }).unwrap();
  let written = entries(5);
  wal.append(STREAM, &written[0]).unwrap();
  assert_eq!(wal.get(STREAM, 0).unwrap().as_deref(), Some(written[0].as_slice()), "get missed a buffered append");

  let rest: Vec<&[u8]> = written[1..].iter().map(Vec::as_slice).collect();
  wal.append_batch(STREAM, &rest).unwrap();
  let batch = wal.get_batch(STREAM, &[1, 2, 3, 4]).unwrap();
  assert_eq!(batch, written[1..].iter().cloned().map(Some).collect::<Vec<_>>(), "get_batch missed buffered appends");

  wal.append(STREAM, &ENTRY).unwrap();
  let iterated: Vec<Vec<u8>> = wal.iter(STREAM, 0).unwrap().map(Result::unwrap).collect();
  assert_eq!(iterated.len(), 6, "iter missed buffered appends");

  wal.append(STREAM, &ENTRY).unwrap();
  let ids: Vec<u64> = wal.iter_with_ids(STREAM, 0).unwrap().map(|r| r.unwrap().0).collect();
  assert_eq!(ids, (0..7).collect::<Vec<_>>(), "iter_with_ids missed buffered appends");
}

#[test]
fn reads_with_standard_io_see_every_buffered_append_that_returned() {
  reads_see_buffered_appends(ReadStrategy::StandardIo);
}

#[test]
fn reads_with_mmap_see_every_buffered_append_that_returned() {
  reads_see_buffered_appends(ReadStrategy::Mmap);
}

#[test]
fn buffered_appends_on_both_sides_of_a_segment_roll_read_back() {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(WalOptions { max_segment_size: 1024, ..options(dir.path(), SyncMode::Async, true) }).unwrap();
  let written = entries(40);
  for entry in &written[..20] {
    wal.append(STREAM, entry).unwrap();
  }
  let rest: Vec<&[u8]> = written[20..].iter().map(Vec::as_slice).collect();
  wal.append_batch(STREAM, &rest).unwrap();
  assert!(wal.current_segment_start_id(STREAM).unwrap() > 0, "the appends did not roll a segment");

  let read: Vec<Vec<u8>> = wal.iter(STREAM, 0).unwrap().map(Result::unwrap).collect();
  assert!(read == written, "entries across a segment roll read back differently");
  assert_eq!(wal.get(STREAM, 3).unwrap().as_deref(), Some(written[3].as_slice()));
}

#[test]
fn buffered_appends_survive_their_segment_leaving_the_write_cache() {
  let dir = tempfile::tempdir().unwrap();
  let wal = Wal::new(WalOptions { max_open_segments: 1, ..options(dir.path(), SyncMode::Async, true) }).unwrap();
  let written = entries(200);
  for round in 0..100 {
    wal.append(STREAM, &written[round * 2]).unwrap();
    wal.append("other", &ENTRY).unwrap();
    assert_eq!(wal.get(STREAM, (round * 2) as u64).unwrap().as_deref(), Some(written[round * 2].as_slice()), "round {round}: a read after eviction missed a buffered append");
    wal.append(STREAM, &written[round * 2 + 1]).unwrap();
    wal.append("other", &ENTRY).unwrap();
  }
  let read: Vec<Vec<u8>> = wal.iter(STREAM, 0).unwrap().map(Result::unwrap).collect();
  assert!(read == written, "appends to a segment reopened after eviction read back differently");
  drop(wal);
  let wal = Wal::new(options(dir.path(), SyncMode::Async, true)).unwrap();
  let reopened: Vec<Vec<u8>> = wal.iter(STREAM, 0).unwrap().map(Result::unwrap).collect();
  assert!(reopened == written, "after a reopen, appends made across evictions read back differently");
}

#[test]
fn a_dropped_wal_writes_its_buffered_appends() {
  let dir = tempfile::tempdir().unwrap();
  let written = entries(10);
  {
    let wal = Wal::new(options(dir.path(), SyncMode::Async, true)).unwrap();
    let batch: Vec<&[u8]> = written.iter().map(Vec::as_slice).collect();
    wal.append_batch(STREAM, &batch).unwrap();
  }
  let wal = Wal::new(options(dir.path(), SyncMode::Async, true)).unwrap();
  let read: Vec<Vec<u8>> = wal.iter(STREAM, 0).unwrap().map(Result::unwrap).collect();
  assert!(read == written, "entries buffered before the WAL was dropped were lost");
}
