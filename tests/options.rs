use ironwal::{CompressionType, CorruptionPolicy, ReadStrategy, SyncMode, WalOptions};
use std::path::PathBuf;

#[test]
fn each_setter_sets_its_field_and_leaves_the_rest_at_their_defaults() {
  let defaults = WalOptions::new("/data/wal");
  let options = WalOptions::new("/data/wal")
    .root_path("/data/other")
    .max_segment_size(1)
    .max_entries_per_segment(2)
    .max_open_segments(3)
    .max_open_read_segments(4)
    .write_buffer_size(5)
    .read_buffer_size(6)
    .block_cache_size(7)
    .sync_mode(SyncMode::Async)
    .buffer_appends(true)
    .compression(CompressionType::None)
    .read_strategy(ReadStrategy::Mmap)
    .on_corruption(CorruptionPolicy::Error)
    .min_compression_size(8)
    .archive_path("/data/archive");

  assert_eq!(options.root_path, PathBuf::from("/data/other"));
  assert_eq!(options.max_segment_size, 1);
  assert_eq!(options.max_entries_per_segment, 2);
  assert_eq!(options.max_open_segments, 3);
  assert_eq!(options.max_open_read_segments, 4);
  assert_eq!(options.write_buffer_size, 5);
  assert_eq!(options.read_buffer_size, 6);
  assert_eq!(options.block_cache_size, Some(7));
  assert_eq!(options.sync_mode, SyncMode::Async);
  assert!(options.buffer_appends);
  assert_eq!(options.compression, CompressionType::None);
  assert_eq!(options.read_strategy, ReadStrategy::Mmap);
  assert_eq!(options.on_corruption, CorruptionPolicy::Error);
  assert_eq!(options.min_compression_size, 8);
  assert_eq!(options.archive_path, Some(PathBuf::from("/data/archive")));

  let one = WalOptions::new("/data/wal").max_segment_size(1);
  assert_eq!(one.max_entries_per_segment, defaults.max_entries_per_segment);
  assert_eq!(one.sync_mode, defaults.sync_mode);
  assert_eq!(one.buffer_appends, defaults.buffer_appends);
  assert_eq!(one.block_cache_size, defaults.block_cache_size);
  assert_eq!(one.archive_path, defaults.archive_path);
}
