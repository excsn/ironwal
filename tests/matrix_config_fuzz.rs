use ironwal::{CompressionType, ReadStrategy, SyncMode, Wal, WalOptions};
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

type Model = HashMap<String, Vec<Vec<u8>>>;

#[derive(Debug, Clone)]
enum Action {
  Append { stream_id: u8, data: Vec<u8> },
  AppendBatch { stream_id: u8, batch: Vec<Vec<u8>> },
  Restart,
}

fn action_strategy() -> impl Strategy<Value = Action> {
  prop_oneof![
    4 => (0..3u8, prop::collection::vec(any::<u8>(), 0..128))
      .prop_map(|(s, d)| Action::Append { stream_id: s, data: d }),
    4 => (
        0..3u8,
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..5),
      )
      .prop_map(|(s, b)| Action::AppendBatch { stream_id: s, batch: b }),
    1 => Just(Action::Restart),
  ]
}

fn sync_mode_strategy() -> impl Strategy<Value = SyncMode> {
  prop_oneof![
    Just(SyncMode::Strict),
    Just(SyncMode::BatchOnly),
    Just(SyncMode::Async),
  ]
}

fn read_strategy_strategy() -> impl Strategy<Value = ReadStrategy> {
  prop_oneof![
    Just(ReadStrategy::StandardIo),
    Just(ReadStrategy::Mmap),
  ]
}

/// Builds the set of available compression variants at compile time so the
/// strategy works correctly regardless of whether the `compression` feature
/// is enabled.
fn compression_variants() -> Vec<CompressionType> {
  let mut v = vec![CompressionType::None];
  #[cfg(feature = "compression")]
  v.push(CompressionType::Lz4);
  v
}

fn compression_strategy() -> impl Strategy<Value = CompressionType> {
  proptest::sample::select(compression_variants())
}

/// Strategy that yields a full WalOptions configuration tuple.
/// Segment size and entry-count limits are deliberately tiny to provoke
/// constant segment rotation and index updates.
fn options_strategy() -> impl Strategy<Value = (SyncMode, ReadStrategy, CompressionType, u64, u64)> {
  (
    sync_mode_strategy(),
    read_strategy_strategy(),
    compression_strategy(),
    64u64..8192,  // max_segment_size  – tiny to force frequent rotation
    1u64..20,     // max_entries_per_segment
  )
}

fn stream_name(id: u8) -> String {
  format!("matrix_stream_{id}")
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(50))]

  #[test]
  fn fuzz_wal_matrix_consistency(
    (sync_mode, read_strategy, compression, max_seg_size, max_entries) in options_strategy(),
    actions in prop::collection::vec(action_strategy(), 1..60),
  ) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let mut opts = WalOptions::new(&root);
    opts.sync_mode        = sync_mode;
    opts.read_strategy    = read_strategy;
    opts.compression      = compression;
    opts.max_segment_size = max_seg_size;
    opts.max_entries_per_segment = max_entries;
    opts.min_compression_size    = 32; // compress small payloads too

    let mut wal = Wal::new(opts.clone()).unwrap();
    let mut model: Model = HashMap::new();

    for action in actions {
      match action {
        Action::Append { stream_id, data } => {
          let name = stream_name(stream_id);
          let stream_data = model.entry(name.clone()).or_default();
          let expected_id = stream_data.len() as u64;
          stream_data.push(data.clone());

          let id = wal.append(&name, &data).unwrap();
          prop_assert_eq!(id, expected_id, "ID mismatch in stream {}", name);
        }

        Action::AppendBatch { stream_id, batch } => {
          let name = stream_name(stream_id);
          let stream_data = model.entry(name.clone()).or_default();
          let start_id = stream_data.len() as u64;
          let count    = batch.len() as u64;
          stream_data.extend(batch.clone());

          let refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
          let range = wal.append_batch(&name, &refs).unwrap();
          prop_assert_eq!(
            range,
            start_id..(start_id + count),
            "Batch range mismatch in stream {}",
            name
          );
        }

        Action::Restart => {
          drop(wal);
          wal = Wal::new(opts.clone()).unwrap();
        }
      }

      // After every action, spot-check the tail entry in each stream.
      for i in 0..3u8 {
        let name = stream_name(i);
        if let Some(model_data) = model.get(&name) {
          if !model_data.is_empty() {
            let last_idx = model_data.len() as u64 - 1;
            let val = wal.get(&name, last_idx).unwrap();
            prop_assert_eq!(
              val.as_ref(),
              Some(&model_data[last_idx as usize]),
              "Tail mismatch in stream {} with opts {:?}",
              name,
              opts
            );
          }
        }
      }
    }

    // Full end-to-end verification via the sequential iterator.
    for (name, data) in &model {
      let mut iter = wal.iter(name, 0).unwrap();
      let mut count = 0usize;

      for (i, expected_item) in data.iter().enumerate() {
        let item = iter
          .next()
          .unwrap_or_else(|| panic!("Iterator ended early at index {i} in stream {name}"))
          .unwrap();
        prop_assert_eq!(
          &item,
          expected_item,
          "Entry mismatch at index {} in stream {}",
          i,
          name
        );
        count += 1;
      }

      prop_assert!(
        iter.next().is_none(),
        "WAL has extra entries beyond model in stream {}",
        name
      );
      prop_assert_eq!(
        count,
        data.len(),
        "Entry count mismatch in stream {}",
        name
      );
    }
  }
}
