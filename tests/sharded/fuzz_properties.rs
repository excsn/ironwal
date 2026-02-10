//! Property-based fuzzing tests for ShardedWal

use ironwal::sharded::ShardedWal;
use ironwal::{SyncMode, WalOptions};
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

// Model: HashMap<user_key, Vec<data>>
// We track what SHOULD be in each shard based on deterministic routing
type Model = HashMap<Vec<u8>, Vec<Vec<u8>>>;

#[derive(Debug, Clone)]
enum Action {
  Append { key: Vec<u8>, data: Vec<u8> },
  AppendBatch { entries: Vec<(Vec<u8>, Vec<u8>)> },
  CreateCheckpoint { checkpoint_id: Vec<u8> },
  Restart,
}

fn action_strategy() -> impl Strategy<Value = Action> {
  prop_oneof![
    // Append - common operation
    5 => (prop::collection::vec(any::<u8>(), 1..32),
      prop::collection::vec(any::<u8>(), 0..128))
    .prop_map(|(k, d)| Action::Append { key: k, data: d }),

    // Batch - common
    3 => prop::collection::vec(
      (prop::collection::vec(any::<u8>(), 1..32),
        prop::collection::vec(any::<u8>(), 0..64)),
      1..10
    ).prop_map(|entries| Action::AppendBatch { entries }),

    // Checkpoint - less common but important
    1 => prop::collection::vec(any::<u8>(), 1..32)
      .prop_map(|id| Action::CreateCheckpoint { checkpoint_id: id }),

    // Restart - rare but critical
    1 => Just(Action::Restart),
  ]
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(30))]

  #[test]
  fn fuzz_sharded_consistency(actions in prop::collection::vec(action_strategy(), 1..50)) {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let mut opts = WalOptions::new(&root);
    opts.sync_mode = SyncMode::BatchOnly;
    opts.max_entries_per_segment = 5; // Force rotation

    let mut wal = ShardedWal::new(opts.clone(), 4).unwrap();
    let mut model: Model = HashMap::new();
    let mut checkpoint_models: HashMap<Vec<u8>, (Model, Vec<u64>)> = HashMap::new();

    for action in actions {
      match action {
        Action::Append { key, data } => {
          // Apply to system
          let (shard_id, seq_id) = wal.append(&key, &data).unwrap();

          // Apply to model
          model.entry(key.clone()).or_default().push(data.clone());

          // Verify routing is deterministic
          let (shard_id2, _) = wal.append(&key, b"test").unwrap();
          model.entry(key.clone()).or_default().push(b"test".to_vec());
          assert_eq!(shard_id, shard_id2, "Same key must route to same shard");
        }

        Action::AppendBatch { entries } => {
          if entries.is_empty() {
            continue;
          }

          // Apply to system
          let results = wal.append_batch(&entries).unwrap();
          assert_eq!(results.len(), entries.len());

          // Apply to model
          for (key, data) in &entries {
            model.entry(key.clone()).or_default().push(data.clone());
          }

          // Verify routing consistency
          for ((key, _), (shard_id, _)) in entries.iter().zip(results.iter()) {
            let (shard_id2, _) = wal.append(key, b"verify").unwrap();
            model.entry(key.clone()).or_default().push(b"verify".to_vec());
            assert_eq!(*shard_id, shard_id2, "Routing must be consistent");
          }
        }

        Action::CreateCheckpoint { checkpoint_id } => {
          // Create checkpoint in system
          if let Ok(()) = wal.create_checkpoint(&checkpoint_id) {
            // Snapshot current model state
            let checkpoint_data = wal.load_checkpoint(&checkpoint_id).unwrap();
            checkpoint_models.insert(
              checkpoint_id.clone(),
              (model.clone(), checkpoint_data.offsets.clone())
            );
          }
        }

        Action::Restart => {
          drop(wal);
          wal = ShardedWal::new(opts.clone(), 4).unwrap();
          // Model remains unchanged - data should persist
        }
      }
    }

    // Final verification: read all data and compare with model
    let mut read_model: Model = HashMap::new();

    for shard_id in 0..4 {
      let mut iter = wal.iter_shard(shard_id, 0).unwrap();
      while let Some(entry) = iter.next() {
        let data = entry.unwrap();
        // We can't easily reverse-map data to keys without tracking writes,
        // so we just verify total count matches
        read_model.entry(vec![shard_id as u8]).or_default().push(data);
      }
    }

    // Count total entries in model vs read
    let model_count: usize = model.values().map(|v| v.len()).sum();
    let read_count: usize = read_model.values().map(|v| v.len()).sum();

    assert_eq!(model_count, read_count, "Entry count mismatch after operations");

    // Verify checkpoints are loadable
    for checkpoint_id in checkpoint_models.keys() {
      let result = wal.load_checkpoint(checkpoint_id);
      assert!(result.is_ok(), "Checkpoint {:?} should be loadable",
              String::from_utf8_lossy(checkpoint_id));
    }
  }
}
