use ironwal::{SyncMode, Wal, WalOptions};
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

// --- The Model ---
// A simple reference implementation: Map<StreamName, List<Entries>>
type Model = HashMap<String, Vec<Vec<u8>>>;

// --- The Actions ---
#[derive(Debug, Clone)]
enum Action {
  Append { stream_id: u8, data: Vec<u8> },
  AppendBatch { stream_id: u8, batch: Vec<Vec<u8>> },
  Restart,
}

// --- The Strategy ---
// Generates random sequences of actions
fn action_strategy() -> impl Strategy<Value = Action> {
  prop_oneof![
    // 1. Append: Weight 4 (Common)
    4 => (0..3u8, prop::collection::vec(any::<u8>(), 0..128))
      .prop_map(|(s, d)| Action::Append { stream_id: s, data: d }),

    // 2. AppendBatch: Weight 4 (Common)
    4 => (0..3u8, prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..10))
      .prop_map(|(s, b)| Action::AppendBatch { stream_id: s, batch: b }),

    // 3. Restart: Weight 1 (Rare)
    // Simulates a DB restart to test persistence state.
    1 => Just(Action::Restart),
  ]
}

// --- The Helper ---
fn stream_name(id: u8) -> String {
  format!("stream_{}", id)
}

// --- The Test Execution ---
proptest! {
  // Run 50 random scenarios.
  // Each scenario consists of a sequence of 1 to 100 actions.
  #![proptest_config(ProptestConfig::with_cases(50))]

  #[test]
  fn fuzz_wal_consistency(actions in prop::collection::vec(action_strategy(), 1..100)) {
    // 1. Setup
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let mut opts = WalOptions::new(&root);
    opts.sync_mode = SyncMode::BatchOnly; // Speed up fuzzing
    // Force frequent rotation to stress index/segment logic
    opts.max_entries_per_segment = 5;
    opts.max_segment_size = u64::MAX;

    let mut wal = Wal::new(opts.clone()).unwrap();
    let mut model: Model = HashMap::new();

    // 2. Execute Actions
    for action in actions {
      match action {
        Action::Append { stream_id, data } => {
          let name = stream_name(stream_id);
          // Apply to Model
          let stream_data = model.entry(name.clone()).or_default();
          let expected_id = stream_data.len() as u64;
          stream_data.push(data.clone());

          // Apply to System
          let id = wal.append(&name, &data).unwrap();

          // Verify ID
          assert_eq!(id, expected_id, "Sequence ID mismatch for stream {}", name);
        },

        Action::AppendBatch { stream_id, batch } => {
          let name = stream_name(stream_id);
          // Apply to Model
          let stream_data = model.entry(name.clone()).or_default();
          let start_id = stream_data.len() as u64;
          let count = batch.len() as u64;
          stream_data.extend(batch.clone());

          // Apply to System
          let refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
          let range = wal.append_batch(&name, &refs).unwrap();

          // Verify Range
          assert_eq!(range, start_id..(start_id + count), "Batch range mismatch for {}", name);
        },

        Action::Restart => {
          // Close old WAL (drop)
          drop(wal);
          // Open new WAL on same directory
          wal = Wal::new(opts.clone()).unwrap();
        }
      }

      // 3. Continuous Verification (Random Sampling)
      // After every action, check a random entry in a random stream to ensure consistency.
      // Checking *everything* every step is too slow, so we check "something".
      // Note: In a real crash, we'd need to verify all, but proptest runs many iterations.

      for i in 0..3 {
          let name = stream_name(i);
          if let Some(model_data) = model.get(&name) {
              if !model_data.is_empty() {
                  // Check the LAST item (boundary condition)
                  let last_idx = model_data.len() as u64 - 1;
                  let val = wal.get(&name, last_idx).unwrap();
                  assert_eq!(val.as_ref(), Some(&model_data[last_idx as usize]), "Data mismatch at tip for {}", name);

                  // Check the FIRST item (old data persistence)
                  let first_val = wal.get(&name, 0).unwrap();
                  assert_eq!(first_val.as_ref(), Some(&model_data[0]), "Data mismatch at genesis for {}", name);
              }
          }
      }
    }

    // 4. Final Full Verification
    // At the end of the sequence, check EVERYTHING.
    for (name, data) in model {
      // Check count via iterator
      let mut iter = wal.iter(&name, 0).unwrap();
      let mut count = 0;
      for (i, expected_item) in data.iter().enumerate() {
        let wal_item = iter.next().expect("WAL ended prematurely").unwrap();
        assert_eq!(&wal_item, expected_item, "Mismatch at index {} in stream {}", i, name);
        count += 1;
      }
      assert!(iter.next().is_none(), "WAL has extra items in stream {}", name);
      assert_eq!(count, data.len(), "Total count mismatch for {}", name);
    }
  }
}
