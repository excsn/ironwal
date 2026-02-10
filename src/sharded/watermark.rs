//! Durable offset tracking for checkpoint consistency.
//!
//! Watermarks track the highest durable (fsynced) WAL offset per shard.
//! This prevents checkpoints from referencing data that hasn't been flushed yet.

use crate::config::SyncMode;
use crate::error::Result;
use crate::sharded::router::Router;
use crate::wal::Wal;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks durable offsets for all shards.
pub struct WatermarkTracker {
  watermarks: Vec<AtomicU64>,
  sync_mode: SyncMode,
}

impl WatermarkTracker {
  /// Creates a new watermark tracker.
  ///
  /// All watermarks are initialized to zero.
  pub fn new(shard_count: u16, sync_mode: SyncMode) -> Self {
    let watermarks = (0..shard_count).map(|_| AtomicU64::new(0)).collect();

    Self {
      watermarks,
      sync_mode,
    }
  }

  /// Updates the watermark for a shard if appropriate for the sync mode.
  ///
  /// # Behavior by SyncMode
  ///
  /// - `Strict`: Always updates (data is durable immediately)
  /// - `BatchOnly`: Updates only during batch commits (caller's responsibility)
  /// - `Async`: No automatic updates (requires manual flush tracking)
  pub fn update(&self, shard_id: u16, offset: u64) {
    if self.sync_mode == SyncMode::Strict {
      self.watermarks[shard_id as usize].store(offset, Ordering::Release);
    }
    // For BatchOnly and Async, caller must explicitly call set_durable
  }

  /// Forcibly sets a watermark (used for batch commits and manual updates).
  pub fn set_durable(&self, shard_id: u16, offset: u64) {
    self.watermarks[shard_id as usize].store(offset, Ordering::Release);
  }

  /// Gets the current durable offset for a shard.
  pub fn get(&self, shard_id: u16) -> u64 {
    self.watermarks[shard_id as usize].load(Ordering::Acquire)
  }

  /// Takes a consistent snapshot of all watermarks.
  ///
  /// Returns a Vec where index = shard_id.
  pub fn snapshot(&self) -> Vec<u64> {
    self
      .watermarks
      .iter()
      .map(|w| w.load(Ordering::Acquire))
      .collect()
  }

  /// Initializes watermarks by querying the WAL for each shard's state.
  ///
  /// This is called during startup to determine the current durable position.
  pub fn initialize_from_wal(&self, wal: &Wal, router: &Router) -> Result<()> {
    for shard_id in 0..router.shard_count() {
      let stream = router.shard_name(shard_id);

      // Get the next ID that will be written (current durable position)
      if let Some(state) = wal.get_stream_state(&stream)? {
        self.set_durable(shard_id, state.next_id);
      }
      // If stream doesn't exist yet, watermark stays at 0
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_initial_watermarks_are_zero() {
    let tracker = WatermarkTracker::new(16, SyncMode::Strict);

    for shard_id in 0..16 {
      assert_eq!(tracker.get(shard_id), 0);
    }
  }

  #[test]
  fn test_update_in_strict_mode() {
    let tracker = WatermarkTracker::new(4, SyncMode::Strict);

    tracker.update(0, 100);
    tracker.update(2, 200);

    assert_eq!(tracker.get(0), 100);
    assert_eq!(tracker.get(1), 0); // Untouched
    assert_eq!(tracker.get(2), 200);
  }

  #[test]
  fn test_update_ignored_in_batch_mode() {
    let tracker = WatermarkTracker::new(4, SyncMode::BatchOnly);

    tracker.update(0, 100);

    // Should NOT update in BatchOnly mode
    assert_eq!(tracker.get(0), 0);

    // But set_durable works
    tracker.set_durable(0, 100);
    assert_eq!(tracker.get(0), 100);
  }

  #[test]
  fn test_snapshot_consistency() {
    let tracker = WatermarkTracker::new(4, SyncMode::Strict);

    tracker.update(0, 10);
    tracker.update(1, 20);
    tracker.update(2, 30);
    tracker.update(3, 40);

    let snapshot = tracker.snapshot();

    assert_eq!(snapshot, vec![10, 20, 30, 40]);
  }

  #[test]
  fn test_concurrent_updates() {
    use std::sync::Arc;
    use std::thread;

    let tracker = Arc::new(WatermarkTracker::new(4, SyncMode::Strict));
    let mut handles = vec![];

    for shard_id in 0..4 {
      let tracker_clone = tracker.clone();
      handles.push(thread::spawn(move || {
        for i in 0..100 {
          tracker_clone.update(shard_id, i);
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    // All shards should have value 99 (last update)
    for shard_id in 0..4 {
      assert_eq!(tracker.get(shard_id), 99);
    }
  }

  #[test]
  fn test_snapshot_is_point_in_time() {
    let tracker = WatermarkTracker::new(4, SyncMode::Strict);

    tracker.update(0, 100);
    let snap1 = tracker.snapshot();

    tracker.update(0, 200);
    let snap2 = tracker.snapshot();

    assert_eq!(snap1[0], 100);
    assert_eq!(snap2[0], 200);
  }

  #[test]
  fn test_large_shard_count() {
    let tracker = WatermarkTracker::new(256, SyncMode::Strict);

    for i in 0..256 {
      tracker.update(i, i as u64 * 1000);
    }

    let snapshot = tracker.snapshot();
    assert_eq!(snapshot.len(), 256);

    for i in 0..256 {
      assert_eq!(snapshot[i as usize], i as u64 * 1000);
    }
  }
}
