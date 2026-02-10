//! Key-based routing to determine shard assignment.
//!
//! Uses SipHash-1-3 for fast, uniform distribution across shards.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Routes keys to shard IDs via consistent hashing.
pub struct Router {
  shard_count: u16,
}

impl Router {
  /// Creates a new router with the specified shard count.
  ///
  /// # Panics
  ///
  /// Panics if `shard_count` is zero.
  pub fn new(shard_count: u16) -> Self {
    assert!(shard_count > 0, "shard_count must be greater than zero");
    Self { shard_count }
  }

  /// Routes a key to its assigned shard ID.
  ///
  /// # Returns
  ///
  /// A shard ID in the range `0..shard_count`.
  ///
  /// # Determinism
  ///
  /// The same key will always route to the same shard.
  #[inline]
  pub fn route(&self, key: &[u8]) -> u16 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % self.shard_count as u64) as u16
  }

  /// Generates the stream name for a given shard ID.
  ///
  /// Format: `"shard_00"`, `"shard_01"`, etc.
  ///
  /// # Panics
  ///
  /// Panics if `shard_id >= shard_count`.
  pub fn shard_name(&self, shard_id: u16) -> String {
    assert!(
      shard_id < self.shard_count,
      "shard_id {} out of range (max: {})",
      shard_id,
      self.shard_count - 1
    );
    format!("shard_{:02}", shard_id)
  }

  /// Returns the total number of shards.
  pub fn shard_count(&self) -> u16 {
    self.shard_count
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::HashSet;

  #[test]
  fn test_deterministic_routing() {
    let router = Router::new(16);
    let key = b"user_123";

    let shard1 = router.route(key);
    let shard2 = router.route(key);
    let shard3 = router.route(key);

    assert_eq!(shard1, shard2);
    assert_eq!(shard2, shard3);
  }

  #[test]
  fn test_routes_within_range() {
    let router = Router::new(16);

    for i in 0..1000 {
      let key = format!("key_{}", i);
      let shard = router.route(key.as_bytes());
      assert!(shard < 16, "shard {} out of range", shard);
    }
  }

  #[test]
  fn test_uniform_distribution() {
    let router = Router::new(16);
    let mut counts = vec![0usize; 16];

    // Route 10,000 keys
    for i in 0..10_000 {
      let key = format!("key_{}", i);
      let shard = router.route(key.as_bytes());
      counts[shard as usize] += 1;
    }

    // Each shard should get roughly 625 ± 100 keys
    for (shard_id, count) in counts.iter().enumerate() {
      assert!(
        *count > 500 && *count < 750,
        "Shard {} has uneven distribution: {} keys",
        shard_id,
        count
      );
    }
  }

  #[test]
  fn test_shard_name_format() {
    let router = Router::new(100);

    assert_eq!(router.shard_name(0), "shard_00");
    assert_eq!(router.shard_name(9), "shard_09");
    assert_eq!(router.shard_name(10), "shard_10");
    assert_eq!(router.shard_name(99), "shard_99");
  }

  #[test]
  fn test_different_keys_may_collide() {
    let router = Router::new(4); // Small shard count for collisions

    let mut assigned_shards = HashSet::new();

    for i in 0..100 {
      let key = format!("key_{}", i);
      let shard = router.route(key.as_bytes());
      assigned_shards.insert(shard);
    }

    // With 100 keys and 4 shards, all shards should be used
    assert_eq!(assigned_shards.len(), 4);
  }

  #[test]
  #[should_panic(expected = "shard_count must be greater than zero")]
  fn test_panics_on_zero_shards() {
    Router::new(0);
  }

  #[test]
  #[should_panic(expected = "out of range")]
  fn test_shard_name_panics_on_invalid_id() {
    let router = Router::new(16);
    router.shard_name(16); // Should panic
  }

  // Router distribution and edge case tests
  #[test]
  fn test_single_shard_routes_everything_to_zero() {
    let router = Router::new(1);

    for i in 0..100 {
      let key = format!("key_{}", i);
      assert_eq!(router.route(key.as_bytes()), 0);
    }
  }

  #[test]
  fn test_power_of_two_shards() {
    for &shard_count in &[2, 4, 8, 16, 32, 64, 128, 256] {
      let router = Router::new(shard_count);
      let key = b"test_key";
      let shard = router.route(key);
      assert!(shard < shard_count);
    }
  }

  #[test]
  fn test_non_power_of_two_shards() {
    for &shard_count in &[3, 5, 7, 13, 17, 100] {
      let router = Router::new(shard_count);
      let key = b"test_key";
      let shard = router.route(key);
      assert!(shard < shard_count);
    }
  }
}
