use crate::state::StreamState;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

/// A thread-safe container for managing stream states.
///
/// Internally uses a sharded locking strategy (via RwLock) to allow
/// concurrent access to different streams.
pub(crate) struct StreamStateMap {
  // Map StreamKey -> Protected State
  inner: RwLock<HashMap<String, Arc<Mutex<StreamState>>>>,
}

impl StreamStateMap {
  /// Creates a new map, optionally populated with recovered states.
  pub fn new(initial_streams: HashMap<String, StreamState>) -> Self {
    let mut map = HashMap::new();
    for (k, v) in initial_streams {
      map.insert(k, Arc::new(Mutex::new(v)));
    }
    Self {
      inner: RwLock::new(map),
    }
  }

  /// Gets a handle to the stream's state, creating it if it doesn't exist.
  ///
  /// This uses a double-checked locking pattern optimized for the "read" case,
  /// minimizing contention on the global map lock.
  pub fn get_or_create(&self, key: &str) -> Arc<Mutex<StreamState>> {
    // 1. Optimistic Read
    {
      let map = self.inner.read();
      if let Some(state) = map.get(key) {
        return state.clone();
      }
    } // Drop read lock

    // 2. Write Lock (Creation)
    let mut map = self.inner.write();
    // Check again in case another thread created it while we waited for the lock
    if let Some(state) = map.get(key) {
      return state.clone();
    }

    let new_state = Arc::new(Mutex::new(StreamState {
      next_id: 0,
      active_segment_start_id: 0,
    }));
    map.insert(key.to_string(), new_state.clone());
    new_state
  }

  /// Gets a handle to an existing stream's state, or None if it doesn't exist.
  /// Does not create new streams.
  pub fn get(&self, key: &str) -> Option<Arc<Mutex<StreamState>>> {
    let map = self.inner.read();
    map.get(key).cloned()
  }

  /// Helper to check existence without cloning the Arc.
  pub fn contains(&self, key: &str) -> bool {
    let map = self.inner.read();
    map.contains_key(key)
  }
}
