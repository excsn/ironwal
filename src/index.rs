use crate::error::{Error, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// Manages a memory-mapped `segments.idx` file for a single stream.
/// Provides fast, disk-backed binary search for segment lookups.
pub struct SegmentIndex {
  _file: File, // Keep the file handle alive for the lifetime of the map
  mmap: Mmap,
}

impl SegmentIndex {
  pub(crate) const FILENAME: &'static str = "segments.idx";

  /// Opens and memory-maps the index file for a stream.
  pub fn open(stream_dir: &Path) -> Result<Self> {
    let path = stream_dir.join(Self::FILENAME);
    let file = OpenOptions::new().read(true).open(path)?;

    let metadata = file.metadata()?;
    if metadata.len() % 8 != 0 {
      return Err(Error::Corruption(format!(
        "Index file size is not a multiple of 8: {}",
        metadata.len()
      )));
    }

    // Safety: We have an exclusive WAL lock, so the file won't be modified
    // underneath us by another ironwal process. External modification is a risk.
    let mmap = unsafe { Mmap::map(&file)? };

    Ok(Self { _file: file, mmap })
  }

  /// Finds the start ID of the segment file that should contain the `target_id`.
  pub fn find_segment_start_id(&self, target_id: u64) -> Result<u64> {
    if self.mmap.is_empty() {
      // An empty index means only the '0' segment can exist.
      return Ok(0);
    }

    // Zero-cost transmutation of the byte slice to a u64 slice.
    // Safety: We verified the length is a multiple of 8 in `open`.
    let ids: &[u64] = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const u64, self.mmap.len() / 8) };

    // Use Rust's built-in binary search on the slice.
    match ids.binary_search(&target_id) {
      // Exact match: the segment starts exactly at our target ID.
      Ok(idx) => Ok(ids[idx]),
      // No exact match: `idx` is the insertion point. The ID we want is
      // in the segment that starts *before* this insertion point.
      Err(idx) => {
        if idx == 0 {
          // This should not happen if the index is valid (first entry is always 0),
          // but as a safeguard, we return 0.
          Ok(0)
        } else {
          Ok(ids[idx - 1])
        }
      }
    }
  }

  /// Returns an iterator over all segment Start IDs in the index.
  pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
    if self.mmap.is_empty() {
      return [].iter().copied();
    }

    // Safety: The mmap is alive as long as &self is alive.
    // Alignment verified in open().
    let ids: &[u64] = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const u64, self.mmap.len() / 8) };

    ids.iter().copied()
  }
}

/// Appends a new start ID to the index file.
pub fn append_index(stream_dir: &Path, new_start_id: u64) -> Result<()> {
  let path = stream_dir.join(SegmentIndex::FILENAME);
  let mut file = OpenOptions::new().append(true).create(true).open(path)?;

  file.write_u64::<LittleEndian>(new_start_id)?;
  file.sync_all()?;
  Ok(())
}
