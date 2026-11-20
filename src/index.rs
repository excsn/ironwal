use crate::error::{Error, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::path::Path;

#[inline(always)]
fn read_u64_at(slice: &[u8], slot: usize) -> u64 {
  u64::from_le_bytes(slice[slot * 8..slot * 8 + 8].try_into().unwrap())
}

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

    let count = self.mmap.len() / 8;
    let mut lo = 0usize;
    let mut hi = count;

    while lo < hi {
      let mid = lo + (hi - lo) / 2;
      let v = read_u64_at(&self.mmap, mid);
      match v.cmp(&target_id) {
        std::cmp::Ordering::Equal => return Ok(v),
        std::cmp::Ordering::Less => lo = mid + 1,
        std::cmp::Ordering::Greater => hi = mid,
      }
    }

    if lo == 0 {
      Ok(0)
    } else {
      Ok(read_u64_at(&self.mmap, lo - 1))
    }
  }

  /// Returns an iterator over all segment start IDs in the index.
  pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
    let count = self.mmap.len() / 8;
    (0..count).map(move |i| read_u64_at(&self.mmap, i))
  }
}

/// Appends a new start ID to the index file without syncing it. Recovery rebuilds the index from the
/// segment files when it lags them.
pub fn append_index(stream_dir: &Path, new_start_id: u64) -> Result<()> {
  let path = stream_dir.join(SegmentIndex::FILENAME);
  let mut file = OpenOptions::new().append(true).create(true).open(path)?;

  file.write_u64::<LittleEndian>(new_start_id)?;
  Ok(())
}
