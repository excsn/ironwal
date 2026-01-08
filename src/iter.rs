use std::collections::VecDeque;

use crate::error::Result;
use crate::index::SegmentIndex;
use crate::segment::SegmentReader;
use crate::util::segment_filename;
use crate::wal::Wal;

/// An iterator that sequentially yields entries from a WAL stream.
/// Transparently handles segment switching and batch offsets.
pub struct WalIterator {
  wal: Wal,
  stream: String,
  /// The next Sequence ID the user expects to see.
  next_id: u64,

  reader: Option<SegmentReader>,

  /// A buffer of entries from the most recently read batch.
  buffer: VecDeque<Vec<u8>>,

  /// The Start ID of the segment currently being read.
  current_segment_start_id: Option<u64>,
}

impl WalIterator {
  pub(crate) fn new(wal: Wal, stream: String, start_id: u64) -> Result<Self> {
    let mut iter = Self {
      wal,
      stream,
      next_id: start_id,
      reader: None,
      buffer: VecDeque::new(),
      current_segment_start_id: None,
    };

    // 1. Locate and Open the correct segment file
    iter.open_segment_containing(start_id)?;

    // 2. Efficiently seek within the file to the correct frame
    if let Some(reader) = &mut iter.reader {
      reader.seek_to_frame(start_id)?;
    }

    Ok(iter)
  }

  /// Finds and opens the segment file containing `target_id`.
  fn open_segment_containing(&mut self, target_id: u64) -> Result<()> {
    let stream_dir = self.wal.get_stream_dir(&self.stream)?;

    if !stream_dir.exists() {
      // Stream doesn't exist, so there's nothing to read.
      self.reader = None;
      self.current_segment_start_id = None;
      return Ok(());
    }

    let idx_path = stream_dir.join(SegmentIndex::FILENAME);

    // Load Index
    let index = self.wal.get_or_load_index(&stream_dir, &idx_path)?;

    // Find Start ID
    let start_id = index.find_segment_start_id(target_id)?;

    // Check for infinite loop (re-opening the same completed segment)
    if let Some(current) = self.current_segment_start_id {
      if start_id <= current && self.reader.is_none() {
        // We finished the last segment, but index says the next ID is still in the old segment?
        // This implies EOF on the stream.
        return Ok(());
      }
    }

    let segment_path = stream_dir.join(segment_filename(start_id));
    if segment_path.exists() {
      let reader = SegmentReader::open(&segment_path, &self.wal.options())?;
      self.reader = Some(reader);
      self.current_segment_start_id = Some(start_id);
    } else {
      self.reader = None;
      self.current_segment_start_id = None;
    }

    Ok(())
  }

  /// Tries to advance to the next segment file.
  fn switch_segment(&mut self) -> Result<bool> {
    let current_start = match self.current_segment_start_id {
      Some(id) => id,
      None => return Ok(false),
    };

    // We need to find the segment strictly *after* the current one.
    // We can query the index for `next_id`.
    // Since we just exhausted a batch, `next_id` should point to the start of the next batch.

    // Close current
    self.reader = None;

    // Open next
    self.open_segment_containing(self.next_id)?;

    // If we successfully opened a *new* segment
    if let Some(new_start) = self.current_segment_start_id {
      if new_start > current_start {
        // We found a new file. Reset cursor to start (seek_to_frame is smart enough)
        if let Some(reader) = &mut self.reader {
          // Ensure we start from the beginning of this new file
          reader.seek_to_frame(self.next_id)?;
        }
        return Ok(true);
      }
    }

    Ok(false)
  }
}

impl Iterator for WalIterator {
  type Item = Result<Vec<u8>>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      // 1. Try to yield from buffer
      if let Some(entry) = self.buffer.pop_front() {
        self.next_id += 1;
        return Some(Ok(entry));
      }

      // 2. Refill Buffer
      let reader = match &mut self.reader {
        Some(r) => r,
        None => return None,
      };

      match reader.next_batch() {
        Ok(Some((header, mut batch))) => {
          // INTEGRITY CHECK & DISCARD LOGIC
          // The user wants `self.next_id`. The batch starts at `header.start_id`.

          if header.start_id > self.next_id {
            // Gap detected! This should not happen in a valid WAL.
            return Some(Err(crate::error::Error::Corruption(format!(
              "Gap in sequence IDs. Expected {}, found batch starting at {}",
              self.next_id, header.start_id
            ))));
          }

          if header.start_id + header.entry_count as u64 <= self.next_id {
            // This batch is entirely *before* the ID we want.
            // This can happen if seek_to_frame wasn't perfect or we are skimming.
            // Discard whole batch and continue.
            continue;
          }

          // Calculate how many items to discard from the front
          let discard_count = (self.next_id - header.start_id) as usize;

          if discard_count > 0 {
            // Efficiently remove the first N items
            if discard_count >= batch.len() {
              // Should be covered by the check above, but for safety:
              continue;
            }
            batch.drain(0..discard_count);
          }

          // Push valid items to buffer
          self.buffer.extend(batch);
        }
        Ok(None) => {
          // EOF on current segment. Try switch.
          match self.switch_segment() {
            Ok(true) => continue,     // Found new segment, retry loop
            Ok(false) => return None, // End of stream
            Err(e) => return Some(Err(e)),
          }
        }
        Err(e) => return Some(Err(e)),
      }
    }
  }
}
