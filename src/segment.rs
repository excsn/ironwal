use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::config::{CompressionType, ReadStrategy, WalOptions};
use crate::error::{Error, Result};
use crate::frame::{FrameHeader, FrameType, calculate_checksum, deserialize_batch, serialize_batch};

use memmap2::Mmap;

#[cfg(feature = "compression")]
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

/// Represents the physical location and metadata of a frame on disk.
/// Used to decouple the "Scan" phase from the "Fetch" phase.
#[derive(Debug, Clone)]
pub(crate) struct FrameLocation {
  pub offset: u64,
  pub header: FrameHeader,
}

struct InnerSegment {
  file: BufWriter<File>,
  current_size: u64,
}

/// Represents an active segment file opened for WRITING.
/// Uses internal mutability to allow concurrent access via Arc (cached).
pub(crate) struct ActiveSegment {
  path: PathBuf,
  // The internal state is protected to allow usage via Arc from the Cache
  inner: Mutex<InnerSegment>,
}

impl ActiveSegment {
  pub fn create(path: PathBuf, _start_id: u64, options: &WalOptions) -> Result<Self> {
    let file = OpenOptions::new().create(true).append(true).open(&path)?;

    let metadata = file.metadata()?;
    let current_size = metadata.len();

    let writer = BufWriter::with_capacity(options.write_buffer_size, file);

    Ok(Self {
      path,
      inner: Mutex::new(InnerSegment {
        file: writer,
        current_size,
      }),
    })
  }

  /// Appends a batch of entries to this segment.
  /// `batch_start_id` is provided by the WalState sequencer.
  pub fn append(&self, entries: &[&[u8]], batch_start_id: u64, options: &WalOptions) -> Result<std::ops::Range<u64>> {
    let count = entries.len() as u32;
    if count == 0 {
      return Ok(batch_start_id..batch_start_id);
    }

    let mut inner = self.inner.lock();

    // 1. Serialize entries into a raw buffer
    let raw_payload = serialize_batch(entries).map_err(Error::Io)?;
    let uncompressed_size = raw_payload.len() as u32;

    // 2. Decide compression
    let (frame_type, disk_payload) = self.compress_if_needed(&raw_payload, options)?;

    // 3. Prepare Header
    let crc = calculate_checksum(batch_start_id, count, frame_type, &disk_payload);

    let header = FrameHeader {
      crc,
      start_id: batch_start_id,
      entry_count: count,
      frame_type,
      disk_size: disk_payload.len() as u32,
      uncompressed_size,
    };

    // 4. Write Header + Payload
    header.write(&mut inner.file).map_err(Error::Io)?;
    inner.file.write_all(&disk_payload).map_err(Error::Io)?;

    // 5. Update stats
    let bytes_written = FrameHeader::SIZE as u64 + disk_payload.len() as u64;
    inner.current_size += bytes_written;

    Ok(batch_start_id..batch_start_id + count as u64)
  }

  /// Returns current file size for rotation checks.
  pub fn size(&self) -> u64 {
    self.inner.lock().current_size
  }

  pub fn flush(&self) -> Result<()> {
    let mut inner = self.inner.lock();
    inner.file.flush().map_err(Error::Io)?;
    inner.file.get_ref().sync_data().map_err(Error::Io)?;
    Ok(())
  }

  pub fn flush_buffer(&self) -> Result<()> {
    let mut inner = self.inner.lock();
    inner.file.flush().map_err(Error::Io)?;
    Ok(())
  }

  /// Scans the file to find the end of the last valid frame and truncates
  /// any corrupted tail data. Returns the number of bytes preserved.
  pub fn repair(&self) -> Result<u64> {
    // 1. Flush any pending writes to disk so the reader can see them
    {
      let mut inner = self.inner.lock();
      inner.file.flush().map_err(Error::Io)?;
    }

    // 2. Open a fresh read-only handle to inspect the file
    // We cannot use the inner.file handle because it is opened in 'append' mode,
    // which may not support reading/seeking on some platforms.
    // The scan must agree exactly with `recover_scan`, so the recovered
    // `next_id` and the repaired file tail always match.
    let mut reader = SegmentReader::open_io(&self.path, 64 * 1024)?;
    let (_, valid_end) = reader.recover_scan()?;

    // 3. Truncate using the write handle
    let mut inner = self.inner.lock();
    inner.file.get_ref().set_len(valid_end)?;
    inner.file.seek(SeekFrom::Start(valid_end))?;
    inner.current_size = valid_end;

    Ok(valid_end)
  }

  fn compress_if_needed<'a>(
    &self,
    raw: &'a [u8],
    options: &WalOptions,
  ) -> Result<(FrameType, std::borrow::Cow<'a, [u8]>)> {
    if options.compression == CompressionType::None || raw.len() < options.min_compression_size {
      return Ok((FrameType::Raw, std::borrow::Cow::Borrowed(raw)));
    }

    #[cfg(feature = "compression")]
    {
      if options.compression == CompressionType::Lz4 {
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(raw).map_err(Error::Io)?;

        let compressed = encoder
          .finish()
          .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e)))?;

        // Only use compression if we actually saved space
        if compressed.len() < raw.len() {
          return Ok((FrameType::Lz4, std::borrow::Cow::Owned(compressed)));
        }
      }
    }

    Ok((FrameType::Raw, std::borrow::Cow::Borrowed(raw)))
  }
}

/// Represents a segment opened for READING.
pub(crate) enum SegmentReader {
  Io(BufReader<File>),
  Mmap(Mmap, usize), // Added usize to track cursor position
}

impl SegmentReader {
  pub fn open(path: &Path, options: &WalOptions) -> Result<Self> {
    let file = File::open(path)?;

    if options.read_strategy == ReadStrategy::Mmap {
      // Safety: Caller must ensure file is not modified externally.
      let mmap = unsafe { Mmap::map(&file)? };
      return Ok(SegmentReader::Mmap(mmap, 0)); // Initialize cursor at 0
    }

    let reader = BufReader::with_capacity(options.read_buffer_size, file);
    Ok(SegmentReader::Io(reader))
  }

  /// Opens a segment file using StandardIo regardless of the configured `read_strategy`.
  /// Used to create transient handles for the active segment, which must never be
  /// memory-mapped (growing file bounds cause SIGBUS / stale-EOF risk).
  pub fn open_io(path: &Path, read_buffer_size: usize) -> Result<Self> {
    let file = File::open(path)?;
    Ok(SegmentReader::Io(BufReader::with_capacity(read_buffer_size, file)))
  }

  /// Reads the NEXT batch from the current cursor position.
  /// Returns the Header AND the payload so the iterator knows the ID range.
  pub fn next_batch(&mut self) -> Result<Option<(FrameHeader, Vec<Vec<u8>>)>> {
    match self {
      Self::Io(reader) => {
        // We rely on FrameHeader::read to handle EOF
        let header = match FrameHeader::read(reader) {
          Ok(h) => h,
          Err(e) => {
            if let Error::Io(ref io_e) = e {
              if io_e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
              }
            }
            return Err(e);
          }
        };

        let mut payload = vec![0u8; header.disk_size as usize];
        if let Err(e) = reader.read_exact(&mut payload) {
          if e.kind() == io::ErrorKind::UnexpectedEof {
            // Torn tail: the header was written but the payload was not.
            // This can only occur at the physical end of the log.
            return Ok(None);
          }
          return Err(Error::Io(e));
        }

        let calc_crc = calculate_checksum(header.start_id, header.entry_count, header.frame_type, &payload);
        if calc_crc != header.crc {
          return Err(Error::CrcMismatch {
            expected: header.crc,
            actual: calc_crc,
            offset: 0, // We can't easily track offset in Io mode without tracking manually
          });
        }

        let final_data = decompress(header.frame_type, &payload, header.uncompressed_size)?;
        Ok(Some((header, deserialize_batch(&final_data)?)))
      }
      Self::Mmap(mmap, cursor) => {
        if *cursor + FrameHeader::SIZE > mmap.len() {
          return Ok(None);
        }

        let mut header_slice = &mmap[*cursor..];
        let header = FrameHeader::read(&mut header_slice)?;

        let payload_start = *cursor + FrameHeader::SIZE;
        let payload_end = payload_start + header.disk_size as usize;

        if payload_end > mmap.len() {
          // Torn or still-growing tail beyond this mapping's snapshot.
          // Treat as end-of-log rather than corruption.
          return Ok(None);
        }

        let payload = &mmap[payload_start..payload_end];
        let calc_crc = calculate_checksum(header.start_id, header.entry_count, header.frame_type, payload);
        if calc_crc != header.crc {
          return Err(Error::CrcMismatch {
            expected: header.crc,
            actual: calc_crc,
            offset: *cursor as u64,
          });
        }

        // Advance cursor so the next call reads the next frame
        *cursor = payload_end;

        let final_data = decompress(header.frame_type, payload, header.uncompressed_size)?;
        Ok(Some((header, deserialize_batch(&final_data)?)))
      }
    }
  }

  /// Efficiently scans headers to find the frame containing `target_id`.
  /// positions the cursor at that frame so `next_batch` will read it.
  pub fn seek_to_frame(&mut self, target_id: u64) -> Result<()> {
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(0))?;
        loop {
          // Peek at the header
          // We need to read it to know size, but if it's wrong, we skip payload.
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(e) if matches!(e, Error::Io(ref io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof) => {
              // EOF reached without finding target. Leave cursor at EOF.
              return Ok(());
            }
            Err(e) => return Err(e),
          };

          let frame_end_id = header.start_id + header.entry_count as u64;

          if target_id >= header.start_id && target_id < frame_end_id {
            // Found it!
            // Rewind back to the start of this header so next_batch reads it.
            reader.seek(SeekFrom::Current(-(FrameHeader::SIZE as i64)))?;
            return Ok(());
          }

          // Skip payload
          reader.seek(SeekFrom::Current(header.disk_size as i64))?;
        }
      }
      Self::Mmap(mmap, cursor) => {
        *cursor = 0;
        let len = mmap.len();
        while *cursor + FrameHeader::SIZE <= len {
          let current_pos = *cursor;
          let mut header_slice = &mmap[current_pos..];
          let header = FrameHeader::read(&mut header_slice)?;

          let frame_end_id = header.start_id + header.entry_count as u64;
          let next_pos = current_pos + FrameHeader::SIZE + header.disk_size as usize;

          if target_id >= header.start_id && target_id < frame_end_id {
            // Found it. Cursor is already at the start of this frame (current_pos).
            // Wait, we modified cursor in the loop? No, we used current_pos.
            *cursor = current_pos;
            return Ok(());
          }

          *cursor = next_pos;
        }
        Ok(())
      }
    }
  }

  /// Scans the segment to determine the valid tail of the log.
  ///
  /// Returns `(total_entries, valid_len)` where `valid_len` is the byte offset
  /// just past the last fully-written tail frame. Frames whose payload extends
  /// past the physical end of the file are rejected (a seek past EOF succeeds
  /// silently, so bounds are checked against the real file length). The final
  /// in-bounds frame is additionally CRC-validated: an all-zero payload that
  /// fails the CRC is a torn tail (power failure persisted the length before
  /// the data blocks) and is excluded, while a non-zero CRC failure is bit rot
  /// and is kept so reads surface `CrcMismatch` rather than losing data
  /// silently.
  pub fn recover_scan(&mut self) -> Result<(u64, u64)> {
    match self {
      Self::Io(reader) => {
        let file_len = reader.get_ref().metadata()?.len();
        let mut total_entries: u64 = 0;
        let mut pos: u64 = 0;
        let mut last_frame: Option<(u64, FrameHeader)> = None;
        reader.seek(SeekFrom::Start(0))?;
        loop {
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(Error::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(Error::Corruption(_)) => break,
            Err(e) => return Err(e),
          };
          let frame_len = FrameHeader::SIZE as u64 + header.disk_size as u64;
          if pos + frame_len > file_len {
            // Torn tail: header is present but the payload is not.
            break;
          }
          reader.seek(SeekFrom::Current(header.disk_size as i64))?;
          total_entries += header.entry_count as u64;
          last_frame = Some((pos, header));
          pos += frame_len;
        }

        if let Some((offset, header)) = last_frame {
          reader.seek(SeekFrom::Start(offset + FrameHeader::SIZE as u64))?;
          let mut payload = vec![0u8; header.disk_size as usize];
          let torn = match reader.read_exact(&mut payload) {
            Ok(()) => {
              let crc = calculate_checksum(
                header.start_id,
                header.entry_count,
                header.frame_type,
                &payload,
              );
              // A power failure can persist the file length before the data
              // blocks, which then read back as zeros — that is a torn tail.
              // A CRC mismatch over NON-zero data is bit rot: keep the frame
              // so reads surface CrcMismatch instead of silently deleting it.
              crc != header.crc && payload.iter().all(|&b| b == 0)
            }
            Err(_) => true,
          };
          if torn {
            total_entries -= header.entry_count as u64;
            pos = offset;
          }
        }

        Ok((total_entries, pos))
      }
      Self::Mmap(mmap, _) => {
        let mut cursor = 0;
        let mut total_entries: u64 = 0;
        let len = mmap.len();
        let mut last_frame: Option<(usize, FrameHeader)> = None;
        while cursor + FrameHeader::SIZE <= len {
          let mut header_slice = &mmap[cursor..];
          let header = match FrameHeader::read(&mut header_slice) {
            Ok(h) => h,
            Err(Error::Corruption(_)) => break,
            Err(e) => return Err(e),
          };

          let payload_end = cursor + FrameHeader::SIZE + header.disk_size as usize;
          if payload_end > len {
            break;
          }
          total_entries += header.entry_count as u64;
          last_frame = Some((cursor, header));
          cursor = payload_end;
        }

        if let Some((offset, header)) = last_frame {
          let start = offset + FrameHeader::SIZE;
          let payload = &mmap[start..start + header.disk_size as usize];
          let crc =
            calculate_checksum(header.start_id, header.entry_count, header.frame_type, payload);
          // See the Io arm: all-zero payload = torn tail (truncate); non-zero
          // mismatch = bit rot (keep, surfaced as CrcMismatch on read).
          if crc != header.crc && payload.iter().all(|&b| b == 0) {
            total_entries -= header.entry_count as u64;
            cursor = offset;
          }
        }

        Ok((total_entries, cursor as u64))
      }
    }
  }

  /// Efficiently scans the segment headers to find the frame containing the target ID.
  /// Does NOT read or decompress the payload.
  ///
  /// `hint` is an optional previously-found frame in this segment: when the
  /// target lies at or beyond it, scanning resumes from the hint's offset
  /// instead of the start of the file.
  pub fn find_frame(
    &mut self,
    target_id: u64,
    hint: Option<&FrameLocation>,
  ) -> Result<Option<FrameLocation>> {
    let start_offset = match hint {
      Some(loc) if target_id >= loc.header.start_id => loc.offset,
      _ => 0,
    };
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(start_offset))?;
        let mut offset = start_offset;
        loop {
          let header = match FrameHeader::read(reader) {
            Ok(h) => h,
            Err(e) => {
              if let Error::Io(ref io_err) = e {
                if io_err.kind() == io::ErrorKind::UnexpectedEof {
                  return Ok(None);
                }
              }
              return Err(e);
            }
          };

          let frame_end_id = header.start_id + header.entry_count as u64;

          if target_id >= header.start_id && target_id < frame_end_id {
            return Ok(Some(FrameLocation { offset, header }));
          }

          let increment = FrameHeader::SIZE as u64 + header.disk_size as u64;
          offset += increment;

          // Catch EOF during seek (payload truncation)
          if let Err(e) = reader.seek(SeekFrom::Current(header.disk_size as i64)) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
              return Ok(None);
            }
            return Err(Error::Io(e));
          }
        }
      }
      Self::Mmap(mmap, _) => {
        let mut cursor = start_offset as usize;
        let len = mmap.len();
        while cursor + FrameHeader::SIZE <= len {
          let current_offset = cursor as u64;
          let mut header_slice = &mmap[cursor..];
          let header = FrameHeader::read(&mut header_slice)?;

          let frame_end_id = header.start_id + header.entry_count as u64;
          let payload_size = header.disk_size as usize;

          if target_id >= header.start_id && target_id < frame_end_id {
            return Ok(Some(FrameLocation {
              offset: current_offset,
              header,
            }));
          }

          cursor += FrameHeader::SIZE + payload_size;
        }
        Ok(None)
      }
    }
  }

  /// Reads, validates, and decompresses a frame at a specific location.
  pub fn read_at(&mut self, loc: &FrameLocation) -> Result<Vec<Vec<u8>>> {
    match self {
      Self::Io(reader) => {
        reader.seek(SeekFrom::Start(loc.offset + FrameHeader::SIZE as u64))?;
        let mut payload = vec![0u8; loc.header.disk_size as usize];
        reader.read_exact(&mut payload)?;

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          &payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }

        let final_data = decompress(loc.header.frame_type, &payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
      Self::Mmap(mmap, _) => {
        let start = loc.offset as usize + FrameHeader::SIZE;
        let end = start + loc.header.disk_size as usize;
        if end > mmap.len() {
          return Err(Error::Corruption("Frame payload truncated in mmap".into()));
        }
        let payload = &mmap[start..end];

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }

        let final_data = decompress(loc.header.frame_type, payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
    }
  }

}

/// Decompresses a frame payload according to its `FrameType`.
/// Extracted as a free function so both `SegmentReader` and `CachedReadDescriptor` can reuse it.
pub(crate) fn decompress(ft: FrameType, data: &[u8], _size: u32) -> Result<std::borrow::Cow<'_, [u8]>> {
  match ft {
    FrameType::Raw => Ok(std::borrow::Cow::Borrowed(data)),
    FrameType::Lz4 => {
      #[cfg(feature = "compression")]
      {
        let mut decoder = FrameDecoder::new(data);
        let mut out = Vec::with_capacity(_size as usize);
        decoder.read_to_end(&mut out).map_err(Error::Io)?;
        Ok(std::borrow::Cow::Owned(out))
      }
      #[cfg(not(feature = "compression"))]
      {
        Err(Error::Config("LZ4 frame found but compression feature disabled".into()))
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Stateless positional read helper (platform-conditional)
// ---------------------------------------------------------------------------

/// Reads exactly `buf.len()` bytes from `file` starting at `offset` without
/// moving the file's shared cursor, making it safe to call concurrently.
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
  #[cfg(unix)]
  {
    use std::os::unix::fs::FileExt;
    let mut pos = 0usize;
    while pos < buf.len() {
      match file.read_at(&mut buf[pos..], offset + pos as u64) {
        Ok(0) => {
          return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "pread returned 0"));
        }
        Ok(n) => pos += n,
        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }
    Ok(())
  }
  #[cfg(windows)]
  {
    use std::os::windows::fs::FileExt;
    let mut pos = 0usize;
    while pos < buf.len() {
      match file.seek_read(&mut buf[pos..], offset + pos as u64) {
        Ok(0) => {
          return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "seek_read returned 0"));
        }
        Ok(n) => pos += n,
        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
        Err(e) => return Err(e),
      }
    }
    Ok(())
  }
  #[cfg(not(any(unix, windows)))]
  {
    let _ = (file, buf, offset);
    Err(io::Error::new(
      io::ErrorKind::Unsupported,
      "stateless positional reads are not supported on this platform",
    ))
  }
}

// ---------------------------------------------------------------------------
// CachedReadDescriptor
// ---------------------------------------------------------------------------

/// A stateless read handle for an immutable (rotated) segment file.
///
/// Both variants support concurrent access from multiple threads without a
/// shared mutex: `Io` uses platform pread-style APIs (no shared cursor),
/// `Mmap` does lock-free slice indexing into a read-only mapping.
///
/// Because rotated segments are immutable, the descriptor lazily builds a
/// per-frame `(start_id, offset)` index on the first lookup, turning
/// subsequent `find_frame` calls into a binary search instead of a linear
/// header scan (one pread per frame). The index costs 16 bytes per frame and
/// is evicted together with the descriptor by the LRU cache.
///
/// Only rotated segments are stored here. The active segment is always opened
/// with a fresh transient `SegmentReader` handle to avoid stale-EOF risks
/// from a growing file.
pub(crate) struct CachedReadDescriptor {
  inner: ReadDescriptorInner,
  /// Lazily-built sorted `(start_id, offset)` pairs for every frame.
  frame_index: OnceLock<Box<[(u64, u64)]>>,
}

enum ReadDescriptorInner {
  /// Standard-IO path: each read computes its own offset via `read_exact_at`.
  Io(File),
  /// Mmap path: the file is mapped once; concurrent slice reads are inherently safe.
  Mmap(Mmap),
}

impl CachedReadDescriptor {
  pub fn open(path: &Path, options: &WalOptions) -> Result<Self> {
    let file = File::open(path)?;
    let inner = if options.read_strategy == ReadStrategy::Mmap {
      // Safety: rotated segments are immutable; the mapping will not be
      // invalidated by concurrent writes.
      let mmap = unsafe { Mmap::map(&file)? };
      ReadDescriptorInner::Mmap(mmap)
    } else {
      ReadDescriptorInner::Io(file)
    };
    Ok(Self {
      inner,
      frame_index: OnceLock::new(),
    })
  }

  /// Reads and parses one frame header at `offset`.
  /// Returns `None` on EOF or a corrupt/zero header (end of valid data).
  fn read_header_at(&self, offset: u64) -> Result<Option<FrameHeader>> {
    match &self.inner {
      ReadDescriptorInner::Io(file) => {
        let mut hbuf = [0u8; FrameHeader::SIZE];
        match read_exact_at(file, &mut hbuf, offset) {
          Ok(()) => {}
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
          Err(e) => return Err(Error::Io(e)),
        }
        let mut slice = &hbuf[..];
        Ok(FrameHeader::read(&mut slice).ok())
      }
      ReadDescriptorInner::Mmap(mmap) => {
        let data: &[u8] = mmap;
        let cursor = offset as usize;
        if cursor + FrameHeader::SIZE > data.len() {
          return Ok(None);
        }
        let mut slice = &data[cursor..];
        Ok(FrameHeader::read(&mut slice).ok())
      }
    }
  }

  /// Returns the frame index, building it with a single sequential header
  /// scan on first use. Two racing threads may both build; the first insert
  /// wins and both results are identical (the file is immutable).
  fn frame_index(&self) -> Result<&[(u64, u64)]> {
    if let Some(index) = self.frame_index.get() {
      return Ok(index);
    }

    let mut entries = Vec::new();
    let mut offset = 0u64;
    while let Some(header) = self.read_header_at(offset)? {
      entries.push((header.start_id, offset));
      offset += FrameHeader::SIZE as u64 + header.disk_size as u64;
    }

    let built = entries.into_boxed_slice();
    Ok(self.frame_index.get_or_init(|| built))
  }

  /// Locates the frame containing `target_id` via binary search over the
  /// frame index. Safe to call concurrently on the same descriptor instance.
  pub fn find_frame(&self, target_id: u64) -> Result<Option<FrameLocation>> {
    let index = self.frame_index()?;

    // Last frame whose start_id <= target_id.
    let pos = index.partition_point(|&(start_id, _)| start_id <= target_id);
    if pos == 0 {
      return Ok(None);
    }
    let (_, offset) = index[pos - 1];

    let header = match self.read_header_at(offset)? {
      Some(h) => h,
      None => return Ok(None),
    };
    let end_id = header.start_id + header.entry_count as u64;
    if target_id >= header.start_id && target_id < end_id {
      Ok(Some(FrameLocation { offset, header }))
    } else {
      Ok(None)
    }
  }

  /// Reads, CRC-validates, and decompresses the payload identified by `loc`.
  /// Entirely stateless — safe to call concurrently on the same descriptor instance.
  pub fn read_frame(&self, loc: &FrameLocation) -> Result<Vec<Vec<u8>>> {
    match &self.inner {
      ReadDescriptorInner::Io(file) => {
        let payload_offset = loc.offset + FrameHeader::SIZE as u64;
        let mut payload = vec![0u8; loc.header.disk_size as usize];
        read_exact_at(file, &mut payload, payload_offset).map_err(Error::Io)?;

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          &payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }
        let final_data = decompress(loc.header.frame_type, &payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
      ReadDescriptorInner::Mmap(mmap) => {
        let data: &[u8] = mmap;
        let start = loc.offset as usize + FrameHeader::SIZE;
        let end = start + loc.header.disk_size as usize;
        if end > data.len() {
          return Err(Error::Corruption("Frame payload truncated in mmap".into()));
        }
        let payload = &data[start..end];

        let calc_crc = calculate_checksum(
          loc.header.start_id,
          loc.header.entry_count,
          loc.header.frame_type,
          payload,
        );
        if calc_crc != loc.header.crc {
          return Err(Error::CrcMismatch {
            expected: loc.header.crc,
            actual: calc_crc,
            offset: loc.offset,
          });
        }
        let final_data = decompress(loc.header.frame_type, payload, loc.header.uncompressed_size)?;
        Ok(deserialize_batch(&final_data)?)
      }
    }
  }
}
