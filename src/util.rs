use crate::error::{Error, Result};
use std::path::{Component, Path, PathBuf};

/// Sanitizes a stream key to ensure it is safe for use as a directory name.
/// Replaces potentially dangerous characters with underscores.
pub fn sanitize_stream_key(key: &str) -> Result<String> {
  if key.trim().is_empty() {
    return Err(Error::Config("Stream key cannot be empty".into()));
  }

  let sanitized: String = key
    .chars()
    .map(|c| match c {
      // Safe alphanumeric and common separators
      'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => c,
      // Replace path separators and nulls
      '/' | '\\' | ':' | '\0' => '_',
      // Replace anything else suspicious
      _ => '_',
    })
    .collect();

  // Prevent directory traversal attacks
  if sanitized == ".." || sanitized == "." {
    return Err(Error::Config(format!("Invalid stream key: {}", key)));
  }

  Ok(sanitized)
}

/// Helper to ensure a path is strictly inside the root directory.
pub fn ensure_safe_path(root: &Path, child: &str) -> Result<PathBuf> {
  let safe_child = sanitize_stream_key(child)?;
  let path = root.join(safe_child);

  // Canonicalization check is expensive, but for WAL initialization
  // it's acceptable safety.
  // Here we stick to logical checking to avoid FS calls during high-freq ops.
  if path.components().any(|c| matches!(c, Component::ParentDir)) {
    return Err(Error::Config("Path traversal detected".into()));
  }

  Ok(path)
}

/// Generates a segment filename from a Start ID.
/// Format: 00000000000000001000.wal (20 digits)
pub fn segment_filename(start_id: u64) -> String {
  format!("{:020}.wal", start_id)
}

/// Parses a Start ID from a segment filename.
pub fn parse_segment_id(filename: &str) -> Option<u64> {
  filename.strip_suffix(".wal")?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_sanitization() {
    assert_eq!(sanitize_stream_key("valid-key").unwrap(), "valid-key");
    assert_eq!(sanitize_stream_key("user/123").unwrap(), "user_123");
    assert_eq!(sanitize_stream_key("../hack").unwrap(), ".._hack");
  }

  #[test]
  fn test_filenames() {
    let id = 12345;
    let name = segment_filename(id);
    assert_eq!(name, "00000000000000012345.wal");
    assert_eq!(parse_segment_id(&name), Some(id));
  }
}
