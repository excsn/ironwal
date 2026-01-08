use ironwal::{Wal, WalOptions};
use tempfile::TempDir;

pub struct TestEnv {
  pub wal: Wal,
  // The TempDir guard must be kept alive to prevent premature deletion of the directory.
  pub _dir: TempDir,
  pub root: std::path::PathBuf,
}

impl TestEnv {
  pub fn new(mut options: WalOptions) -> Self {
    let dir = tempfile::tempdir().unwrap();
    options.root_path = dir.path().to_path_buf();
    let root = options.root_path.clone();

    let wal = Wal::new(options).unwrap();

    Self { wal, _dir: dir, root }
  }

  pub fn with_default() -> Self {
    Self::new(WalOptions::default())
  }
}
