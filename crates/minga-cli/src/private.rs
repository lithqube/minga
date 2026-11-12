//! Owner-only files and directories, for private keys and configs that carry tokens.
//!
//! `OpenOptions::mode` only applies to a file that is being created, so
//! `write_file` also tightens one that already exists.

use std::fs::{DirBuilder, OpenOptions, Permissions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// Writes `contents` to `path`, which ends up mode 0600 whether or not it existed.
pub fn write_file(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(Permissions::from_mode(0o600))?;
    file.write_all(contents)
}

/// Creates `path` and any missing parents with mode 0700. A directory that
/// already exists keeps its mode: it may be one the operator chose, such as `.`.
pub fn create_dir(path: &Path) -> std::io::Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn new_and_existing_paths_end_up_owner_only() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("a/b");
        create_dir(&dir).unwrap();
        assert_eq!(mode(&dir), 0o700);

        let file = dir.join("key.pem");
        write_file(&file, b"one").unwrap();
        assert_eq!(mode(&file), 0o600);

        // A pre-existing world-readable file is tightened; an existing
        // directory is left as the operator made it.
        std::fs::set_permissions(&file, Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
        write_file(&file, b"two").unwrap();
        create_dir(&dir).unwrap();
        assert_eq!(mode(&file), 0o600);
        assert_eq!(mode(&dir), 0o755);
        assert_eq!(std::fs::read(&file).unwrap(), b"two");
    }
}
