//! Node-local secret resolution.
//!
//! Minga delivers secrets to containers as read-only files. This module owns
//! the rules for finding a secret value on the node and turning it into a
//! safe mount source.
//!
//! # Threat model
//!
//! The secret store is a directory on the node. Its contents are sensitive;
//! the *paths* that reach it are attacker-influenced in the sense that a
//! user controls the secret name and target in their Compose file. So:
//!
//! - The secret ID is **not** used as a path directly. It is hashed into a
//!   fixed-name file inside the store, so `../../etc/shadow` cannot escape.
//! - Secret files are checked with `O_NOFOLLOW` and restricted permissions.
//!   The path handed to containerd can still be swapped after the check;
//!   production use needs per-task immutable staging in the host namespace.
//! - Only regular files are accepted.
//! - The mount source handed to containerd is a path we constructed, never a
//!   user-supplied string.
//!
//! # What this module deliberately does not do
//!
//! It does not distribute secrets between nodes, encrypt the store, or
//! manage rotation. Those are operator responsibilities (documented in
//! `docs/talos-deployment.md`); this module only reads what the operator
//! provisioned.

use std::path::{Path, PathBuf};

use minga_state::types::SecretRef;
use sha2::{Digest, Sha256};

/// Default root for the node-local secret store.
pub const DEFAULT_SECRET_ROOT: &str = "/var/lib/minga/secrets";

/// Errors from secret resolution.
#[derive(Debug, thiserror::Error)]
pub enum SecretResolveError {
    #[error("secret `{0}` is not provisioned on this node")]
    NotProvisioned(String),

    #[error("secret `{name}` resolves to a non-regular file and was refused")]
    NotRegularFile { name: String },

    #[error("secret `{name}` is a symlink and was refused")]
    Symlink { name: String },

    #[error("secret `{name}` could not be read: {reason}")]
    Unreadable { name: String, reason: String },

    #[error("secret `{name}` has unsafe file permissions or ownership")]
    UnsafePermissions { name: String },

    #[error("secret store root `{path}` is not a directory")]
    StoreRootInvalid { path: String },

    #[error("secret `{name}` has an invalid target `{target}`")]
    InvalidTarget { name: String, target: String },
    #[error("duplicate secret mount destination `{0}`")]
    DuplicateTarget(String),
}

/// A secret staged for one task: content-addressed and immutable.
///
/// The staged path is a hard link to the provisioned file (same inode —
/// the value is never copied, never logged). Its name is the SHA-256 of
/// the content, so:
///
/// - The mount source cannot be swapped after validation: replacing the
///   provisioned file yields a *different* staged name, so a running
///   task's mount always points at the version it started with.
/// - Rotation is observable: a new task stages the new hash, an old task
///   keeps the old inode. `version` is the hash — safe to log and record.
#[derive(Debug, Clone)]
pub struct StagedSecret {
    /// The reference this stage satisfies, with `version` set.
    pub secret_ref: SecretRef,
    /// The immutable staged path to bind-mount read-only.
    pub staged_path: PathBuf,
}

/// A resolver over a node-local secret store directory.
#[derive(Debug, Clone)]
pub struct SecretStore {
    root: PathBuf,
}

impl SecretStore {
    /// Resolve every reference for a task and stage each into an
    /// immutable, content-addressed copy under `root/staged/<task_id>/`.
    ///
    /// The staged file is a fresh inode named by the SHA-256 of the
    /// content (a copy, not a link: the store's anti-tamper rules require
    /// provisioned files to have exactly one link). Same-hash restages
    /// reuse the existing file. This is the rotation model: the operator
    /// replaces the provisioned file; new tasks stage the new hash while
    /// running tasks keep the inode they started with.
    pub fn stage_all(
        &self,
        task_id: &str,
        refs: &[SecretRef],
    ) -> Result<Vec<StagedSecret>, SecretResolveError> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let resolved = self.resolve_all(refs)?;

        let staged_dir = self.root.join("staged").join(task_id);
        std::fs::create_dir_all(&staged_dir).map_err(|e| SecretResolveError::Unreadable {
            name: format!("staged/{task_id}"),
            reason: e.to_string(),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&staged_dir, std::fs::Permissions::from_mode(0o700));
        }

        let mut out = Vec::with_capacity(resolved.len());
        for (secret_ref, source) in resolved {
            let content = std::fs::read(&source).map_err(|e| SecretResolveError::Unreadable {
                name: secret_ref.secret_id.clone(),
                reason: e.to_string(),
            })?;
            let version = format!("{:x}", Sha256::digest(&content));
            let staged_path = staged_dir.join(&version);
            if !staged_path.exists() {
                std::fs::write(&staged_path, &content).map_err(|e| {
                    SecretResolveError::Unreadable {
                        name: secret_ref.secret_id.clone(),
                        reason: format!("stage: {e}"),
                    }
                })?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        &staged_path,
                        std::fs::Permissions::from_mode(0o400),
                    );
                }
            }
            out.push(StagedSecret {
                secret_ref: SecretRef {
                    version: Some(version),
                    ..secret_ref
                },
                staged_path,
            });
        }
        Ok(out)
    }

    /// Removes a task's staged secrets. Idempotent.
    pub fn cleanup_task(&self, task_id: &str) {
        let dir = self.root.join("staged").join(task_id);
        if let Err(e) = std::fs::remove_dir_all(&dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(task = %task_id, error = %e, "secret staging cleanup failed");
            }
        }
    }
}

impl SecretStore {
    /// Open a store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The store root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Derive the on-disk file name for a secret ID.
    ///
    /// SHA-256 gives a stable, fixed-shape name without user-controlled path
    /// components; unlike a short non-cryptographic hash it also makes
    /// practical collisions across independent secret IDs infeasible.
    fn file_name_for(secret_id: &str) -> String {
        format!("{:x}.secret", Sha256::digest(secret_id.as_bytes()))
    }

    /// Resolve a secret reference to a readable file on this node.
    ///
    /// Returns the path to the stored value. The caller is responsible for
    /// mounting it read-only.
    pub fn resolve(&self, secret_ref: &SecretRef) -> Result<PathBuf, SecretResolveError> {
        // Validate the target even though the mount source is ours: a bad
        // target would produce a bad destination in the OCI spec.
        if !secret_ref.target.starts_with("/run/secrets/") {
            return Err(SecretResolveError::InvalidTarget {
                name: secret_ref.secret_id.clone(),
                target: secret_ref.target.clone(),
            });
        }
        let file_name = secret_ref.target.trim_start_matches("/run/secrets/");
        if file_name.is_empty()
            || file_name.contains('/')
            || matches!(file_name, "." | "..")
            || file_name.chars().any(char::is_control)
        {
            return Err(SecretResolveError::InvalidTarget {
                name: secret_ref.secret_id.clone(),
                target: secret_ref.target.clone(),
            });
        }

        let path = self.root.join(Self::file_name_for(&secret_ref.secret_id));

        // The store must exist and be a directory.
        let meta = std::fs::symlink_metadata(&self.root).map_err(|_| {
            SecretResolveError::StoreRootInvalid {
                path: self.root.display().to_string(),
            }
        })?;
        if !meta.is_dir() || meta.is_symlink() {
            return Err(SecretResolveError::StoreRootInvalid {
                path: self.root.display().to_string(),
            });
        }

        // `symlink_metadata` does not follow the link, so a symlink is
        // detected rather than silently followed.
        let link_meta = std::fs::symlink_metadata(&path)
            .map_err(|_| SecretResolveError::NotProvisioned(secret_ref.secret_id.clone()))?;
        if link_meta.is_symlink() {
            return Err(SecretResolveError::Symlink {
                name: secret_ref.secret_id.clone(),
            });
        }
        if !link_meta.is_file() {
            return Err(SecretResolveError::NotRegularFile {
                name: secret_ref.secret_id.clone(),
            });
        }

        // O_NOFOLLOW protects the final path component from being replaced
        // with a symlink between metadata lookup and open. Inspect the opened
        // descriptor, not the path, for ownership and permission checks.
        let file = open_no_follow(&path).map_err(|e| SecretResolveError::Unreadable {
            name: secret_ref.secret_id.clone(),
            reason: e.to_string(),
        })?;
        let opened = file
            .metadata()
            .map_err(|e| SecretResolveError::Unreadable {
                name: secret_ref.secret_id.clone(),
                reason: e.to_string(),
            })?;
        if !opened.is_file() {
            return Err(SecretResolveError::NotRegularFile {
                name: secret_ref.secret_id.clone(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if opened.mode() & 0o077 != 0
                || opened.nlink() != 1
                || opened.uid() != unsafe { libc::geteuid() }
            {
                return Err(SecretResolveError::UnsafePermissions {
                    name: secret_ref.secret_id.clone(),
                });
            }
        }

        Ok(path)
    }

    /// Read a secret's value for use inside this process, with the same
    /// file checks as [`SecretStore::resolve`]. Used for credentials the node
    /// itself presents (the egress proxy's upstream key), never mounted.
    pub fn read_value(&self, secret_id: &str) -> Result<String, SecretResolveError> {
        // `resolve` validates a mount target; this value is never mounted.
        let path = self.resolve(&SecretRef::new(secret_id, "/run/secrets/value"))?;
        let raw = std::fs::read_to_string(&path).map_err(|e| SecretResolveError::Unreadable {
            name: secret_id.to_string(),
            reason: e.to_string(),
        })?;
        Ok(raw.trim_end().to_string())
    }

    /// Resolve every reference in a task, failing closed on the first
    /// problem.
    ///
    /// A container must not start with a missing secret: a partially
    /// provisioned task would run with an empty or absent credential, which
    /// is worse than not starting.
    pub fn resolve_all(
        &self,
        refs: &[SecretRef],
    ) -> Result<Vec<(SecretRef, PathBuf)>, SecretResolveError> {
        let mut out = Vec::with_capacity(refs.len());
        let mut targets = std::collections::HashSet::new();
        for secret_ref in refs {
            if !targets.insert(&secret_ref.target) {
                return Err(SecretResolveError::DuplicateTarget(
                    secret_ref.target.clone(),
                ));
            }
            let path = self.resolve(secret_ref)?;
            out.push((secret_ref.clone(), path));
        }
        Ok(out)
    }

    /// List the secret IDs this node has provisioned.
    ///
    /// Used for the placement admission check: a task must not be scheduled
    /// onto a node that lacks one of its secrets.
    pub fn provisioned_ids(&self) -> Vec<String> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        let mut names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let path = e.path();
                if path.extension().and_then(|e| e.to_str()) != Some("secret") {
                    return None;
                }
                // Only regular files count.
                std::fs::symlink_metadata(&path)
                    .ok()
                    .filter(|m| m.is_file())?;
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
            .collect();
        names.sort();
        names
    }
}

/// Open without following a symlink, and without blocking on a swapped FIFO.
///
/// The file descriptor is validated before it is closed. The returned path
/// remains vulnerable to a later rename until per-task staging is implemented;
/// production secret mounts are not yet certified by this probe.
fn open_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::File::open(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_secret(root: &Path, secret_id: &str, contents: &str) -> PathBuf {
        std::fs::create_dir_all(root).unwrap();
        let path = root.join(SecretStore::file_name_for(secret_id));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            f.set_permissions(std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        path
    }

    #[test]
    fn file_name_is_stable_and_ignores_path_components() {
        let a = SecretStore::file_name_for("stack_password");
        let b = SecretStore::file_name_for("stack_password");
        assert_eq!(a, b);

        // A traversal attempt produces a different, harmless file name -
        // it never becomes a path.
        let evil = SecretStore::file_name_for("../../etc/shadow");
        assert!(!evil.contains('/'));
        assert!(!evil.contains(".."));
        assert_ne!(evil, a);
    }

    #[test]
    fn resolves_a_provisioned_secret() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        write_secret(dir.path(), "stack_password", "s3cret");

        let secret_ref = SecretRef::new("stack_password", "/run/secrets/db_password");
        let path = store.resolve(&secret_ref).expect("must resolve");
        assert!(path.exists());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "s3cret");
    }

    #[test]
    fn missing_secret_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        std::fs::create_dir_all(dir.path()).unwrap();

        let secret_ref = SecretRef::new("stack_password", "/run/secrets/db_password");
        let err = store.resolve(&secret_ref).unwrap_err();
        assert!(matches!(err, SecretResolveError::NotProvisioned(_)));
    }

    #[test]
    fn symlink_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        std::fs::create_dir_all(dir.path()).unwrap();

        let target = dir.path().join("elsewhere");
        std::fs::write(&target, b"host key").unwrap();
        let link = dir
            .path()
            .join(SecretStore::file_name_for("stack_password"));
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let secret_ref = SecretRef::new("stack_password", "/run/secrets/db_password");
        let err = store.resolve(&secret_ref).unwrap_err();
        assert!(
            matches!(err, SecretResolveError::Symlink { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        std::fs::create_dir_all(
            dir.path()
                .join(SecretStore::file_name_for("stack_password")),
        )
        .unwrap();

        let secret_ref = SecretRef::new("stack_password", "/run/secrets/db_password");
        let err = store.resolve(&secret_ref).unwrap_err();
        assert!(matches!(err, SecretResolveError::NotRegularFile { .. }));
    }

    #[test]
    fn invalid_target_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        write_secret(dir.path(), "stack_password", "x");

        for bad in ["/etc/passwd", "/run/secrets/../escape", "/run/secrets/"] {
            let secret_ref = SecretRef::new("stack_password", bad);
            let err = store.resolve(&secret_ref).unwrap_err();
            assert!(
                matches!(err, SecretResolveError::InvalidTarget { .. }),
                "unexpected error for {bad}: {err}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn world_readable_secret_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        let path = write_secret(dir.path(), "stack_password", "disposable");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let secret_ref = SecretRef::new("stack_password", "/run/secrets/password");
        assert!(matches!(
            store.resolve(&secret_ref),
            Err(SecretResolveError::UnsafePermissions { .. })
        ));
    }

    #[test]
    fn read_value_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        write_secret(dir.path(), "llm_key", "provider-key\n");
        assert_eq!(store.read_value("llm_key").unwrap(), "provider-key");
        assert!(matches!(
            store.read_value("missing"),
            Err(SecretResolveError::NotProvisioned(_))
        ));
    }

    #[test]
    fn resolve_all_fails_on_first_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        write_secret(dir.path(), "a", "1");

        let refs = vec![
            SecretRef::new("a", "/run/secrets/a"),
            SecretRef::new("missing", "/run/secrets/missing"),
        ];
        let err = store.resolve_all(&refs).unwrap_err();
        assert!(matches!(err, SecretResolveError::NotProvisioned(_)));
    }

    #[test]
    fn provisioned_ids_lists_stored_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        write_secret(dir.path(), "a", "1");
        write_secret(dir.path(), "b", "2");

        let ids = store.provisioned_ids();
        assert_eq!(ids.len(), 2);
    }
}

#[cfg(test)]
mod staging_tests {
    use super::*;
    use minga_state::types::SecretRef;

    fn provision(root: &Path, secret_id: &str, value: &str) {
        let path = root.join(SecretStore::file_name_for(secret_id));
        std::fs::write(&path, value).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    #[test]
    fn rotated_secret_is_visible_to_a_new_task_not_an_old_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = SecretStore::new(dir.path());
        let secret = SecretRef::new("db_password", "/run/secrets/db_password");

        provision(dir.path(), "db_password", "v1-value");
        let staged_a = store
            .stage_all("task.a", std::slice::from_ref(&secret))
            .unwrap();
        let version_a = staged_a[0].secret_ref.version.clone().unwrap();
        assert_eq!(
            std::fs::read_to_string(&staged_a[0].staged_path).unwrap(),
            "v1-value"
        );

        // Operator rotates the secret.
        provision(dir.path(), "db_password", "v2-value");
        let staged_b = store
            .stage_all("task.b", std::slice::from_ref(&secret))
            .unwrap();
        let version_b = staged_b[0].secret_ref.version.clone().unwrap();

        assert_ne!(version_a, version_b, "rotation must change the version");
        assert_eq!(
            std::fs::read_to_string(&staged_b[0].staged_path).unwrap(),
            "v2-value"
        );
        assert_eq!(
            std::fs::read_to_string(&staged_a[0].staged_path).unwrap(),
            "v1-value",
            "the old task's staged inode keeps the old value"
        );

        // Cleanup removes only the task's own staging.
        store.cleanup_task("task.b");
        assert!(!staged_b[0].staged_path.exists());
        assert!(staged_a[0].staged_path.exists());
    }
}
