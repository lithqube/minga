//! Secret reference validation and translation.
//!
//! Compose secret references become **references**, never values. This
//! module owns the rules that decide whether a reference is acceptable:
//!
//! - the secret must be declared in the project's top-level `secrets` map;
//! - `file:` sources are **rejected**, because the submitter's local path is
//!   not a path on the node that will run the container, and silently
//!   treating it as one would deliver the wrong bytes (or nothing);
//! - `external:` secrets are accepted, since the operator provisions the
//!   value out of band on every eligible node;
//! - the mount target is confined to `/run/secrets/` and must be a plain
//!   file name — no traversal, no absolute paths, no nested directories
//!   that could collide with a system path.

use minga_compose::FileRef;
use minga_state::types::SecretRef;

/// The directory secrets are mounted into, matching Docker Swarm.
pub const SECRETS_DIR: &str = "/run/secrets";

/// Errors from secret reference handling.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret `{0}` is not declared in the project's top-level `secrets`")]
    Undeclared(String),

    #[error(
        "secret `{name}` uses a `file:` source, which is not supported: \
         the submitter's local path does not exist on the node that runs the \
         container. Provision the value on every eligible node and use \
         `external: true`, or add a secure import mechanism first"
    )]
    FileSourceUnsupported { name: String },

    #[error("secret `{name}` has an unsupported target `{target}`: {reason}")]
    InvalidTarget {
        name: String,
        target: String,
        reason: String,
    },

    #[error("secret `{name}` uses unsupported options ({options}); only `source` and `target` are supported")]
    UnsupportedOptions { name: String, options: String },

    #[error("secret `{name}` is referenced more than once with different targets")]
    DuplicateTarget { name: String },
}

/// Validate and translate one service secret reference.
///
/// `stack` qualifies the secret ID so two stacks can declare a secret with
/// the same name without colliding.
pub fn translate_secret_ref(
    service_name: &str,
    file_ref: &FileRef,
    declared: &std::collections::HashSet<String>,
    stack: &str,
) -> Result<SecretRef, SecretError> {
    let name = file_ref.source.clone();

    // The secret must be declared at the top level.
    if !declared.contains(&name) {
        return Err(SecretError::Undeclared(name));
    }

    // uid/gid/mode are not supported yet: honouring them partially (or
    // ignoring them silently) would be worse than refusing.
    let mut unsupported: Vec<&str> = Vec::new();
    if file_ref.uid.is_some() {
        unsupported.push("uid");
    }
    if file_ref.gid.is_some() {
        unsupported.push("gid");
    }
    if file_ref.mode.is_some() {
        unsupported.push("mode");
    }
    if !unsupported.is_empty() {
        return Err(SecretError::UnsupportedOptions {
            name,
            options: unsupported.join(", "),
        });
    }

    // Target: default to `/run/secrets/<source>`; validate anything given.
    let target = match &file_ref.target {
        Some(t) => validate_target(&name, t)?,
        None => format!("{SECRETS_DIR}/{name}"),
    };

    let _ = service_name;
    Ok(SecretRef::new(format!("{stack}_{name}"), target))
}

/// Validate a secret mount target.
///
/// The target must be a plain file name directly under `/run/secrets/`.
fn validate_target(name: &str, target: &str) -> Result<String, SecretError> {
    let reject = |reason: &str| SecretError::InvalidTarget {
        name: name.to_string(),
        target: target.to_string(),
        reason: reason.to_string(),
    };

    if target.is_empty() {
        return Err(reject("target must not be empty"));
    }
    if target.contains('\0') {
        return Err(reject("target must not contain NUL"));
    }
    if target.contains('/') {
        return Err(reject(
            "target must be a plain file name directly under /run/secrets/",
        ));
    }
    if target == "." || target == ".." {
        return Err(reject("target must not be a relative path component"));
    }
    if target.starts_with('-') {
        return Err(reject("target must not start with a dash"));
    }
    // Characters that would be surprising in a file name.
    if target.chars().any(|c| c.is_control()) {
        return Err(reject("target must not contain control characters"));
    }

    Ok(format!("{SECRETS_DIR}/{target}"))
}

/// Validate a whole set of service secret references.
///
/// Catches duplicate targets, which would otherwise produce two mounts at
/// the same path — the second silently shadowing the first.
pub fn translate_secret_refs(
    service_name: &str,
    refs: &[FileRef],
    declared: &std::collections::HashSet<String>,
    stack: &str,
) -> Result<Vec<SecretRef>, SecretError> {
    let mut out: Vec<SecretRef> = Vec::with_capacity(refs.len());
    let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    for file_ref in refs {
        let secret_ref = translate_secret_ref(service_name, file_ref, declared, stack)?;

        if let Some(previous) = seen.get(&secret_ref.target) {
            if previous != &secret_ref.secret_id {
                return Err(SecretError::DuplicateTarget {
                    name: secret_ref.target.clone(),
                });
            }
            // Same secret, same target: a harmless duplicate. Keep one.
            continue;
        }

        seen.insert(secret_ref.target.clone(), secret_ref.secret_id.clone());
        out.push(secret_ref);
    }

    Ok(out)
}

/// Find a secret declaration whose source cannot be distributed safely.
///
/// Only out-of-band provisioned `external: true` is supported. The local
/// submitter's file or environment must never be accepted and then dropped.
pub fn unsupported_source_secret(project: &minga_compose::Project) -> Option<(&str, &str)> {
    for (name, obj) in &project.secrets {
        if obj.file.is_some() {
            return Some((name.as_str(), "file:"));
        }
        if obj.environment.is_some() {
            return Some((name.as_str(), "environment:"));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn declared(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn accepts_external_secret_with_default_target() {
        let refs = vec![FileRef {
            source: "db_password".to_string(),
            target: None,
            uid: None,
            gid: None,
            mode: None,
        }];
        let out = translate_secret_refs("web", &refs, &declared(&["db_password"]), "stack")
            .expect("external secret must be accepted");

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].secret_id, "stack_db_password");
        assert_eq!(out[0].target, "/run/secrets/db_password");
    }

    #[test]
    fn accepts_explicit_target() {
        let refs = vec![FileRef {
            source: "db_password".to_string(),
            target: Some("custom_name".to_string()),
            uid: None,
            gid: None,
            mode: None,
        }];
        let out =
            translate_secret_refs("web", &refs, &declared(&["db_password"]), "stack").unwrap();
        assert_eq!(out[0].target, "/run/secrets/custom_name");
    }

    #[test]
    fn rejects_undeclared_secret() {
        let refs = vec![FileRef {
            source: "nope".to_string(),
            target: None,
            uid: None,
            gid: None,
            mode: None,
        }];
        let err = translate_secret_refs("web", &refs, &declared(&["other"]), "stack").unwrap_err();
        assert!(matches!(err, SecretError::Undeclared(_)));
    }

    #[test]
    fn rejects_traversal_target() {
        for bad in [
            "../etc/passwd",
            "/etc/passwd",
            "sub/dir",
            "..",
            ".",
            "",
            "-rf",
        ] {
            let refs = vec![FileRef {
                source: "s".to_string(),
                target: Some(bad.to_string()),
                uid: None,
                gid: None,
                mode: None,
            }];
            let err = translate_secret_refs("web", &refs, &declared(&["s"]), "stack")
                .expect_err("target {bad:?} must be rejected");
            assert!(
                matches!(err, SecretError::InvalidTarget { .. }),
                "unexpected error for {bad:?}: {err}"
            );
        }
    }

    #[test]
    fn rejects_uid_gid_mode() {
        let refs = vec![FileRef {
            source: "s".to_string(),
            target: None,
            uid: Some("1000".to_string()),
            gid: None,
            mode: None,
        }];
        let err = translate_secret_refs("web", &refs, &declared(&["s"]), "stack").unwrap_err();
        assert!(matches!(err, SecretError::UnsupportedOptions { .. }));
    }

    #[test]
    fn rejects_duplicate_targets() {
        let refs = vec![
            FileRef {
                source: "a".to_string(),
                target: Some("same".to_string()),
                uid: None,
                gid: None,
                mode: None,
            },
            FileRef {
                source: "b".to_string(),
                target: Some("same".to_string()),
                uid: None,
                gid: None,
                mode: None,
            },
        ];
        let err = translate_secret_refs("web", &refs, &declared(&["a", "b"]), "stack").unwrap_err();
        assert!(matches!(err, SecretError::DuplicateTarget { .. }));
    }

    #[test]
    fn deduplicates_identical_reference() {
        let refs = vec![
            FileRef {
                source: "a".to_string(),
                target: None,
                uid: None,
                gid: None,
                mode: None,
            },
            FileRef {
                source: "a".to_string(),
                target: None,
                uid: None,
                gid: None,
                mode: None,
            },
        ];
        let out = translate_secret_refs("web", &refs, &declared(&["a"]), "stack").unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn detects_file_source_secret() {
        let yaml = r#"
services:
  web:
    image: nginx
secrets:
  inline:
    file: ./local.txt
"#;
        let loaded = minga_compose::load(yaml, &minga_compose::LoadOptions::default())
            .expect("compose file must load");
        assert_eq!(
            unsupported_source_secret(&loaded.project),
            Some(("inline", "file:"))
        );
    }

    #[test]
    fn detects_environment_source_secret() {
        let yaml = r#"
services:
  web:
    image: nginx
secrets:
  from_env:
    environment: SECRET_VALUE
"#;
        let loaded = minga_compose::load(yaml, &minga_compose::LoadOptions::default())
            .expect("compose file must load");
        assert_eq!(
            unsupported_source_secret(&loaded.project),
            Some(("from_env", "environment:"))
        );
    }

    #[test]
    fn external_secret_has_no_file_source() {
        let yaml = r#"
services:
  web:
    image: nginx
secrets:
  provisioned:
    external: true
"#;
        let loaded = minga_compose::load(yaml, &minga_compose::LoadOptions::default())
            .expect("compose file must load");
        assert_eq!(unsupported_source_secret(&loaded.project), None);
    }
}
