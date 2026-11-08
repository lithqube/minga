//! Preloaded OCI image metadata resolution for containerd tasks.

use minga_state::types::parse_user;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::client::ContainerdClient;
use super::proto::Image;

const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_INDEX: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const OCI_CONFIG: &str = "application/vnd.oci.image.config.v1+json";
const DOCKER_CONFIG: &str = "application/vnd.docker.container.image.v1+json";
const MAX_IMAGE_METADATA: i64 = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BlobDescriptor {
    pub media_type: String,
    pub digest: String,
    pub size: i64,
}

impl BlobDescriptor {
    fn from_json(value: &Value) -> Result<Self, String> {
        Self::new(
            value["mediaType"].as_str().unwrap_or_default(),
            value["digest"].as_str().unwrap_or_default(),
            value["size"].as_i64().unwrap_or_default(),
        )
    }

    pub fn new(media_type: &str, digest: &str, size: i64) -> Result<Self, String> {
        if size <= 0 || size > MAX_IMAGE_METADATA {
            return Err("image metadata has invalid or excessive size".into());
        }
        validate_digest(digest)?;
        Ok(Self {
            media_type: media_type.to_string(),
            digest: digest.to_string(),
            size,
        })
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() as i64 != self.size {
            return Err("image metadata size does not match its descriptor".into());
        }
        if format!("sha256:{:x}", Sha256::digest(bytes)) != self.digest {
            return Err("image metadata digest does not match its descriptor".into());
        }
        Ok(())
    }
}

/// Expand a Docker-style short image reference to its fully-qualified
/// form, matching what `ctr`/containerd store on pull: `nginx:1.27` is
/// `docker.io/library/nginx:1.27`, a tagless name gets `:latest`. A name
/// whose first component contains `.` or `:` (or is `localhost`) already
/// names a registry and is returned unchanged.
pub fn normalize_ref(raw: &str) -> String {
    let (name_tag, digest) = match raw.split_once('@') {
        Some((n, d)) => (n, Some(d)),
        None => (raw, None),
    };
    // A colon after the last slash introduces a tag; an earlier colon is a
    // registry port. With no slash at all any colon is a tag separator
    // (`localhost:5000` is repo `localhost`, tag `5000` in Docker).
    let (name, tag) = match (name_tag.rfind(':'), name_tag.rfind('/')) {
        (Some(c), s) if Some(c) > s => (&name_tag[..c], Some(&name_tag[c + 1..])),
        _ => (name_tag, None),
    };
    let mut out = match name.split_once('/') {
        Some((first, _)) if first.contains('.') || first.contains(':') || first == "localhost" => {
            name.to_string()
        }
        Some(_) => format!("docker.io/{name}"),
        None => format!("docker.io/library/{name}"),
    };
    if let Some(t) = tag {
        out.push(':');
        out.push_str(t);
    }
    match digest {
        Some(d) => {
            out.push('@');
            out.push_str(d);
        }
        None if tag.is_none() => out.push_str(":latest"),
        None => {}
    }
    out
}

fn validate_digest(digest: &str) -> Result<(), String> {
    let hex = digest
        .strip_prefix("sha256:")
        .ok_or("only sha256 image digests are supported")?;
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("malformed sha256 image digest".into());
    }
    Ok(())
}

pub fn select_manifest(index: &Value, architecture: &str) -> Result<BlobDescriptor, String> {
    let manifests = index["manifests"]
        .as_array()
        .ok_or("image index has no manifests")?;
    manifests
        .iter()
        .filter(|entry| {
            entry["platform"]["os"] == "linux"
                && entry["platform"]["architecture"] == architecture
                && entry["platform"]["os.version"].is_null()
        })
        .find_map(|entry| {
            let descriptor = BlobDescriptor::from_json(entry).ok()?;
            matches!(
                descriptor.media_type.as_str(),
                OCI_MANIFEST | DOCKER_MANIFEST
            )
            .then_some(descriptor)
        })
        .ok_or_else(|| format!("image index has no linux/{architecture} manifest"))
}

pub fn manifest_config(manifest: &Value) -> Result<BlobDescriptor, String> {
    let descriptor = BlobDescriptor::from_json(&manifest["config"])?;
    if !matches!(descriptor.media_type.as_str(), OCI_CONFIG | DOCKER_CONFIG) {
        return Err("image manifest has unsupported config media type".into());
    }
    Ok(descriptor)
}

pub fn chain_id(diff_ids: &[String]) -> Result<String, String> {
    let (first, rest) = diff_ids
        .split_first()
        .ok_or("image has no unpacked rootfs layers")?;
    validate_digest(first)?;
    let mut parent = first.clone();
    for diff_id in rest {
        validate_digest(diff_id)?;
        parent = format!(
            "sha256:{:x}",
            Sha256::digest(format!("{parent} {diff_id}").as_bytes())
        );
    }
    Ok(parent)
}

#[derive(Debug, Clone)]
pub struct ImageConfiguration {
    /// Image Entrypoint (may be empty).
    pub entrypoint: Vec<String>,
    /// Image Cmd (may be empty).
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub cwd: String,
    pub uid: u32,
    pub gid: u32,
    pub parent_snapshot: String,
}

impl ImageConfiguration {
    /// The default argv: entrypoint + cmd. Docker override semantics
    /// (task entrypoint/command replacing parts of this) live in
    /// `oci::spec_to_any_with_image`.
    pub fn args(&self) -> Vec<String> {
        [self.entrypoint.clone(), self.cmd.clone()].concat()
    }
}

impl ImageConfiguration {
    pub fn parse(value: &Value, architecture: &str) -> Result<Self, String> {
        if value["os"] != "linux" || value["architecture"] != architecture {
            return Err(format!("image config is not for linux/{architecture}"));
        }
        let config = &value["config"];
        let argv_of = |key: &str| -> Vec<String> {
            config[key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        };
        let entrypoint = argv_of("Entrypoint");
        let cmd = argv_of("Cmd");
        if entrypoint.is_empty() && cmd.is_empty() {
            return Err("image has no executable entrypoint or command".into());
        }
        let env = config["Env"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| {
                let s = item.as_str().ok_or("non-string image environment entry")?;
                if !s.contains('=') || s.starts_with('=') {
                    return Err("invalid image environment entry");
                }
                Ok(s.to_string())
            })
            .collect::<Result<Vec<_>, &str>>()?;
        let cwd = config["WorkingDir"].as_str().unwrap_or_default();
        let cwd = if cwd.is_empty() { "/" } else { cwd };
        if !cwd.starts_with('/') {
            return Err("image working directory must be absolute".into());
        }
        let user = config["User"].as_str().unwrap_or_default();
        let (uid, gid) = if user.is_empty() {
            (0, 0)
        } else {
            parse_user(user)?
        };
        if value["rootfs"]["type"] != "layers" {
            return Err("image rootfs is not layered".into());
        }
        let diff_ids = value["rootfs"]["diff_ids"]
            .as_array()
            .ok_or("image rootfs has no diff IDs")?
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or("invalid rootfs diff ID")
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            entrypoint,
            cmd,
            env,
            cwd: cwd.to_string(),
            uid,
            gid,
            parent_snapshot: chain_id(&diff_ids)?,
        })
    }
}

/// Resolve only metadata from an image already present and unpacked in the
/// `minga` containerd namespace. No registry access is attempted.
pub async fn load_image(
    client: &mut ContainerdClient,
    image: &Image,
    snapshotter: &str,
) -> Result<ImageConfiguration, String> {
    let architecture = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        "arm" => "arm",
        other => return Err(format!("unsupported host architecture: {other}")),
    };
    let target = image
        .target
        .as_ref()
        .ok_or("image has no target descriptor")?;
    let mut descriptor = BlobDescriptor::new(&target.media_type, &target.digest, target.size)?;
    let bytes = client
        .read_content(&descriptor)
        .await
        .map_err(|error| format!("cannot read preloaded image target: {error}"))?;
    if matches!(descriptor.media_type.as_str(), OCI_INDEX | DOCKER_INDEX) {
        let index: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid image index: {error}"))?;
        descriptor = select_manifest(&index, architecture)?;
    } else if !matches!(
        descriptor.media_type.as_str(),
        OCI_MANIFEST | DOCKER_MANIFEST
    ) {
        return Err("unsupported image target media type".into());
    }
    let manifest_bytes = if descriptor.digest == target.digest {
        bytes
    } else {
        client
            .read_content(&descriptor)
            .await
            .map_err(|error| format!("cannot read preloaded image manifest: {error}"))?
    };
    let manifest: Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("invalid image manifest: {error}"))?;
    let config_descriptor = manifest_config(&manifest)?;
    let config_bytes = client
        .read_content(&config_descriptor)
        .await
        .map_err(|error| format!("cannot read preloaded image config: {error}"))?;
    let config: Value = serde_json::from_slice(&config_bytes)
        .map_err(|error| format!("invalid image config: {error}"))?;
    let config = ImageConfiguration::parse(&config, architecture)?;
    match client
        .stat_snapshot(snapshotter, &config.parent_snapshot)
        .await
        .map_err(|error| format!("cannot inspect unpacked image snapshot: {error}"))?
    {
        Some(info) if info.kind == 3 => Ok(config),
        _ => Err(format!(
            "image {} is not unpacked for snapshotter {snapshotter}; preload and unpack it in namespace minga",
            image.name
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_docker_hub_short_refs() {
        assert_eq!(normalize_ref("nginx:1.27"), "docker.io/library/nginx:1.27");
        assert_eq!(normalize_ref("nginx"), "docker.io/library/nginx:latest");
        assert_eq!(normalize_ref("user/repo:tag"), "docker.io/user/repo:tag");
    }

    #[test]
    fn keeps_qualified_refs_unchanged() {
        for r in [
            "registry.local/vllm:0.6.3",
            "localhost:5000/img:v1",
            "ghcr.io/x/y:latest",
        ] {
            assert_eq!(normalize_ref(r), r);
        }
        // ...but a tagless qualified name still gains :latest.
        assert_eq!(normalize_ref("ghcr.io/x/y"), "ghcr.io/x/y:latest");
        assert_eq!(
            normalize_ref("10.0.0.1:5000/img"),
            "10.0.0.1:5000/img:latest"
        );
    }

    #[test]
    fn normalizes_digest_refs_without_adding_a_tag() {
        let d = format!("sha256:{}", "a".repeat(64));
        assert_eq!(
            normalize_ref(&format!("redis@{d}")),
            format!("docker.io/library/redis@{d}")
        );
        assert_eq!(
            normalize_ref(&format!("img:tag@{d}")),
            format!("docker.io/library/img:tag@{d}")
        );
        assert_eq!(
            normalize_ref(&format!("registry.local/img@{d}")),
            format!("registry.local/img@{d}")
        );
    }

    #[test]
    fn selects_linux_host_manifest_from_multi_platform_index() {
        let index = serde_json::json!({
            "manifests": [
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": format!("sha256:{}", "a".repeat(64)), "size": 123, "platform": {"os": "linux", "architecture": "amd64"}},
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": format!("sha256:{}", "b".repeat(64)), "size": 456, "platform": {"os": "linux", "architecture": "arm64"}}
            ]
        });
        let selected = select_manifest(&index, "arm64").unwrap();
        assert_eq!(selected.digest, format!("sha256:{}", "b".repeat(64)));
    }

    #[test]
    fn config_preserves_entrypoint_command_env_and_workdir() {
        let json = serde_json::json!({
            "os": "linux", "architecture": "arm64",
            "config": {"Entrypoint": ["/bin/sh", "-c"], "Cmd": ["echo ok"], "Env": ["A=1", "B=2"], "WorkingDir": "/app", "User": "1000:1001"},
            "rootfs": {"type": "layers", "diff_ids": [format!("sha256:{}", "c".repeat(64))]}
        });
        let config = ImageConfiguration::parse(&json, "arm64").unwrap();
        assert_eq!(config.args(), vec!["/bin/sh", "-c", "echo ok"]);
        assert_eq!(config.env, vec!["A=1", "B=2"]);
        assert_eq!(config.cwd, "/app");
        assert_eq!((config.uid, config.gid), (1000, 1001));
        assert_eq!(config.parent_snapshot, format!("sha256:{}", "c".repeat(64)));
    }

    #[test]
    fn rejects_empty_command_and_named_user() {
        let mut json = serde_json::json!({
            "os": "linux", "architecture": "arm64",
            "config": {"User": "app"},
            "rootfs": {"type": "layers", "diff_ids": [format!("sha256:{}", "c".repeat(64))]}
        });
        assert!(ImageConfiguration::parse(&json, "arm64").is_err());
        json["config"]["User"] = serde_json::json!("0");
        assert!(ImageConfiguration::parse(&json, "arm64").is_err());
    }

    #[test]
    fn parses_numeric_users_and_refuses_names() {
        assert_eq!(parse_user("0"), Ok((0, 0)));
        assert_eq!(parse_user("1000:1001"), Ok((1000, 1001)));
        assert!(parse_user("app").is_err());
        assert!(parse_user("1000:app").is_err());
    }

    #[test]
    fn chain_id_hashes_multiple_diff_ids() {
        let first = format!("sha256:{}", "a".repeat(64));
        let second = format!("sha256:{}", "b".repeat(64));
        assert_eq!(chain_id(std::slice::from_ref(&first)).unwrap(), first);
        use sha2::{Digest, Sha256};
        let expected = format!(
            "sha256:{:x}",
            Sha256::digest(format!("{first} {second}").as_bytes())
        );
        assert_eq!(chain_id(&[first, second]).unwrap(), expected);
    }
}
