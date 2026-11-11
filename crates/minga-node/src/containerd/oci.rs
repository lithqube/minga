//! OCI runtime spec generation from Minga [`TaskSpec`].
//!
//! Generates a minimal OCI runtime spec (JSON) that containerd can use
//! to create a container. The spec is serialized to JSON bytes and packed
//! into a `google.protobuf.Any` when creating the containerd container.
//!
//! Only the fields Minga needs are populated. Everything else uses OCI
//! spec defaults (the runtime fills them in).

use std::collections::HashMap;
use std::path::PathBuf;

use minga_state::types::TaskSpec;
use serde::Serialize;

use super::image::ImageConfiguration;
use super::proto::Mount;

/// OCI runtime spec version.
const OCI_VERSION: &str = "1.0.2";

/// The full OCI runtime spec.
#[derive(Debug, Clone, Serialize)]
pub struct OciSpec {
    #[serde(rename = "ociVersion")]
    pub oci_version: String,
    pub process: OciProcess,
    pub root: OciRoot,
    pub hostname: String,
    pub mounts: Vec<OciMount>,
    pub linux: OciLinux,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciProcess {
    pub terminal: bool,
    pub user: OciUser,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    pub cwd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<OciCapabilities>,
    #[serde(rename = "rlimits", skip_serializing_if = "Vec::is_empty")]
    pub rlimits: Vec<OciRlimit>,
    #[serde(rename = "noNewPrivileges")]
    pub no_new_privileges: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciUser {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciRoot {
    pub path: String,
    pub readonly: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciMount {
    pub destination: String,
    #[serde(rename = "type")]
    pub mount_type: String,
    pub source: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciLinux {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<OciNamespace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<OciResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroups_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciNamespace {
    #[serde(rename = "type")]
    pub ns_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub devices: Option<Vec<OciDeviceCgroup>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<OciMemory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<OciCpu>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciMemory {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciCpu {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shares: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciDeviceCgroup {
    pub allow: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub major: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minor: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciCapabilities {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bounding: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub effective: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inheritable: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub permitted: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ambient: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OciRlimit {
    #[serde(rename = "type")]
    pub rlimit_type: String,
    pub hard: u64,
    pub soft: u64,
}

/// Default Linux capabilities for a container (same as Docker's default set).
fn default_capabilities() -> OciCapabilities {
    let caps = vec![
        "CAP_CHOWN".to_string(),
        "CAP_DAC_OVERRIDE".to_string(),
        "CAP_FSETID".to_string(),
        "CAP_FOWNER".to_string(),
        "CAP_MKNOD".to_string(),
        "CAP_NET_RAW".to_string(),
        "CAP_SETGID".to_string(),
        "CAP_SETUID".to_string(),
        "CAP_SETFCAP".to_string(),
        "CAP_SETPCAP".to_string(),
        "CAP_NET_BIND_SERVICE".to_string(),
        "CAP_SYS_CHROOT".to_string(),
        "CAP_KILL".to_string(),
        "CAP_AUDIT_WRITE".to_string(),
    ];
    OciCapabilities {
        bounding: caps.clone(),
        effective: caps.clone(),
        inheritable: caps.clone(),
        permitted: caps,
        ambient: vec![],
    }
}

/// A resolved secret, ready to be mounted.
///
/// `source` is a path Minga constructed on the node; `target` is the
/// in-container path. Neither is user-supplied text at this point.
#[derive(Debug, Clone)]
pub struct SecretMount {
    pub source: std::path::PathBuf,
    pub target: String,
}

/// Container networking: a prepared netns plus the resolver config.
#[derive(Debug, Clone)]
pub struct NetworkSpec {
    /// Path of the task's prepared network namespace.
    pub netns_path: String,
    /// Per-task resolv.conf to bind-mount read-only at /etc/resolv.conf.
    pub resolv_conf: Option<std::path::PathBuf>,
    /// Per-task hosts file (extra_hosts entries) to bind-mount at /etc/hosts.
    pub hosts_path: Option<std::path::PathBuf>,
}

/// Generate an OCI runtime spec from a Minga task spec.
///
/// The `container_id` is used as the hostname and in annotations.
/// Port mappings and resource limits from the spec are translated to
/// OCI linux resources and annotations.
///
/// `secret_mounts` are the resolved secrets to deliver as read-only files
/// under `/run/secrets/`. They are mounted with `ro`, `nosuid`, `nodev` and
/// `noexec` so a workload cannot modify, execute or gain privileges from a
/// secret file.
pub fn spec_from_task_with_secrets(
    container_id: &str,
    spec: &TaskSpec,
    secret_mounts: &[SecretMount],
    network: Option<&NetworkSpec>,
) -> OciSpec {
    let mut oci = spec_from_task(container_id, spec);

    if let Some(net) = network {
        for ns in &mut oci.linux.namespaces {
            if ns.ns_type == "network" {
                ns.path = Some(net.netns_path.clone());
            }
        }
        if let Some(resolv) = &net.resolv_conf {
            oci.mounts.push(OciMount {
                destination: "/etc/resolv.conf".to_string(),
                mount_type: "bind".to_string(),
                source: resolv.display().to_string(),
                options: vec![
                    "bind".to_string(),
                    "ro".to_string(),
                    "nosuid".to_string(),
                    "nodev".to_string(),
                ],
            });
        }
        if let Some(hosts) = &net.hosts_path {
            oci.mounts.push(OciMount {
                destination: "/etc/hosts".to_string(),
                mount_type: "bind".to_string(),
                source: hosts.display().to_string(),
                options: vec![
                    "bind".to_string(),
                    "ro".to_string(),
                    "nosuid".to_string(),
                    "nodev".to_string(),
                ],
            });
        }
    }

    // Volume/bind mounts. Sources are host paths — volume sources were
    // already materialized by the runtime.
    for m in &spec.mounts {
        oci.mounts.push(OciMount {
            destination: m.target.clone(),
            mount_type: "bind".to_string(),
            source: m.source.clone(),
            options: vec![
                "bind".to_string(),
                if m.read_only { "ro" } else { "rw" }.to_string(),
                "nosuid".to_string(),
                "nodev".to_string(),
            ],
        });
    }

    for secret in secret_mounts {
        oci.mounts.push(OciMount {
            destination: secret.target.clone(),
            mount_type: "bind".to_string(),
            source: secret.source.display().to_string(),
            options: vec![
                "bind".to_string(),
                "ro".to_string(),
                "nosuid".to_string(),
                "nodev".to_string(),
                "noexec".to_string(),
            ],
        });
    }

    oci
}

/// Generate an OCI runtime spec from a Minga task spec.
///
/// The `container_id` is used as the hostname and in annotations.
/// Port mappings and resource limits from the spec are translated to
/// OCI linux resources and annotations.
pub fn spec_from_task(container_id: &str, spec: &TaskSpec) -> OciSpec {
    // Environment variables as "KEY=VALUE" strings, sorted for determinism.
    let mut env: Vec<String> = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    env.sort();
    // Always set PATH if not provided.
    if !env.iter().any(|e| e.starts_with("PATH=")) {
        env.push("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string());
    }

    // Resource limits → OCI linux resources.
    let resources = if spec.resources.memory_bytes > 0 || spec.resources.cpus > 0.0 {
        let memory = if spec.resources.memory_bytes > 0 {
            Some(OciMemory {
                limit: Some(spec.resources.memory_bytes as i64),
                reservation: None,
            })
        } else {
            None
        };
        let cpu = if spec.resources.cpus > 0.0 {
            // Convert CPUs to quota/period: quota = cpus * period
            let period = 100_000u64;
            let quota = (spec.resources.cpus * period as f64) as i64;
            Some(OciCpu {
                shares: None,
                quota: Some(quota),
                period: Some(period),
            })
        } else {
            None
        };
        Some(OciResources {
            devices: Some(vec![OciDeviceCgroup {
                allow: false,
                r#type: None,
                major: None,
                minor: None,
                access: Some("rwm".to_string()),
            }]),
            memory,
            cpu,
        })
    } else {
        None
    };

    // Standard container namespaces.
    let namespaces = vec![
        OciNamespace {
            ns_type: "pid".to_string(),
            path: None,
        },
        OciNamespace {
            ns_type: "ipc".to_string(),
            path: None,
        },
        OciNamespace {
            ns_type: "uts".to_string(),
            path: None,
        },
        OciNamespace {
            ns_type: "mount".to_string(),
            path: None,
        },
        OciNamespace {
            ns_type: "network".to_string(),
            path: None,
        },
        OciNamespace {
            ns_type: "cgroup".to_string(),
            path: None,
        },
    ];

    // Port mappings as annotations (containerd/runc don't use OCI spec for ports;
    // port mapping is handled by the network layer, not the runtime spec).
    let mut annotations = HashMap::new();
    annotations.insert(
        "minga/task/container_id".to_string(),
        container_id.to_string(),
    );
    if !spec.ports.is_empty() {
        let ports_json = serde_json::to_string(&spec.ports).unwrap_or_default();
        annotations.insert("minga/task/ports".to_string(), ports_json);
    }

    OciSpec {
        oci_version: OCI_VERSION.to_string(),
        process: OciProcess {
            terminal: false,
            user: OciUser { uid: 0, gid: 0 },
            args: vec![], // Use image's default entrypoint/cmd
            env,
            cwd: "/".to_string(),
            capabilities: Some(default_capabilities()),
            rlimits: vec![],
            no_new_privileges: true,
        },
        root: OciRoot {
            path: "rootfs".to_string(),
            readonly: false,
        },
        hostname: container_id.to_string(),
        mounts: vec![
            OciMount {
                destination: "/proc".to_string(),
                mount_type: "proc".to_string(),
                source: "proc".to_string(),
                options: vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                ],
            },
            OciMount {
                destination: "/dev".to_string(),
                mount_type: "tmpfs".to_string(),
                source: "tmpfs".to_string(),
                options: vec![
                    "nosuid".to_string(),
                    "strictatime".to_string(),
                    "mode=755".to_string(),
                    "size=65536k".to_string(),
                ],
            },
            OciMount {
                destination: "/dev/pts".to_string(),
                mount_type: "devpts".to_string(),
                source: "devpts".to_string(),
                options: vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "newinstance".to_string(),
                    "ptmxmode=0666".to_string(),
                    "mode=0620".to_string(),
                ],
            },
            OciMount {
                destination: "/sys".to_string(),
                mount_type: "sysfs".to_string(),
                source: "sysfs".to_string(),
                options: vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "ro".to_string(),
                ],
            },
            OciMount {
                destination: "/sys/fs/cgroup".to_string(),
                mount_type: "cgroup".to_string(),
                source: "cgroup".to_string(),
                options: vec![
                    "nosuid".to_string(),
                    "noexec".to_string(),
                    "nodev".to_string(),
                    "ro".to_string(),
                ],
            },
        ],
        linux: OciLinux {
            namespaces,
            resources,
            cgroups_path: None,
        },
        annotations,
    }
}

/// Serialize the OCI spec to JSON bytes, suitable for packing into a
/// `google.protobuf.Any` with type URL
/// `types.containerd.io/opencontainers/runtime-spec/1/Spec`.
pub fn spec_to_any(container_id: &str, spec: &TaskSpec) -> Result<prost_types::Any, String> {
    let oci = spec_from_task(container_id, spec);
    let json =
        serde_json::to_vec(&oci).map_err(|e| format!("failed to serialize OCI spec: {e}"))?;
    Ok(prost_types::Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: json,
    })
}

/// The process spec for running `args` inside a container: same user, env
/// and working directory as the container's own process.
pub fn exec_process_any(
    container_spec: &prost_types::Any,
    args: &[String],
) -> Result<prost_types::Any, String> {
    let spec: serde_json::Value = serde_json::from_slice(&container_spec.value)
        .map_err(|e| format!("invalid container spec: {e}"))?;
    let mut process = spec["process"].clone();
    if !process.is_object() {
        return Err("container spec has no process".to_string());
    }
    process["args"] = serde_json::json!(args);
    process["terminal"] = serde_json::json!(false);
    Ok(prost_types::Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Process".to_string(),
        value: serde_json::to_vec(&process).map_err(|e| e.to_string())?,
    })
}

/// Serialize an OCI spec that includes secret mounts.
pub fn spec_to_any_with_secrets(
    container_id: &str,
    spec: &TaskSpec,
    secret_mounts: &[SecretMount],
    network: Option<&NetworkSpec>,
) -> Result<prost_types::Any, String> {
    let oci = spec_from_task_with_secrets(container_id, spec, secret_mounts, network);
    let json =
        serde_json::to_vec(&oci).map_err(|e| format!("failed to serialize OCI spec: {e}"))?;
    Ok(prost_types::Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: json,
    })
}

/// The uid and gid a task's process runs as: its own `user`, else the image's.
/// Names are looked up in the image's `/etc/passwd` and `/etc/group`, read
/// from `rootfs` (the snapshot's directories, top layer first). A user name
/// without a group takes that user's primary group, as Docker does.
pub fn task_user(
    spec: &TaskSpec,
    image: &ImageConfiguration,
    rootfs: &[PathBuf],
) -> Result<(u32, u32), String> {
    let user = match &spec.user {
        Some(user) => user,
        None if image.user.is_empty() => return Ok((0, 0)),
        None => &image.user,
    };
    if let Ok(ids) = minga_state::types::parse_user(user) {
        return Ok(ids);
    }
    resolve_user(user, rootfs)
        .map_err(|e| format!("user `{user}`: {e}; or set a numeric `user:` on the service"))
}

fn resolve_user(user: &str, rootfs: &[PathBuf]) -> Result<(u32, u32), String> {
    let (name, group) = match user.split_once(':') {
        Some((name, group)) => (name, Some(group)),
        None => (user, None),
    };
    let (uid, primary_gid) = match name.parse::<u32>() {
        Ok(uid) => (uid, 0),
        Err(_) => match lookup(&read_etc(rootfs, "passwd")?, name) {
            Some((uid, Some(gid))) => (uid, gid),
            _ => return Err(format!("no user `{name}` in the image's /etc/passwd")),
        },
    };
    let gid = match group {
        None => primary_gid,
        Some(group) => match group.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => lookup(&read_etc(rootfs, "group")?, group)
                .map(|(gid, _)| gid)
                .ok_or_else(|| format!("no group `{group}` in the image's /etc/group"))?,
        },
    };
    Ok((uid, gid))
}

/// The id fields of `name`'s line in a passwd (`name:x:uid:gid:…`) or group
/// (`name:x:gid:members`) file. The second id is `None` for group files.
fn lookup(file: &str, name: &str) -> Option<(u32, Option<u32>)> {
    file.lines().find_map(|line| {
        let mut fields = line.split(':');
        if fields.next()? != name {
            return None;
        }
        fields.next()?; // password
        let id = fields.next()?.parse().ok()?;
        Some((id, fields.next().and_then(|f| f.parse().ok())))
    })
}

/// Reads `etc/<file>` from the topmost snapshot directory that has it.
/// Symlinks are refused: they would resolve against the host, not the image.
fn read_etc(rootfs: &[PathBuf], file: &str) -> Result<String, String> {
    for dir in rootfs {
        let etc = dir.join("etc");
        let path = etc.join(file);
        for p in [&etc, &path] {
            if std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(format!("the image's /etc/{file} is behind a symlink"));
            }
        }
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_file() => {
                return std::fs::read_to_string(&path)
                    .map_err(|e| format!("read the image's /etc/{file}: {e}"));
            }
            // An overlay whiteout (or anything else) hides the lower layers.
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("read the image's /etc/{file}: {e}")),
        }
    }
    Err(format!("the image has no /etc/{file}"))
}

/// The directories behind a prepared snapshot's mounts, top layer first:
/// overlay `upperdir` then each `lowerdir`, or a bind mount's source.
pub fn rootfs_dirs(mounts: &[Mount]) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for mount in mounts {
        if mount.r#type != "overlay" {
            dirs.push(PathBuf::from(&mount.source));
            continue;
        }
        let option = |key: &str| mount.options.iter().find_map(|o| o.strip_prefix(key));
        dirs.extend(option("upperdir=").map(PathBuf::from));
        if let Some(lower) = option("lowerdir=") {
            dirs.extend(lower.split(':').map(PathBuf::from));
        }
    }
    dirs
}

/// Serialize a runnable OCI spec using the selected image's process defaults.
/// Explicit task environment entries override image values by key.
pub fn spec_to_any_with_image(
    container_id: &str,
    spec: &TaskSpec,
    secret_mounts: &[SecretMount],
    image: &ImageConfiguration,
    (uid, gid): (u32, u32),
    network: Option<&NetworkSpec>,
) -> Result<prost_types::Any, String> {
    // Docker override semantics: an explicit task entrypoint replaces the
    // image Entrypoint (keeping image Cmd unless a task command is given);
    // an explicit task command replaces the image Cmd (keeping the image
    // Entrypoint).
    let args = if !spec.entrypoint.is_empty() {
        let cmd = if spec.command.is_empty() {
            image.cmd.clone()
        } else {
            spec.command.clone()
        };
        [spec.entrypoint.clone(), cmd].concat()
    } else if !spec.command.is_empty() {
        [image.entrypoint.clone(), spec.command.clone()].concat()
    } else {
        image.args()
    };
    if args.is_empty() || args[0].is_empty() {
        return Err("image has no executable command".into());
    }
    let mut oci = spec_from_task_with_secrets(container_id, spec, secret_mounts, network);
    oci.process.args = args;
    oci.process.cwd = image.cwd.clone();
    oci.process.user = OciUser { uid, gid };
    let mut env = image.env.clone();
    let mut overrides: Vec<_> = spec.env.iter().collect();
    overrides.sort_by_key(|(key, _)| *key);
    for (key, value) in overrides {
        env.retain(|entry| match entry.split_once('=') {
            Some((name, _)) => name != key,
            None => true,
        });
        env.push(format!("{key}={value}"));
    }
    if !env.iter().any(|entry| entry.starts_with("PATH=")) {
        env.push("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into());
    }
    oci.process.env = env;
    let json = serde_json::to_vec(&oci)
        .map_err(|error| format!("failed to serialize OCI spec: {error}"))?;
    Ok(prost_types::Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".into(),
        value: json,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{PortMapping, ResourceSpec};

    fn base_spec() -> TaskSpec {
        TaskSpec {
            image: "nginx:latest".to_string(),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            ..Default::default()
        }
    }

    #[test]
    fn preloaded_image_process_defaults_and_task_env_override() {
        let mut task = base_spec();
        task.env.insert("A".into(), "override".into());
        let image = ImageConfiguration {
            entrypoint: vec!["/bin/sh".into(), "-c".into()],
            cmd: vec!["echo ok".into()],
            env: vec!["A=image".into(), "PATH=/bin".into(), "B=keep".into()],
            cwd: "/app".into(),
            user: "1000:1001".into(),
            parent_snapshot: "unused".into(),
        };
        let any = spec_to_any_with_image("c1", &task, &[], &image, (1000, 1001), None).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&any.value).unwrap();
        assert_eq!(
            json["process"]["args"],
            serde_json::json!(["/bin/sh", "-c", "echo ok"])
        );
        assert_eq!(json["process"]["cwd"], "/app");
        assert_eq!(json["process"]["user"]["uid"], 1000);
        assert_eq!(
            json["process"]["env"],
            serde_json::json!(["PATH=/bin", "B=keep", "A=override"])
        );
    }

    #[test]
    fn spec_has_required_fields() {
        let spec = spec_from_task("c1", &base_spec());
        assert_eq!(spec.oci_version, "1.0.2");
        assert_eq!(spec.root.path, "rootfs");
        assert!(!spec.process.terminal);
        assert_eq!(spec.process.cwd, "/");
        assert_eq!(spec.hostname, "c1");
    }

    #[test]
    fn spec_includes_env_sorted() {
        let mut ts = base_spec();
        ts.env.insert("Z_VAR".to_string(), "z".to_string());
        ts.env.insert("A_VAR".to_string(), "a".to_string());
        let spec = spec_from_task("c1", &ts);
        let env = &spec.process.env;
        // Sorted: A_VAR before Z_VAR, then PATH
        assert!(
            env.iter().position(|e| e == "A_VAR=a").unwrap()
                < env.iter().position(|e| e == "Z_VAR=z").unwrap()
        );
        assert!(env.iter().any(|e| e.starts_with("PATH=")));
    }

    #[test]
    fn spec_sets_path_if_missing() {
        let spec = spec_from_task("c1", &base_spec());
        assert!(spec.process.env.iter().any(|e| e.starts_with("PATH=")));
    }

    #[test]
    fn spec_does_not_override_path() {
        let mut ts = base_spec();
        ts.env.insert("PATH".to_string(), "/custom".to_string());
        let spec = spec_from_task("c1", &ts);
        let paths: Vec<_> = spec
            .process
            .env
            .iter()
            .filter(|e| e.starts_with("PATH="))
            .collect();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "PATH=/custom");
    }

    #[test]
    fn spec_memory_limit() {
        let mut ts = base_spec();
        ts.resources.memory_bytes = 512 * 1024 * 1024;
        let spec = spec_from_task("c1", &ts);
        let res = spec.linux.resources.unwrap();
        let mem = res.memory.unwrap();
        assert_eq!(mem.limit, Some(512 * 1024 * 1024));
    }

    #[test]
    fn spec_cpu_limit() {
        let mut ts = base_spec();
        ts.resources.cpus = 2.0;
        let spec = spec_from_task("c1", &ts);
        let res = spec.linux.resources.unwrap();
        let cpu = res.cpu.unwrap();
        // 2.0 CPUs * 100000 period = 200000 quota
        assert_eq!(cpu.quota, Some(200_000));
        assert_eq!(cpu.period, Some(100_000));
    }

    #[test]
    fn spec_no_resources_when_zero() {
        let spec = spec_from_task("c1", &base_spec());
        assert!(spec.linux.resources.is_none());
    }

    #[test]
    fn spec_includes_standard_namespaces() {
        let spec = spec_from_task("c1", &base_spec());
        let ns_types: Vec<&str> = spec
            .linux
            .namespaces
            .iter()
            .map(|n| n.ns_type.as_str())
            .collect();
        for expected in &["pid", "ipc", "uts", "mount", "network", "cgroup"] {
            assert!(ns_types.contains(expected), "missing namespace: {expected}");
        }
    }

    #[test]
    fn spec_includes_standard_mounts() {
        let spec = spec_from_task("c1", &base_spec());
        let dests: Vec<&str> = spec.mounts.iter().map(|m| m.destination.as_str()).collect();
        for expected in &["/proc", "/dev", "/dev/pts", "/sys", "/sys/fs/cgroup"] {
            assert!(dests.contains(expected), "missing mount: {expected}");
        }
    }

    #[test]
    fn spec_has_default_capabilities() {
        let spec = spec_from_task("c1", &base_spec());
        let caps = spec.process.capabilities.unwrap();
        assert!(caps.effective.contains(&"CAP_NET_BIND_SERVICE".to_string()));
        assert!(caps.effective.contains(&"CAP_SYS_CHROOT".to_string()));
    }

    #[test]
    fn spec_annotations_include_container_id() {
        let spec = spec_from_task("my-container", &base_spec());
        assert_eq!(
            spec.annotations.get("minga/task/container_id").unwrap(),
            "my-container"
        );
    }

    #[test]
    fn spec_annotations_include_ports() {
        let mut ts = base_spec();
        ts.ports.push(PortMapping {
            target: 80,
            published: Some(8080),
            protocol: "tcp".to_string(),
        });
        let spec = spec_from_task("c1", &ts);
        let ports_json = spec.annotations.get("minga/task/ports").unwrap();
        assert!(ports_json.contains("8080"));
        assert!(ports_json.contains("80"));
    }

    #[test]
    fn spec_serializes_to_json() {
        let spec = spec_from_task("c1", &base_spec());
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"ociVersion\":\"1.0.2\""));
        assert!(json.contains("\"hostname\":\"c1\""));
        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["ociVersion"], "1.0.2");
    }

    #[test]
    fn secret_mount_is_read_only_and_confined() {
        let mut ts = base_spec();
        ts.secrets = vec![minga_state::types::SecretRef::new(
            "stack_password",
            "/run/secrets/db_password",
        )];

        let mount = SecretMount {
            source: std::path::PathBuf::from("/var/lib/minga/secrets/abc123.secret"),
            target: "/run/secrets/db_password".to_string(),
        };
        let spec = spec_from_task_with_secrets("c1", &ts, std::slice::from_ref(&mount), None);

        let m = spec
            .mounts
            .iter()
            .find(|m| m.destination == "/run/secrets/db_password")
            .expect("the secret mount must be present");

        assert_eq!(m.mount_type, "bind");
        assert_eq!(m.source, "/var/lib/minga/secrets/abc123.secret");
        // Read-only, and no privilege can be gained from the file.
        assert!(m.options.contains(&"ro".to_string()));
        assert!(m.options.contains(&"nosuid".to_string()));
        assert!(m.options.contains(&"nodev".to_string()));
        assert!(m.options.contains(&"noexec".to_string()));
        assert!(m.options.contains(&"bind".to_string()));
    }

    #[test]
    fn secret_mounts_preserve_existing_mounts() {
        let ts = base_spec();
        let mount = SecretMount {
            source: std::path::PathBuf::from("/var/lib/minga/secrets/abc123.secret"),
            target: "/run/secrets/db_password".to_string(),
        };
        let plain = spec_from_task("c1", &ts);
        let with_secret =
            spec_from_task_with_secrets("c1", &ts, std::slice::from_ref(&mount), None);

        assert_eq!(
            with_secret.mounts.len(),
            plain.mounts.len() + 1,
            "adding a secret must not disturb the standard mounts"
        );
        for m in &plain.mounts {
            assert!(
                with_secret
                    .mounts
                    .iter()
                    .any(|w| w.destination == m.destination),
                "standard mount {} must survive",
                m.destination
            );
        }
    }

    #[test]
    fn spec_without_secrets_has_no_secret_mount() {
        let ts = base_spec();
        let spec = spec_from_task("c1", &ts);
        assert!(
            !spec
                .mounts
                .iter()
                .any(|m| m.destination.starts_with("/run/secrets/")),
            "a task with no secrets must have no secret mount"
        );
    }

    #[test]
    fn spec_to_any_produces_valid_any() {
        let any = spec_to_any("c1", &base_spec()).unwrap();
        assert_eq!(
            any.type_url,
            "types.containerd.io/opencontainers/runtime-spec/1/Spec"
        );
        // The value should be valid JSON
        let json: serde_json::Value = serde_json::from_slice(&any.value).unwrap();
        assert_eq!(json["ociVersion"], "1.0.2");
    }

    #[test]
    fn spec_no_new_privileges() {
        let spec = spec_from_task("c1", &base_spec());
        assert!(spec.process.no_new_privileges);
    }
}

#[cfg(test)]
mod override_tests {
    use super::*;
    use minga_state::types::TaskSpec;

    fn image() -> ImageConfiguration {
        ImageConfiguration {
            entrypoint: vec!["/entry".into()],
            cmd: vec!["img-cmd".into()],
            env: vec![],
            cwd: "/".into(),
            user: String::new(),
            parent_snapshot: "unused".into(),
        }
    }

    #[test]
    fn task_user_prefers_the_task_and_refuses_an_unknown_name() {
        let named = ImageConfiguration {
            user: "nodejs".into(),
            ..image()
        };
        let mut task = TaskSpec::default();
        let error = task_user(&task, &named, &[]).unwrap_err();
        assert!(error.contains("nodejs"), "{error}");
        assert_eq!(task_user(&task, &image(), &[]), Ok((0, 0)));

        task.user = Some("1001:65533".into());
        assert_eq!(task_user(&task, &named, &[]), Ok((1001, 65533)));
    }

    /// Two snapshot layers: `upper` overrides `/etc/passwd`, `lower` holds
    /// `/etc/group`.
    fn layered_rootfs() -> (tempfile::TempDir, Vec<PathBuf>) {
        let root = tempfile::tempdir().unwrap();
        let (upper, lower) = (root.path().join("upper"), root.path().join("lower"));
        for dir in [&upper, &lower] {
            std::fs::create_dir_all(dir.join("etc")).unwrap();
        }
        std::fs::write(
            upper.join("etc/passwd"),
            "root:x:0:0:root:/root:/bin/sh\nzitadel:x:1000:1000::/home/zitadel:/sbin/nologin\n",
        )
        .unwrap();
        std::fs::write(lower.join("etc/passwd"), "zitadel:x:1:1::/:/bin/sh\n").unwrap();
        std::fs::write(lower.join("etc/group"), "root:x:0:\nstaff:x:50:zitadel\n").unwrap();
        (root, vec![upper, lower])
    }

    #[test]
    fn task_user_resolves_names_from_the_top_layer() {
        let (_root, rootfs) = layered_rootfs();
        let user = |u: &str| {
            let task = TaskSpec {
                user: Some(u.into()),
                ..Default::default()
            };
            task_user(&task, &image(), &rootfs)
        };
        assert_eq!(user("zitadel"), Ok((1000, 1000)));
        assert_eq!(user("zitadel:staff"), Ok((1000, 50)));
        assert_eq!(user("zitadel:7"), Ok((1000, 7)));
        assert_eq!(user("1234:staff"), Ok((1234, 50)));
        assert!(user("nobody").unwrap_err().contains("no user `nobody`"));
        assert!(user("zitadel:wheel")
            .unwrap_err()
            .contains("no group `wheel`"));
    }

    #[cfg(unix)]
    #[test]
    fn task_user_refuses_a_symlinked_passwd() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("etc")).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", root.path().join("etc/passwd")).unwrap();
        let task = TaskSpec {
            user: Some("root".into()),
            ..Default::default()
        };
        let error = task_user(&task, &image(), &[root.path().to_path_buf()]).unwrap_err();
        assert!(error.contains("symlink"), "{error}");
    }

    #[test]
    fn rootfs_dirs_lists_overlay_layers_top_first() {
        let overlay = Mount {
            r#type: "overlay".into(),
            source: "overlay".into(),
            target: String::new(),
            options: vec![
                "workdir=/s/9/work".into(),
                "upperdir=/s/9/fs".into(),
                "lowerdir=/s/8/fs:/s/7/fs".into(),
            ],
        };
        let bind = Mount {
            r#type: "bind".into(),
            source: "/s/1/fs".into(),
            target: String::new(),
            options: vec!["rbind".into(), "ro".into()],
        };
        assert_eq!(
            rootfs_dirs(&[overlay]),
            ["/s/9/fs", "/s/8/fs", "/s/7/fs"].map(PathBuf::from)
        );
        assert_eq!(rootfs_dirs(&[bind]), [PathBuf::from("/s/1/fs")]);
    }

    fn args_of(spec: &TaskSpec) -> Vec<String> {
        let any = spec_to_any_with_image("c1", spec, &[], &image(), (0, 0), None).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&any.value).unwrap();
        serde_json::from_value(json["process"]["args"].clone()).unwrap()
    }

    #[test]
    fn image_defaults_when_no_overrides() {
        assert_eq!(args_of(&TaskSpec::default()), vec!["/entry", "img-cmd"]);
    }

    #[test]
    fn command_replaces_image_cmd_keeps_entrypoint() {
        let spec = TaskSpec {
            command: vec!["run".into(), "--fast".into()],
            ..Default::default()
        };
        assert_eq!(args_of(&spec), vec!["/entry", "run", "--fast"]);
    }

    #[test]
    fn entrypoint_replaces_image_entrypoint_keeps_cmd() {
        let spec = TaskSpec {
            entrypoint: vec!["/custom".into()],
            ..Default::default()
        };
        assert_eq!(args_of(&spec), vec!["/custom", "img-cmd"]);
    }

    #[test]
    fn entrypoint_and_command_replace_both() {
        let spec = TaskSpec {
            entrypoint: vec!["/custom".into()],
            command: vec!["go".into()],
            ..Default::default()
        };
        assert_eq!(args_of(&spec), vec!["/custom", "go"]);
    }

    #[test]
    fn mounts_become_bind_mounts_with_ro_flag() {
        let spec = TaskSpec {
            mounts: vec![
                minga_state::types::ServiceMount {
                    kind: minga_state::types::MountKind::Volume,
                    source: "/var/lib/minga/volumes/shop_data".into(),
                    target: "/data".into(),
                    read_only: false,
                },
                minga_state::types::ServiceMount {
                    kind: minga_state::types::MountKind::Bind,
                    source: "/etc/hosts".into(),
                    target: "/etc/hosts".into(),
                    read_only: true,
                },
            ],
            ..Default::default()
        };
        let any = spec_to_any_with_image("c1", &spec, &[], &image(), (0, 0), None).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&any.value).unwrap();
        let mounts = json["mounts"].as_array().unwrap();
        let data = mounts
            .iter()
            .find(|m| m["destination"] == "/data")
            .expect("data mount present");
        assert_eq!(data["source"], "/var/lib/minga/volumes/shop_data");
        assert!(data["options"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("rw")));
        let hosts = mounts
            .iter()
            .find(|m| m["destination"] == "/etc/hosts")
            .expect("hosts mount present");
        assert!(hosts["options"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("ro")));
    }
}
