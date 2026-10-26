//! OCI runtime spec generation from Minga [`TaskSpec`].
//!
//! Generates a minimal OCI runtime spec (JSON) that containerd can use
//! to create a container. The spec is serialized to JSON bytes and packed
//! into a `google.protobuf.Any` when creating the containerd container.
//!
//! Only the fields Minga needs are populated. Everything else uses OCI
//! spec defaults (the runtime fills them in).

use std::collections::HashMap;

use minga_state::types::TaskSpec;
use serde::Serialize;

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
/// `types.containerd.io/opencontainers.runtime.spec.v1.Spec`.
pub fn spec_to_any(container_id: &str, spec: &TaskSpec) -> Result<prost_types::Any, String> {
    let oci = spec_from_task(container_id, spec);
    let json =
        serde_json::to_vec(&oci).map_err(|e| format!("failed to serialize OCI spec: {e}"))?;
    Ok(prost_types::Any {
        type_url: "types.containerd.io/opencontainers.runtime.spec.v1.Spec".to_string(),
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
        }
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
    fn spec_to_any_produces_valid_any() {
        let any = spec_to_any("c1", &base_spec()).unwrap();
        assert_eq!(
            any.type_url,
            "types.containerd.io/opencontainers.runtime.spec.v1.Spec"
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
