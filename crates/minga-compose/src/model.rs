//! Typed Compose model. Every place the spec allows a short and a long syntax
//! (ports, volumes, depends_on, devices, secrets, ...) is normalised to one
//! struct here, so later stages never see the short forms.

use std::fmt::Display;
use std::str::FromStr;

use indexmap::IndexMap;
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer};
use serde_yaml::Value;

/// A whole Compose file after interpolation and key checks.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ComposeFile {
    /// Obsolete in the spec; accepted and ignored with a warning.
    #[serde(deserialize_with = "opt_parse")]
    pub version: Option<String>,
    pub name: Option<String>,
    pub services: IndexMap<String, Service>,
    #[serde(deserialize_with = "map_of_optional")]
    pub networks: IndexMap<String, Network>,
    #[serde(deserialize_with = "map_of_optional")]
    pub volumes: IndexMap<String, Volume>,
    #[serde(deserialize_with = "map_of_optional")]
    pub secrets: IndexMap<String, FileObject>,
    #[serde(deserialize_with = "map_of_optional")]
    pub configs: IndexMap<String, FileObject>,
    /// Top-level `x-` keys.
    #[serde(skip)]
    pub extensions: IndexMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Service {
    pub image: Option<String>,
    pub command: Option<StringOrList>,
    pub entrypoint: Option<StringOrList>,
    pub environment: ListOrMap,
    pub labels: ListOrMap,
    pub ports: Vec<Port>,
    pub volumes: Vec<ServiceVolume>,
    #[serde(deserialize_with = "service_networks")]
    pub networks: IndexMap<String, ServiceNetwork>,
    #[serde(deserialize_with = "depends_on")]
    pub depends_on: IndexMap<String, Dependency>,
    pub healthcheck: Option<Healthcheck>,
    pub deploy: Option<Deploy>,
    pub restart: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub privileged: Option<bool>,
    pub devices: Vec<Device>,
    pub cap_add: Vec<String>,
    pub cap_drop: Vec<String>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
    pub hostname: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub init: Option<bool>,
    #[serde(deserialize_with = "opt_parse")]
    pub read_only: Option<bool>,
    pub secrets: Vec<FileRef>,
    pub configs: Vec<FileRef>,
    pub profiles: Vec<String>,
    pub stop_grace_period: Option<String>,
    pub stop_signal: Option<String>,
    pub sysctls: ListOrMap,
    pub tmpfs: Option<StringOrList>,
    pub ulimits: IndexMap<String, Ulimit>,
    pub logging: Option<Logging>,
    /// This service's `x-` keys.
    #[serde(skip)]
    pub extensions: IndexMap<String, Value>,
}

/// `command`, `entrypoint`, `tmpfs` and `healthcheck.test`: one string or a list.
#[derive(Debug, Clone, PartialEq)]
pub enum StringOrList {
    String(String),
    List(Vec<String>),
}

impl<'de> Deserialize<'de> for StringOrList {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::Sequence(items) => items
                .iter()
                .map(|v| scalar(v).ok_or_else(|| D::Error::custom("list entries must be strings")))
                .collect::<Result<_, _>>()
                .map(Self::List),
            v => scalar(&v)
                .map(Self::String)
                .ok_or_else(|| D::Error::custom("expected a string or a list of strings")),
        }
    }
}

/// `environment`, `labels`, `sysctls`: a list of `KEY=value` or a mapping.
/// A key with no value (`- DEBUG` or `DEBUG:`) is kept as `None`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListOrMap(pub IndexMap<String, Option<String>>);

impl<'de> Deserialize<'de> for ListOrMap {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut out = IndexMap::new();
        match Value::deserialize(d)? {
            Value::Null => {}
            Value::Sequence(items) => {
                for item in items {
                    let s = item.as_str().ok_or_else(|| {
                        D::Error::custom("list entries must be strings like KEY=value")
                    })?;
                    match s.split_once('=') {
                        Some((k, v)) => out.insert(k.to_string(), Some(v.to_string())),
                        None => out.insert(s.to_string(), None),
                    };
                }
            }
            Value::Mapping(map) => {
                for (k, v) in map {
                    let k = scalar(&k).ok_or_else(|| D::Error::custom("keys must be strings"))?;
                    let v = match v {
                        Value::Null => None,
                        other => Some(scalar(&other).ok_or_else(|| {
                            D::Error::custom(format!(
                                "value of {k} must be a string, number or boolean"
                            ))
                        })?),
                    };
                    out.insert(k, v);
                }
            }
            _ => {
                return Err(D::Error::custom(
                    "expected a list of KEY=value strings or a mapping",
                ))
            }
        }
        Ok(Self(out))
    }
}

/// An inclusive port range; a single port has `start == end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

impl PortRange {
    pub fn single(port: u16) -> Self {
        Self {
            start: port,
            end: port,
        }
    }

    /// Number of ports in the range (at least 1).
    pub fn count(&self) -> u32 {
        u32::from(self.end - self.start) + 1
    }
}

impl FromStr for PortRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let port = |p: &str| {
            p.trim()
                .parse::<u16>()
                .map_err(|_| format!("invalid port {p:?}"))
        };
        let range = match s.split_once('-') {
            Some((a, b)) => Self {
                start: port(a)?,
                end: port(b)?,
            },
            None => Self::single(port(s)?),
        };
        if range.end < range.start {
            return Err(format!("port range {s:?} ends before it starts"));
        }
        Ok(range)
    }
}

impl Display for PortRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Port {
    pub target: PortRange,
    pub published: Option<PortRange>,
    pub host_ip: Option<String>,
    pub protocol: String,
    /// `ingress` (routing mesh) or `host`.
    pub mode: Option<String>,
    pub name: Option<String>,
    pub app_protocol: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PortLong {
    target: Value,
    published: Option<Value>,
    host_ip: Option<String>,
    protocol: Option<String>,
    mode: Option<String>,
    name: Option<String>,
    app_protocol: Option<String>,
}

impl Port {
    /// Parses the short syntax: `80`, `8080:80`, `127.0.0.1:8080:80`,
    /// `[::1]:8080:80`, `8000-8010:8000-8010/udp`, `127.0.0.1::80`.
    pub fn parse_short(s: &str) -> Result<Self, String> {
        let (spec, protocol) = match s.rsplit_once('/') {
            Some((spec, proto)) => (spec, proto.to_string()),
            None => (s, "tcp".to_string()),
        };
        let (host_ip, rest) = if let Some(after) = spec.strip_prefix('[') {
            let (ip, rest) = after
                .split_once("]:")
                .ok_or_else(|| format!("invalid IPv6 port mapping {s:?}"))?;
            (Some(ip.to_string()), rest)
        } else {
            (None, spec)
        };
        let parts: Vec<&str> = rest.split(':').collect();
        let (host_ip, published, target) = match (host_ip, parts.as_slice()) {
            (None, [target]) => (None, None, *target),
            (ip, [published, target]) => (ip, Some(*published), *target),
            (None, [ip, published, target]) => (Some(ip.to_string()), Some(*published), *target),
            _ => return Err(format!("invalid port mapping {s:?}")),
        };
        let published = match published {
            Some("") | None => None,
            Some(p) => Some(p.parse()?),
        };
        let port = Self {
            target: target.parse()?,
            published,
            host_ip,
            protocol,
            mode: None,
            name: None,
            app_protocol: None,
        };
        port.check(s)?;
        Ok(port)
    }

    fn check(&self, s: &str) -> Result<(), String> {
        if let Some(p) = self.published {
            if self.target.count() > 1 && p.count() != self.target.count() {
                return Err(format!("{s:?}: published and target ranges differ in size"));
            }
        }
        if !matches!(self.protocol.as_str(), "tcp" | "udp" | "sctp") {
            return Err(format!("{s:?}: protocol must be tcp, udp or sctp"));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for Port {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::Number(n) => match n.as_u64().and_then(|n| u16::try_from(n).ok()) {
                Some(p) => Self::parse_short(&p.to_string()).map_err(D::Error::custom),
                None => Err(D::Error::custom(format!("invalid port {n}"))),
            },
            Value::String(s) => Self::parse_short(&s).map_err(D::Error::custom),
            v @ Value::Mapping(_) => {
                let long: PortLong = from_value(v).map_err(D::Error::custom)?;
                let range = |v: &Value| -> Result<PortRange, String> {
                    scalar(v)
                        .ok_or_else(|| "port must be a number or string".to_string())?
                        .parse()
                };
                let port = Self {
                    target: range(&long.target).map_err(D::Error::custom)?,
                    published: long
                        .published
                        .as_ref()
                        .map(range)
                        .transpose()
                        .map_err(D::Error::custom)?,
                    host_ip: long.host_ip,
                    protocol: long.protocol.unwrap_or_else(|| "tcp".to_string()),
                    mode: long.mode,
                    name: long.name,
                    app_protocol: long.app_protocol,
                };
                port.check("ports entry").map_err(D::Error::custom)?;
                Ok(port)
            }
            _ => Err(D::Error::custom(
                "expected a port number, string or mapping",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeKind {
    Bind,
    Volume,
    Tmpfs,
    Npipe,
    Cluster,
    Image,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceVolume {
    pub kind: VolumeKind,
    /// Host path for binds, volume name for named volumes, `None` for anonymous volumes.
    pub source: Option<String>,
    pub target: String,
    pub read_only: bool,
    /// Other short-syntax mode flags, such as `z` or `Z` for SELinux.
    pub flags: Vec<String>,
    pub bind: Option<Value>,
    pub volume: Option<Value>,
    pub tmpfs: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VolumeLong {
    #[serde(rename = "type")]
    kind: VolumeKind,
    source: Option<String>,
    target: String,
    #[serde(default, deserialize_with = "opt_parse")]
    read_only: Option<bool>,
    bind: Option<Value>,
    volume: Option<Value>,
    tmpfs: Option<Value>,
    #[allow(dead_code)]
    image: Option<Value>,
    #[allow(dead_code)]
    consistency: Option<String>,
}

impl ServiceVolume {
    /// Parses `target`, `source:target` or `source:target:mode`.
    pub fn parse_short(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();
        let (source, target, mode) = match parts.as_slice() {
            [target] => (None, *target, ""),
            [source, target] => (Some(*source), *target, ""),
            [source, target, mode] => (Some(*source), *target, *mode),
            _ => return Err(format!("invalid volume {s:?}")),
        };
        if !target.starts_with('/') {
            return Err(format!("volume {s:?}: target must be an absolute path"));
        }
        let kind = match source {
            Some(src) if src.starts_with(['/', '.', '~']) => VolumeKind::Bind,
            _ => VolumeKind::Volume,
        };
        let mut read_only = false;
        let mut flags = Vec::new();
        for m in mode.split(',').filter(|m| !m.is_empty()) {
            match m {
                "ro" => read_only = true,
                "rw" => read_only = false,
                "z" | "Z" | "nocopy" | "cached" | "delegated" | "consistent" => {
                    flags.push(m.to_string())
                }
                other => return Err(format!("volume {s:?}: unknown mode {other:?}")),
            }
        }
        Ok(Self {
            kind,
            source: source.map(str::to_string),
            target: target.to_string(),
            read_only,
            flags,
            bind: None,
            volume: None,
            tmpfs: None,
        })
    }
}

impl<'de> Deserialize<'de> for ServiceVolume {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::String(s) => Self::parse_short(&s).map_err(D::Error::custom),
            v @ Value::Mapping(_) => {
                let long: VolumeLong = from_value(v).map_err(D::Error::custom)?;
                Ok(Self {
                    kind: long.kind,
                    source: long.source,
                    target: long.target,
                    read_only: long.read_only.unwrap_or(false),
                    flags: Vec::new(),
                    bind: long.bind,
                    volume: long.volume,
                    tmpfs: long.tmpfs,
                })
            }
            _ => Err(D::Error::custom("expected a volume string or mapping")),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServiceNetwork {
    pub aliases: Vec<String>,
    pub ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub link_local_ips: Vec<String>,
    pub mac_address: Option<String>,
    pub driver_opts: IndexMap<String, Value>,
    pub priority: Option<i64>,
    pub gw_priority: Option<i64>,
    pub interface_name: Option<String>,
}

fn service_networks<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<IndexMap<String, ServiceNetwork>, D::Error> {
    match Value::deserialize(d)? {
        Value::Null => Ok(IndexMap::new()),
        Value::Sequence(items) => items
            .iter()
            .map(|v| {
                scalar(v)
                    .map(|n| (n, ServiceNetwork::default()))
                    .ok_or_else(|| D::Error::custom("network names must be strings"))
            })
            .collect(),
        v @ Value::Mapping(_) => {
            let m: IndexMap<String, Option<ServiceNetwork>> =
                from_value(v).map_err(D::Error::custom)?;
            Ok(m.into_iter()
                .map(|(k, v)| (k, v.unwrap_or_default()))
                .collect())
        }
        _ => Err(D::Error::custom(
            "expected a list of network names or a mapping",
        )),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    #[default]
    ServiceStarted,
    ServiceHealthy,
    ServiceCompletedSuccessfully,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dependency {
    pub condition: Condition,
    /// Restart this service when the dependency is updated.
    pub restart: bool,
    /// When false, a missing or disabled dependency is skipped instead of failing.
    pub required: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyLong {
    #[serde(default)]
    condition: Condition,
    #[serde(default, deserialize_with = "opt_parse")]
    restart: Option<bool>,
    #[serde(default, deserialize_with = "opt_parse")]
    required: Option<bool>,
}

fn depends_on<'de, D: Deserializer<'de>>(d: D) -> Result<IndexMap<String, Dependency>, D::Error> {
    let started = Dependency {
        condition: Condition::ServiceStarted,
        restart: false,
        required: true,
    };
    match Value::deserialize(d)? {
        Value::Null => Ok(IndexMap::new()),
        Value::Sequence(items) => items
            .iter()
            .map(|v| {
                scalar(v)
                    .map(|n| (n, started.clone()))
                    .ok_or_else(|| D::Error::custom("service names must be strings"))
            })
            .collect(),
        v @ Value::Mapping(_) => {
            let m: IndexMap<String, DependencyLong> = from_value(v).map_err(D::Error::custom)?;
            Ok(m.into_iter()
                .map(|(k, l)| {
                    let dep = Dependency {
                        condition: l.condition,
                        restart: l.restart.unwrap_or(false),
                        required: l.required.unwrap_or(true),
                    };
                    (k, dep)
                })
                .collect())
        }
        _ => Err(D::Error::custom(
            "expected a list of service names or a mapping",
        )),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Healthcheck {
    pub test: Option<StringOrList>,
    pub interval: Option<String>,
    pub timeout: Option<String>,
    pub start_period: Option<String>,
    pub start_interval: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub retries: Option<u32>,
    #[serde(deserialize_with = "opt_parse")]
    pub disable: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeployMode {
    #[default]
    Replicated,
    Global,
    ReplicatedJob,
    GlobalJob,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Deploy {
    pub mode: DeployMode,
    #[serde(deserialize_with = "opt_parse")]
    pub replicas: Option<u32>,
    pub endpoint_mode: Option<String>,
    pub labels: ListOrMap,
    pub placement: Option<Placement>,
    pub resources: Option<Resources>,
    pub restart_policy: Option<RestartPolicy>,
    pub update_config: Option<UpdateConfig>,
    pub rollback_config: Option<UpdateConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Placement {
    pub constraints: Vec<String>,
    pub preferences: Vec<Value>,
    #[serde(deserialize_with = "opt_parse")]
    pub max_replicas_per_node: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Resources {
    pub limits: Option<ResourceSpec>,
    pub reservations: Option<ResourceSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ResourceSpec {
    /// CPUs as a decimal, e.g. `0.5`.
    #[serde(deserialize_with = "opt_parse")]
    pub cpus: Option<f64>,
    /// Memory as written, e.g. `512M`.
    #[serde(deserialize_with = "opt_parse")]
    pub memory: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub pids: Option<i64>,
    pub devices: Vec<DeviceRequest>,
    pub generic_resources: Vec<Value>,
}

/// A request for devices such as GPUs, from `deploy.resources.reservations.devices`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DeviceRequest {
    pub capabilities: Vec<String>,
    pub driver: Option<String>,
    pub count: Option<DeviceCount>,
    pub device_ids: Vec<String>,
    pub options: IndexMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceCount {
    All,
    Count(u32),
}

impl<'de> Deserialize<'de> for DeviceCount {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        let s = scalar(&v).ok_or_else(|| D::Error::custom("count must be a number or \"all\""))?;
        if s == "all" {
            return Ok(Self::All);
        }
        s.parse()
            .map(Self::Count)
            .map_err(|_| D::Error::custom(format!("count must be a number or \"all\", got {s:?}")))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RestartPolicy {
    pub condition: Option<String>,
    pub delay: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub max_attempts: Option<u32>,
    pub window: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UpdateConfig {
    #[serde(deserialize_with = "opt_parse")]
    pub parallelism: Option<u32>,
    pub delay: Option<String>,
    pub failure_action: Option<String>,
    pub monitor: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub max_failure_ratio: Option<f64>,
    pub order: Option<String>,
}

/// A host device mapped into the container, or a CDI device name.
#[derive(Debug, Clone, PartialEq)]
pub struct Device {
    pub source: String,
    pub target: Option<String>,
    pub permissions: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceLong {
    source: String,
    target: Option<String>,
    permissions: Option<String>,
}

impl Device {
    /// Parses `/dev/x`, `/dev/x:/dev/y`, `/dev/x:/dev/y:rwm` or a CDI name
    /// such as `nvidia.com/gpu=all`.
    pub fn parse_short(s: &str) -> Result<Self, String> {
        if !s.starts_with('/') && s.contains('=') {
            return Ok(Self {
                source: s.to_string(),
                target: None,
                permissions: None,
            });
        }
        let parts: Vec<&str> = s.split(':').collect();
        let (source, target, permissions) = match parts.as_slice() {
            [src] => (*src, None, None),
            [src, dst] => (*src, Some(*dst), None),
            [src, dst, perms] => (*src, Some(*dst), Some(*perms)),
            _ => return Err(format!("invalid device {s:?}")),
        };
        if let Some(p) = permissions {
            if p.is_empty() || !p.chars().all(|c| matches!(c, 'r' | 'w' | 'm')) {
                return Err(format!("device {s:?}: permissions must use r, w and m"));
            }
        }
        Ok(Self {
            source: source.to_string(),
            target: target.map(str::to_string),
            permissions: permissions.map(str::to_string),
        })
    }
}

impl<'de> Deserialize<'de> for Device {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::String(s) => Self::parse_short(&s).map_err(D::Error::custom),
            v @ Value::Mapping(_) => {
                let l: DeviceLong = from_value(v).map_err(D::Error::custom)?;
                Ok(Self {
                    source: l.source,
                    target: l.target,
                    permissions: l.permissions,
                })
            }
            _ => Err(D::Error::custom("expected a device string or mapping")),
        }
    }
}

/// A service's reference to a top-level secret or config.
#[derive(Debug, Clone, PartialEq)]
pub struct FileRef {
    pub source: String,
    pub target: Option<String>,
    pub uid: Option<String>,
    pub gid: Option<String>,
    /// File mode as written, e.g. `0440`.
    pub mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRefLong {
    source: String,
    target: Option<String>,
    #[serde(default, deserialize_with = "opt_parse")]
    uid: Option<String>,
    #[serde(default, deserialize_with = "opt_parse")]
    gid: Option<String>,
    #[serde(default, deserialize_with = "opt_parse")]
    mode: Option<String>,
}

impl<'de> Deserialize<'de> for FileRef {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Value::deserialize(d)? {
            Value::String(source) => Ok(Self {
                source,
                target: None,
                uid: None,
                gid: None,
                mode: None,
            }),
            v @ Value::Mapping(_) => {
                let l: FileRefLong = from_value(v).map_err(D::Error::custom)?;
                Ok(Self {
                    source: l.source,
                    target: l.target,
                    uid: l.uid,
                    gid: l.gid,
                    mode: l.mode,
                })
            }
            _ => Err(D::Error::custom(
                "expected a name or a mapping with `source`",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ulimit {
    Single(i64),
    Range { soft: i64, hard: i64 },
}

impl<'de> Deserialize<'de> for Ulimit {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Range {
            soft: i64,
            hard: i64,
        }
        match Value::deserialize(d)? {
            v @ Value::Mapping(_) => {
                let r: Range = from_value(v).map_err(D::Error::custom)?;
                Ok(Self::Range {
                    soft: r.soft,
                    hard: r.hard,
                })
            }
            v => scalar(&v)
                .and_then(|s| s.parse().ok())
                .map(Self::Single)
                .ok_or_else(|| D::Error::custom("expected a number or {soft, hard}")),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Logging {
    pub driver: Option<String>,
    pub options: IndexMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Network {
    pub name: Option<String>,
    pub driver: Option<String>,
    pub driver_opts: IndexMap<String, Value>,
    #[serde(deserialize_with = "opt_parse")]
    pub attachable: Option<bool>,
    #[serde(deserialize_with = "opt_parse")]
    pub enable_ipv4: Option<bool>,
    #[serde(deserialize_with = "opt_parse")]
    pub enable_ipv6: Option<bool>,
    #[serde(deserialize_with = "opt_parse")]
    pub external: Option<bool>,
    #[serde(deserialize_with = "opt_parse")]
    pub internal: Option<bool>,
    pub ipam: Option<Value>,
    pub labels: ListOrMap,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Volume {
    pub name: Option<String>,
    pub driver: Option<String>,
    pub driver_opts: IndexMap<String, Value>,
    #[serde(deserialize_with = "opt_parse")]
    pub external: Option<bool>,
    pub labels: ListOrMap,
}

/// A top-level secret or config.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FileObject {
    pub name: Option<String>,
    pub file: Option<String>,
    pub environment: Option<String>,
    pub content: Option<String>,
    #[serde(deserialize_with = "opt_parse")]
    pub external: Option<bool>,
    pub labels: ListOrMap,
    pub driver: Option<String>,
    pub driver_opts: IndexMap<String, Value>,
    pub template_driver: Option<String>,
}

// --- helpers -----------------------------------------------------------------

/// A YAML scalar as a string. Interpolation turns numbers into strings, so
/// every numeric or boolean field must accept both.
pub(crate) fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Parses an optional scalar that may be written as a string or a number.
fn opt_parse<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: Display,
{
    let v = Value::deserialize(d)?;
    if v.is_null() {
        return Ok(None);
    }
    let s = scalar(&v)
        .ok_or_else(|| D::Error::custom("expected a single value, not a list or mapping"))?;
    s.trim()
        .parse()
        .map(Some)
        .map_err(|e| D::Error::custom(format!("invalid value {s:?}: {e}")))
}

/// A mapping whose values may be empty (`volumes: { data: }`).
fn map_of_optional<'de, D, T>(d: D) -> Result<IndexMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Default,
{
    let m: Option<IndexMap<String, Option<T>>> = Option::deserialize(d)?;
    Ok(m.unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect())
}

fn from_value<T: DeserializeOwned>(v: Value) -> Result<T, String> {
    serde_yaml::from_value(v).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_ports() {
        let p = Port::parse_short("8080:80").unwrap();
        assert_eq!(
            (p.published, p.target),
            (Some(PortRange::single(8080)), PortRange::single(80))
        );
        let p = Port::parse_short("127.0.0.1:5432:5432").unwrap();
        assert_eq!(p.host_ip.as_deref(), Some("127.0.0.1"));
        let p = Port::parse_short("[::1]:8080:80/udp").unwrap();
        assert_eq!(
            (p.host_ip.as_deref(), p.protocol.as_str()),
            (Some("::1"), "udp")
        );
        let p = Port::parse_short("127.0.0.1::80").unwrap();
        assert_eq!(p.published, None);
        let p = Port::parse_short("9000-9002:9000-9002").unwrap();
        assert_eq!(p.target.count(), 3);
        assert!(Port::parse_short("9000-9002:80-81").is_err());
        assert!(Port::parse_short("80/http").is_err());
        assert!(Port::parse_short("99999").is_err());
    }

    #[test]
    fn short_volumes() {
        let v = ServiceVolume::parse_short("./data:/var/lib/data:ro,z").unwrap();
        assert_eq!(v.kind, VolumeKind::Bind);
        assert!(v.read_only);
        assert_eq!(v.flags, ["z"]);
        let v = ServiceVolume::parse_short("db-data:/var/lib/postgresql/data").unwrap();
        assert_eq!(
            (v.kind, v.source.as_deref()),
            (VolumeKind::Volume, Some("db-data"))
        );
        let v = ServiceVolume::parse_short("/scratch").unwrap();
        assert_eq!(v.source, None);
        assert!(ServiceVolume::parse_short("data:relative").is_err());
        assert!(ServiceVolume::parse_short("a:/b:bogus").is_err());
    }

    #[test]
    fn short_devices() {
        let d = Device::parse_short("/dev/ttyUSB0:/dev/ttyUSB0:rw").unwrap();
        assert_eq!(d.permissions.as_deref(), Some("rw"));
        let d = Device::parse_short("nvidia.com/gpu=all").unwrap();
        assert_eq!((d.source.as_str(), d.target), ("nvidia.com/gpu=all", None));
        assert!(Device::parse_short("/dev/x:/dev/x:rwx").is_err());
    }
}
