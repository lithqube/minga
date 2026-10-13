//! Key checks that run before typed parsing, so errors can tell three cases
//! apart: a typo (unknown key), a Compose key Minga does not support yet, and
//! an `x-` extension (kept aside, never an error).

use indexmap::IndexMap;
use serde_yaml::{Mapping, Value};

use crate::error::ComposeError;

/// Top-level keys in the Compose Specification.
const SPEC_TOP: &[&str] = &[
    "version", "name", "include", "services", "networks", "volumes", "secrets", "configs", "models",
];

/// Top-level keys Minga parses today.
const MODELLED_TOP: &[&str] = &[
    "version", "name", "services", "networks", "volumes", "secrets", "configs",
];

/// Service keys in the Compose Specification.
const SPEC_SERVICE: &[&str] = &[
    "annotations",
    "attach",
    "blkio_config",
    "build",
    "cap_add",
    "cap_drop",
    "cgroup",
    "cgroup_parent",
    "command",
    "configs",
    "container_name",
    "cpu_count",
    "cpu_percent",
    "cpu_period",
    "cpu_quota",
    "cpu_rt_period",
    "cpu_rt_runtime",
    "cpu_shares",
    "cpus",
    "cpuset",
    "credential_spec",
    "depends_on",
    "deploy",
    "develop",
    "device_cgroup_rules",
    "devices",
    "dns",
    "dns_opt",
    "dns_search",
    "domainname",
    "entrypoint",
    "env_file",
    "environment",
    "expose",
    "extends",
    "external_links",
    "extra_hosts",
    "gpus",
    "group_add",
    "healthcheck",
    "hostname",
    "image",
    "init",
    "ipc",
    "isolation",
    "label_file",
    "labels",
    "links",
    "logging",
    "mac_address",
    "mem_limit",
    "mem_reservation",
    "mem_swappiness",
    "memswap_limit",
    "models",
    "network_mode",
    "networks",
    "oom_kill_disable",
    "oom_score_adj",
    "pid",
    "pids_limit",
    "platform",
    "ports",
    "post_start",
    "pre_stop",
    "privileged",
    "profiles",
    "provider",
    "pull_policy",
    "read_only",
    "restart",
    "runtime",
    "scale",
    "secrets",
    "security_opt",
    "shm_size",
    "stdin_open",
    "stop_grace_period",
    "stop_signal",
    "storage_opt",
    "sysctls",
    "tmpfs",
    "tty",
    "ulimits",
    "use_api_socket",
    "user",
    "userns_mode",
    "uts",
    "volumes",
    "volumes_from",
    "working_dir",
];

/// Service keys Minga parses today. Must match the fields of `model::Service`.
pub(crate) const MODELLED_SERVICE: &[&str] = &[
    "cap_add",
    "cap_drop",
    "command",
    "configs",
    "depends_on",
    "deploy",
    "devices",
    "entrypoint",
    "environment",
    "healthcheck",
    "hostname",
    "image",
    "init",
    "labels",
    "logging",
    "networks",
    "ports",
    "privileged",
    "profiles",
    "read_only",
    "restart",
    "secrets",
    "stop_grace_period",
    "stop_signal",
    "sysctls",
    "tmpfs",
    "ulimits",
    "user",
    "volumes",
    "working_dir",
];

/// Why some spec keys are not supported, where there is more to say than "not yet".
fn unsupported_note(key: &str) -> Option<&'static str> {
    match key {
        "build" => Some("build and push images with BuildKit, then reference them with `image`"),
        "container_name" => Some("a cluster runs many replicas, so names come from the service"),
        "links" | "external_links" => Some("services reach each other by name on shared networks"),
        "network_mode" => Some("attach the service to a network instead"),
        "include" | "extends" => Some("planned for phase 1; flatten the file for now"),
        "gpus" => Some("use deploy.resources.reservations.devices with capabilities: [gpu]"),
        "env_file" => Some("planned for phase 1; use `environment` for now"),
        _ => None,
    }
}

/// Checks keys at the top level and in each service, and removes `x-` keys.
/// Returns the removed top-level extensions and, per service, its extensions.
pub(crate) fn check_and_strip(
    root: &mut Mapping,
    errors: &mut Vec<ComposeError>,
) -> (
    IndexMap<String, Value>,
    IndexMap<String, IndexMap<String, Value>>,
) {
    let top_ext = strip_extensions(root);
    check_level(root, "", SPEC_TOP, MODELLED_TOP, errors);

    let mut service_ext = IndexMap::new();
    if let Some(Value::Mapping(services)) = root.get_mut("services") {
        for (name, svc) in services.iter_mut() {
            let name = name.as_str().unwrap_or_default().to_string();
            if let Value::Mapping(svc) = svc {
                let ext = strip_extensions(svc);
                if !ext.is_empty() {
                    service_ext.insert(name.clone(), ext);
                }
                check_level(
                    svc,
                    &format!("services.{name}"),
                    SPEC_SERVICE,
                    MODELLED_SERVICE,
                    errors,
                );
            }
        }
    }
    // Extensions are also allowed in each network, volume, secret and config.
    for section in ["networks", "volumes", "secrets", "configs"] {
        if let Some(Value::Mapping(items)) = root.get_mut(section) {
            for (_, item) in items.iter_mut() {
                if let Value::Mapping(item) = item {
                    strip_extensions(item);
                }
            }
        }
    }
    (top_ext, service_ext)
}

fn strip_extensions(map: &mut Mapping) -> IndexMap<String, Value> {
    let keys: Vec<Value> = map
        .keys()
        .filter(|k| k.as_str().is_some_and(|s| s.starts_with("x-")))
        .cloned()
        .collect();
    keys.into_iter()
        .filter_map(|k| {
            let v = map.remove(&k)?;
            Some((k.as_str()?.to_string(), v))
        })
        .collect()
}

fn check_level(
    map: &mut Mapping,
    path: &str,
    spec: &[&str],
    modelled: &[&str],
    errors: &mut Vec<ComposeError>,
) {
    let at = if path.is_empty() {
        "(root)".to_string()
    } else {
        path.to_string()
    };
    let mut remove = Vec::new();
    for key in map.keys() {
        let Some(key_str) = key.as_str() else {
            errors.push(ComposeError::invalid(
                &at,
                format!("non-string key {key:?}"),
            ));
            continue;
        };
        if modelled.contains(&key_str) {
            continue;
        }
        if spec.contains(&key_str) {
            errors.push(ComposeError::Unsupported {
                path: at.clone(),
                key: key_str.to_string(),
                note: unsupported_note(key_str).map(str::to_string),
            });
        } else {
            errors.push(ComposeError::UnknownKey {
                path: at.clone(),
                key: key_str.to_string(),
                suggestion: suggest(key_str, spec),
            });
        }
        // Removed so typed parsing does not report the same key a second time.
        remove.push(key.clone());
    }
    for key in remove {
        map.remove(&key);
    }
}

/// The closest known key, if it is near enough to be a likely typo.
fn suggest(key: &str, known: &[&str]) -> Option<String> {
    known
        .iter()
        .map(|k| (levenshtein(key, k), *k))
        .filter(|(d, k)| *d <= 2.max(k.len() / 4))
        .min_by_key(|(d, _)| *d)
        .map(|(_, k)| k.to_string())
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != *cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modelled_keys_are_spec_keys() {
        for k in MODELLED_SERVICE {
            assert!(
                SPEC_SERVICE.contains(k),
                "{k} is modelled but not in the spec list"
            );
        }
        for k in MODELLED_TOP {
            assert!(
                SPEC_TOP.contains(k),
                "{k} is modelled but not in the spec list"
            );
        }
    }

    #[test]
    fn suggests_close_typos_only() {
        assert_eq!(suggest("imgae", SPEC_SERVICE).as_deref(), Some("image"));
        assert_eq!(
            suggest("enviroment", SPEC_SERVICE).as_deref(),
            Some("environment")
        );
        assert_eq!(
            suggest("dependson", SPEC_SERVICE).as_deref(),
            Some("depends_on")
        );
        assert_eq!(suggest("kubernetes", SPEC_SERVICE), None);
    }
}
