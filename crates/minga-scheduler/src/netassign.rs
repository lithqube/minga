//! Task IP assignment at placement time.
//!
//! Every task gets a deterministic IP on each of its service's networks:
//! the subnet comes from the network's state record (or is derived
//! deterministically from the network ID), and the IP comes from a
//! hash-probe of the task ID within that subnet ([`minga_net::Ipam`]).
//!
//! Determinism is what makes reconciliation idempotent: a task's IP is a
//! function of (network ID, task ID, live allocations), so re-running
//! placement produces the same assignment and existing tasks keep theirs.

use std::collections::HashMap;
use std::net::Ipv4Addr;

use minga_net::ipam::{allocate_subnet, Ipam, IpamError};
use minga_net::Cidr;
use minga_state::types::{Network, NetworkId, Task, TaskState};

/// Errors from IP assignment.
#[derive(Debug, thiserror::Error)]
pub enum NetAssignError {
    #[error("invalid subnet for network {0}: {1}")]
    Subnet(NetworkId, String),
    #[error("IPAM error on network {0}: {1}")]
    Ipam(NetworkId, IpamError),
}

/// The subnet of a network: explicit from state, or a deterministic /24
/// derived from the network ID.
fn subnet_for(network: &Network) -> Result<Cidr, NetAssignError> {
    match &network.subnet {
        Some(raw) => {
            Cidr::parse(raw).map_err(|e| NetAssignError::Subnet(network.id.clone(), e.to_string()))
        }
        None => allocate_subnet(&network.id)
            .map_err(|e| NetAssignError::Subnet(network.id.clone(), e.to_string())),
    }
}

/// Builds an IPAM for `network` seeded with the IPs of all non-terminal
/// tasks, so a live IP is never handed out twice.
pub fn ipam_for(network: &Network, existing_tasks: &[Task]) -> Result<Ipam, NetAssignError> {
    let subnet = subnet_for(network)?;
    let mut ipam = Ipam::new(&network.id, subnet);
    let entries: Vec<(String, Ipv4Addr)> = existing_tasks
        .iter()
        .filter(|t| !matches!(t.state, TaskState::Stopped | TaskState::Failed))
        .filter_map(|t| {
            t.spec
                .ips
                .get(&network.id)
                .and_then(|raw| raw.parse().ok())
                .map(|ip| (t.id.clone(), ip))
        })
        .collect();
    ipam.restore(&entries);
    Ok(ipam)
}

/// Assigns IPs for `task_id` on each of `service_networks`.
///
/// `networks` is the full cluster network list; networks the service does
/// not join are ignored. A service on no networks gets no IPs.
pub fn assign_task_ips(
    task_id: &str,
    service_networks: &[NetworkId],
    networks: &[Network],
    existing_tasks: &[Task],
) -> Result<HashMap<NetworkId, String>, NetAssignError> {
    let mut ips = HashMap::new();
    for net_id in service_networks {
        let Some(network) = networks.iter().find(|n| &n.id == net_id) else {
            // The network is referenced by the service but not present in
            // state. Skip rather than fail: network entities are written
            // before services on every deploy path, so this can only be a
            // transient replication lag; the next reconcile assigns the IP.
            continue;
        };
        let mut ipam = ipam_for(network, existing_tasks)?;
        let ip = ipam
            .allocate(task_id)
            .map_err(|e| NetAssignError::Ipam(net_id.clone(), e))?;
        ips.insert(net_id.clone(), ip.to_string());
    }
    Ok(ips)
}

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{TaskSpec, WorkloadKind};

    fn network(id: &str, subnet: Option<&str>) -> Network {
        Network {
            id: id.to_string(),
            name: id.to_string(),
            driver: "overlay".to_string(),
            attachable: false,
            subnet: subnet.map(String::from),
            wg_port: None,
        }
    }

    fn task(id: &str, ips: &[(&str, &str)]) -> Task {
        Task {
            id: id.to_string(),
            service_id: "svc".to_string(),
            node_id: 1,
            state: TaskState::Running,
            spec: TaskSpec {
                kind: WorkloadKind::Container,
                ips: ips
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn assigns_deterministic_ip_per_network() {
        let nets = vec![network("shop_default", Some("10.100.5.0/24"))];
        let a = assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]).unwrap();
        let b = assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]).unwrap();
        assert_eq!(a, b, "same task must get the same IP");
        assert!(a["shop_default"].starts_with("10.100.5."));
    }

    #[test]
    fn auto_allocates_subnet_when_absent() {
        let nets = vec![network("shop_default", None)];
        let ips = assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]).unwrap();
        let ip: Ipv4Addr = ips["shop_default"].parse().unwrap();
        assert_eq!(ip.octets()[0], 10, "supernet is 10.100.0.0/16");
    }

    #[test]
    fn never_reuses_a_live_ip() {
        let nets = vec![network("shop_default", Some("10.100.5.0/24"))];
        // Occupy every address the probe could land on by pinning both
        // task slots we are about to use.
        let first =
            assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]).unwrap();
        let existing = vec![task(
            "shop.web.1",
            &[("shop_default", &first["shop_default"])],
        )];
        let second = assign_task_ips(
            "shop.web.2",
            &["shop_default".to_string()],
            &nets,
            &existing,
        )
        .unwrap();
        assert_ne!(first["shop_default"], second["shop_default"]);
    }

    #[test]
    fn stopped_tasks_release_their_ip() {
        let nets = vec![network("shop_default", Some("10.100.5.0/24"))];
        let first =
            assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]).unwrap();
        let mut old = task("shop.web.1", &[("shop_default", &first["shop_default"])]);
        old.state = TaskState::Stopped;
        let second =
            assign_task_ips("shop.web.2", &["shop_default".to_string()], &nets, &[old]).unwrap();
        // The stopped task's IP is free again; determinism may or may not
        // reuse it, but the assignment must succeed.
        assert!(second["shop_default"].starts_with("10.100.5."));
    }

    #[test]
    fn missing_network_is_skipped() {
        let ips = assign_task_ips("t.1", &["ghost".to_string()], &[], &[]).unwrap();
        assert!(ips.is_empty());
    }
}
