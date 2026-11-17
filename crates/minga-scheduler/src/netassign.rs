//! Task IP assignment at placement time.
//!
//! Every task gets a deterministic IP on each of its service's networks:
//! the subnet comes from the network's state record (the leader stores
//! one for every network, see [`fill_subnets`]), and the IP comes from a
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

/// The subnet stored for a network. A network without one is an error:
/// only the leader assigns subnets ([`fill_subnets`]).
fn subnet_for(network: &Network) -> Result<Cidr, NetAssignError> {
    let raw = network.subnet.as_deref().ok_or_else(|| {
        NetAssignError::Subnet(network.id.clone(), "no subnet stored yet".to_string())
    })?;
    Cidr::parse(raw).map_err(|e| NetAssignError::Subnet(network.id.clone(), e.to_string()))
}

/// Stores a subnet on every network that has none and returns the indices
/// it changed, plus one error per network it could not place.
///
/// A network created before subnets were stored, with live tasks, keeps
/// the /24 its tasks already use (the old per-ID hash), so nothing moves.
/// Every other network gets the first /24 of `supernet` that no network
/// overlaps.
pub fn fill_subnets(
    networks: &mut [Network],
    tasks: &[Task],
    supernet: &Cidr,
) -> (Vec<usize>, Vec<NetAssignError>) {
    let mut changed = Vec::new();
    for (i, net) in networks.iter_mut().enumerate() {
        if net.subnet.is_some() {
            continue;
        }
        let live_ip = tasks
            .iter()
            .filter(|t| !matches!(t.state, TaskState::Stopped | TaskState::Failed))
            .find_map(|t| t.spec.ips.get(&net.id)?.parse::<Ipv4Addr>().ok());
        if let Some(ip) = live_ip {
            let base = Ipv4Addr::from(u32::from(ip) & !0xff);
            net.subnet = Some(format!("{base}/24"));
            changed.push(i);
        }
    }
    let mut taken: Vec<Cidr> = networks
        .iter()
        .filter_map(|n| Cidr::parse(n.subnet.as_deref()?).ok())
        .collect();
    let mut errors = Vec::new();
    for (i, net) in networks.iter_mut().enumerate() {
        if net.subnet.is_some() {
            continue;
        }
        match allocate_subnet(supernet, &taken) {
            Ok(subnet) => {
                net.subnet = Some(subnet.to_cidr_string());
                taken.push(subnet);
                changed.push(i);
            }
            Err(e) => errors.push(NetAssignError::Ipam(net.id.clone(), e)),
        }
    }
    (changed, errors)
}

/// Pairs of networks whose stored subnets overlap, as (a, b, a's subnet,
/// b's subnet). Tasks on such a pair break each other's bridges.
pub fn subnet_collisions(networks: &[Network]) -> Vec<(NetworkId, NetworkId, String, String)> {
    let parsed: Vec<(&Network, Cidr)> = networks
        .iter()
        .filter_map(|n| Some((n, Cidr::parse(n.subnet.as_deref()?).ok()?)))
        .collect();
    let mut out = Vec::new();
    for (i, (a, sa)) in parsed.iter().enumerate() {
        for (b, sb) in &parsed[i + 1..] {
            if sa.overlaps(sb) {
                out.push((a.id.clone(), b.id.clone(), sa.to_string(), sb.to_string()));
            }
        }
    }
    out
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
            tenant_id: "default".to_string(),
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
            tenant_id: "default".to_string(),
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
            restarts: 0,
            running_since_unix: None,
            last_error: None,
            last_exit_code: None,
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
    fn network_without_stored_subnet_is_an_error() {
        let nets = vec![network("shop_default", None)];
        let err = assign_task_ips("shop.web.1", &["shop_default".to_string()], &nets, &[]);
        assert!(err.unwrap_err().to_string().contains("no subnet stored"));
    }

    fn supernet() -> Cidr {
        Cidr::parse(minga_net::DEFAULT_SUPERNET).unwrap()
    }

    #[test]
    fn three_hundred_networks_get_256_distinct_subnets_then_exhaust() {
        let mut nets: Vec<Network> = (0..300).map(|i| network(&format!("n{i}"), None)).collect();
        let (changed, errors) = fill_subnets(&mut nets, &[], &supernet());
        assert_eq!(changed.len(), 256);
        let distinct: std::collections::HashSet<_> =
            nets.iter().filter_map(|n| n.subnet.clone()).collect();
        assert_eq!(distinct.len(), 256);
        assert!(subnet_collisions(&nets).is_empty());
        assert_eq!(errors.len(), 44);
        assert!(
            errors[0].to_string().contains("supernet exhausted"),
            "{}",
            errors[0]
        );
    }

    #[test]
    fn legacy_network_keeps_the_subnet_its_tasks_use() {
        let mut nets = vec![
            network("explicit", Some("10.100.0.0/24")),
            network("idle", None),
            network("legacy", None),
        ];
        let tasks = vec![task("legacy.web.1", &[("legacy", "10.100.1.9")])];
        let (changed, errors) = fill_subnets(&mut nets, &tasks, &supernet());
        assert!(errors.is_empty());
        assert_eq!(changed, vec![2, 1]);
        assert_eq!(nets[2].subnet.as_deref(), Some("10.100.1.0/24"));
        // The idle network skips both taken /24s.
        assert_eq!(nets[1].subnet.as_deref(), Some("10.100.2.0/24"));
        // A second pass changes nothing.
        assert!(fill_subnets(&mut nets, &tasks, &supernet()).0.is_empty());
    }

    #[test]
    fn colliding_legacy_networks_are_reported_not_moved() {
        let mut nets = vec![network("a", None), network("b", Some("10.100.7.0/24"))];
        let tasks = vec![task("a.web.1", &[("a", "10.100.7.20")])];
        fill_subnets(&mut nets, &tasks, &supernet());
        assert_eq!(nets[0].subnet.as_deref(), Some("10.100.7.0/24"));
        let hits = subnet_collisions(&nets);
        assert_eq!(hits.len(), 1);
        assert_eq!((hits[0].0.as_str(), hits[0].1.as_str()), ("a", "b"));
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
