//! Subject namespace helpers.
//!
//! All NATS subjects follow a hierarchical dot-separated scheme. Use these
//! helpers to build subject strings — never format them by hand.

/// Prefix for all Minga subjects.
const PREFIX: &str = "minga";

// -- Node subjects ----------------------------------------------------------

/// `minga.node.{id}.status` — node status reports (JetStream).
/// `minga.{tenant}.node.{id}.status` — node status reports (JetStream).
pub fn node_status(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.status")
}

/// `minga.{tenant}.node.{id}.event` — node lifecycle events (JetStream).
pub fn node_event(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.event")
}

/// `minga.{tenant}.node.{id}.heartbeat` — ephemeral liveness ping (core NATS).
pub fn node_heartbeat(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.heartbeat")
}

// -- Task subjects ----------------------------------------------------------

/// `minga.{tenant}.task.{id}.status` — task status reports (JetStream).
pub fn task_status(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.{task_id}.status")
}

/// `minga.{tenant}.task.{id}.event` — task lifecycle events (JetStream).
pub fn task_event(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.{task_id}.event")
}

/// `minga.{tenant}.log.node.{node_id}.{task_id}` — worker-owned log chunks.
pub fn node_task_log(tenant_id: &str, node_id: u64, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.log.node.{node_id}.{task_id}")
}

/// Log subject filter for a task across nodes in one tenant.
pub fn task_log(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.log.node.*.{task_id}")
}

/// `minga.{tenant}.ctl.node.{id}.ping` — synchronous health probe.
pub fn ctl_ping(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.ctl.node.{node_id}.ping")
}

/// `minga.{tenant}.ctl.node.{id}.drain` — synchronous drain.
pub fn ctl_drain(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.ctl.node.{node_id}.drain")
}

// -- Wildcard patterns ------------------------------------------------------

/// All task events in one tenant.
pub fn task_events_wildcard(tenant_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.>")
}

/// All node events in one tenant.
pub fn node_events_wildcard(tenant_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.node.>")
}

/// All log subjects across tenants.
pub fn log_events_wildcard() -> String {
    format!("{PREFIX}.*.log.>")
}

/// All events for one tenant.
pub fn all_events_wildcards(tenant_id: &str) -> Vec<String> {
    vec![
        node_events_wildcard(tenant_id),
        task_events_wildcard(tenant_id),
        format!("{PREFIX}.{tenant_id}.egress.>"),
    ]
}

// -- KV keys ----------------------------------------------------------------

/// KV key for a task's latest status, scoped to its tenant and owning node.
pub fn kv_node_task_status(tenant_id: &str, node_id: u64, task_id: &str) -> String {
    format!("{tenant_id}.node.{node_id}.task.{task_id}")
}

/// KV key for a node's latest status, scoped to its tenant.
pub fn kv_node_status(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.node.{node_id}")
}

/// KV key prefix for a node's task assignments within a tenant.
pub fn kv_assignment_node_prefix(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.node.{node_id}.task.")
}

/// KV key prefix for one node's ingress backend assignments within a tenant.
pub fn kv_assignment_ingress_prefix(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.node.{node_id}.ingress.")
}

/// KV key for one node's ingress port assignment.
pub fn kv_assignment_ingress(
    tenant_id: &str,
    node_id: u64,
    published: u16,
    protocol: &str,
) -> String {
    format!("{}{published}.{protocol}", kv_assignment_ingress_prefix(tenant_id, node_id))
}

/// KV key for a task assignment.
pub fn kv_assignment_task(tenant_id: &str, node_id: u64, task_id: &str) -> String {
    format!("{}{task_id}", kv_assignment_node_prefix(tenant_id, node_id))
}

/// KV key prefix for the networks a node's tasks are attached to.
pub fn kv_assignment_node_network_prefix(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.node.{node_id}.net.")
}

/// KV key for a network's metadata and DNS records, as projected to one node.
pub fn kv_assignment_network(tenant_id: &str, node_id: u64, network_id: &str) -> String {
    format!("{}{network_id}", kv_assignment_node_network_prefix(tenant_id, node_id))
}

pub fn kv_lease(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.lease.node.{node_id}")
}



/// Parse a tenant-qualified lease key, returning `(tenant_id, node_id)`.
pub fn parse_kv_lease(key: &str) -> Option<(&str, u64)> {
    let (tenant_id, node_id) = key.split_once(".lease.node.")?;
    if tenant_id.is_empty() || tenant_id.contains('.') {
        return None;
    }
    Some((tenant_id, node_id.parse().ok()?))
}
/// NATS permissions for a tenant/node worker identity: `(publish, subscribe)`.
pub fn worker_acl(tenant_id: &str, node_id: u64) -> (Vec<String>, Vec<String>) {
    let own_tasks = format!(
        "$KV.MINGA_ASSIGN.{}>",
        kv_assignment_node_prefix(tenant_id, node_id)
    );
    let own_ingress_prefix = kv_assignment_ingress_prefix(tenant_id, node_id);
    let own_ingress = format!("$KV.MINGA_ASSIGN.{own_ingress_prefix}>");
    let own_wireguard = format!("$KV.MINGA_ASSIGN.{tenant_id}.node.{node_id}.wg");
    let networks = format!(
        "$KV.MINGA_ASSIGN.{}>",
        kv_assignment_node_network_prefix(tenant_id, node_id)
    );
    let own_status = format!("$KV.MINGA_STATUS.{tenant_id}.node.{node_id}.task.>");
    let own_lease = format!("$KV.MINGA_LEASES.{}", kv_lease(tenant_id, node_id));
    let publish = vec![
        own_lease.clone(),
        own_status.clone(),
        format!("{PREFIX}.{tenant_id}.log.node.{node_id}.>"),
        "$JS.API.STREAM.INFO.KV_MINGA_ASSIGN".into(),
        "$JS.API.STREAM.INFO.KV_MINGA_STATUS".into(),
        "$JS.API.STREAM.INFO.KV_MINGA_LEASES".into(),
        "$JS.FC.KV_MINGA_ASSIGN.>".into(),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_tasks}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_ingress}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{networks}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_wireguard}"),
        "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.>".into(),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_tasks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_ingress}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{networks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_STATUS.{own_status}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_LEASES.{own_lease}"),
    ];
    let subscribe = vec![
        own_tasks,
        own_ingress,
        own_wireguard,
        networks,
        own_lease,
        own_status,
        "_INBOX.>".into(),
    ];
    (publish, subscribe)
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_formats() {
        assert_eq!(node_status("tenant-a", 1), "minga.tenant-a.node.1.status");
        assert_eq!(node_event("tenant-a", 2), "minga.tenant-a.node.2.event");
        assert_eq!(
            node_heartbeat("tenant-a", 3),
            "minga.tenant-a.node.3.heartbeat"
        );
        assert_eq!(
            task_status("tenant-a", "plant-7.inference.1"),
            "minga.tenant-a.task.plant-7.inference.1.status"
        );
        assert_eq!(
            task_event("tenant-a", "plant-7.worker.0"),
            "minga.tenant-a.task.plant-7.worker.0.event"
        );
        assert_eq!(task_log("tenant-a", "abc"), "minga.tenant-a.log.node.*.abc");
        assert_eq!(
            node_task_log("tenant-a", 7, "abc"),
            "minga.tenant-a.log.node.7.abc"
        );
        assert_eq!(ctl_ping("tenant-a", 1), "minga.tenant-a.ctl.node.1.ping");
        assert_eq!(ctl_drain("tenant-a", 1), "minga.tenant-a.ctl.node.1.drain");
    }

    #[test]
    fn wildcard_formats() {
        assert_eq!(task_events_wildcard("tenant-a"), "minga.tenant-a.task.>");
        assert_eq!(node_events_wildcard("tenant-a"), "minga.tenant-a.node.>");
        assert_eq!(log_events_wildcard("tenant-a"), "minga.tenant-a.log.>");
    }

    #[test]
    fn tenant_qualified_kv_keys_do_not_collide() {
        assert_eq!(kv_lease("tenant-a", 2), "tenant-a.lease.node.2");
        assert_eq!(kv_lease("tenant-b", 2), "tenant-b.lease.node.2");
        assert_eq!(
            kv_assignment_node_prefix("tenant-a", 4),
            "tenant-a.node.4.task."
        );
        assert_eq!(
            kv_assignment_task("tenant-a", 4, "plant-7.db.0"),
            "tenant-a.node.4.task.plant-7.db.0"
        );
        assert_eq!(
            kv_assignment_network("tenant-a", 4, "shop_default"),
            "tenant-a.node.4.net.shop_default"
        );
        assert_eq!(
            kv_assignment_ingress("tenant-a", 7, 8080, "tcp"),
            "tenant-a.node.7.ingress.8080.tcp"
        );
    }

    #[test]
    fn worker_acl_is_tenant_and_node_scoped() {
        let (publish, subscribe) = worker_acl("tenant-a", 7);
        assert!(subscribe.contains(
            &"$KV.MINGA_ASSIGN.tenant-a.node.7.net.>".to_string()
        ));
        assert!(subscribe.contains(
            &"$KV.MINGA_ASSIGN.tenant-a.node.7.task.>".to_string()
        ));
        assert!(publish.contains(
            &"$KV.MINGA_LEASES.tenant-a.lease.node.7".to_string()
        ));
        assert!(!publish.iter().any(|subject| subject.contains("audit")));
        for subject in publish.iter().chain(&subscribe) {
            assert!(
                !subject.contains("tenant-b") && !subject.contains("node.8"),
                "worker must not reach another tenant or node: {subject}"
            );
        }
    }

    #[test]
    fn worker_wireguard_acl_is_exactly_tenant_node_scoped() {
        let (publish, subscribe) = worker_acl("tenant-a", 7);
        assert!(publish.contains(
            &"$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.$KV.MINGA_ASSIGN.tenant-a.node.7.wg"
                .to_string()
        ));
        assert!(subscribe.contains(
            &"$KV.MINGA_ASSIGN.tenant-a.node.7.wg".to_string()
        ));
        assert!(publish.contains(
            &"$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.$KV.MINGA_ASSIGN.tenant-a.node.7.ingress.>"
                .to_string()
        ));
    }
}
