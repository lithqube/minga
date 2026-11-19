//! Subject namespace helpers.
//!
//! All NATS subjects follow a hierarchical dot-separated scheme. Use these
//! helpers to build subject strings — never format them by hand.

/// Prefix for all Minga subjects.
const PREFIX: &str = "minga";

// -- Node subjects ----------------------------------------------------------

/// `minga.{tenant}.node.{id}.status` — manager-owned node status reports.
pub fn node_status(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.status")
}

/// `minga.{tenant}.node.{id}.event` — node lifecycle events.
pub fn node_event(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.event")
}

/// `minga.{tenant}.node.{id}.heartbeat` — ephemeral liveness ping.
pub fn node_heartbeat(tenant_id: &str, node_id: u64) -> String {
    format!("{PREFIX}.{tenant_id}.node.{node_id}.heartbeat")
}

// -- Task subjects ----------------------------------------------------------

/// `minga.{tenant}.task.{id}.status` — manager-owned task status events.
pub fn task_status(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.{task_id}.status")
}

/// `minga.{tenant}.task.{id}.event` — task lifecycle events.
pub fn task_event(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.{task_id}.event")
}

/// `minga.{tenant}.log.node.{node_id}.{task_id}` — worker-owned log chunks.
pub fn node_task_log(tenant_id: &str, node_id: u64, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.log.node.{node_id}.{task_id}")
}

/// Log subject filter for a task across nodes.
pub fn task_log(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.log.node.*.{task_id}")
}

/// Tenant-owned worker event subjects.
pub fn tenant_task_event(tenant_id: &str, task_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.{task_id}.event")
}

/// Tenant stream subject filters.
pub fn tenant_log_wildcard(tenant_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.log.>")
}

pub fn tenant_event_wildcard(tenant_id: &str) -> String {
    format!("{PREFIX}.{tenant_id}.task.>")
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

/// All event subjects in one tenant.
pub fn all_events_wildcards(tenant_id: &str) -> Vec<String> {
    vec![
        node_events_wildcard(tenant_id),
        task_events_wildcard(tenant_id),
        format!("{PREFIX}.{tenant_id}.egress.>"),
    ]
}

// -- KV keys ----------------------------------------------------------------

/// KV key for a task's latest status inside its tenant's status bucket.
pub fn kv_node_task_status(node_id: u64, task_id: &str) -> String {
    format!("node.{node_id}.task.{task_id}")
}

/// KV key for a node's latest status inside its tenant's status bucket.
pub fn kv_node_status(node_id: u64) -> String {
    format!("node.{node_id}")
}

/// Suffix of the manager-written status key recording a node's joined tenant.
pub const NODE_JOIN_PREFIX: &str = "internal.node-join.";

/// Status key holding the tenant a node joined; only managers write it.
pub fn kv_node_join(_tenant_id: &str, node_id: u64) -> String {
    format!("{NODE_JOIN_PREFIX}{node_id}")
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
    format!(
        "{}{published}.{protocol}",
        kv_assignment_ingress_prefix(tenant_id, node_id)
    )
}

/// KV key for a task assignment.
pub fn kv_assignment_task(tenant_id: &str, node_id: u64, task_id: &str) -> String {
    format!("{}{task_id}", kv_assignment_node_prefix(tenant_id, node_id))
}

/// KV key prefix for the networks a node's tasks are attached to.
pub fn kv_assignment_node_network_prefix(tenant_id: &str, node_id: u64) -> String {
    format!("{tenant_id}.node.{node_id}.net.")
}

/// KV key for a network's metadata and DNS records, projected to one node.
pub fn kv_assignment_network(tenant_id: &str, node_id: u64, network_id: &str) -> String {
    format!(
        "{}{network_id}",
        kv_assignment_node_network_prefix(tenant_id, node_id)
    )
}

/// KV key for a node's liveness lease.
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

/// Inbox prefix a worker connects with. Replies to its requests and its
/// consumer deliveries land under this prefix, so its ACL never needs the
/// shared `_INBOX.>` that would expose other tenants' replies.
pub fn worker_inbox_prefix(tenant_id: &str, node_id: u64) -> String {
    format!("_INBOX.{tenant_id}.{node_id}")
}

/// NATS permissions for a tenant/node worker identity: `(publish, subscribe)`.
pub fn worker_acl(tenant_id: &str, node_id: u64) -> crate::Result<(Vec<String>, Vec<String>)> {
    minga_state::types::Tenant::validate_id(tenant_id)
        .map_err(|error| crate::BusError::Other(format!("invalid tenant ID: {error}")))?;
    let status_bucket = format!("MINGA_STATUS_{tenant_id}");
    let own_status = format!("$KV.{status_bucket}.node.{node_id}.task.>");
    let own_lease = format!("$KV.MINGA_LEASES.{}", kv_lease(tenant_id, node_id));
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
    let log_bucket = format!("MINGA_LOGS_{tenant_id}");
    let publish = vec![
        own_lease.clone(),
        own_status.clone(),
        format!("{PREFIX}.{tenant_id}.log.node.{node_id}.*"),
        format!("{PREFIX}.{tenant_id}.usage.>"),
        "$JS.API.STREAM.INFO.KV_MINGA_ASSIGN".into(),
        format!("$JS.API.STREAM.INFO.KV_{status_bucket}"),
        "$JS.API.STREAM.INFO.KV_MINGA_LEASES".into(),
        format!("$JS.API.STREAM.INFO.{log_bucket}"),
        format!("$JS.API.DIRECT.GET.KV_{status_bucket}.$KV.{status_bucket}.node.{node_id}.task.>"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_LEASES.{own_lease}"),
        "$JS.FC.KV_MINGA_ASSIGN.>".into(),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_tasks}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_ingress}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{networks}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_wireguard}"),
        "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.*".into(),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_tasks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_ingress}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{networks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_wireguard}"),
    ];
    let own_ping = format!("{PREFIX}.{tenant_id}.ctl.node.{node_id}.ping");
    let own_drain = format!("{PREFIX}.{tenant_id}.ctl.node.{node_id}.drain");
    let subscribe = vec![
        format!("{}.>", worker_inbox_prefix(tenant_id, node_id)),
        "$JS.ACK.KV_MINGA_ASSIGN.>".into(),
        "$JS.ACK.KV_MINGA_LEASES.>".into(),
        own_ping,
        own_drain,
    ];
    Ok((publish, subscribe))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_formats() {
        assert_eq!(node_status("acme", 1), "minga.acme.node.1.status");
        assert_eq!(node_event("acme", 2), "minga.acme.node.2.event");
        assert_eq!(node_heartbeat("acme", 3), "minga.acme.node.3.heartbeat");
        assert_eq!(
            task_status("acme", "plant-7.inference.1"),
            "minga.acme.task.plant-7.inference.1.status"
        );
        assert_eq!(
            task_event("acme", "plant-7.worker.0"),
            "minga.acme.task.plant-7.worker.0.event"
        );
        assert_eq!(task_log("acme", "abc"), "minga.acme.log.node.*.abc");
        assert_eq!(node_task_log("acme", 7, "abc"), "minga.acme.log.node.7.abc");
        assert_eq!(ctl_ping("acme", 1), "minga.acme.ctl.node.1.ping");
        assert_eq!(ctl_drain("acme", 1), "minga.acme.ctl.node.1.drain");
    }

    #[test]
    fn wildcard_formats() {
        assert_eq!(task_events_wildcard("acme"), "minga.acme.task.>");
        assert_eq!(node_events_wildcard("acme"), "minga.acme.node.>");
        assert_eq!(log_events_wildcard(), "minga.*.log.>");
    }

    #[test]
    fn kv_key_formats() {
        assert_eq!(
            kv_node_task_status(4, "plant-7.db.0"),
            "node.4.task.plant-7.db.0"
        );
        assert_eq!(kv_node_status(1), "node.1");
        assert_eq!(kv_lease("tenant_a", 2), "tenant_a.lease.node.2");
        assert_eq!(
            kv_assignment_node_prefix("tenant_a", 4),
            "tenant_a.node.4.task."
        );
        assert_eq!(
            kv_assignment_task("tenant_a", 4, "plant-7.db.0"),
            "tenant_a.node.4.task.plant-7.db.0"
        );
        assert_eq!(
            kv_assignment_network("tenant_a", 4, "shop_default"),
            "tenant_a.node.4.net.shop_default"
        );
        assert_eq!(
            kv_assignment_ingress("tenant_a", 7, 8080, "tcp"),
            "tenant_a.node.7.ingress.8080.tcp"
        );
    }
    #[test]
    fn worker_acl_is_tenant_and_node_scoped() {
        let (publish, subscribe) = worker_acl("tenant_a", 7).unwrap();
        assert!(publish.contains(&"$KV.MINGA_LEASES.tenant_a.lease.node.7".to_string()));
        assert!(publish.contains(&"minga.tenant_a.usage.>".to_string()));
        assert!(subscribe.contains(&"$JS.ACK.KV_MINGA_ASSIGN.>".to_string()));
        assert!(subscribe.contains(&"_INBOX.tenant_a.7.>".to_string()));
        assert!(!subscribe.iter().any(|subject| subject == "_INBOX.>"));
        assert!(!subscribe
            .iter()
            .any(|subject| subject.starts_with("$KV.MINGA_ASSIGN.")));
        assert!(subscribe.contains(&"minga.tenant_a.ctl.node.7.ping".to_string()));
        assert!(subscribe.contains(&"minga.tenant_a.ctl.node.7.drain".to_string()));
        assert!(!publish.iter().any(|subject| subject.contains("audit")));
        assert!(!publish
            .iter()
            .chain(&subscribe)
            .any(|subject| subject.contains("tenant_b")));
    }

    #[test]
    fn worker_acl_rejects_invalid_tenant_names() {
        assert!(worker_acl("tenant.other", 7).is_err());
    }
}
