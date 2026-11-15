//! Subject namespace helpers.
//!
//! All NATS subjects follow a hierarchical dot-separated scheme. Use these
//! helpers to build subject strings — never format them by hand.

/// Prefix for all Minga subjects.
const PREFIX: &str = "minga";

// -- Node subjects ----------------------------------------------------------

/// `minga.node.{id}.status` — node status reports (JetStream).
pub fn node_status(node_id: u64) -> String {
    format!("{PREFIX}.node.{node_id}.status")
}

/// `minga.node.{id}.event` — node lifecycle events (JetStream).
pub fn node_event(node_id: u64) -> String {
    format!("{PREFIX}.node.{node_id}.event")
}

/// `minga.node.{id}.heartbeat` — ephemeral liveness ping (core NATS).
pub fn node_heartbeat(node_id: u64) -> String {
    format!("{PREFIX}.node.{node_id}.heartbeat")
}

// -- Task subjects ----------------------------------------------------------

/// `minga.task.{id}.status` — task status reports (JetStream).
pub fn task_status(task_id: &str) -> String {
    format!("{PREFIX}.task.{task_id}.status")
}

/// `minga.task.{id}.event` — task lifecycle events (JetStream).
pub fn task_event(task_id: &str) -> String {
    format!("{PREFIX}.task.{task_id}.event")
}

/// `minga.log.node.{node_id}.{task_id}` — worker-owned log chunks.
pub fn node_task_log(node_id: u64, task_id: &str) -> String {
    format!("{PREFIX}.log.node.{node_id}.{task_id}")
}

/// Log subject filter for a task across nodes.
pub fn task_log(task_id: &str) -> String {
    format!("{PREFIX}.log.node.*.{task_id}")
}

/// `minga.ctl.node.{id}.ping` — synchronous health probe (core NATS, req/reply).
pub fn ctl_ping(node_id: u64) -> String {
    format!("{PREFIX}.ctl.node.{node_id}.ping")
}

/// `minga.ctl.node.{id}.drain` — synchronous drain (core NATS, req/reply).
pub fn ctl_drain(node_id: u64) -> String {
    format!("{PREFIX}.ctl.node.{node_id}.drain")
}

// -- Wildcard patterns ------------------------------------------------------

/// All task events: `minga.task.>`
pub fn task_events_wildcard() -> String {
    format!("{PREFIX}.task.>")
}

/// All node events: `minga.node.>`
pub fn node_events_wildcard() -> String {
    format!("{PREFIX}.node.>")
}

/// All log subjects: `minga.log.>`
pub fn log_events_wildcard() -> String {
    format!("{PREFIX}.log.>")
}

/// All events (nodes + tasks): patterns for the MINGA_EVENTS stream.
pub fn all_events_wildcards() -> Vec<String> {
    vec![
        node_events_wildcard(),
        task_events_wildcard(),
        format!("{PREFIX}.egress.>"),
    ]
}

// -- KV keys ----------------------------------------------------------------

/// KV key for a task's latest status, scoped to its owning node.
pub fn kv_node_task_status(node_id: u64, task_id: &str) -> String {
    format!("node.{node_id}.task.{task_id}")
}

/// KV key for a node's latest status.
pub fn kv_node_status(node_id: u64) -> String {
    format!("node.{node_id}")
}

/// KV key prefix for a node's task assignments.
pub fn kv_assignment_node_prefix(node_id: u64) -> String {
    format!("node.{node_id}.task.")
}

/// KV key for a task assignment.
pub fn kv_assignment_task(node_id: u64, task_id: &str) -> String {
    format!("{}{task_id}", kv_assignment_node_prefix(node_id))
}

/// KV key for network metadata and DNS assignments.
pub fn kv_assignment_network(network_id: &str) -> String {
    format!("net.{network_id}")
}

/// KV key for a node's liveness lease.
pub fn kv_lease(node_id: u64) -> String {
    format!("lease.node.{node_id}")
}

/// NATS permissions for the `minga-worker-{id}` identity: `(publish, subscribe)`.
///
/// Worker N may read only its own assignments (plus network metadata) and
/// write only its own status and lease. Consumer creation and direct gets are
/// granted only on subjects that carry the filter or key, so the server can
/// bind them to `node.{id}`; a consumer whose filter rides in the payload is
/// refused.
pub fn worker_acl(node_id: u64) -> (Vec<String>, Vec<String>) {
    let own_tasks = format!("$KV.MINGA_ASSIGN.{}>", kv_assignment_node_prefix(node_id));
    let networks = "$KV.MINGA_ASSIGN.net.>".to_string();
    let own_status = format!("$KV.MINGA_STATUS.node.{node_id}.task.>");
    let own_lease = format!("$KV.MINGA_LEASES.{}", kv_lease(node_id));
    let publish = vec![
        own_lease.clone(),
        own_status.clone(),
        format!("{PREFIX}.log.node.{node_id}.>"),
        "$JS.API.STREAM.INFO.KV_MINGA_ASSIGN".into(),
        "$JS.API.STREAM.INFO.KV_MINGA_STATUS".into(),
        "$JS.API.STREAM.INFO.KV_MINGA_LEASES".into(),
        // CONSUMER.CREATE.<stream>.<name>.<filter>: the server rejects a
        // payload filter that differs from the one in the subject.
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{own_tasks}"),
        format!("$JS.API.CONSUMER.CREATE.KV_MINGA_ASSIGN.*.{networks}"),
        "$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.>".into(),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{own_tasks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_ASSIGN.{networks}"),
        format!("$JS.API.DIRECT.GET.KV_MINGA_STATUS.{own_status}"),
    ];
    let subscribe = vec![
        own_tasks,
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
        assert_eq!(node_status(1), "minga.node.1.status");
        assert_eq!(node_event(2), "minga.node.2.event");
        assert_eq!(node_heartbeat(3), "minga.node.3.heartbeat");
        assert_eq!(
            task_status("plant-7.inference.1"),
            "minga.task.plant-7.inference.1.status"
        );
        assert_eq!(
            task_event("plant-7.worker.0"),
            "minga.task.plant-7.worker.0.event"
        );
        assert_eq!(task_log("abc"), "minga.log.node.*.abc");
        assert_eq!(node_task_log(7, "abc"), "minga.log.node.7.abc");
        assert_eq!(ctl_ping(1), "minga.ctl.node.1.ping");
        assert_eq!(ctl_drain(1), "minga.ctl.node.1.drain");
    }

    #[test]
    fn wildcard_formats() {
        assert_eq!(task_events_wildcard(), "minga.task.>");
        assert_eq!(node_events_wildcard(), "minga.node.>");
        assert_eq!(log_events_wildcard(), "minga.log.>");
    }

    #[test]
    fn kv_key_formats() {
        assert_eq!(
            kv_node_task_status(4, "plant-7.db.0"),
            "node.4.task.plant-7.db.0"
        );
        assert_eq!(kv_node_status(1), "node.1");
        assert_eq!(kv_lease(2), "lease.node.2");
        assert_eq!(kv_assignment_node_prefix(4), "node.4.task.");
        assert_eq!(
            kv_assignment_task(4, "plant-7.db.0"),
            "node.4.task.plant-7.db.0"
        );
        assert_eq!(kv_assignment_network("shop_default"), "net.shop_default");
    }
}
