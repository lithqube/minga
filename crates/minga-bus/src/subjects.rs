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

/// `minga.task.{id}.log` — container log chunks (JetStream).
pub fn task_log(task_id: &str) -> String {
    format!("{PREFIX}.task.{task_id}.log")
}

// -- Control subjects (commands + request/reply) ----------------------------

/// `minga.ctl.node.{id}.assign` — task assignment command (JetStream, work queue).
pub fn ctl_assign(node_id: u64) -> String {
    format!("{PREFIX}.ctl.node.{node_id}.assign")
}

/// `minga.ctl.node.{id}.stop` — stop/remove command (JetStream, work queue).
pub fn ctl_stop(node_id: u64) -> String {
    format!("{PREFIX}.ctl.node.{node_id}.stop")
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

/// All command subjects for a node: `minga.ctl.node.{id}.>`
pub fn ctl_node_wildcard(node_id: u64) -> String {
    format!("{PREFIX}.ctl.node.{node_id}.>")
}

/// All task events: `minga.task.>`
pub fn task_events_wildcard() -> String {
    format!("{PREFIX}.task.>")
}

/// All node events: `minga.node.>`
pub fn node_events_wildcard() -> String {
    format!("{PREFIX}.node.>")
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

/// KV key for a task's latest status.
pub fn kv_task_status(task_id: &str) -> String {
    format!("task.{task_id}")
}

/// KV key for a node's latest status.
pub fn kv_node_status(node_id: u64) -> String {
    format!("node.{node_id}")
}

/// KV key for a node's liveness lease.
pub fn kv_lease(node_id: u64) -> String {
    format!("lease.node.{node_id}")
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
        assert_eq!(task_log("abc"), "minga.task.abc.log");
        assert_eq!(ctl_assign(1), "minga.ctl.node.1.assign");
        assert_eq!(ctl_stop(1), "minga.ctl.node.1.stop");
        assert_eq!(ctl_ping(1), "minga.ctl.node.1.ping");
        assert_eq!(ctl_drain(1), "minga.ctl.node.1.drain");
    }

    #[test]
    fn wildcard_formats() {
        assert_eq!(ctl_node_wildcard(1), "minga.ctl.node.1.>");
        assert_eq!(task_events_wildcard(), "minga.task.>");
        assert_eq!(node_events_wildcard(), "minga.node.>");
    }

    #[test]
    fn kv_key_formats() {
        assert_eq!(kv_task_status("plant-7.db.0"), "task.plant-7.db.0");
        assert_eq!(kv_node_status(1), "node.1");
        assert_eq!(kv_lease(2), "lease.node.2");
    }
}
