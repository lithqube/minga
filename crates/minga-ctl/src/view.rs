//! Reading Docker Engine API JSON into the rows minga-ctl prints.
//!
//! Specs stay `serde_json::Value` (plan section 3), so these helpers read the
//! few fields the read commands show and ignore everything else.

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::output::Rows;

/// The label that marks a service as part of a stack.
pub const STACK_LABEL: &str = "com.docker.stack.namespace";

/// Docker `filters` selecting one stack's services or tasks.
pub fn stack_filter(stack: &str) -> Value {
    json!({ "label": [format!("{STACK_LABEL}={stack}")] })
}

/// The items of a JSON array response; anything else is empty.
pub fn items(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or_default()
}

/// A string field, or `""`.
fn text<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or("")
}

/// The service's ID.
pub fn id(service: &Value) -> &str {
    text(service, "/ID")
}

/// The stack a service belongs to, from its label.
pub fn stack_of(service: &Value) -> Option<&str> {
    service
        .pointer("/Spec/Labels")
        .and_then(|labels| labels.get(STACK_LABEL))
        .and_then(Value::as_str)
}

/// The service's image.
pub fn image(service: &Value) -> &str {
    text(service, "/Spec/TaskTemplate/ContainerSpec/Image")
}

/// The scheduling mode and, for replicated modes, the desired replica count.
pub fn mode(service: &Value) -> (&'static str, Option<u64>) {
    let Some(mode) = service.pointer("/Spec/Mode").and_then(Value::as_object) else {
        return ("replicated", Some(1));
    };
    let replicas = |key: &str| mode.get(key).and_then(|m| m["Replicas"].as_u64());
    if mode.contains_key("Global") {
        ("global", None)
    } else if mode.contains_key("GlobalJob") {
        ("global-job", None)
    } else if mode.contains_key("ReplicatedJob") {
        ("replicated-job", replicas("ReplicatedJob"))
    } else {
        ("replicated", replicas("Replicated"))
    }
}

/// Whether the service has a healthcheck that runs: `Test` present, not
/// empty and not `["NONE"]`.
pub fn has_healthcheck(service: &Value) -> bool {
    let test = service
        .pointer("/Spec/TaskTemplate/ContainerSpec/Healthcheck/Test")
        .and_then(Value::as_array);
    match test {
        Some(test) => !test.is_empty() && test[0].as_str() != Some("NONE"),
        None => false,
    }
}

/// The derived HEALTH column (plan section 5.2): tasks carry no health field.
pub fn health(has_healthcheck: bool, state: &str) -> &'static str {
    match (has_healthcheck, state) {
        (false, _) => "-",
        (true, "preparing") => "starting",
        (true, "running") => "healthy",
        (true, _) => "-",
    }
}

/// Node IDs to host names, from `GET /nodes`.
pub fn node_names(nodes: &Value) -> HashMap<String, String> {
    items(nodes)
        .iter()
        .map(|node| {
            let id = text(node, "/ID").to_string();
            let host = text(node, "/Description/Hostname");
            let name = if host.is_empty() {
                id.clone()
            } else {
                host.to_string()
            };
            (id, name)
        })
        .collect()
}

/// The task table shared by `stack ps` and `service ps`, sorted by task ID.
///
/// `services` are the services the tasks belong to; their healthcheck
/// decides the HEALTH column.
pub fn task_rows(tasks: &Value, services: &[Value], nodes: &HashMap<String, String>) -> Rows {
    let checked: HashMap<&str, bool> = services
        .iter()
        .map(|s| (id(s), has_healthcheck(s)))
        .collect();
    let mut tasks: Vec<&Value> = items(tasks).iter().collect();
    tasks.sort_by(|a, b| text(a, "/ID").cmp(text(b, "/ID")));

    let mut rows = Rows::new(&["ID", "SERVICE", "NODE", "DESIRED", "STATE", "HEALTH"]);
    for task in tasks {
        let task_id = text(task, "/ID");
        let service = text(task, "/ServiceID");
        let node_id = text(task, "/NodeID");
        let node = match nodes.get(node_id) {
            Some(name) => name.as_str(),
            None if node_id.is_empty() => "-",
            None => node_id,
        };
        let desired = text(task, "/DesiredState");
        let state = text(task, "/Status/State");
        let health = health(checked.get(service).copied().unwrap_or(false), state);
        rows.push(
            [task_id, service, node, desired, state, health]
                .map(str::to_string)
                .to_vec(),
            json!({
                "id": task_id,
                "service": service,
                "node": node,
                "desired": desired,
                "state": state,
                "health": health,
            }),
        );
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(mode: Value, healthcheck: Option<Value>) -> Value {
        let mut container = json!({"Image": "nginx:1.27"});
        if let Some(test) = healthcheck {
            container["Healthcheck"] = json!({ "Test": test });
        }
        json!({
            "ID": "shop.web",
            "Spec": {
                "Labels": { STACK_LABEL: "shop" },
                "Mode": mode,
                "TaskTemplate": { "ContainerSpec": container }
            }
        })
    }

    #[test]
    fn modes_and_replicas_are_read() {
        let replicated = service(json!({"Replicated": {"Replicas": 3}}), None);
        assert_eq!(mode(&replicated), ("replicated", Some(3)));
        let global = service(json!({"Global": {}}), None);
        assert_eq!(mode(&global), ("global", None));
        assert_eq!(stack_of(&replicated), Some("shop"));
        assert_eq!(image(&replicated), "nginx:1.27");
    }

    #[test]
    fn health_is_derived_from_the_healthcheck_and_state() {
        let none = service(
            json!({"Replicated": {"Replicas": 1}}),
            Some(json!(["NONE"])),
        );
        let checked = service(
            json!({"Replicated": {"Replicas": 1}}),
            Some(json!(["CMD", "true"])),
        );
        assert!(!has_healthcheck(&none));
        assert!(has_healthcheck(&checked));
        assert_eq!(health(true, "preparing"), "starting");
        assert_eq!(health(true, "running"), "healthy");
        assert_eq!(health(true, "failed"), "-");
        assert_eq!(health(false, "running"), "-");
    }

    #[test]
    fn task_rows_name_nodes_and_sort_by_id() {
        let services = vec![service(
            json!({"Replicated": {"Replicas": 2}}),
            Some(json!(["CMD", "true"])),
        )];
        let tasks = json!([
            {"ID": "shop.web.2.a", "ServiceID": "shop.web", "NodeID": "1",
             "DesiredState": "running", "Status": {"State": "preparing"}},
            {"ID": "shop.web.1.a", "ServiceID": "shop.web", "NodeID": "1",
             "DesiredState": "running", "Status": {"State": "running"}},
        ]);
        let nodes = node_names(&json!([{"ID": "1", "Description": {"Hostname": "mgr1"}}]));
        let rows = task_rows(&tasks, &services, &nodes);
        assert_eq!(
            rows.cells[0],
            [
                "shop.web.1.a",
                "shop.web",
                "mgr1",
                "running",
                "running",
                "healthy"
            ]
        );
        assert_eq!(rows.cells[1][5], "starting");
    }
}
