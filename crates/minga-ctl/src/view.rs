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

/// Longest ERROR cell in the `ps` tables; `-o json` and `-o yaml` keep the
/// whole text.
const ERROR_CELL: usize = 50;

/// `text` cut to `max` characters, ending in `…` when it was cut.
fn cut(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut cut: String = text.chars().take(max - 1).collect();
    cut.push('…');
    cut
}

/// Why the task last ended or failed to start (`Status.Err`).
fn task_error(task: &Value) -> Option<&str> {
    task.pointer("/Status/Err")
        .and_then(Value::as_str)
        .filter(|e| !e.is_empty())
}

/// The exit code of the task's last run, when the runtime reported one.
fn task_exit_code(task: &Value) -> Option<i64> {
    task.pointer("/Status/ContainerStatus/ExitCode")
        .and_then(Value::as_i64)
}

/// The task table shared by `stack ps` and `service ps`, sorted by task ID.
///
/// `services` are the services the tasks belong to; their healthcheck
/// decides the HEALTH column. ERROR comes last so scripts that read the
/// earlier columns keep working.
pub fn task_rows(tasks: &Value, services: &[Value], nodes: &HashMap<String, String>) -> Rows {
    let checked: HashMap<&str, bool> = services
        .iter()
        .map(|s| (id(s), has_healthcheck(s)))
        .collect();
    let mut tasks: Vec<&Value> = items(tasks).iter().collect();
    tasks.sort_by(|a, b| text(a, "/ID").cmp(text(b, "/ID")));

    let mut rows = Rows::new(&[
        "ID", "SERVICE", "NODE", "DESIRED", "STATE", "HEALTH", "ERROR",
    ]);
    for task in tasks {
        let task_id = text(task, "/ID");
        let service = text(task, "/ServiceID");
        let node = node_name(task, nodes);
        let desired = text(task, "/DesiredState");
        let state = text(task, "/Status/State");
        let health = health(checked.get(service).copied().unwrap_or(false), state);
        let error = task_error(task);
        rows.push(
            [
                task_id,
                service,
                node,
                desired,
                state,
                health,
                &cut(error.unwrap_or("-"), ERROR_CELL),
            ]
            .map(str::to_string)
            .to_vec(),
            json!({
                "id": task_id,
                "service": service,
                "node": node,
                "desired": desired,
                "state": state,
                "health": health,
                "error": error,
                "exit_code": task_exit_code(task),
            }),
        );
    }
    rows
}

/// The node's host name, its ID when unknown, or `-` when unassigned.
fn node_name<'a>(task: &'a Value, nodes: &'a HashMap<String, String>) -> &'a str {
    let node_id = text(task, "/NodeID");
    match nodes.get(node_id) {
        Some(name) => name.as_str(),
        None if node_id.is_empty() => "-",
        None => node_id,
    }
}

/// The hint `ps` prints on stderr when tasks are not running, else `None`.
/// It names the service when only one has such tasks.
pub fn not_running_hint(tasks: &Value) -> Option<String> {
    let down: Vec<&str> = items(tasks)
        .iter()
        .filter(|t| text(t, "/Status/State") != "running")
        .map(|t| text(t, "/ServiceID"))
        .collect();
    let first = *down.first()?;
    let service = if down.iter().all(|s| *s == first) {
        first
    } else {
        "<service>"
    };
    Some(format!(
        "{} task(s) not running: see `minga-ctl service ps {service}` and `minga-ctl service logs {service}`",
        down.len()
    ))
}

/// Docker durations are nanoseconds; `service inspect` shows seconds.
fn secs(ns: Option<u64>) -> Value {
    ns.map_or(Value::Null, |ns| json!(ns / 1_000_000_000))
}

/// The key fields of a service spec, as `service inspect -o json` prints
/// them.
pub fn spec_summary(service: &Value) -> Value {
    let spec = service.pointer("/Spec").unwrap_or(&Value::Null);
    let (mode, replicas) = mode(service);
    let ports: Vec<Value> = spec
        .pointer("/EndpointSpec/Ports")
        .map(|p| items(p).to_vec())
        .unwrap_or_default()
        .iter()
        .map(|p| {
            json!({
                "target": p["TargetPort"],
                "published": p["PublishedPort"],
                "protocol": p["Protocol"],
            })
        })
        .collect();
    let networks: Vec<&str> = spec
        .pointer("/Networks")
        .map(items)
        .unwrap_or_default()
        .iter()
        .filter_map(|n| n["Target"].as_str())
        .collect();
    let update = spec.get("UpdateConfig").filter(|u| u.is_object()).map(|u| {
        json!({
            "parallelism": u["Parallelism"],
            "delay_secs": secs(u["Delay"].as_u64()),
            "monitor_secs": secs(u["Monitor"].as_u64()),
            "failure_action": u["FailureAction"],
            "order": u["Order"],
        })
    });
    let healthcheck = has_healthcheck(service).then(|| {
        let h = &spec["TaskTemplate"]["ContainerSpec"]["Healthcheck"];
        json!({
            "test": h["Test"],
            "interval_secs": secs(h["Interval"].as_u64()),
            "timeout_secs": secs(h["Timeout"].as_u64()),
            "retries": h["Retries"],
            "start_period_secs": secs(h["StartPeriod"].as_u64()),
        })
    });
    json!({
        "id": id(service),
        "image": image(service),
        "mode": mode,
        "replicas": replicas,
        "ports": ports,
        "networks": networks,
        "update": update,
        "healthcheck": healthcheck,
    })
}

/// A scalar for the `service inspect` text: its value, or `-` when absent.
fn show(value: &Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// [`spec_summary`] as the `key: value` lines `service inspect` prints.
pub fn spec_text(summary: &Value) -> String {
    let list = |key: &str, one: &dyn Fn(&Value) -> String| -> String {
        let all: Vec<String> = items(&summary[key]).iter().map(one).collect();
        if all.is_empty() {
            "-".to_string()
        } else {
            all.join(", ")
        }
    };
    let ports = list("ports", &|p| match p["published"].as_u64() {
        Some(published) => format!(
            "{published}->{}/{}",
            show(&p["target"]),
            show(&p["protocol"])
        ),
        None => format!("{}/{}", show(&p["target"]), show(&p["protocol"])),
    });
    let networks = list("networks", &show);
    let update = match summary["update"].as_object() {
        None => "-".to_string(),
        Some(u) => format!(
            "parallelism {}, delay {}s, monitor {}s, on failure {}, order {}",
            show(&u["parallelism"]),
            show(&u["delay_secs"]),
            show(&u["monitor_secs"]),
            show(&u["failure_action"]),
            show(&u["order"]),
        ),
    };
    let healthcheck = match summary["healthcheck"].as_object() {
        None => "-".to_string(),
        Some(h) => {
            let test: Vec<&str> = items(&h["test"]).iter().filter_map(Value::as_str).collect();
            format!(
                "{} (interval {}s, timeout {}s, retries {}, start period {}s)",
                test.join(" "),
                show(&h["interval_secs"]),
                show(&h["timeout_secs"]),
                show(&h["retries"]),
                show(&h["start_period_secs"]),
            )
        }
    };
    format!(
        "Service:      {}\nImage:        {}\nMode:         {}\nReplicas:     {}\nPorts:        {ports}\n\
         Networks:     {networks}\nUpdate:       {update}\nHealthcheck:  {healthcheck}",
        show(&summary["id"]),
        show(&summary["image"]),
        show(&summary["mode"]),
        show(&summary["replicas"]),
    )
}

/// The task table of `service inspect`: state, restarts, exit code and the
/// whole last error.
pub fn inspect_task_rows(tasks: &Value, nodes: &HashMap<String, String>) -> Rows {
    let mut tasks: Vec<&Value> = items(tasks).iter().collect();
    tasks.sort_by(|a, b| text(a, "/ID").cmp(text(b, "/ID")));
    let mut rows = Rows::new(&[
        "ID", "NODE", "DESIRED", "STATE", "RESTARTS", "EXIT", "ERROR",
    ]);
    for task in tasks {
        let (task_id, node) = (text(task, "/ID"), node_name(task, nodes));
        let (desired, state) = (text(task, "/DesiredState"), text(task, "/Status/State"));
        let restarts = task["MingaRestarts"].as_u64();
        let (exit, error) = (task_exit_code(task), task_error(task));
        rows.push(
            vec![
                task_id.to_string(),
                node.to_string(),
                desired.to_string(),
                state.to_string(),
                restarts.map_or("-".to_string(), |n| n.to_string()),
                exit.map_or("-".to_string(), |c| c.to_string()),
                error.unwrap_or("-").to_string(),
            ],
            json!({
                "id": task_id,
                "node": node,
                "desired": desired,
                "state": state,
                "restarts": restarts,
                "exit_code": exit,
                "error": error,
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
                "healthy",
                "-"
            ]
        );
        assert_eq!(rows.cells[1][5], "starting");
    }

    #[test]
    fn a_long_error_is_cut_in_the_table_and_whole_in_json() {
        let error = "failed to start container: ".to_string() + &"é".repeat(60);
        let tasks = json!([{
            "ID": "shop.web.1.a", "ServiceID": "shop.web", "NodeID": "1",
            "DesiredState": "running",
            "Status": {"State": "preparing", "Err": error,
                       "ContainerStatus": {"ExitCode": 3}}
        }]);
        let rows = task_rows(&tasks, &[], &HashMap::new());
        let cell = &rows.cells[0][6];
        assert_eq!(cell.chars().count(), ERROR_CELL);
        assert!(cell.ends_with('…'));
        assert_eq!(rows.values[0]["error"], error);
        assert_eq!(rows.values[0]["exit_code"], 3);
        assert_eq!(rows.headers.last().unwrap(), "ERROR");
    }

    #[test]
    fn the_hint_counts_tasks_that_are_not_running_and_names_one_service() {
        let task =
            |service: &str, state: &str| json!({"ServiceID": service, "Status": {"State": state}});
        assert_eq!(not_running_hint(&json!([task("a.b", "running")])), None);
        assert_eq!(not_running_hint(&json!([])), None);
        assert_eq!(
            not_running_hint(&json!([task("a.b", "failed"), task("a.b", "preparing")])).unwrap(),
            "2 task(s) not running: see `minga-ctl service ps a.b` and `minga-ctl service logs a.b`"
        );
        assert!(
            not_running_hint(&json!([task("a.b", "failed"), task("a.c", "failed")]))
                .unwrap()
                .contains("service ps <service>")
        );
    }
}
