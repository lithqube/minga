//! `minga-ctl metrics` (plan section 5.6).
//!
//! The Prometheus text is parsed by a small line parser; the table shows the
//! metrics that exist in `minga-telemetry`, and JSON/YAML show every sample.

use std::collections::BTreeMap;

use serde_json::json;

use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::Connection;

/// One parsed sample line.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parses Prometheus text: `#` lines and blank lines are skipped, the rest
/// are `name value` or `name{labels} value` (an optional timestamp after the
/// value is ignored). A line that does not parse is skipped.
pub fn parse(text: &str) -> Vec<Sample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<Sample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let name_end = line.find(|c: char| c == '{' || c.is_whitespace())?;
    let (name, mut rest) = line.split_at(name_end);
    let mut labels = BTreeMap::new();
    if let Some(body) = rest.strip_prefix('{') {
        (labels, rest) = parse_labels(body)?;
    }
    Some(Sample {
        name: name.to_string(),
        labels,
        value: rest.split_whitespace().next()?.parse().ok()?,
    })
}

/// Parses `k="v",k2="v2"}` (after the opening brace); returns the labels and
/// the text after the closing brace. Values may contain `\"`, `\\` and `\n`.
fn parse_labels(s: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let mut labels = BTreeMap::new();
    let mut key = String::new();
    let mut it = s.char_indices();
    while let Some((i, c)) = it.next() {
        match c {
            '}' if key.is_empty() => return Some((labels, &s[i + 1..])),
            ',' if key.is_empty() => {}
            ',' | ' ' => return None,
            '=' => {
                if it.next()?.1 != '"' {
                    return None;
                }
                let mut value = String::new();
                loop {
                    match it.next()?.1 {
                        '"' => break,
                        '\\' => value.push(match it.next()?.1 {
                            'n' => '\n',
                            other => other,
                        }),
                        other => value.push(other),
                    }
                }
                labels.insert(std::mem::take(&mut key), value);
            }
            _ => key.push(c),
        }
    }
    None
}

fn label<'a>(s: &'a Sample, key: &str) -> &'a str {
    s.labels.get(key).map(String::as_str).unwrap_or("-")
}

fn named<'a>(samples: &'a [Sample], name: &str) -> impl Iterator<Item = &'a Sample> {
    let name = name.to_string();
    samples.iter().filter(move |s| s.name == name)
}

fn first(samples: &[Sample], name: &str) -> Option<f64> {
    named(samples, name).next().map(|s| s.value)
}

/// Histogram averages (`_sum` / `_count`), one per label set with a non-zero
/// count; the `le` label does not occur on `_sum` and `_count`.
fn averages<'a>(samples: &'a [Sample], base: &str) -> Vec<(&'a Sample, f64)> {
    let (sum_name, count_name) = (format!("{base}_sum"), format!("{base}_count"));
    named(samples, &count_name)
        .filter(|c| c.value > 0.0)
        .filter_map(|c| {
            let sum = named(samples, &sum_name).find(|s| s.labels == c.labels)?;
            Some((c, sum.value / c.value))
        })
        .collect()
}

fn ms(seconds: f64) -> String {
    format!("{:.1} ms", seconds * 1000.0)
}

/// The table rows, in the order of plan section 5.6. Metrics that are not in
/// the response are left out.
pub fn summary(samples: &[Sample]) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = Vec::new();
    for s in named(samples, "minga_tasks_total") {
        rows.push((format!("tasks {}", label(s, "state")), s.value.to_string()));
    }
    for (label_text, name) in [
        ("task starts", "minga_task_starts_total"),
        ("task stops", "minga_task_stops_total"),
        ("task restarts", "minga_task_restarts_total"),
    ] {
        if let Some(v) = first(samples, name) {
            rows.push((label_text.to_string(), v.to_string()));
        }
    }
    for s in named(samples, "minga_scheduler_placement_decisions_total") {
        rows.push((
            format!("placement {}", label(s, "result")),
            s.value.to_string(),
        ));
    }
    for (_, avg) in averages(samples, "minga_scheduler_reconcile_duration_seconds") {
        rows.push(("reconcile avg".to_string(), ms(avg)));
    }
    if let Some(v) = first(samples, "minga_raft_leader") {
        rows.push((
            "raft leader".to_string(),
            if v == 1.0 { "yes" } else { "no" }.to_string(),
        ));
    }
    if let Some(v) = first(samples, "minga_raft_replication_lag") {
        rows.push(("raft replication lag".to_string(), v.to_string()));
    }
    for (s, avg) in averages(samples, "minga_bus_publish_latency_seconds") {
        rows.push((format!("bus publish avg {}", label(s, "topic")), ms(avg)));
    }
    let dead: Vec<_> = named(samples, "minga_bus_dead_letters_total").collect();
    if !dead.is_empty() {
        let total: f64 = dead.iter().map(|s| s.value).sum();
        rows.push(("bus dead letters".to_string(), total.to_string()));
    }
    if let (Some(idle), Some(busy), Some(total)) = (
        first(samples, "minga_agent_pool_idle"),
        first(samples, "minga_agent_pool_busy"),
        first(samples, "minga_agent_pool_total"),
    ) {
        rows.push((
            "agent pool".to_string(),
            format!("{idle} idle / {busy} busy / {total} total"),
        ));
    }
    for (s, avg) in averages(samples, "minga_containerd_grpc_latency_seconds") {
        rows.push((format!("containerd {} avg", label(s, "method")), ms(avg)));
    }
    rows
}

/// `GET <metrics root>/metrics` and print it.
pub async fn run(conn: &Connection, format: Format, quiet: bool) -> Result<(), CtlError> {
    let (status, body) = conn.api.text(&format!("{}/metrics", conn.metrics)).await?;
    match status {
        200 => {}
        401 => {
            return Err(CtlError::Auth {
                endpoint: conn.metrics.clone(),
                message: body.trim().to_string(),
            })
        }
        _ => {
            return Err(CtlError::Api {
                status,
                message: body.trim().to_string(),
            })
        }
    }
    let samples = parse(&body);

    // The table shows the summary; JSON and YAML show every sample.
    let mut rows = Rows::new(&["METRIC", "VALUE"]);
    rows.cells = summary(&samples)
        .into_iter()
        .map(|(k, v)| vec![k, v])
        .collect();
    rows.values = samples
        .iter()
        .map(|s| json!({"metric": s.name, "labels": s.labels, "value": s.value}))
        .collect();
    output::print(format, quiet, &rows)?;

    if format == Format::Table && !quiet {
        eprintln!(
            "note: Minga exports no image pull latency or update count metrics, so they are not shown"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = r#"
# HELP minga_tasks_total Total tasks by state
# TYPE minga_tasks_total gauge
minga_tasks_total{state="running"} 12
minga_tasks_total{state="failed"} 1

minga_task_starts_total 48
minga_raft_leader 1
minga_scheduler_reconcile_duration_seconds_sum 0.0062
minga_scheduler_reconcile_duration_seconds_count 2
minga_bus_dead_letters_total{topic="a",reason="x"} 2
minga_bus_dead_letters_total{topic="b",reason="y"} 3
minga_agent_pool_idle 2
minga_agent_pool_busy 1
minga_agent_pool_total 3
minga_containerd_grpc_latency_seconds_sum{method="Create"} 0.0368
minga_containerd_grpc_latency_seconds_count{method="Create"} 2
"#;

    #[test]
    fn parses_names_labels_and_values() {
        let samples = parse(TEXT);
        assert_eq!(samples.len(), 13);
        assert_eq!(samples[0].name, "minga_tasks_total");
        assert_eq!(samples[0].labels["state"], "running");
        assert_eq!(samples[0].value, 12.0);
        assert_eq!(samples[2].name, "minga_task_starts_total");
        assert!(samples[2].labels.is_empty());
    }

    #[test]
    fn comments_blank_lines_and_junk_are_skipped() {
        assert!(parse("# HELP x y\n\n   \n# TYPE x gauge\n").is_empty());
        assert!(parse("novalue\nname notanumber\nname{a=\"b\" 1").is_empty());
    }

    #[test]
    fn label_values_may_hold_escapes_commas_and_braces() {
        let s = &parse(r#"m{a="x,y}",b="q\"z\\",c="l1\nl2"} 1.5 1700000000"#)[0];
        assert_eq!(s.labels["a"], "x,y}");
        assert_eq!(s.labels["b"], "q\"z\\");
        assert_eq!(s.labels["c"], "l1\nl2");
        assert_eq!(s.value, 1.5);
    }

    #[test]
    fn special_float_values_parse() {
        let samples = parse("a NaN\nb +Inf\nc -Inf\n");
        assert!(samples[0].value.is_nan());
        assert_eq!(samples[1].value, f64::INFINITY);
        assert_eq!(samples[2].value, f64::NEG_INFINITY);
    }

    #[test]
    fn the_summary_follows_the_plan_and_skips_missing_metrics() {
        let rows = summary(&parse(TEXT));
        let get = |k: &str| rows.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("tasks running"), Some("12"));
        assert_eq!(get("tasks failed"), Some("1"));
        assert_eq!(get("task starts"), Some("48"));
        assert_eq!(get("task stops"), None);
        assert_eq!(get("raft leader"), Some("yes"));
        assert_eq!(get("reconcile avg"), Some("3.1 ms"));
        assert_eq!(get("bus dead letters"), Some("5"));
        assert_eq!(get("agent pool"), Some("2 idle / 1 busy / 3 total"));
        assert_eq!(get("containerd Create avg"), Some("18.4 ms"));
    }

    #[test]
    fn a_histogram_without_observations_has_no_average() {
        let rows = summary(&parse(
            "minga_scheduler_reconcile_duration_seconds_sum 0\nminga_scheduler_reconcile_duration_seconds_count 0\n",
        ));
        assert!(rows.is_empty(), "{rows:?}");
    }
}
