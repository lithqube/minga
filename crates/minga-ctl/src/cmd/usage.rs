//! `minga-ctl agent usage`: query retained, tenant-scoped agent usage.

use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;
use serde_json::Value;

use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::Connection;

#[derive(Debug, Args)]
pub struct UsageArgs {
    /// Restrict rows to this customer (not available to cluster operators).
    #[arg(long)]
    customer: Option<String>,
    /// Retention-relative window such as 30d, 12h, 15m, or 20s.
    #[arg(long, default_value = "30d", value_parser = parse_since)]
    since: String,
}

pub async fn run(
    args: &UsageArgs,
    conn: &Connection,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let duration_ms = parse_duration_ms(&args.since)?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CtlError::Config(format!("system clock precedes Unix epoch: {error}")))?
        .as_millis() as u64;
    let since_unix_ms = now_ms.saturating_sub(duration_ms);
    let params = usage_query(since_unix_ms, args.customer.as_deref());
    let values: Vec<(&str, String)> = params
        .iter()
        .map(|(key, value)| (*key, value.clone()))
        .collect();
    let response = conn.api.get_query("agent/usage", &values).await?;
    let rows = rows(&response);
    output::print(format, quiet, &rows)
}

fn usage_query(since_unix_ms: u64, customer: Option<&str>) -> Vec<(&'static str, String)> {
    let mut query = vec![("since_unix_ms", since_unix_ms.to_string())];
    if let Some(customer) = customer {
        query.push(("customer", customer.to_owned()));
    }
    query
}

fn parse_since(value: &str) -> Result<String, String> {
    parse_duration_ms(value)
        .map(|_| value.to_owned())
        .map_err(|error| error.to_string())
}

fn parse_duration_ms(value: &str) -> Result<u64, CtlError> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| CtlError::Config("--since requires a unit: d, h, m, or s".into()))?;
    let (amount, unit) = value.split_at(split);
    let amount: u64 = amount
        .parse()
        .map_err(|_| CtlError::Config(format!("invalid --since duration `{value}`")))?;
    let multiplier = match unit {
        "d" => 86_400_000,
        "h" => 3_600_000,
        "m" => 60_000,
        "s" => 1_000,
        _ => {
            return Err(CtlError::Config(format!(
                "unsupported --since unit in `{value}`"
            )))
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| CtlError::Config(format!("--since duration `{value}` is too large")))
}

fn rows(response: &Value) -> Rows {
    let items = response
        .as_array()
        .cloned()
        .or_else(|| {
            ["events", "tenants", "items"]
                .iter()
                .find_map(|key| response.get(key).and_then(Value::as_array).cloned())
        })
        .unwrap_or_default();
    let operator_totals = items
        .first()
        .is_some_and(|item| item.get("request_count").is_some());
    if operator_totals {
        let mut rows = Rows::new(&[
            "TENANT",
            "REQUESTS",
            "INPUT TOKENS",
            "OUTPUT TOKENS",
            "COST MICROS",
        ]);
        for item in items {
            let tenant = field(&item, "tenant_id");
            rows.push(
                vec![
                    tenant.clone(),
                    field(&item, "request_count"),
                    field(&item, "input_tokens"),
                    field(&item, "output_tokens"),
                    field(&item, "cost_micros"),
                ],
                item,
            );
        }
        return rows;
    }

    let mut rows = Rows::new(&[
        "REQUEST",
        "CUSTOMER",
        "SERVICE",
        "TASK",
        "REQUESTED MODEL",
        "ANSWERED MODEL",
        "INPUT",
        "OUTPUT",
        "CACHE HIT",
        "LATENCY MS",
        "STATUS",
        "COST MICROS",
    ]);
    for item in items {
        rows.push(
            [
                "request_id",
                "customer_id",
                "service_id",
                "task_id",
                "requested_model",
                "answered_model",
                "input_tokens",
                "output_tokens",
                "cache_hit",
                "latency_ms",
                "status",
                "cost_micros",
            ]
            .iter()
            .map(|name| field(&item, name))
            .collect(),
            item,
        );
    }
    rows
}

fn field(item: &Value, name: &str) -> String {
    match item.get(name) {
        Some(Value::Null) | None => "-".into(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn since_parser_accepts_expected_units_and_rejects_bad_values() {
        assert_eq!(parse_duration_ms("30d").unwrap(), 2_592_000_000);
        assert_eq!(parse_duration_ms("12h").unwrap(), 43_200_000);
        assert_eq!(parse_duration_ms("15m").unwrap(), 900_000);
        assert_eq!(parse_duration_ms("20s").unwrap(), 20_000);
        assert!(parse_duration_ms("3w").is_err());
        assert!(parse_duration_ms("many days").is_err());
    }

    #[test]
    fn query_contains_only_customer_and_since_filters() {
        assert_eq!(
            usage_query(1234, Some("acme")),
            vec![
                ("since_unix_ms", "1234".to_owned()),
                ("customer", "acme".to_owned())
            ]
        );
        assert_eq!(
            usage_query(1234, None),
            vec![("since_unix_ms", "1234".to_owned())]
        );
    }

    #[test]
    fn operator_rows_render_aggregate_objects_from_the_response_envelope() {
        let view = rows(&json!({"tenants": [{
            "tenant_id": "acme",
            "request_count": 3,
            "input_tokens": 11,
            "output_tokens": 9,
            "cost_micros": 7,
        }]}));
        assert_eq!(
            view.headers,
            [
                "TENANT",
                "REQUESTS",
                "INPUT TOKENS",
                "OUTPUT TOKENS",
                "COST MICROS"
            ]
        );
        assert_eq!(view.cells[0][0], "acme");
    }
    #[test]
    fn tenant_rows_render_events_from_the_response_envelope() {
        let view = rows(&json!({"events": [{
            "request_id": "request-1",
            "customer_id": "acme",
            "service_id": "stack.inference",
            "task_id": "task-1",
            "requested_model": "model-a",
            "answered_model": "model-b",
            "input_tokens": 1,
            "output_tokens": 2,
            "cache_hit": false,
            "latency_ms": 3,
            "status": 200,
            "cost_micros": null,
        }]}));
        assert_eq!(view.cells[0][0], "request-1");
        assert_eq!(view.cells[0][3], "task-1");
    }
}
