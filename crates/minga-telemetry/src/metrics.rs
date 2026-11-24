use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramVec, Registry, TextEncoder,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Central metrics registry for the Minga process.
pub struct Metrics {
    registry: Registry,

    // Task metrics
    pub tasks_total: GaugeVec,
    pub task_starts_total: Counter,
    pub task_stops_total: Counter,
    pub task_restarts_total: Counter,

    // Scheduler metrics
    pub scheduler_reconcile_duration_seconds: Histogram,
    pub scheduler_placement_decisions_total: CounterVec,
    /// Tenants left out of a reconcile pass by an error, by tenant and stage.
    pub scheduler_tenant_failures_total: CounterVec,

    // Raft metrics
    pub raft_leader: Gauge,
    pub raft_replication_lag: Gauge,

    // Bus metrics
    pub bus_publish_latency_seconds: HistogramVec,

    // Agent executor metrics
    pub agent_pool_idle: Gauge,
    pub agent_pool_busy: Gauge,
    pub agent_pool_total: Gauge,

    // Agent usage is aggregated by tenant only, never by request/customer/task.
    pub agent_usage_requests_total: CounterVec,
    pub agent_usage_input_tokens_total: CounterVec,
    pub agent_usage_output_tokens_total: CounterVec,
    pub agent_usage_cost_micros_total: CounterVec,
    /// Usage events the egress proxy gave up publishing after its retries.
    pub agent_usage_events_dropped_total: CounterVec,

    // Containerd gRPC metrics
    pub containerd_grpc_latency_seconds: HistogramVec,
}

impl Metrics {
    /// Create and register all metrics.
    pub fn new() -> Self {
        let registry = Registry::new();

        let tasks_total = GaugeVec::new(
            prometheus::Opts::new("minga_tasks_total", "Total tasks by state"),
            &["state"],
        )
        .expect("minga_tasks_total");
        registry
            .register(Box::new(tasks_total.clone()))
            .expect("register tasks_total");

        let task_starts_total = Counter::new("minga_task_starts_total", "Total task starts")
            .expect("minga_task_starts_total");
        registry
            .register(Box::new(task_starts_total.clone()))
            .expect("register task_starts_total");

        let task_stops_total = Counter::new("minga_task_stops_total", "Total task stops")
            .expect("minga_task_stops_total");
        registry
            .register(Box::new(task_stops_total.clone()))
            .expect("register task_stops_total");

        let task_restarts_total = Counter::new("minga_task_restarts_total", "Total task restarts")
            .expect("minga_task_restarts_total");
        registry
            .register(Box::new(task_restarts_total.clone()))
            .expect("register task_restarts_total");

        let scheduler_reconcile_duration_seconds = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "minga_scheduler_reconcile_duration_seconds",
                "Scheduler reconcile loop duration",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )
        .expect("minga_scheduler_reconcile_duration_seconds");
        registry
            .register(Box::new(scheduler_reconcile_duration_seconds.clone()))
            .expect("register scheduler_reconcile_duration_seconds");

        let scheduler_placement_decisions_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_scheduler_placement_decisions_total",
                "Scheduler placement decisions",
            ),
            &["result"],
        )
        .expect("minga_scheduler_placement_decisions_total");
        registry
            .register(Box::new(scheduler_placement_decisions_total.clone()))
            .expect("register scheduler_placement_decisions_total");

        let scheduler_tenant_failures_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_scheduler_tenant_failures_total",
                "Reconcile passes in which one tenant was skipped after an error",
            ),
            &["tenant", "stage"],
        )
        .expect("minga_scheduler_tenant_failures_total");
        registry
            .register(Box::new(scheduler_tenant_failures_total.clone()))
            .expect("register scheduler_tenant_failures_total");

        let raft_leader = Gauge::new(
            "minga_raft_leader",
            "1 if this node is Raft leader, 0 otherwise",
        )
        .expect("minga_raft_leader");
        registry
            .register(Box::new(raft_leader.clone()))
            .expect("register raft_leader");

        let raft_replication_lag = Gauge::new(
            "minga_raft_replication_lag",
            "Raft replication lag (committed - applied index)",
        )
        .expect("minga_raft_replication_lag");
        registry
            .register(Box::new(raft_replication_lag.clone()))
            .expect("register raft_replication_lag");

        let bus_publish_latency_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "minga_bus_publish_latency_seconds",
                "Bus message publish latency",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
            &["topic"],
        )
        .expect("minga_bus_publish_latency_seconds");
        registry
            .register(Box::new(bus_publish_latency_seconds.clone()))
            .expect("register bus_publish_latency_seconds");

        let agent_pool_idle = Gauge::new("minga_agent_pool_idle", "Idle agent executor sandboxes")
            .expect("minga_agent_pool_idle");
        registry
            .register(Box::new(agent_pool_idle.clone()))
            .expect("register agent_pool_idle");

        let agent_pool_busy = Gauge::new("minga_agent_pool_busy", "Busy agent executor sandboxes")
            .expect("minga_agent_pool_busy");
        registry
            .register(Box::new(agent_pool_busy.clone()))
            .expect("register agent_pool_busy");

        let agent_pool_total =
            Gauge::new("minga_agent_pool_total", "Total agent executor sandboxes")
                .expect("minga_agent_pool_total");
        registry
            .register(Box::new(agent_pool_total.clone()))
            .expect("register agent_pool_total");

        let containerd_grpc_latency_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "minga_containerd_grpc_latency_seconds",
                "Containerd gRPC call latency",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["method"],
        )
        .expect("minga_containerd_grpc_latency_seconds");
        registry
            .register(Box::new(containerd_grpc_latency_seconds.clone()))
            .expect("register containerd_grpc_latency_seconds");

        let agent_usage_requests_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_agent_usage_requests_total",
                "Agent requests by tenant",
            ),
            &["tenant"],
        )
        .expect("minga_agent_usage_requests_total");
        registry
            .register(Box::new(agent_usage_requests_total.clone()))
            .expect("register agent_usage_requests_total");
        let agent_usage_input_tokens_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_agent_usage_input_tokens_total",
                "Agent input tokens by tenant",
            ),
            &["tenant"],
        )
        .expect("minga_agent_usage_input_tokens_total");
        registry
            .register(Box::new(agent_usage_input_tokens_total.clone()))
            .expect("register agent_usage_input_tokens_total");
        let agent_usage_output_tokens_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_agent_usage_output_tokens_total",
                "Agent output tokens by tenant",
            ),
            &["tenant"],
        )
        .expect("minga_agent_usage_output_tokens_total");
        registry
            .register(Box::new(agent_usage_output_tokens_total.clone()))
            .expect("register agent_usage_output_tokens_total");
        let agent_usage_cost_micros_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_agent_usage_cost_micros_total",
                "Agent cost in micros by tenant",
            ),
            &["tenant"],
        )
        .expect("minga_agent_usage_cost_micros_total");
        registry
            .register(Box::new(agent_usage_cost_micros_total.clone()))
            .expect("register agent_usage_cost_micros_total");
        let agent_usage_events_dropped_total = CounterVec::new(
            prometheus::Opts::new(
                "minga_agent_usage_events_dropped_total",
                "Agent usage events dropped after failed publish retries, by tenant",
            ),
            &["tenant"],
        )
        .expect("minga_agent_usage_events_dropped_total");
        registry
            .register(Box::new(agent_usage_events_dropped_total.clone()))
            .expect("register agent_usage_events_dropped_total");

        Self {
            registry,
            tasks_total,
            task_starts_total,
            task_stops_total,
            task_restarts_total,
            scheduler_reconcile_duration_seconds,
            scheduler_placement_decisions_total,
            scheduler_tenant_failures_total,
            raft_leader,
            raft_replication_lag,
            bus_publish_latency_seconds,
            agent_pool_idle,
            agent_pool_busy,
            agent_pool_total,
            agent_usage_requests_total,
            agent_usage_input_tokens_total,
            agent_usage_output_tokens_total,
            agent_usage_cost_micros_total,
            agent_usage_events_dropped_total,
            containerd_grpc_latency_seconds,
        }
    }

    /// Add usage counters grouped only by tenant.
    pub fn record_agent_usage(
        &self,
        tenant_id: &str,
        input_tokens: u64,
        output_tokens: u64,
        cost_micros: u64,
    ) {
        self.agent_usage_requests_total
            .with_label_values(&[tenant_id])
            .inc();
        self.agent_usage_input_tokens_total
            .with_label_values(&[tenant_id])
            .inc_by(input_tokens as f64);
        self.agent_usage_output_tokens_total
            .with_label_values(&[tenant_id])
            .inc_by(output_tokens as f64);
        self.agent_usage_cost_micros_total
            .with_label_values(&[tenant_id])
            .inc_by(cost_micros as f64);
    }

    /// Get the Prometheus registry for `/metrics` endpoint.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Encode only families that contain no tenant-labelled samples.
    pub(crate) fn gather_without_tenants(&self) -> Result<String, MetricsError> {
        let families: Vec<_> = self
            .registry
            .gather()
            .into_iter()
            .filter(|family| {
                !family.get_metric().iter().any(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.get_name() == "tenant")
                })
            })
            .collect();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    /// Encode metrics in Prometheus text format.
    pub fn gather(&self) -> Result<String, MetricsError> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_counters_export_only_tenant_label() {
        let metrics = Metrics::new();
        metrics.record_agent_usage("tenant-a", 3, 5, 7);
        let exposition = metrics.gather().unwrap();
        assert!(exposition.contains("minga_agent_usage_requests_total{tenant=\"tenant-a\"} 1"));
        assert!(exposition.contains("minga_agent_usage_input_tokens_total{tenant=\"tenant-a\"} 3"));
        assert!(!exposition.contains("customer="));
        assert!(!exposition.contains("request_id="));
        assert!(!exposition.contains("task_id="));
    }
}
