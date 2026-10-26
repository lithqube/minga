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

    // Raft metrics
    pub raft_leader: Gauge,
    pub raft_replication_lag: Gauge,

    // Bus metrics
    pub bus_publish_latency_seconds: HistogramVec,

    // Agent executor metrics
    pub agent_pool_idle: Gauge,
    pub agent_pool_busy: Gauge,
    pub agent_pool_total: Gauge,

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

        Self {
            registry,
            tasks_total,
            task_starts_total,
            task_stops_total,
            task_restarts_total,
            scheduler_reconcile_duration_seconds,
            scheduler_placement_decisions_total,
            raft_leader,
            raft_replication_lag,
            bus_publish_latency_seconds,
            agent_pool_idle,
            agent_pool_busy,
            agent_pool_total,
            containerd_grpc_latency_seconds,
        }
    }

    /// Get the Prometheus registry for `/metrics` endpoint.
    pub fn registry(&self) -> &Registry {
        &self.registry
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
