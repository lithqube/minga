use std::sync::LazyLock;

pub mod health;
pub mod logging;
pub mod metrics;

pub use health::{HealthState, Healthz, ReadyState, Readyz};
pub use logging::{init_logging, span_with_correlation_id, CorrelationIdLayer};
pub use metrics::Metrics;

/// Global metrics registry — instantiated once at process start.
pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);
