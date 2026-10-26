use std::sync::OnceLock;
use tracing::Subscriber;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

static INIT: OnceLock<()> = OnceLock::new();

/// Initialize global structured JSON logging.
/// Safe to call multiple times — subsequent calls are no-ops.
pub fn init_logging() {
    INIT.get_or_init(|| {
        let env_filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info,minga=debug"))
            .expect("valid log filter");

        let json_layer = fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(json_layer)
            .init();
    });
}

/// Layer that extracts correlation ID from bus message headers
/// and adds it to the current span.
#[derive(Clone, Default)]
pub struct CorrelationIdLayer;

impl<S> Layer<S> for CorrelationIdLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).expect("span exists");
        let mut extensions = span.extensions_mut();

        // Check if correlation_id is in the span attributes
        // Use the visitor pattern to extract correlation_id
        let mut visitor = CorrelationIdVisitor::default();
        attrs.record(&mut visitor);
        if let Some(correlation_id) = visitor.correlation_id {
            extensions.insert(CorrelationId(correlation_id));
        }
    }
}

/// Visitor to extract correlation_id from span attributes.
#[derive(Default)]
struct CorrelationIdVisitor {
    correlation_id: Option<String>,
}

impl tracing::field::Visit for CorrelationIdVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "correlation_id" {
            self.correlation_id = Some(value.to_string());
        }
    }
}

/// Thread-local correlation ID storage for structured logging.
#[allow(dead_code)]
#[derive(Clone)]
struct CorrelationId(String);

/// Helper to create a span with correlation ID from bus headers.
pub fn span_with_correlation_id(correlation_id: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!("bus_message", correlation_id = correlation_id.unwrap_or(""));
    span
}
