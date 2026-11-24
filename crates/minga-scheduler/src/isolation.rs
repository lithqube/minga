//! The tenant as the unit of failure in one reconcile pass.
//!
//! Tenants are untrusted and independent, so a fault found while working for
//! one tenant (an unreadable status bucket, a refused write, a node record
//! the mesh cannot address) must not stop the pass for the others. Every
//! per-tenant step reports its error to one [`Isolation`] instead of
//! returning it; the tenant is then *frozen* for the rest of the pass:
//!
//! - nothing is planned, written or swept for it, so its tasks are not
//!   re-placed and its existing assignment keys are not deleted on the
//!   strength of data that could not be read;
//! - the pass carries on for every other tenant;
//! - the tenant is retried whole on the next pass.
//!
//! The first fault of a tenant is the one reported, once per pass, when the
//! [`Isolation`] is dropped: logged with the tenant ID and counted in
//! `minga_scheduler_tenant_failures_total`.

use std::collections::BTreeMap;
use std::fmt;

use thiserror::Error;

use crate::SchedulerError;

/// The step of a pass in which a tenant failed. Also the metric's `stage` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// A node record of the tenant cannot be projected.
    Nodes,
    /// The tenant's task status could not be read.
    Status,
    /// A network subnet of the tenant could not be stored.
    Subnet,
    /// A task or service write of the tenant failed.
    Placement,
    /// An assignment of the tenant could not be built or written.
    Projection,
    /// A stale assignment key of the tenant could not be deleted.
    Sweep,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Nodes => "nodes",
            Stage::Status => "status",
            Stage::Subnet => "subnet",
            Stage::Placement => "placement",
            Stage::Projection => "projection",
            Stage::Sweep => "sweep",
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why one tenant was left out of a pass.
#[derive(Debug, Error)]
#[error("tenant {tenant_id} skipped at {stage}: {source}")]
pub struct TenantError {
    pub tenant_id: String,
    pub stage: Stage,
    #[source]
    pub source: SchedulerError,
}

/// The tenants of one pass that failed, and why. Reports on drop, so a pass
/// that ends early with a cluster-level error still reports its tenants.
#[derive(Default)]
pub(crate) struct Isolation {
    failed: BTreeMap<String, TenantError>,
}

impl Isolation {
    /// Freezes `tenant_id` for the rest of the pass. A tenant that already
    /// failed keeps its first error.
    pub(crate) fn fail(
        &mut self,
        tenant_id: &str,
        stage: Stage,
        source: impl Into<SchedulerError>,
    ) {
        self.failed
            .entry(tenant_id.to_owned())
            .or_insert_with(|| TenantError {
                tenant_id: tenant_id.to_owned(),
                stage,
                source: source.into(),
            });
    }

    pub(crate) fn has_failed(&self, tenant_id: &str) -> bool {
        self.failed.contains_key(tenant_id)
    }
}

impl Drop for Isolation {
    fn drop(&mut self) {
        for error in self.failed.values() {
            tracing::error!(
                tenant = %error.tenant_id,
                stage = %error.stage,
                error = %error.source,
                "tenant skipped in this reconcile pass; retrying next pass"
            );
            minga_telemetry::METRICS
                .scheduler_tenant_failures_total
                .with_label_values(&[&error.tenant_id, error.stage.as_str()])
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failures(tenant: &str, stage: Stage) -> f64 {
        minga_telemetry::METRICS
            .scheduler_tenant_failures_total
            .with_label_values(&[tenant, stage.as_str()])
            .get()
    }

    #[test]
    fn a_tenant_is_counted_once_per_pass_with_its_first_error() {
        let mut isolation = Isolation::default();
        isolation.fail("unit-a", Stage::Status, SchedulerError::Bus("first".into()));
        isolation.fail("unit-a", Stage::Sweep, SchedulerError::Bus("second".into()));
        assert!(isolation.has_failed("unit-a"));
        assert!(!isolation.has_failed("unit-b"));
        drop(isolation);
        assert_eq!(failures("unit-a", Stage::Status), 1.0);
        assert_eq!(failures("unit-a", Stage::Sweep), 0.0);
        assert_eq!(failures("unit-b", Stage::Status), 0.0);
    }
}
