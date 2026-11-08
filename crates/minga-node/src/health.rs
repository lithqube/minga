//! Container healthchecks: when to probe, and what a result means.
//!
//! The supervisor runs the probes. A task with an enforced healthcheck
//! stays `Starting` (out of DNS, and not yet counted by a rollout) until
//! its first probe passes, and is killed after `retries` straight failures
//! so the restart policy takes over.

use std::time::{Duration, Instant};

use minga_state::types::Healthcheck;

// Docker's defaults.
const DEFAULT_INTERVAL_SECS: u64 = 30;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_RETRIES: u32 = 3;

/// Whether a healthcheck has a real test (`["NONE"]` disables one).
pub(crate) fn is_enforced(check: &Healthcheck) -> bool {
    !check.test.is_empty() && check.test[0] != "NONE"
}

/// The command to exec for a Compose `test`.
pub(crate) fn probe_args(test: &[String]) -> Vec<String> {
    match test.split_first() {
        Some((first, rest)) if first == "CMD" => rest.to_vec(),
        Some((first, rest)) if first == "CMD-SHELL" => {
            vec!["/bin/sh".into(), "-c".into(), rest.join(" ")]
        }
        _ => vec!["/bin/sh".into(), "-c".into(), test.join(" ")],
    }
}

/// Probe state for one running container.
#[derive(Debug)]
pub(crate) struct Health {
    pub(crate) args: Vec<String>,
    pub(crate) timeout: Duration,
    interval: Duration,
    start_period: Duration,
    retries: u32,
    started: Instant,
    next_probe: Instant,
    failures: u32,
    healthy: bool,
    newly_healthy: bool,
    probing: bool,
}

impl Health {
    pub(crate) fn new(check: &Healthcheck, now: Instant) -> Self {
        let interval = Duration::from_secs(check.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS));
        Self {
            args: probe_args(&check.test),
            timeout: Duration::from_secs(check.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS)),
            interval,
            start_period: Duration::from_secs(check.start_period_secs.unwrap_or(0)),
            retries: check.retries.unwrap_or(DEFAULT_RETRIES),
            started: now,
            next_probe: now + interval,
            failures: 0,
            healthy: false,
            newly_healthy: false,
            probing: false,
        }
    }

    /// True if a probe should start now; marks it in flight.
    pub(crate) fn begin_probe(&mut self, now: Instant) -> bool {
        if self.probing || now < self.next_probe {
            return false;
        }
        self.probing = true;
        true
    }

    /// Records a finished probe. Before the first success, failures inside
    /// the start period don't count.
    pub(crate) fn record(&mut self, passed: bool, now: Instant) {
        self.probing = false;
        self.next_probe = now + self.interval;
        if passed {
            self.failures = 0;
            self.newly_healthy = !self.healthy;
            self.healthy = true;
        } else if self.healthy || now >= self.started + self.start_period {
            self.failures += 1;
        }
    }

    /// True once, the first time the container passes a probe.
    pub(crate) fn take_newly_healthy(&mut self) -> bool {
        std::mem::take(&mut self.newly_healthy)
    }

    pub(crate) fn is_unhealthy(&self) -> bool {
        self.failures >= self.retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(start_period: u64, retries: u32) -> Healthcheck {
        Healthcheck {
            test: vec!["CMD".to_string(), "true".to_string()],
            interval_secs: Some(10),
            timeout_secs: Some(1),
            start_period_secs: Some(start_period),
            retries: Some(retries),
        }
    }

    #[test]
    fn probe_args_follow_the_compose_forms() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(probe_args(&s(&["CMD", "a", "b"])), s(&["a", "b"]));
        assert_eq!(
            probe_args(&s(&["CMD-SHELL", "a b"])),
            s(&["/bin/sh", "-c", "a b"])
        );
        assert_eq!(probe_args(&s(&["a b"])), s(&["/bin/sh", "-c", "a b"]));
    }

    #[test]
    fn none_disables_the_check() {
        let mut c = check(0, 1);
        assert!(is_enforced(&c));
        c.test = vec!["NONE".to_string()];
        assert!(!is_enforced(&c));
    }

    #[test]
    fn first_probe_waits_one_interval_and_never_overlaps() {
        let t0 = Instant::now();
        let mut h = Health::new(&check(0, 3), t0);
        assert!(!h.begin_probe(t0));
        let t1 = t0 + Duration::from_secs(10);
        assert!(h.begin_probe(t1));
        assert!(!h.begin_probe(t1 + Duration::from_secs(60)));
    }

    #[test]
    fn start_period_failures_do_not_count_until_first_success() {
        let t0 = Instant::now();
        let mut h = Health::new(&check(30, 1), t0);
        h.record(false, t0 + Duration::from_secs(10));
        assert!(!h.is_unhealthy());
        h.record(false, t0 + Duration::from_secs(30));
        assert!(h.is_unhealthy());
    }

    #[test]
    fn a_pass_reports_once_and_clears_failures() {
        let t0 = Instant::now();
        let mut h = Health::new(&check(0, 2), t0);
        h.record(false, t0);
        h.record(true, t0);
        assert!(h.take_newly_healthy());
        assert!(!h.take_newly_healthy());
        h.record(false, t0);
        assert!(!h.is_unhealthy());
        h.record(false, t0);
        assert!(h.is_unhealthy());
    }
}
