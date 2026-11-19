//! Per-workload token budget tracking.
//!
//! Each workload (agent, service) gets a [`Budget`] with a token limit
//! and a time window. The [`BudgetTracker`] records token usage and
//! rejects requests that would exceed the budget.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from budget operations.
#[derive(Debug, Error)]
pub enum BudgetTrackerError {
    #[error("workload `{0}` not found")]
    NotFound(String),
    #[error("budget exceeded for workload `{workload}`: used {used} + requested {requested} > limit {limit}")]
    Exceeded {
        workload: String,
        used: u64,
        requested: u64,
        limit: u64,
    },
    #[error("invalid budget configuration: {0}")]
    InvalidConfig(String),
}

/// A token budget for a single workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budget {
    /// Maximum tokens allowed per window.
    pub max_tokens: u64,
    /// Window duration in seconds. Usage resets after each window.
    #[serde(rename = "window_secs")]
    pub window_secs: u64,
    /// Tokens settled in the current window; held reservations are counted
    /// separately and added on top.
    pub used: u64,
    /// When the current window started (not serialized).
    #[serde(skip, default = "Instant::now")]
    pub window_start: Instant,
    /// Tokens held by reservations that have not settled yet, kept exact
    /// (never saturated) so releasing one hold removes only that hold. They
    /// stay counted across a window reset, so a reset cannot free a balance
    /// an in-flight request is already using.
    #[serde(skip)]
    in_flight: u128,
    /// Which registration this is; reservations settle only against the
    /// registration that issued them.
    #[serde(skip)]
    generation: u64,
}

impl Budget {
    /// Creates a new budget with the given limit and window.
    pub fn new(max_tokens: u64, window: Duration) -> Self {
        Self {
            max_tokens,
            window_secs: window.as_secs(),
            used: 0,
            window_start: Instant::now(),
            in_flight: 0,
            generation: 0,
        }
    }

    fn window(&self) -> Duration {
        Duration::from_secs(self.window_secs)
    }

    /// Settled plus held tokens, exact.
    fn committed(&self) -> u128 {
        u128::from(self.used) + self.in_flight
    }

    /// Settled plus held tokens, saturated for reporting.
    fn total(&self) -> u64 {
        u64::try_from(self.committed()).unwrap_or(u64::MAX)
    }

    /// Returns the remaining tokens in the current window.
    pub fn remaining(&self) -> u64 {
        self.max_tokens.saturating_sub(self.total())
    }

    /// Returns true if the budget allows consuming `tokens` more tokens.
    pub fn allows(&self, tokens: u64) -> bool {
        // `u64::MAX` is the unlimited budget.
        self.max_tokens == u64::MAX
            || self.committed() + u128::from(tokens) <= u128::from(self.max_tokens)
    }

    /// Records token usage. Returns an error if it would exceed the budget.
    pub fn consume(&mut self, tokens: u64) -> Result<(), BudgetTrackerError> {
        if !self.allows(tokens) {
            return Err(BudgetTrackerError::Exceeded {
                workload: String::new(), // filled by tracker
                used: self.total(),
                requested: tokens,
                limit: self.max_tokens,
            });
        }
        self.used = self.used.saturating_add(tokens);
        Ok(())
    }

    /// Resets the window if it has elapsed.
    pub fn maybe_reset_window(&mut self) {
        if self.window_start.elapsed() >= self.window() {
            self.used = 0;
            self.window_start = Instant::now();
        }
    }
}

/// Status of a workload's budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetStatus {
    pub workload: String,
    pub max_tokens: u64,
    pub used: u64,
    pub remaining: u64,
    /// Seconds until the current window resets.
    pub window_reset_in: u64,
}

/// Tracks token budgets for multiple workloads.
#[derive(Debug, Default)]
pub struct BudgetTracker {
    budgets: Mutex<HashMap<String, Budget>>,
    generations: AtomicU64,
}

/// Tokens held for one in-flight request. Dropping it releases them;
/// [`Reservation::settle`] replaces them with the request's actual usage.
#[derive(Debug)]
pub struct Reservation {
    tracker: Arc<BudgetTracker>,
    workload: String,
    generation: u64,
    tokens: u64,
    settled: bool,
}

impl Reservation {
    /// Charges `actual` tokens in place of the reservation. The provider has
    /// already served them, so the charge always lands; returns `false`
    /// when it took the workload past its limit.
    pub fn settle(mut self, actual: u64) -> bool {
        self.settled = true;
        self.tracker
            .adjust(&self.workload, self.generation, self.tokens, actual)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.settled {
            self.tracker
                .adjust(&self.workload, self.generation, self.tokens, 0);
        }
    }
}

impl BudgetTracker {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Budget>> {
        self.budgets.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Holds `tokens` of the workload's budget for an in-flight request, so
    /// concurrent requests cannot spend the same balance.
    pub fn reserve(
        self: &Arc<Self>,
        workload: &str,
        tokens: u64,
    ) -> Result<Reservation, BudgetTrackerError> {
        self.reserve_with(workload, tokens, |_| tokens)
    }

    /// Reserves the whole remaining balance, and at least `floor`, for a
    /// request whose output has no cap: nothing smaller bounds it, so no
    /// concurrent request may spend that balance until this one settles. An
    /// unlimited budget has nothing to protect and reserves `floor` only.
    pub fn reserve_rest(
        self: &Arc<Self>,
        workload: &str,
        floor: u64,
    ) -> Result<Reservation, BudgetTrackerError> {
        self.reserve_with(workload, floor, |budget| {
            if budget.max_tokens == u64::MAX {
                floor
            } else {
                budget.remaining()
            }
        })
    }

    fn reserve_with(
        self: &Arc<Self>,
        workload: &str,
        floor: u64,
        amount: impl FnOnce(&Budget) -> u64,
    ) -> Result<Reservation, BudgetTrackerError> {
        let mut budgets = self.lock();
        let budget = budgets
            .get_mut(workload)
            .ok_or_else(|| BudgetTrackerError::NotFound(workload.to_string()))?;
        budget.maybe_reset_window();
        // Every request reaches the provider, so even an empty one needs a
        // token of headroom; a fully held budget admits nothing.
        let floor = floor.max(1);
        if !budget.allows(floor) {
            return Err(BudgetTrackerError::Exceeded {
                workload: workload.to_string(),
                used: budget.total(),
                requested: floor,
                limit: budget.max_tokens,
            });
        }
        let tokens = amount(budget).max(floor);
        budget.in_flight += u128::from(tokens);
        Ok(Reservation {
            tracker: Arc::clone(self),
            workload: workload.to_owned(),
            generation: budget.generation,
            tokens,
            settled: false,
        })
    }

    fn adjust(&self, workload: &str, generation: u64, reserved: u64, actual: u64) -> bool {
        let mut budgets = self.lock();
        // A re-registered key is a different budget: the old run's hold and
        // usage are not the replacement's.
        let Some(budget) = budgets
            .get_mut(workload)
            .filter(|b| b.generation == generation)
        else {
            return true;
        };
        // Resets carry in-flight reservations forward, so the current window
        // always holds this one; actual usage lands in the current window.
        budget.maybe_reset_window();
        budget.in_flight = budget.in_flight.saturating_sub(u128::from(reserved));
        budget.used = budget.used.saturating_add(actual);
        budget.allows(0)
    }

    /// Creates a new empty budget tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a budget for a workload. Overwrites any existing budget.
    pub fn register(&self, workload: &str, mut budget: Budget) {
        let max_tokens = budget.max_tokens;
        budget.generation = self.generations.fetch_add(1, Ordering::Relaxed) + 1;
        budget.in_flight = 0;
        self.lock().insert(workload.to_string(), budget);
        tracing::info!(workload, max_tokens, "budget registered");
    }

    /// Removes a workload's budget.
    pub fn unregister(&self, workload: &str) -> bool {
        self.lock().remove(workload).is_some()
    }

    /// Checks if a workload can consume `tokens` tokens without recording.
    pub fn check(&self, workload: &str, tokens: u64) -> Result<(), BudgetTrackerError> {
        let mut budgets = self.lock();
        let budget = budgets
            .get_mut(workload)
            .ok_or_else(|| BudgetTrackerError::NotFound(workload.to_string()))?;
        budget.maybe_reset_window();
        if !budget.allows(tokens) {
            return Err(BudgetTrackerError::Exceeded {
                workload: workload.to_string(),
                used: budget.total(),
                requested: tokens,
                limit: budget.max_tokens,
            });
        }
        Ok(())
    }

    /// Records token usage for a workload. Returns an error if it would
    /// exceed the budget.
    pub fn record(&self, workload: &str, tokens: u64) -> Result<(), BudgetTrackerError> {
        let mut budgets = self.lock();
        let budget = budgets
            .get_mut(workload)
            .ok_or_else(|| BudgetTrackerError::NotFound(workload.to_string()))?;
        budget.maybe_reset_window();
        if !budget.allows(tokens) {
            return Err(BudgetTrackerError::Exceeded {
                workload: workload.to_string(),
                used: budget.total(),
                requested: tokens,
                limit: budget.max_tokens,
            });
        }
        budget.used = budget.used.saturating_add(tokens);
        tracing::debug!(workload, tokens, used = budget.total(), "tokens recorded");
        Ok(())
    }

    /// Returns the budget status for a workload.
    pub fn status(&self, workload: &str) -> Option<BudgetStatus> {
        let mut budgets = self.lock();
        budgets.get_mut(workload).map(|b| {
            b.maybe_reset_window();
            let elapsed = b.window_start.elapsed();
            let reset_in = b.window().saturating_sub(elapsed);
            BudgetStatus {
                workload: workload.to_string(),
                max_tokens: b.max_tokens,
                used: b.total(),
                remaining: b.remaining(),
                window_reset_in: reset_in.as_secs(),
            }
        })
    }

    /// Returns the status of all registered workloads.
    pub fn list(&self) -> Vec<BudgetStatus> {
        let mut budgets = self.lock();
        budgets
            .iter_mut()
            .map(|(workload, b)| {
                b.maybe_reset_window();
                let elapsed = b.window_start.elapsed();
                let reset_in = b.window().saturating_sub(elapsed);
                BudgetStatus {
                    workload: workload.clone(),
                    max_tokens: b.max_tokens,
                    used: b.total(),
                    remaining: b.remaining(),
                    window_reset_in: reset_in.as_secs(),
                }
            })
            .collect()
    }

    /// Returns the number of registered workloads.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Returns true if no workloads are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_new() {
        let b = Budget::new(1000, Duration::from_secs(60));
        assert_eq!(b.max_tokens, 1000);
        assert_eq!(b.used, 0);
        assert_eq!(b.remaining(), 1000);
    }

    #[test]
    fn budget_consume() {
        let mut b = Budget::new(100, Duration::from_secs(60));
        assert!(b.consume(30).is_ok());
        assert_eq!(b.used, 30);
        assert_eq!(b.remaining(), 70);
    }

    #[test]
    fn budget_consume_exceeds() {
        let mut b = Budget::new(100, Duration::from_secs(60));
        b.consume(80).unwrap();
        let err = b.consume(30).unwrap_err();
        assert!(matches!(err, BudgetTrackerError::Exceeded { .. }));
        assert_eq!(b.used, 80); // unchanged
    }

    #[test]
    fn budget_allows() {
        let b = Budget::new(100, Duration::from_secs(60));
        assert!(b.allows(100));
        assert!(!b.allows(101));
    }

    #[test]
    fn budget_window_reset() {
        let mut b = Budget::new(100, Duration::from_millis(50));
        b.consume(100).unwrap();
        assert_eq!(b.remaining(), 0);
        std::thread::sleep(Duration::from_millis(60));
        b.maybe_reset_window();
        assert_eq!(b.used, 0);
        assert_eq!(b.remaining(), 100);
    }

    #[test]
    fn tracker_register_and_check() {
        let tracker = BudgetTracker::new();
        tracker.register("agent-1", Budget::new(1000, Duration::from_secs(60)));
        assert!(tracker.check("agent-1", 500).is_ok());
        assert!(tracker.check("agent-1", 1001).is_err());
    }

    #[test]
    fn tracker_record() {
        let tracker = BudgetTracker::new();
        tracker.register("agent-1", Budget::new(1000, Duration::from_secs(60)));
        tracker.record("agent-1", 300).unwrap();
        tracker.record("agent-1", 300).unwrap();
        assert!(tracker.record("agent-1", 500).is_err()); // would exceed
        let status = tracker.status("agent-1").unwrap();
        assert_eq!(status.used, 600);
        assert_eq!(status.remaining, 400);
    }

    #[test]
    fn a_window_reset_keeps_in_flight_reservations() {
        let tracker = Arc::new(BudgetTracker::new());
        tracker.register("w", Budget::new(100, Duration::from_secs(3600)));
        let held = tracker.reserve("w", 80).unwrap();
        // Roll the window over exactly once, without sleeping.
        tracker.lock().get_mut("w").unwrap().window_start = Instant::now()
            .checked_sub(Duration::from_secs(3601))
            .unwrap();
        // The window rolled over, but the 80 in flight still count.
        assert!(tracker.reserve("w", 30).is_err());
        assert!(tracker.reserve("w", 20).is_ok_and(|r| r.settle(0)));
        assert!(held.settle(10));
        assert_eq!(tracker.status("w").unwrap().used, 10);
    }

    #[test]
    fn a_reservation_settles_only_against_the_registration_that_issued_it() {
        let tracker = Arc::new(BudgetTracker::new());
        tracker.register("run", Budget::new(100, Duration::from_secs(60)));
        let old_settled = tracker.reserve("run", 60).unwrap();
        let old_dropped = tracker.reserve("run", 30).unwrap();
        // The same key is registered again while the old run still streams.
        tracker.register("run", Budget::new(100, Duration::from_secs(60)));
        let fresh = tracker.reserve("run", 50).unwrap();
        assert!(old_settled.settle(40));
        drop(old_dropped);
        let status = tracker.status("run").unwrap();
        assert_eq!(status.used, 50, "only the replacement's hold counts");
        assert!(fresh.settle(20));
        assert_eq!(tracker.status("run").unwrap().used, 20);
    }

    #[test]
    fn releasing_a_saturating_hold_keeps_the_others() {
        let tracker = Arc::new(BudgetTracker::new());
        tracker.register("unlimited", Budget::new(u64::MAX, Duration::from_secs(60)));
        let small = tracker.reserve("unlimited", 100).unwrap();
        drop(tracker.reserve("unlimited", u64::MAX).unwrap());
        assert_eq!(tracker.status("unlimited").unwrap().used, 100);
        assert!(small.settle(7));
        assert_eq!(tracker.status("unlimited").unwrap().used, 7);
    }

    #[test]
    fn consume_saturates_on_an_unlimited_budget() {
        let mut b = Budget::new(u64::MAX, Duration::from_secs(60));
        b.consume(u64::MAX - 1).unwrap();
        assert!(b.consume(2).is_ok());
        assert_eq!(b.used, u64::MAX);
    }

    #[test]
    fn an_uncapped_reservation_holds_the_rest_of_a_limited_budget() {
        let tracker = Arc::new(BudgetTracker::new());
        tracker.register("limited", Budget::new(100, Duration::from_secs(60)));
        let held = tracker.reserve_rest("limited", 10).unwrap();
        assert!(tracker.reserve("limited", 1).is_err());
        // An empty body bounds its input at zero; it must still be refused.
        assert!(tracker.reserve_rest("limited", 0).is_err());
        assert!(tracker.reserve("limited", 0).is_err());
        assert!(held.settle(30));
        assert_eq!(tracker.status("limited").unwrap().used, 30);
        // Unlimited budgets reserve only the floor, so uncapped requests overlap.
        tracker.register("unlimited", Budget::new(u64::MAX, Duration::from_secs(60)));
        let _first = tracker.reserve_rest("unlimited", 10).unwrap();
        assert!(tracker.reserve_rest("unlimited", 10).is_ok());
    }

    #[test]
    fn reservations_hold_balance_until_settled_or_dropped() {
        let tracker = Arc::new(BudgetTracker::new());
        tracker.register("a", Budget::new(100, Duration::from_secs(60)));
        let first = tracker.reserve("a", 60).unwrap();
        assert!(
            tracker.reserve("a", 60).is_err(),
            "held balance is not reusable"
        );
        drop(first);
        let second = tracker.reserve("a", 60).unwrap();
        assert!(second.settle(30));
        assert_eq!(tracker.status("a").unwrap().used, 30);
        let third = tracker.reserve("a", 70).unwrap();
        assert!(!third.settle(90), "actual usage past the limit is charged");
        assert_eq!(tracker.status("a").unwrap().used, 120);

        tracker.register("unlimited", Budget::new(u64::MAX, Duration::from_secs(60)));
        assert!(tracker.reserve("unlimited", u64::MAX).unwrap().settle(5));
        assert!(
            tracker.check("unlimited", u64::MAX - 5).is_ok(),
            "no overflow"
        );
    }

    #[test]
    fn tracker_not_found() {
        let tracker = BudgetTracker::new();
        assert!(tracker.check("nonexistent", 1).is_err());
        assert!(tracker.record("nonexistent", 1).is_err());
        assert!(tracker.status("nonexistent").is_none());
    }

    #[test]
    fn tracker_unregister() {
        let tracker = BudgetTracker::new();
        tracker.register("agent-1", Budget::new(1000, Duration::from_secs(60)));
        assert!(tracker.unregister("agent-1"));
        assert!(!tracker.unregister("agent-1"));
        assert!(tracker.is_empty());
    }

    #[test]
    fn tracker_list() {
        let tracker = BudgetTracker::new();
        tracker.register("a", Budget::new(100, Duration::from_secs(60)));
        tracker.register("b", Budget::new(200, Duration::from_secs(60)));
        let statuses = tracker.list();
        assert_eq!(statuses.len(), 2);
    }

    #[test]
    fn tracker_multiple_workloads_independent() {
        let tracker = BudgetTracker::new();
        tracker.register("a", Budget::new(100, Duration::from_secs(60)));
        tracker.register("b", Budget::new(100, Duration::from_secs(60)));
        tracker.record("a", 100).unwrap();
        // b should still have full budget
        assert!(tracker.check("b", 100).is_ok());
        // a should be exhausted
        assert!(tracker.check("a", 1).is_err());
    }

    #[test]
    fn tracker_window_reset_on_record() {
        let tracker = BudgetTracker::new();
        // Use a long window so it doesn't expire mid-test.
        tracker.register("agent-1", Budget::new(100, Duration::from_secs(60)));
        tracker.record("agent-1", 100).unwrap();
        // Budget is exhausted.
        assert!(tracker.record("agent-1", 1).is_err());
        // Manually reset by registering a fresh budget (simulates window expiry).
        tracker.register("agent-1", Budget::new(100, Duration::from_secs(60)));
        tracker.record("agent-1", 50).unwrap();
        let status = tracker.status("agent-1").unwrap();
        assert_eq!(status.used, 50);
    }

    #[test]
    fn budget_status_fields() {
        let tracker = BudgetTracker::new();
        tracker.register("test", Budget::new(500, Duration::from_secs(60)));
        tracker.record("test", 100).unwrap();
        let status = tracker.status("test").unwrap();
        assert_eq!(status.workload, "test");
        assert_eq!(status.max_tokens, 500);
        assert_eq!(status.used, 100);
        assert_eq!(status.remaining, 400);
        assert!(status.window_reset_in > 0);
        assert!(status.window_reset_in <= 60);
    }
}
