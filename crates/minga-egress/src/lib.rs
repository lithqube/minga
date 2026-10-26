//! LLM egress proxy: token and cost budgets, caching, secret redaction.
//!
//! This crate provides an HTTP reverse proxy that sits between agent
//! workloads and LLM APIs (OpenAI, Anthropic, etc.). It enforces
//! per-workload token budgets, caches responses to reduce cost and
//! latency, and redacts secrets from logs and error messages.
//!
//! # Architecture
//!
//! ```text
//!  Agent → EgressProxy → BudgetTracker (check) → Cache (lookup)
//!                                           ↓ miss
//!                                     Upstream LLM API
//!                                           ↓
//!                                     Cache (store) → Response
//! ```
//!
//! # Components
//!
//! - [`BudgetTracker`] — per-workload token budgets with time windows.
//! - [`ResponseCache`] — LRU cache for LLM API responses.
//! - [`SecretRedactor`] — regex-based secret/PII redaction.
//! - [`EgressProxy`] — axum-based reverse proxy server.

mod budget;
mod cache;
mod proxy;
mod redact;

pub use budget::{Budget, BudgetStatus, BudgetTracker, BudgetTrackerError};
pub use cache::{CacheConfig, CacheStats, ResponseCache};
pub use proxy::{EgressProxy, EgressProxyConfig, EgressProxyError};
pub use redact::{RedactionRule, SecretRedactor};
