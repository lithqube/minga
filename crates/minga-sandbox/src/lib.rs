//! Agent sandboxes: wasmtime and Firecracker warm pools.
//!
//! This crate provides the [`SandboxRuntime`] trait and implementations
//! for running AI-agent workloads in isolated sandboxes:
//!
//! - [`WasmtimeRuntime`] — runs WASM modules via [wasmtime](https://wasmtime.dev).
//!   Works on all platforms (macOS, Linux, Windows).
//! - [`FirecrackerRuntime`] — runs microVMs via [Firecracker](https://firecracker-microvm.github.io).
//!   Linux/KVM only.
//! - [`MockSandboxRuntime`] — in-memory mock for testing.
//! - [`WarmPool`] — manages a pool of pre-warmed sandboxes to eliminate
//!   cold-start latency.
//!
//! # Architecture
//!
//! ```text
//!  SandboxRuntime trait
//!  ├── WasmtimeRuntime  (WASM, cross-platform)
//!  ├── FirecrackerRuntime (microVM, Linux/KVM)
//!  └── MockSandboxRuntime (testing)
//!
//!  WarmPool wraps any SandboxRuntime:
//!  pre-warm N sandboxes → acquire() → run() → release() → reuse
//!  ```

mod pool;
mod runtime;
mod wasmtime_rt;

#[cfg(target_os = "linux")]
mod firecracker;

pub use pool::{PoolConfig, PoolStats, WarmPool};
pub use runtime::{
    MockSandboxRuntime, SandboxError, SandboxInfo, SandboxResult, SandboxRuntime, SandboxSpec,
    SandboxStatus,
};
pub use wasmtime_rt::WasmtimeRuntime;

#[cfg(target_os = "linux")]
pub use firecracker::FirecrackerRuntime;

use std::sync::Arc;

/// Convenience: wrap a sandbox runtime in `Arc` for sharing.
pub fn shared_runtime<R: SandboxRuntime + 'static>(runtime: R) -> Arc<dyn SandboxRuntime> {
    Arc::new(runtime)
}
