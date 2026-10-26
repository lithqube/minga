//! Firecracker microVM sandbox runtime (Linux/KVM only).
//!
//! This is a stub implementation that compiles on Linux but returns
//! `SandboxError::Other` at runtime until the full Firecracker
//! integration (via the firecracker-rust SDK or `firecracker` binary)
//! is implemented.
//!
//! The full implementation will:
//! - Create a Firecracker microVM from a kernel image + rootfs.
//! - Communicate via the Firecracker REST API (Unix socket).
//! - Run the agent workload inside the microVM.
//! - Support warm pools by snapshotting/restoring microVM state.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;

use crate::runtime::{
    SandboxError, SandboxInfo, SandboxResult, SandboxRuntime, SandboxSpec, SandboxStatus,
};

/// A sandbox runtime backed by Firecracker microVMs.
///
/// Only available on Linux with KVM (`/dev/kvm`).
#[derive(Debug, Default)]
pub struct FirecrackerRuntime {
    sandboxes: Mutex<HashMap<String, FirecrackerSandbox>>,
    counter: Mutex<u64>,
}

#[derive(Debug)]
struct FirecrackerSandbox {
    id: String,
    status: SandboxStatus,
    created_at: Instant,
}

impl FirecrackerRuntime {
    /// Creates a new Firecracker runtime.
    ///
    /// Returns an error if `/dev/kvm` is not available.
    pub fn new() -> Result<Self, SandboxError> {
        if !std::path::Path::new("/dev/kvm").exists() {
            return Err(SandboxError::Other(
                "Firecracker requires /dev/kvm (Linux with KVM)".into(),
            ));
        }
        Ok(Self::default())
    }

    /// Returns the number of currently tracked sandboxes.
    pub fn sandbox_count(&self) -> usize {
        self.sandboxes.lock().unwrap().len()
    }
}

#[async_trait]
impl SandboxRuntime for FirecrackerRuntime {
    async fn create(&self, _spec: &SandboxSpec) -> Result<String, SandboxError> {
        // TODO: implement Firecracker microVM creation.
        // - Build the Firecracker API client (Unix socket).
        // - Configure kernel image, rootfs, vCPU count, memory.
        // - Start the microVM.
        Err(SandboxError::Other(
            "Firecracker runtime is a stub — not yet implemented".into(),
        ))
    }

    async fn run(&self, sandbox_id: &str, _input: &[u8]) -> Result<SandboxResult, SandboxError> {
        Err(SandboxError::Other(format!(
            "Firecracker runtime is a stub — cannot run sandbox {sandbox_id}"
        )))
    }

    async fn destroy(&self, sandbox_id: &str) -> Result<(), SandboxError> {
        let mut sandboxes = self.sandboxes.lock().unwrap();
        match sandboxes.get_mut(sandbox_id) {
            Some(sbx) => {
                sbx.status = SandboxStatus::Destroyed;
                sandboxes.remove(sandbox_id);
                Ok(())
            }
            None => Err(SandboxError::NotFound(sandbox_id.to_string())),
        }
    }

    async fn list(&self) -> Vec<SandboxInfo> {
        self.sandboxes
            .lock()
            .unwrap()
            .values()
            .map(|s| SandboxInfo {
                id: s.id.clone(),
                status: s.status,
                fuel_consumed: None,
                age: s.created_at.elapsed(),
            })
            .collect()
    }

    async fn status(&self, sandbox_id: &str) -> Option<SandboxStatus> {
        self.sandboxes
            .lock()
            .unwrap()
            .get(sandbox_id)
            .map(|s| s.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_returns_stub_error() {
        // On macOS (no /dev/kvm), new() should fail.
        // On Linux with KVM, create() should return the stub error.
        match FirecrackerRuntime::new() {
            Ok(rt) => {
                let spec = SandboxSpec::default();
                let result = rt.create(&spec).await;
                assert!(result.is_err());
                let err = result.unwrap_err().to_string();
                assert!(err.contains("stub"));
            }
            Err(e) => {
                assert!(e.to_string().contains("/dev/kvm"));
            }
        }
    }

    #[tokio::test]
    async fn list_empty() {
        if let Ok(rt) = FirecrackerRuntime::new() {
            assert!(rt.list().await.is_empty());
            assert_eq!(rt.sandbox_count(), 0);
        }
    }
}
