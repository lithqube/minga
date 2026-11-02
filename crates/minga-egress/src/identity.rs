//! Workload identity for the egress proxy.
//!
//! The node mints one random token per sandbox run and hands it to the
//! guest. The proxy maps the token back to the workload; a guest can name
//! itself in a header, but it cannot name a workload whose token it never
//! received. Tokens live only in this process's memory: the proxy and the
//! agent executor run in the same `minga worker`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Header carrying the per-sandbox token.
pub const SANDBOX_TOKEN_HEADER: &str = "x-minga-sandbox-token";

/// Live sandbox tokens, shared by the agent executor and the proxy.
#[derive(Debug, Default)]
pub struct SandboxTokens {
    tokens: Mutex<HashMap<String, String>>,
}

impl SandboxTokens {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mints a token for `workload`. It is revoked when the returned guard
    /// drops, so a token can't outlive the sandbox run it was issued for.
    pub fn issue(self: &Arc<Self>, workload: &str) -> SandboxToken {
        let token: String = rand::random::<[u8; 32]>()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        self.tokens
            .lock()
            .unwrap()
            .insert(token.clone(), workload.to_string());
        SandboxToken {
            tokens: Arc::clone(self),
            token,
        }
    }

    /// The workload a live token was issued to.
    pub fn workload(&self, token: &str) -> Option<String> {
        self.tokens.lock().unwrap().get(token).cloned()
    }
}

/// A live token; revoked on drop.
#[derive(Debug)]
pub struct SandboxToken {
    tokens: Arc<SandboxTokens>,
    token: String,
}

impl SandboxToken {
    pub fn as_str(&self) -> &str {
        &self.token
    }
}

impl Drop for SandboxToken {
    fn drop(&mut self) {
        self.tokens.tokens.lock().unwrap().remove(&self.token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_maps_to_workload_until_dropped() {
        let tokens = Arc::new(SandboxTokens::new());
        let a = tokens.issue("task-a");
        let b = tokens.issue("task-b");
        assert_ne!(a.as_str(), b.as_str());
        assert_eq!(tokens.workload(a.as_str()).as_deref(), Some("task-a"));
        let raw = a.as_str().to_string();
        drop(a);
        assert_eq!(tokens.workload(&raw), None);
        assert_eq!(tokens.workload(b.as_str()).as_deref(), Some("task-b"));
    }
}
