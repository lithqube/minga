//! Agent executor: runs WASM sandbox workloads on this node.
//!
//! The [`AgentExecutor`] trait abstracts over the sandbox execution backend.
//! The supervisor talks to agent workloads exclusively through this trait,
//! parallel to how it talks to containers through [`crate::Runtime`].
//!
//! Two implementations:
//! - [`MockAgentExecutor`] — in-memory simulation for testing
//! - [`WasmtimeAgentExecutor`] — real WASM execution via `minga-sandbox`

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use minga_sandbox::{SandboxResult, SandboxRuntime, SandboxSpec};
use minga_state::types::{AgentSpec, TaskId};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("agent `{0}` not found")]
    NotFound(String),
    #[error("failed to start agent: {0}")]
    Start(String),
    #[error("failed to stop agent: {0}")]
    Stop(String),
    #[error("failed to read module: {0}")]
    ModuleRead(String),
    #[error("sandbox error: {0}")]
    Sandbox(String),
    #[error("executor error: {0}")]
    Other(String),
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Status of a running (or stopped) agent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AgentStatus {
    #[default]
    Running,
    Completed,
    Failed,
}

/// Information about an agent known to the executor.
#[derive(Debug, Clone)]
pub struct AgentInfo {
    pub id: String,
    pub task_id: TaskId,
    pub status: AgentStatus,
    /// Fuel consumed by the last run (if available).
    pub fuel_used: Option<u64>,
    /// Exit code of the last run (if available).
    pub exit_code: Option<i32>,
}

/// Result of an agent run.
#[derive(Debug, Clone)]
pub struct AgentRunResult {
    pub exit_code: i32,
    pub fuel_used: u64,
    pub stdout: String,
    pub stderr: String,
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// An agent executor. Implementations must be thread-safe.
///
/// The supervisor calls [`AgentExecutor::start`] when an agent task is
/// assigned to this node, and [`AgentExecutor::stop`] when the task is
/// removed. Each agent runs to completion — there is no long-running
/// daemon; the executor manages the sandbox lifecycle internally.
#[async_trait]
pub trait AgentExecutor: Send + Sync {
    /// Starts an agent for the given task. The agent spec contains the
    /// module reference, resource limits, and prompt.
    ///
    /// Returns the agent ID (same as task_id for simplicity).
    /// The agent runs asynchronously; use [`AgentExecutor::status`] to
    /// check completion.
    async fn start(&self, task_id: &TaskId, spec: &AgentSpec) -> Result<String, AgentError>;

    /// Stops and cleans up an agent. Safe to call even if the agent
    /// has already completed.
    async fn stop(&self, agent_id: &str) -> Result<(), AgentError>;

    /// Returns the status of an agent, or `None` if not found.
    async fn status(&self, agent_id: &str) -> Option<AgentStatus>;

    /// Returns the result of a completed agent run, or `None` if the
    /// agent hasn't completed yet or doesn't exist.
    async fn result(&self, agent_id: &str) -> Option<AgentRunResult>;

    /// Lists all agents known to this executor.
    async fn list(&self) -> Vec<AgentInfo>;
}

// ---------------------------------------------------------------------------
// MockAgentExecutor
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MockAgent {
    status: AgentStatus,
    result: Option<AgentRunResult>,
}

/// An in-memory mock agent executor for testing.
#[derive(Debug, Default)]
pub struct MockAgentExecutor {
    agents: Mutex<HashMap<String, MockAgent>>,
    /// If true, `start()` returns an error.
    pub fail_starts: Mutex<bool>,
    /// If true, agents immediately complete with exit code 0.
    pub auto_complete: Mutex<bool>,
}

impl MockAgentExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of currently tracked agents.
    pub fn agent_count(&self) -> usize {
        self.agents.lock().unwrap().len()
    }
}

#[async_trait]
impl AgentExecutor for MockAgentExecutor {
    async fn start(&self, task_id: &TaskId, spec: &AgentSpec) -> Result<String, AgentError> {
        if *self.fail_starts.lock().unwrap() {
            return Err(AgentError::Start("mock executor: starts disabled".into()));
        }

        let id = task_id.clone();
        let status = if *self.auto_complete.lock().unwrap() {
            AgentStatus::Completed
        } else {
            AgentStatus::Running
        };

        let result = if *self.auto_complete.lock().unwrap() {
            Some(AgentRunResult {
                exit_code: 0,
                fuel_used: 100,
                stdout: format!("mock output for {id}"),
                stderr: String::new(),
                duration: Duration::from_millis(10),
            })
        } else {
            None
        };

        self.agents
            .lock()
            .unwrap()
            .insert(id.clone(), MockAgent { status, result });

        tracing::info!(task_id = %task_id, module = %spec.module_ref, "mock agent executor: started agent");
        Ok(id)
    }

    async fn stop(&self, agent_id: &str) -> Result<(), AgentError> {
        let mut agents = self.agents.lock().unwrap();
        match agents.get_mut(agent_id) {
            Some(a) => {
                a.status = AgentStatus::Completed;
                agents.remove(agent_id);
                Ok(())
            }
            None => Err(AgentError::NotFound(agent_id.to_string())),
        }
    }

    async fn status(&self, agent_id: &str) -> Option<AgentStatus> {
        self.agents.lock().unwrap().get(agent_id).map(|a| a.status)
    }

    async fn result(&self, agent_id: &str) -> Option<AgentRunResult> {
        self.agents
            .lock()
            .unwrap()
            .get(agent_id)
            .and_then(|a| a.result.clone())
    }

    async fn list(&self) -> Vec<AgentInfo> {
        self.agents
            .lock()
            .unwrap()
            .iter()
            .map(|(id, a)| AgentInfo {
                id: id.clone(),
                task_id: id.clone(),
                status: a.status,
                fuel_used: a.result.as_ref().map(|r| r.fuel_used),
                exit_code: a.result.as_ref().map(|r| r.exit_code),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// WasmtimeAgentExecutor
// ---------------------------------------------------------------------------

/// An agent executor backed by wasmtime.
///
/// Each `start()` call:
/// 1. Reads the WASM module from `spec.module_ref` (resolved relative to `module_dir`)
/// 2. Creates a wasmtime sandbox and runs the agent with the prompt as input
/// 3. Stores the result for later retrieval via `result()`
///
/// A warm pool integration (for module compilation caching) is a follow-up.
pub struct WasmtimeAgentExecutor {
    module_dir: std::path::PathBuf,
    results: Mutex<HashMap<String, AgentRunResult>>,
    statuses: Mutex<HashMap<String, AgentStatus>>,
}

impl WasmtimeAgentExecutor {
    /// Creates a new wasmtime agent executor.
    ///
    /// `module_dir` is the base directory for resolving relative module paths.
    pub async fn new(module_dir: impl AsRef<Path>) -> Result<Self, AgentError> {
        // Verify wasmtime is available.
        minga_sandbox::WasmtimeRuntime::new()
            .map_err(|e| AgentError::Other(format!("cannot create wasmtime runtime: {e}")))?;

        Ok(Self {
            module_dir: module_dir.as_ref().to_path_buf(),
            results: Mutex::new(HashMap::new()),
            statuses: Mutex::new(HashMap::new()),
        })
    }

    /// Creates a wasmtime agent executor with the current directory as module base.
    pub async fn with_defaults() -> Result<Self, AgentError> {
        Self::new(".").await
    }

    fn resolve_module_path(&self, module_ref: &str) -> std::path::PathBuf {
        let path = Path::new(module_ref);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.module_dir.join(path)
        }
    }
}

#[async_trait]
impl AgentExecutor for WasmtimeAgentExecutor {
    async fn start(&self, task_id: &TaskId, spec: &AgentSpec) -> Result<String, AgentError> {
        let id = task_id.clone();

        // Read the WASM module from the filesystem.
        let module_path = self.resolve_module_path(&spec.module_ref);
        let module_bytes = tokio::fs::read(&module_path)
            .await
            .map_err(|e| AgentError::ModuleRead(format!("{}: {e}", module_path.display())))?;

        // Build the sandbox spec from the agent spec.
        let mut sandbox_spec = SandboxSpec {
            module: module_bytes,
            fuel_limit: spec.fuel_limit,
            memory_limit: spec.memory_limit,
            timeout: spec.timeout_secs.map(Duration::from_secs),
            env: spec.sandbox_env.clone(),
            ..Default::default()
        };

        // Add egress URL to env if configured.
        if let Some(ref url) = spec.egress_url {
            sandbox_spec
                .env
                .insert("MINGA_EGRESS_URL".to_string(), url.clone());
        }

        // Prepare input: use the prompt as input data.
        let input = spec
            .prompt
            .as_ref()
            .map(|p| p.clone().into_bytes())
            .unwrap_or_default();

        // Mark as running.
        self.statuses
            .lock()
            .unwrap()
            .insert(id.clone(), AgentStatus::Running);

        // Run the sandbox. We create a fresh runtime per task for isolation.
        // The warm pool is used for module compilation caching in a future phase.
        let runtime = minga_sandbox::WasmtimeRuntime::new()
            .map_err(|e| AgentError::Sandbox(e.to_string()))?;

        let sandbox_id = runtime
            .create(&sandbox_spec)
            .await
            .map_err(|e| AgentError::Sandbox(e.to_string()))?;

        let run_result = runtime
            .run(&sandbox_id, &input)
            .await
            .map_err(|e| AgentError::Sandbox(e.to_string()));

        let _ = runtime.destroy(&sandbox_id).await;

        match run_result {
            Ok(SandboxResult {
                exit_code,
                fuel_consumed,
                stdout,
                stderr,
                duration,
                ..
            }) => {
                let status = if exit_code == 0 {
                    AgentStatus::Completed
                } else {
                    AgentStatus::Failed
                };
                let result = AgentRunResult {
                    exit_code,
                    fuel_used: fuel_consumed,
                    stdout,
                    stderr,
                    duration,
                };
                self.results.lock().unwrap().insert(id.clone(), result);
                self.statuses.lock().unwrap().insert(id.clone(), status);

                tracing::info!(
                    task_id = %task_id,
                    exit_code,
                    fuel = fuel_consumed,
                    "wasmtime agent executor: completed"
                );
            }
            Err(e) => {
                self.statuses
                    .lock()
                    .unwrap()
                    .insert(id.clone(), AgentStatus::Failed);
                tracing::error!(task_id = %task_id, error = %e, "wasmtime agent executor: failed");
                return Err(AgentError::Sandbox(e.to_string()));
            }
        }

        Ok(id)
    }

    async fn stop(&self, agent_id: &str) -> Result<(), AgentError> {
        // Agents run to completion; stop just cleans up state.
        self.statuses.lock().unwrap().remove(agent_id);
        self.results.lock().unwrap().remove(agent_id);
        Ok(())
    }

    async fn status(&self, agent_id: &str) -> Option<AgentStatus> {
        self.statuses.lock().unwrap().get(agent_id).copied()
    }

    async fn result(&self, agent_id: &str) -> Option<AgentRunResult> {
        self.results.lock().unwrap().get(agent_id).cloned()
    }

    async fn list(&self) -> Vec<AgentInfo> {
        let statuses = self.statuses.lock().unwrap();
        let results = self.results.lock().unwrap();
        statuses
            .iter()
            .map(|(id, status)| AgentInfo {
                id: id.clone(),
                task_id: id.clone(),
                status: *status,
                fuel_used: results.get(id).map(|r| r.fuel_used),
                exit_code: results.get(id).map(|r| r.exit_code),
            })
            .collect()
    }
}

/// Convenience: wrap an agent executor in `Arc` for sharing.
pub fn shared_executor<E: AgentExecutor + 'static>(executor: E) -> Arc<dyn AgentExecutor> {
    Arc::new(executor)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agent_spec(module: &str) -> AgentSpec {
        AgentSpec {
            module_ref: module.to_string(),
            fuel_limit: Some(1_000_000),
            memory_limit: Some(64 * 1024 * 1024),
            timeout_secs: Some(10),
            token_budget: Some(10_000),
            egress_url: Some("https://api.openai.com".to_string()),
            prompt: Some("hello".to_string()),
            sandbox_env: HashMap::from([("KEY".to_string(), "value".to_string())]),
        }
    }

    // --- MockAgentExecutor tests -----------------------------------------------

    #[tokio::test]
    async fn mock_start_and_status() {
        let exec = MockAgentExecutor::new();
        let id = exec
            .start(&"task-1".to_string(), &test_agent_spec("./agent.wasm"))
            .await
            .unwrap();
        assert_eq!(id, "task-1");
        assert_eq!(exec.status(&id).await, Some(AgentStatus::Running));
        assert_eq!(exec.agent_count(), 1);
    }

    #[tokio::test]
    async fn mock_auto_complete() {
        let exec = MockAgentExecutor::new();
        *exec.auto_complete.lock().unwrap() = true;

        let id = exec
            .start(&"task-1".to_string(), &test_agent_spec("./agent.wasm"))
            .await
            .unwrap();
        assert_eq!(exec.status(&id).await, Some(AgentStatus::Completed));

        let result = exec.result(&id).await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.fuel_used, 100);
        assert!(result.stdout.contains("task-1"));
    }

    #[tokio::test]
    async fn mock_stop() {
        let exec = MockAgentExecutor::new();
        let id = exec
            .start(&"task-1".to_string(), &test_agent_spec("./agent.wasm"))
            .await
            .unwrap();
        assert_eq!(exec.agent_count(), 1);

        exec.stop(&id).await.unwrap();
        assert_eq!(exec.agent_count(), 0);
        assert_eq!(exec.status(&id).await, None);
    }

    #[tokio::test]
    async fn mock_stop_missing() {
        let exec = MockAgentExecutor::new();
        assert!(exec.stop("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn mock_fail_starts() {
        let exec = MockAgentExecutor::new();
        *exec.fail_starts.lock().unwrap() = true;
        let result = exec
            .start(&"task-1".to_string(), &test_agent_spec("./agent.wasm"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn mock_list() {
        let exec = MockAgentExecutor::new();
        exec.start(&"t1".to_string(), &test_agent_spec("a.wasm"))
            .await
            .unwrap();
        exec.start(&"t2".to_string(), &test_agent_spec("b.wasm"))
            .await
            .unwrap();
        let agents = exec.list().await;
        assert_eq!(agents.len(), 2);
    }

    #[tokio::test]
    async fn mock_result_none_for_running() {
        let exec = MockAgentExecutor::new();
        let id = exec
            .start(&"task-1".to_string(), &test_agent_spec("./agent.wasm"))
            .await
            .unwrap();
        assert!(
            exec.result(&id).await.is_none(),
            "running agent should have no result yet"
        );
    }

    // --- WasmtimeAgentExecutor tests (integration with minga-sandbox) ----------

    #[tokio::test]
    async fn wasmtime_exec_echo_module() {
        // Echo module: input is at offset 1024 with length = param 0.
        // Output: write length at offset 0, copy input data to offset 4.
        let wat = r#"
            (module
                (memory (export "memory") 1)
                (func (export "run") (param $input_len i32) (result i32)
                    ;; Write output length at offset 0 (same as input length)
                    (i32.store (i32.const 0) (local.get $input_len))
                    ;; Copy input from offset 1024 to offset 4
                    (memory.copy
                        (i32.const 4)       ;; dest
                        (i32.const 1024)    ;; src
                        (local.get $input_len)
                    )
                    (i32.const 0)
                )
            )
        "#;

        let dir = tempfile::tempdir().unwrap();
        let module_path = dir.path().join("echo.wat");
        std::fs::write(&module_path, wat).unwrap();

        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();

        let mut spec = test_agent_spec(module_path.to_str().unwrap());
        spec.prompt = Some("hello".to_string());

        let id = exec.start(&"task-echo".to_string(), &spec).await.unwrap();

        assert_eq!(exec.status(&id).await, Some(AgentStatus::Completed));

        let result = exec.result(&id).await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.fuel_used > 0);
        assert_eq!(result.stdout, "hello");
    }

    #[tokio::test]
    async fn wasmtime_exec_missing_module() {
        let dir = tempfile::tempdir().unwrap();
        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();

        let spec = test_agent_spec("./nonexistent.wasm");
        let result = exec.start(&"task-missing".to_string(), &spec).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn wasmtime_exec_stop_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();

        // Insert a fake status
        exec.statuses
            .lock()
            .unwrap()
            .insert("task-1".to_string(), AgentStatus::Running);

        exec.stop("task-1").await.unwrap();
        assert_eq!(exec.status("task-1").await, None);
    }

    #[tokio::test]
    async fn wasmtime_exec_list_empty() {
        let dir = tempfile::tempdir().unwrap();
        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();
        let agents = exec.list().await;
        assert!(agents.is_empty());
    }

    // --- Trait object tests ----------------------------------------------------

    #[tokio::test]
    async fn shared_executor_works() {
        let exec = shared_executor(MockAgentExecutor::new());
        let id = exec
            .start(&"task-1".to_string(), &test_agent_spec("./a.wasm"))
            .await
            .unwrap();
        assert_eq!(exec.status(&id).await, Some(AgentStatus::Running));
    }
}
