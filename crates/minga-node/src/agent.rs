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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use minga_sandbox::{SandboxResult, SandboxRuntime, SandboxSpec};
use minga_state::types::{AgentGrant, AgentSpec, TaskId};
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
    /// check completion. `tenant_id` and `service_id` come from the
    /// assignment and attribute the run's egress usage.
    async fn start(
        &self,
        task_id: &TaskId,
        tenant_id: &str,
        service_id: &str,
        spec: &AgentSpec,
    ) -> Result<String, AgentError>;

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
    async fn start(
        &self,
        task_id: &TaskId,
        _tenant_id: &str,
        _service_id: &str,
        spec: &AgentSpec,
    ) -> Result<String, AgentError> {
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
/// 2. Creates a sandbox and runs the agent with the prompt as input
/// 3. Stores the result for later retrieval via `result()`
///
/// The runtime and its compiled-module cache are shared for this node.
pub struct WasmtimeAgentExecutor {
    module_dir: std::path::PathBuf,
    runtime: minga_sandbox::WasmtimeRuntime,
    results: Mutex<HashMap<String, AgentRunResult>>,
    statuses: Mutex<HashMap<String, AgentStatus>>,
    egress_tokens: Option<Arc<minga_egress::SandboxTokens>>,
    egress_budgets: Option<Arc<minga_egress::BudgetTracker>>,
    allowed_dir_roots: Vec<PathBuf>,
    egress_enabled: bool,
}

/// One run's egress token and budget; both end when the run does.
struct EgressRun {
    token: minga_egress::SandboxToken,
    workload: String,
    budgets: Option<Arc<minga_egress::BudgetTracker>>,
}

impl Drop for EgressRun {
    fn drop(&mut self) {
        if let Some(budgets) = &self.budgets {
            budgets.unregister(&self.workload);
        }
    }
}

impl WasmtimeAgentExecutor {
    /// Creates a new wasmtime agent executor.
    ///
    /// `module_dir` is the base directory for resolving relative module paths.
    pub async fn new(module_dir: impl AsRef<Path>) -> Result<Self, AgentError> {
        let runtime = minga_sandbox::WasmtimeRuntime::new()
            .map_err(|e| AgentError::Other(format!("cannot create wasmtime runtime: {e}")))?;

        Ok(Self {
            module_dir: module_dir.as_ref().to_path_buf(),
            runtime,
            results: Mutex::new(HashMap::new()),
            statuses: Mutex::new(HashMap::new()),
            egress_tokens: None,
            egress_budgets: None,
            allowed_dir_roots: Vec::new(),
            egress_enabled: false,
        })
    }

    /// Mints a per-run egress token from `tokens` for every agent this
    /// executor starts, so the co-located proxy knows which task is calling.
    pub fn with_egress_tokens(mut self, tokens: Arc<minga_egress::SandboxTokens>) -> Self {
        self.egress_tokens = Some(tokens);
        self
    }

    /// Registers each `http` run's token budget in the proxy's tracker.
    pub fn with_egress_budgets(mut self, budgets: Arc<minga_egress::BudgetTracker>) -> Self {
        self.egress_budgets = Some(budgets);
        self
    }

    /// Restricts directory grants to `<root>/<tenant_id>` under each
    /// canonicalized node operator root, so a tenant never reaches another
    /// tenant's directories.
    pub fn with_agent_dir_roots<I, P>(
        mut self,
        roots: I,
        tenant_id: &str,
    ) -> Result<Self, AgentError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        // A valid tenant ID is one plain path component.
        minga_state::types::Tenant::validate_id(tenant_id)
            .map_err(|e| AgentError::Other(format!("invalid tenant `{tenant_id}`: {e}")))?;
        for root in roots {
            let path = root.as_ref();
            let canonical = path.canonicalize().map_err(|e| {
                AgentError::Other(format!(
                    "cannot resolve agent directory root `{}`: {e}",
                    path.display()
                ))
            })?;
            if !canonical.is_dir() {
                return Err(AgentError::Other(format!(
                    "agent directory root `{}` is not a directory",
                    path.display()
                )));
            }
            // Not canonicalized: a symlinked tenant directory resolves
            // elsewhere, so its grants fail the prefix check (fail closed).
            self.allowed_dir_roots.push(canonical.join(tenant_id));
        }
        Ok(self)
    }

    /// Mints the token and budget of an `http` run, attributed to the
    /// assignment's tenant and service.
    fn egress_run(
        &self,
        id: &str,
        tenant_id: &str,
        service_id: &str,
        spec: &AgentSpec,
    ) -> Option<EgressRun> {
        let tokens = self.egress_tokens.as_ref()?;
        if !spec.capabilities.grants.contains(&AgentGrant::Http) {
            return None;
        }
        if let Some(budgets) = &self.egress_budgets {
            // Per-run budget; no `token_budget` means no token limit.
            let limit = spec.token_budget.unwrap_or(u64::MAX);
            budgets.register(
                id,
                minga_egress::Budget::new(limit, Duration::from_secs(24 * 3600)),
            );
        }
        let token = tokens.issue_identity(minga_egress::WorkloadIdentity {
            workload_id: id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            service_id: service_id.to_owned(),
            policy: None,
        });
        Some(EgressRun {
            token,
            workload: id.to_owned(),
            budgets: self.egress_budgets.clone(),
        })
    }

    /// Indicates whether this node has an outbound egress proxy configured.
    pub fn with_egress_enabled(mut self, enabled: bool) -> Self {
        self.egress_enabled = enabled;
        self
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

fn validate_agent_capabilities(
    spec: &AgentSpec,
    allowed_dir_roots: &[PathBuf],
    egress_enabled: bool,
) -> Result<(), AgentError> {
    if !spec.sandbox_env.is_empty() && !spec.capabilities.grants.contains(&AgentGrant::Env) {
        return Err(AgentError::Start(
            "agent env requires the `env` capability grant".into(),
        ));
    }
    if spec.capabilities.grants.contains(&AgentGrant::Http) && !egress_enabled {
        return Err(AgentError::Start(
            "agent requested `http` but this node has no `--egress-bind`".into(),
        ));
    }

    spec.capabilities
        .validate_dir_paths()
        .map_err(AgentError::Start)?;
    for dir in &spec.capabilities.dirs {
        let host = Path::new(&dir.host);
        let canonical_host = host.canonicalize().map_err(|e| {
            AgentError::Start(format!(
                "cannot resolve agent directory `{}`: {e}",
                dir.host
            ))
        })?;
        if !canonical_host.is_dir() {
            return Err(AgentError::Start(format!(
                "agent directory `{}` is not a directory",
                dir.host
            )));
        }
        if !allowed_dir_roots
            .iter()
            .any(|root| canonical_host.starts_with(root))
        {
            return Err(AgentError::Start(format!(
                "agent directory `{}` is outside configured `--agent-dir-root` paths",
                dir.host
            )));
        }
    }
    Ok(())
}

/// The declared env map, plus the egress URL only with the `http` grant.
fn sandbox_env(spec: &AgentSpec) -> HashMap<String, String> {
    let mut env = spec.sandbox_env.clone();
    if let Some(url) = spec
        .egress_url
        .as_ref()
        .filter(|_| spec.capabilities.grants.contains(&AgentGrant::Http))
    {
        env.insert("MINGA_EGRESS_URL".to_string(), url.clone());
    }
    env
}

#[async_trait]
impl AgentExecutor for WasmtimeAgentExecutor {
    async fn start(
        &self,
        task_id: &TaskId,
        tenant_id: &str,
        service_id: &str,
        spec: &AgentSpec,
    ) -> Result<String, AgentError> {
        let id = task_id.clone();

        // Read the WASM module from the filesystem.
        validate_agent_capabilities(spec, &self.allowed_dir_roots, self.egress_enabled)?;
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
            env: sandbox_env(spec),
            ..Default::default()
        };

        // Only an `http` grant gets a proxy token. Revoked when `start`
        // returns, i.e. when the run ends.
        let _egress_run = self
            .egress_run(&id, tenant_id, service_id, spec)
            .inspect(|run| {
                sandbox_spec.env.insert(
                    "MINGA_EGRESS_TOKEN".to_string(),
                    run.token.as_str().to_string(),
                );
            });

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

        let sandbox_id = self
            .runtime
            .create(&sandbox_spec)
            .await
            .map_err(|e| AgentError::Sandbox(e.to_string()))?;

        let run_result = self
            .runtime
            .run(&sandbox_id, &input)
            .await
            .map_err(|e| AgentError::Sandbox(e.to_string()));

        let _ = self.runtime.destroy(&sandbox_id).await;

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
    use minga_state::types::AgentDirGrant;
    use std::collections::BTreeSet;

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
            capabilities: minga_state::types::AgentCapabilities {
                grants: BTreeSet::from([AgentGrant::Env]),
                ..Default::default()
            },
        }
    }

    #[test]
    fn capability_validation_requires_env_grant_and_node_egress() {
        let mut spec = AgentSpec::default();
        spec.sandbox_env.insert("MODEL".into(), "local".into());
        assert!(validate_agent_capabilities(&spec, &[], false)
            .unwrap_err()
            .to_string()
            .contains("env"));

        spec.capabilities.grants.insert(AgentGrant::Env);
        assert!(validate_agent_capabilities(&spec, &[], false).is_ok());
        spec.capabilities.grants.insert(AgentGrant::Http);
        assert!(validate_agent_capabilities(&spec, &[], false)
            .unwrap_err()
            .to_string()
            .contains("--egress-bind"));
        assert!(validate_agent_capabilities(&spec, &[], true).is_ok());
    }

    #[test]
    fn directory_grants_require_a_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("allowed");
        let data = root.join("datasets");
        std::fs::create_dir_all(&data).unwrap();

        let mut spec = AgentSpec::default();
        spec.capabilities.dirs.push(AgentDirGrant {
            host: data.display().to_string(),
            guest: "/data".into(),
        });
        let canonical_root = root.canonicalize().unwrap();
        assert!(
            validate_agent_capabilities(&spec, std::slice::from_ref(&canonical_root), false)
                .is_ok()
        );
        assert!(validate_agent_capabilities(&spec, &[], false)
            .unwrap_err()
            .to_string()
            .contains("--agent-dir-root"));
    }

    #[test]
    fn egress_url_reaches_the_sandbox_only_with_the_http_grant() {
        let mut spec = test_agent_spec("a.wasm");
        assert!(!sandbox_env(&spec).contains_key("MINGA_EGRESS_URL"));
        spec.capabilities.grants.insert(AgentGrant::Http);
        assert_eq!(
            sandbox_env(&spec)
                .get("MINGA_EGRESS_URL")
                .map(String::as_str),
            Some("https://api.openai.com")
        );
    }

    #[tokio::test]
    async fn directory_grants_are_scoped_to_the_node_tenant() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("agents");
        for tenant in ["tenant-a", "tenant-b"] {
            std::fs::create_dir_all(root.join(tenant).join("data")).unwrap();
        }
        let dir_spec = |tenant: &str| {
            let mut spec = AgentSpec::default();
            spec.capabilities.dirs.push(AgentDirGrant {
                host: root.join(tenant).join("data").display().to_string(),
                guest: "/data".into(),
            });
            spec
        };
        for (own, other) in [("tenant-a", "tenant-b"), ("tenant-b", "tenant-a")] {
            let exec = WasmtimeAgentExecutor::new(temp.path())
                .await
                .unwrap()
                .with_agent_dir_roots([&root], own)
                .unwrap();
            assert!(
                validate_agent_capabilities(&dir_spec(own), &exec.allowed_dir_roots, false).is_ok()
            );
            let error =
                validate_agent_capabilities(&dir_spec(other), &exec.allowed_dir_roots, false)
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("outside configured"), "{error}");
        }
        // The bare root, shared by every tenant, is not grantable either.
        let mut spec = AgentSpec::default();
        spec.capabilities.dirs.push(AgentDirGrant {
            host: root.display().to_string(),
            guest: "/data".into(),
        });
        let exec = WasmtimeAgentExecutor::new(temp.path())
            .await
            .unwrap()
            .with_agent_dir_roots([&root], "tenant-a")
            .unwrap();
        assert!(validate_agent_capabilities(&spec, &exec.allowed_dir_roots, false).is_err());
        assert!(WasmtimeAgentExecutor::new(temp.path())
            .await
            .unwrap()
            .with_agent_dir_roots([&root], "../tenant-a")
            .is_err());
    }

    #[tokio::test]
    async fn a_migrated_v8_agent_with_env_still_starts() {
        // State migration decodes each stored record and re-encodes it; do
        // the same with the frozen v8 task record.
        let v8 = include_str!("../../minga-state/tests/fixtures/v8/task.json");
        let task: minga_state::types::Task = minga_state::codec::decode(v8.as_bytes()).unwrap();
        let task: minga_state::types::Task =
            minga_state::codec::decode(&minga_state::codec::encode(&task).unwrap()).unwrap();
        let minga_state::types::WorkloadKind::Agent(spec) = task.spec.kind else {
            panic!("the v8 task fixture is an agent task");
        };
        assert!(!spec.sandbox_env.is_empty(), "the fixture must carry env");

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("start.wat"),
            r#"(module (memory (export "memory") 1) (func (export "_start")))"#,
        )
        .unwrap();
        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();
        let spec = AgentSpec {
            module_ref: "start.wat".into(),
            ..*spec
        };
        let id = exec
            .start(&"task-legacy".to_string(), "default", "svc", &spec)
            .await
            .unwrap();
        assert_eq!(exec.status(&id).await, Some(AgentStatus::Completed));
    }

    #[cfg(unix)]
    #[test]
    fn directory_grant_symlink_cannot_escape_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("allowed");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();

        let mut spec = AgentSpec::default();
        spec.capabilities.dirs.push(AgentDirGrant {
            host: root.join("escape").display().to_string(),
            guest: "/escape".into(),
        });
        let error = validate_agent_capabilities(&spec, &[root.canonicalize().unwrap()], false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("outside configured"), "{error}");
    }

    // --- MockAgentExecutor tests -----------------------------------------------

    #[tokio::test]
    async fn mock_start_and_status() {
        let exec = MockAgentExecutor::new();
        let id = exec
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./agent.wasm"),
            )
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
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./agent.wasm"),
            )
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
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./agent.wasm"),
            )
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
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./agent.wasm"),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn mock_list() {
        let exec = MockAgentExecutor::new();
        exec.start(
            &"t1".to_string(),
            "default",
            "svc",
            &test_agent_spec("a.wasm"),
        )
        .await
        .unwrap();
        exec.start(
            &"t2".to_string(),
            "default",
            "svc",
            &test_agent_spec("b.wasm"),
        )
        .await
        .unwrap();
        let agents = exec.list().await;
        assert_eq!(agents.len(), 2);
    }

    #[tokio::test]
    async fn mock_result_none_for_running() {
        let exec = MockAgentExecutor::new();
        let id = exec
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./agent.wasm"),
            )
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

        let id = exec
            .start(&"task-echo".to_string(), "default", "svc", &spec)
            .await
            .unwrap();

        assert_eq!(exec.status(&id).await, Some(AgentStatus::Completed));

        let result = exec.result(&id).await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.fuel_used > 0);
        assert_eq!(result.stdout, "hello");
    }

    #[tokio::test]
    async fn http_run_token_carries_the_assignment_tenant_and_budget() {
        let dir = tempfile::tempdir().unwrap();
        let tokens = Arc::new(minga_egress::SandboxTokens::new());
        let budgets = Arc::new(minga_egress::BudgetTracker::new());
        let exec = WasmtimeAgentExecutor::new(dir.path())
            .await
            .unwrap()
            .with_egress_tokens(Arc::clone(&tokens))
            .with_egress_budgets(Arc::clone(&budgets));
        let mut spec = test_agent_spec("./a.wasm");
        spec.token_budget = Some(500);
        assert!(exec.egress_run("t", "acme", "svc", &spec).is_none());
        spec.capabilities.grants.insert(AgentGrant::Http);
        let run = exec.egress_run("t", "acme", "acme.svc", &spec).unwrap();
        let identity = tokens.identity(run.token.as_str()).unwrap();
        assert_eq!(identity.tenant_id, "acme");
        assert_eq!(identity.service_id, "acme.svc");
        assert_eq!(budgets.status("t").unwrap().max_tokens, 500);
        drop(run);
        assert!(budgets.status("t").is_none());
    }

    #[tokio::test]
    async fn wasmtime_exec_missing_module() {
        let dir = tempfile::tempdir().unwrap();
        let exec = WasmtimeAgentExecutor::new(dir.path()).await.unwrap();

        let spec = test_agent_spec("./nonexistent.wasm");
        let result = exec
            .start(&"task-missing".to_string(), "default", "svc", &spec)
            .await;
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
            .start(
                &"task-1".to_string(),
                "default",
                "svc",
                &test_agent_spec("./a.wasm"),
            )
            .await
            .unwrap();
        assert_eq!(exec.status(&id).await, Some(AgentStatus::Running));
    }
}
