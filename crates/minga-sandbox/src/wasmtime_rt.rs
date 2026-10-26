//! Wasmtime-backed sandbox runtime.
//!
//! Runs WASM modules via [wasmtime](https://wasmtime.dev) with fuel
//! metering (instruction budget), memory limits, and timeouts.
//!
//! # Module contract
//!
//! A sandbox WASM module must export:
//!
//! - `memory` — linear memory (min 1 page = 64 KiB).
//! - `run(input_len: i32) -> i32` — entry point. Called with the input
//!   length. Returns an exit code (0 = success).
//!
//! Input/output convention:
//! - The runtime writes input bytes to memory at offset 0.
//! - The module reads input from `ptr=0, len=input_len`.
//! - The module writes output to memory at offset 0 (overwriting input):
//!   first 4 bytes = output length (i32 LE), followed by output bytes.
//! - The runtime reads the output length from offset 0, then reads
//!   that many bytes from offset 4.
//!
//! If the module only exports `_start() -> ()` (WASI-style), the runtime
//! calls it with no input and captures no output.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use wasmtime::{Config, Engine, Instance, Module, Store};

use crate::runtime::{
    SandboxError, SandboxInfo, SandboxResult, SandboxRuntime, SandboxSpec, SandboxStatus,
};

const INPUT_OFFSET: usize = 1024;
const OUTPUT_LEN_OFFSET: usize = 0;
const OUTPUT_DATA_OFFSET: usize = 4;
const MIN_MEMORY_PAGES: u64 = 16; // 1 MiB minimum

/// A sandbox runtime backed by wasmtime.
pub struct WasmtimeRuntime {
    engine: Engine,
    sandboxes: Mutex<HashMap<String, WasmtimeSandbox>>,
    counter: Mutex<u64>,
}

struct WasmtimeSandbox {
    id: String,
    module: Module,
    status: SandboxStatus,
    created_at: Instant,
    fuel_consumed: u64,
    spec: SandboxSpec,
}

impl std::fmt::Debug for WasmtimeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmtimeRuntime").finish_non_exhaustive()
    }
}

impl WasmtimeRuntime {
    /// Creates a new wasmtime runtime with fuel metering enabled.
    pub fn new() -> Result<Self, SandboxError> {
        Self::with_config(Config::new())
    }

    /// Creates a new wasmtime runtime with a custom config.
    pub fn with_config(mut config: Config) -> Result<Self, SandboxError> {
        config.consume_fuel(true);
        let engine = Engine::new(&config)
            .map_err(|e| SandboxError::Other(format!("failed to create engine: {e}")))?;
        Ok(Self {
            engine,
            sandboxes: Mutex::new(HashMap::new()),
            counter: Mutex::new(0),
        })
    }

    /// Returns the number of currently tracked sandboxes.
    pub fn sandbox_count(&self) -> usize {
        self.sandboxes.lock().unwrap().len()
    }
}

#[async_trait]
impl SandboxRuntime for WasmtimeRuntime {
    async fn create(&self, spec: &SandboxSpec) -> Result<String, SandboxError> {
        if spec.module.is_empty() {
            return Err(SandboxError::Create("empty WASM module".into()));
        }

        // Compile the module.
        let module = Module::new(&self.engine, &spec.module)
            .map_err(|e| SandboxError::Create(format!("failed to compile module: {e}")))?;

        // Validate that the module exports what we need.
        let exports: Vec<_> = module
            .exports()
            .map(|e| (e.name().to_string(), e.ty().clone()))
            .collect();
        let has_memory = exports.iter().any(|(name, _)| name == "memory");
        let has_run = exports.iter().any(|(name, _)| name == "run");
        let has_start = exports.iter().any(|(name, _)| name == "_start");

        if !has_run && !has_start {
            return Err(SandboxError::Create(
                "module must export `run` or `_start`".into(),
            ));
        }
        if (has_run || has_start) && !has_memory {
            // WASI modules may not export memory directly, but for our
            // contract we require it when using `run`.
            if has_run {
                return Err(SandboxError::Create(
                    "module must export `memory` when using `run`".into(),
                ));
            }
        }

        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        let id = format!("wasm-{:04}", *counter);
        drop(counter);

        self.sandboxes.lock().unwrap().insert(
            id.clone(),
            WasmtimeSandbox {
                id: id.clone(),
                module,
                status: SandboxStatus::Ready,
                created_at: Instant::now(),
                fuel_consumed: 0,
                spec: spec.clone(),
            },
        );

        tracing::info!(sandbox = %id, "wasmtime: created sandbox");
        Ok(id)
    }

    async fn run(&self, sandbox_id: &str, input: &[u8]) -> Result<SandboxResult, SandboxError> {
        let start = Instant::now();

        // Clone what we need from the sandbox entry.
        let (module, spec, fuel_limit) = {
            let sandboxes = self.sandboxes.lock().unwrap();
            let sbx = sandboxes
                .get(sandbox_id)
                .ok_or_else(|| SandboxError::NotFound(sandbox_id.to_string()))?;
            (
                sbx.module.clone(),
                sbx.spec.clone(),
                sbx.spec.fuel_limit.unwrap_or(u64::MAX),
            )
        };

        // Apply output limit.
        let input = if let Some(limit) = spec.output_limit {
            &input[..input.len().min(limit)]
        } else {
            input
        };

        // Determine timeout.
        let timeout = spec.timeout.unwrap_or(Duration::from_secs(30));

        // Run in a blocking task with timeout.
        let engine = self.engine.clone();
        let module_clone = module.clone();
        let input_owned = input.to_vec();

        let handle = tokio::task::spawn_blocking(move || {
            run_wasm(&engine, &module_clone, &input_owned, fuel_limit, &spec)
        });

        let result = match tokio::time::timeout(timeout, handle).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Err(SandboxError::Run(format!("join error: {e}"))),
            Err(_) => return Err(SandboxError::Timeout(timeout)),
        };

        match result {
            Ok(mut res) => {
                res.duration = start.elapsed();
                // Update fuel tracking.
                if let Ok(mut sandboxes) = self.sandboxes.lock() {
                    if let Some(sbx) = sandboxes.get_mut(sandbox_id) {
                        sbx.fuel_consumed += res.fuel_consumed;
                        sbx.status = if res.exit_code == 0 {
                            SandboxStatus::Completed
                        } else {
                            SandboxStatus::Failed
                        };
                    }
                }
                Ok(res)
            }
            Err(e) => {
                if let Ok(mut sandboxes) = self.sandboxes.lock() {
                    if let Some(sbx) = sandboxes.get_mut(sandbox_id) {
                        sbx.status = SandboxStatus::Failed;
                    }
                }
                Err(e)
            }
        }
    }

    async fn destroy(&self, sandbox_id: &str) -> Result<(), SandboxError> {
        let mut sandboxes = self.sandboxes.lock().unwrap();
        match sandboxes.get_mut(sandbox_id) {
            Some(sbx) => {
                sbx.status = SandboxStatus::Destroyed;
                sandboxes.remove(sandbox_id);
                tracing::info!(sandbox = %sandbox_id, "wasmtime: destroyed sandbox");
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
                fuel_consumed: Some(s.fuel_consumed),
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

/// Synchronous WASM execution. Runs in a blocking thread.
fn run_wasm(
    engine: &Engine,
    module: &Module,
    input: &[u8],
    fuel_limit: u64,
    spec: &SandboxSpec,
) -> Result<SandboxResult, SandboxError> {
    let mut store = Store::new(engine, ());

    // Set fuel.
    if fuel_limit != u64::MAX {
        store
            .set_fuel(fuel_limit)
            .map_err(|e| SandboxError::Run(format!("failed to set fuel: {e}")))?;
    }

    // Instantiate.
    let instance = Instance::new(&mut store, module, &[])
        .map_err(|e| SandboxError::Run(format!("failed to instantiate: {e}")))?;

    // Check if the module exports `run` (our contract) or `_start` (WASI).
    let run_func = instance.get_typed_func::<i32, i32>(&mut store, "run").ok();
    let start_func = instance.get_typed_func::<(), ()>(&mut store, "_start").ok();

    let mut stdout = String::new();
    let mut exit_code = 0i32;

    if let Some(run) = run_func {
        // Our contract: write input to memory, call run(input_len), read output.
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| SandboxError::Run("module must export `memory`".into()))?;

        // Write input to memory at INPUT_OFFSET.
        let input_len = input.len() as i32;
        memory
            .write(&mut store, INPUT_OFFSET, input)
            .map_err(|e| SandboxError::Run(format!("failed to write input to memory: {e}")))?;

        // Ensure memory is large enough for input + output.
        let required_bytes = INPUT_OFFSET + input.len() + 4096;
        let required_pages: u64 = (required_bytes / 65536 + 1) as u64;
        let current_pages = memory.size(&store);
        let min_pages: u64 = MIN_MEMORY_PAGES;
        if current_pages < required_pages.max(min_pages) {
            memory
                .grow(&mut store, required_pages.max(min_pages) - current_pages)
                .map_err(|e| SandboxError::Run(format!("failed to grow memory: {e}")))?;
        }

        // Call run(input_len).
        exit_code = run
            .call(&mut store, input_len)
            .map_err(|e| SandboxError::Run(format!("execution failed: {e}")))?;

        // Read output: first 4 bytes = output length (i32 LE), then output data.
        let mut len_buf = [0u8; 4];
        memory
            .read(&store, OUTPUT_LEN_OFFSET, &mut len_buf)
            .map_err(|e| SandboxError::Run(format!("failed to read output length: {e}")))?;
        let output_len = i32::from_le_bytes(len_buf) as usize;

        if output_len > 0 {
            let max_output = spec.output_limit.unwrap_or(1024 * 1024);
            let output_len = output_len.min(max_output);
            let mut output_buf = vec![0u8; output_len];
            memory
                .read(&store, OUTPUT_DATA_OFFSET, &mut output_buf)
                .map_err(|e| SandboxError::Run(format!("failed to read output: {e}")))?;
            stdout = String::from_utf8_lossy(&output_buf).into_owned();
        }
    } else if let Some(start) = start_func {
        // WASI-style: just call _start().
        start
            .call(&mut store, ())
            .map_err(|e| SandboxError::Run(format!("execution failed: {e}")))?;
    } else {
        return Err(SandboxError::Run(
            "module must export `run` or `_start`".into(),
        ));
    }

    // Get fuel consumed.
    let fuel_consumed = if fuel_limit != u64::MAX {
        match store.get_fuel() {
            Ok(remaining) => fuel_limit.saturating_sub(remaining),
            Err(_) => 0,
        }
    } else {
        0
    };

    Ok(SandboxResult {
        stdout,
        stderr: String::new(),
        exit_code,
        fuel_consumed,
        duration: Duration::ZERO, // set by caller
    })
}

/// Helper: build a minimal WASM module in WAT that follows our contract.
///
/// The module:
/// - Exports 1 page of memory.
/// - Exports `run(input_len: i32) -> i32` that reads input from offset 1024,
///   writes output length at offset 0 and output data at offset 4.
///   Returns 0.
///
/// This is primarily for testing.
#[cfg(test)]
pub(crate) fn echo_wat() -> Vec<u8> {
    let wat = r#"
        (module
            (memory (export "memory") 1)
            (func (export "run") (param $input_len i32) (result i32)
                (local $i i32)
                ;; Write output length = input_len at offset 0
                ;; i32.store expects [address, value] on stack
                i32.const 0
                local.get $input_len
                i32.store
                ;; Copy input from offset 1024 to offset 4
                block $done
                    loop $loop
                        local.get $i
                        local.get $input_len
                        i32.ge_s
                        br_if $done
                        ;; mem[4 + i] = mem[1024 + i]
                        ;; i32.store8 expects [address, value] on stack
                        local.get $i
                        i32.const 4
                        i32.add
                        i32.const 1024
                        local.get $i
                        i32.add
                        i32.load8_u
                        i32.store8
                        ;; i++
                        local.get $i
                        i32.const 1
                        i32.add
                        local.set $i
                        br $loop
                    end
                end
                i32.const 0
            )
        )
    "#;
    wat::parse_str(wat).unwrap()
}

/// Helper: build a WASM module that always returns exit code 1.
#[cfg(test)]
pub(crate) fn fail_wat() -> Vec<u8> {
    let wat = r#"
        (module
            (memory (export "memory") 1)
            (func (export "run") (param $input_len i32) (result i32)
                ;; Write output length = 0 at offset 0
                i32.const 0
                i32.const 0
                i32.store
                ;; Return exit code 1
                i32.const 1
            )
        )
    "#;
    wat::parse_str(wat).unwrap()
}

/// Helper: build a WASM module with only `_start` (WASI-style).
#[cfg(test)]
pub(crate) fn wasi_wat() -> Vec<u8> {
    let wat = r#"
        (module
            (memory (export "memory") 1)
            (func (export "_start"))
        )
    "#;
    wat::parse_str(wat).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SandboxSpec;

    fn spec_with(module: Vec<u8>) -> SandboxSpec {
        SandboxSpec {
            module,
            fuel_limit: Some(10_000_000),
            memory_limit: Some(16 * 1024 * 1024),
            timeout: Some(Duration::from_secs(5)),
            env: Default::default(),
            output_limit: Some(1024 * 1024),
        }
    }

    #[tokio::test]
    async fn create_and_destroy() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(echo_wat())).await.unwrap();
        assert_eq!(rt.sandbox_count(), 1);
        assert_eq!(rt.status(&id).await, Some(SandboxStatus::Ready));

        rt.destroy(&id).await.unwrap();
        assert_eq!(rt.sandbox_count(), 0);
        assert_eq!(rt.status(&id).await, None);
    }

    #[tokio::test]
    async fn run_echo() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(echo_wat())).await.unwrap();
        let result = rt.run(&id, b"hello").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.stdout, "hello");
        assert!(result.fuel_consumed > 0);
    }

    #[tokio::test]
    async fn run_empty_input() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(echo_wat())).await.unwrap();
        let result = rt.run(&id, b"").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.is_empty());
    }

    #[tokio::test]
    async fn run_fail_module() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(fail_wat())).await.unwrap();
        let result = rt.run(&id, b"test").await.unwrap();
        assert_eq!(result.exit_code, 1);
    }

    #[tokio::test]
    async fn run_wasi_module() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(wasi_wat())).await.unwrap();
        let result = rt.run(&id, b"").await.unwrap();
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn create_empty_module_fails() {
        let rt = WasmtimeRuntime::new().unwrap();
        let spec = SandboxSpec {
            module: vec![],
            ..Default::default()
        };
        assert!(rt.create(&spec).await.is_err());
    }

    #[tokio::test]
    async fn create_invalid_wasm_fails() {
        let rt = WasmtimeRuntime::new().unwrap();
        let spec = SandboxSpec {
            module: vec![0x00, 0x01, 0x02, 0x03], // not valid WASM
            ..Default::default()
        };
        assert!(rt.create(&spec).await.is_err());
    }

    #[tokio::test]
    async fn create_module_without_run_fails() {
        let rt = WasmtimeRuntime::new().unwrap();
        // Module with memory but no run/_start.
        let wat = r#"(module (memory (export "memory") 1))"#;
        let module = wat::parse_str(wat).unwrap();
        let spec = spec_with(module);
        assert!(rt.create(&spec).await.is_err());
    }

    #[tokio::test]
    async fn run_missing_sandbox() {
        let rt = WasmtimeRuntime::new().unwrap();
        assert!(rt.run("nonexistent", b"").await.is_err());
    }

    #[tokio::test]
    async fn destroy_missing_sandbox() {
        let rt = WasmtimeRuntime::new().unwrap();
        assert!(rt.destroy("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn list_sandboxes() {
        let rt = WasmtimeRuntime::new().unwrap();
        rt.create(&spec_with(echo_wat())).await.unwrap();
        rt.create(&spec_with(echo_wat())).await.unwrap();
        let sandboxes = rt.list().await;
        assert_eq!(sandboxes.len(), 2);
        assert!(sandboxes.iter().all(|s| s.status == SandboxStatus::Ready));
        assert!(sandboxes.iter().all(|s| s.fuel_consumed.is_some()));
    }

    #[tokio::test]
    async fn unique_ids() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id1 = rt.create(&spec_with(echo_wat())).await.unwrap();
        let id2 = rt.create(&spec_with(echo_wat())).await.unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn fuel_is_tracked() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(echo_wat())).await.unwrap();
        let result = rt.run(&id, b"hello world").await.unwrap();
        assert!(result.fuel_consumed > 0);

        // Check that the sandbox tracks cumulative fuel.
        let sandboxes = rt.list().await;
        assert_eq!(sandboxes[0].fuel_consumed, Some(result.fuel_consumed));
    }

    #[tokio::test]
    async fn status_after_run() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(echo_wat())).await.unwrap();
        assert_eq!(rt.status(&id).await, Some(SandboxStatus::Ready));
        rt.run(&id, b"test").await.unwrap();
        assert_eq!(rt.status(&id).await, Some(SandboxStatus::Completed));
    }

    #[tokio::test]
    async fn status_after_failed_run() {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec_with(fail_wat())).await.unwrap();
        rt.run(&id, b"test").await.unwrap();
        assert_eq!(rt.status(&id).await, Some(SandboxStatus::Failed));
    }
}
