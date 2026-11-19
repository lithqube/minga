//! Wasmtime-backed sandbox runtime.
//!
//! Runs WASM modules via [wasmtime](https://wasmtime.dev) with fuel
//! metering (instruction budget), memory limits, and timeouts.
//!
//! # Module contract
//!
//! - `memory` — exported linear memory with a declared minimum large enough for
//!   the workload's input and output; the host does not grow it.
//! - `run(input_len: i32) -> i32` — entry point. Called with the input
//!   length. Returns an exit code (0 = success).
//!
//! Input/output convention:
//! - The runtime writes input bytes to memory at offset 1024.
//! - The module reads input from `ptr=1024, len=input_len`.
//! - The module writes output length at offset 0 (i32 LE), followed by output
//!   bytes at offset 4.
//! - The runtime reads the output length from offset 0, then reads that many
//!   bytes from offset 4.
//!
//! If the module only exports `_start() -> ()` (WASI-style), the runtime
//! calls it with no input and captures no output.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lru::LruCache;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimitsBuilder, Trap};

use crate::runtime::{
    SandboxError, SandboxInfo, SandboxResult, SandboxRuntime, SandboxSpec, SandboxStatus,
};

const INPUT_OFFSET: usize = 1024;
const OUTPUT_LEN_OFFSET: usize = 0;
const OUTPUT_DATA_OFFSET: usize = 4;
const DEFAULT_MEMORY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OUTPUT_LIMIT_BYTES: usize = 1024 * 1024;
// Node ceilings on tenant-supplied limits. Specs above them are refused, never clamped.
/// Up to `available_parallelism` runs share an edge node's RAM at once.
const MAX_MEMORY_LIMIT_BYTES: u64 = 512 * 1024 * 1024;
/// A run holds one of `available_parallelism` run slots for its whole timeout.
const MAX_TIMEOUT: Duration = Duration::from_secs(300);
/// Agent output is published as one NATS log message (default `max_payload` 1 MiB).
const MAX_OUTPUT_LIMIT_BYTES: usize = 1024 * 1024;
/// Live sandboxes per runtime; each pins a compiled module of up to `MAX_MODULE_BYTES` input.
const MAX_SANDBOXES: usize = 256;
const EPOCH_TICK: Duration = Duration::from_millis(10);
const MODULE_CACHE_CAPACITY: usize = 64;
/// Largest module accepted for compilation; bounds Cranelift time and memory.
const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
/// Per-store table cap. Each funcref element costs host memory outside `memory_limit`.
const MAX_TABLE_ELEMENTS: usize = 100_000;

/// A sandbox runtime backed by wasmtime.
pub struct WasmtimeRuntime {
    engine: Engine,
    /// Shared with in-flight runs: a guest without fuel stops only while the ticker runs.
    epoch_ticker: Arc<EpochTicker>,
    module_cache: Arc<Mutex<LruCache<[u8; 32], Module>>>,
    sandboxes: Mutex<HashMap<String, WasmtimeSandbox>>,
    /// Bounds concurrent runs so blocking threads and store memory stay finite.
    run_slots: Arc<Semaphore>,
    /// Bounds concurrent Cranelift compiles of tenant modules.
    compile_slots: Arc<Semaphore>,
}

/// Locks a mutex, recovering from poisoning: the guarded maps stay consistent
/// across a panic, and one panicking caller must not wedge the node-wide runtime.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct EpochTicker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl EpochTicker {
    fn new(engine: Engine) -> Result<Self, SandboxError> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("minga-wasmtime-epoch".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(EPOCH_TICK);
                    if !thread_stop.load(Ordering::Acquire) {
                        engine.increment_epoch();
                    }
                }
            })
            .map_err(|e| SandboxError::Other(format!("failed to start epoch ticker: {e}")))?;

        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct WasmtimeSandbox {
    id: String,
    module: Module,
    status: SandboxStatus,
    created_at: Instant,
    fuel_consumed: u64,
    limits: RunLimits,
}

/// Limits resolved from a spec at create. Module bytes are not kept: the
/// compiled module is stored separately.
#[derive(Clone, Copy)]
struct RunLimits {
    fuel: u64,
    memory: usize,
    timeout: Duration,
    output: usize,
}

impl RunLimits {
    fn from_spec(spec: &SandboxSpec) -> Result<Self, SandboxError> {
        let memory = spec.memory_limit.unwrap_or(DEFAULT_MEMORY_LIMIT_BYTES);
        if memory > MAX_MEMORY_LIMIT_BYTES {
            return Err(SandboxError::Create(format!(
                "memory_limit is {memory} bytes, node maximum is {MAX_MEMORY_LIMIT_BYTES}"
            )));
        }
        let timeout = spec.timeout.unwrap_or(DEFAULT_TIMEOUT);
        if timeout > MAX_TIMEOUT {
            return Err(SandboxError::Create(format!(
                "timeout is {timeout:?}, node maximum is {MAX_TIMEOUT:?}"
            )));
        }
        let output = spec.output_limit.unwrap_or(DEFAULT_OUTPUT_LIMIT_BYTES);
        if output > MAX_OUTPUT_LIMIT_BYTES {
            return Err(SandboxError::Create(format!(
                "output_limit is {output} bytes, node maximum is {MAX_OUTPUT_LIMIT_BYTES}"
            )));
        }
        Ok(Self {
            fuel: spec.fuel_limit.unwrap_or(u64::MAX),
            memory: memory.try_into().unwrap_or(usize::MAX),
            timeout,
            output,
        })
    }
}

impl std::fmt::Debug for WasmtimeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmtimeRuntime").finish_non_exhaustive()
    }
}

impl WasmtimeRuntime {
    /// Creates a new wasmtime runtime with fuel metering and epoch interruption enabled.
    pub fn new() -> Result<Self, SandboxError> {
        Self::with_config(Config::new())
    }

    /// Creates a new wasmtime runtime with a custom config.
    pub fn with_config(mut config: Config) -> Result<Self, SandboxError> {
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config)
            .map_err(|e| SandboxError::Other(format!("failed to create engine: {e}")))?;
        let epoch_ticker = Arc::new(EpochTicker::new(engine.clone())?);
        let cpus = thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Ok(Self {
            engine,
            epoch_ticker,
            module_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(MODULE_CACHE_CAPACITY).unwrap(),
            ))),
            sandboxes: Mutex::new(HashMap::new()),
            run_slots: Arc::new(Semaphore::new(cpus)),
            compile_slots: Arc::new(Semaphore::new(cpus)),
        })
    }

    /// Returns the number of currently tracked sandboxes.
    pub fn sandbox_count(&self) -> usize {
        lock(&self.sandboxes).len()
    }
}

#[async_trait]
impl SandboxRuntime for WasmtimeRuntime {
    async fn create(&self, spec: &SandboxSpec) -> Result<String, SandboxError> {
        if spec.module.is_empty() {
            return Err(SandboxError::Create("empty WASM module".into()));
        }

        if spec.module.len() > MAX_MODULE_BYTES {
            return Err(SandboxError::Create(format!(
                "module is {} bytes, limit is {MAX_MODULE_BYTES}",
                spec.module.len()
            )));
        }
        let limits = RunLimits::from_spec(spec)?;

        // Hashing and Cranelift compilation of tenant bytes are CPU-bound; keep
        // them off the async workers that serve heartbeats and the API.
        let engine = self.engine.clone();
        let cache = Arc::clone(&self.module_cache);
        let bytes = spec.module.clone();
        let permit = Arc::clone(&self.compile_slots)
            .acquire_owned()
            .await
            .map_err(|e| SandboxError::Create(format!("compile slots closed: {e}")))?;
        let module = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            compile_cached(&engine, &cache, &bytes)
        })
        .await
        .map_err(|e| SandboxError::Create(format!("compile task failed: {e}")))??;
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

        let mut sandboxes = lock(&self.sandboxes);
        if sandboxes.len() >= MAX_SANDBOXES {
            return Err(SandboxError::Create(format!(
                "node already has {MAX_SANDBOXES} live sandboxes, the maximum"
            )));
        }
        let id = format!("wasm-{:032x}", rand::random::<u128>());
        sandboxes.insert(
            id.clone(),
            WasmtimeSandbox {
                id: id.clone(),
                module,
                status: SandboxStatus::Ready,
                created_at: Instant::now(),
                fuel_consumed: 0,
                limits,
            },
        );
        drop(sandboxes);

        tracing::info!(sandbox = %id, "wasmtime: created sandbox");
        Ok(id)
    }

    async fn run(&self, sandbox_id: &str, input: &[u8]) -> Result<SandboxResult, SandboxError> {
        let start = Instant::now();

        // Clone what we need from the sandbox entry.
        let (module, limits) = {
            let sandboxes = lock(&self.sandboxes);
            let sbx = sandboxes
                .get(sandbox_id)
                .ok_or_else(|| SandboxError::NotFound(sandbox_id.to_string()))?;
            (sbx.module.clone(), sbx.limits)
        };

        // Owned permit moves into the thread, so a dropped caller can't free
        // the slot while the Wasm is still running. The queue wait counts
        // toward the timeout.
        let permit =
            tokio::time::timeout(limits.timeout, Arc::clone(&self.run_slots).acquire_owned())
                .await
                .map_err(|_| SandboxError::Timeout(limits.timeout))?
                .map_err(|e| SandboxError::Run(format!("run slots closed: {e}")))?;

        // The epoch deadline stops the blocking Wasm thread; an async timeout
        // alone would only abandon its JoinHandle.
        let deadline = epoch_ticks(limits.timeout.saturating_sub(start.elapsed()));
        let engine = self.engine.clone();
        let ticker = Arc::clone(&self.epoch_ticker);
        let input_owned = input.to_vec();
        let handle = tokio::task::spawn_blocking(move || {
            let _ticker = ticker;
            let _permit = permit;
            run_wasm(&engine, &module, &input_owned, limits, deadline)
        });

        let result = match handle.await {
            Ok(result) => result,
            Err(e) => return Err(SandboxError::Run(format!("join error: {e}"))),
        };

        match result {
            Ok(mut res) => {
                res.duration = start.elapsed();
                // Update fuel tracking.
                if let Some(sbx) = lock(&self.sandboxes).get_mut(sandbox_id) {
                    sbx.fuel_consumed = sbx.fuel_consumed.saturating_add(res.fuel_consumed);
                    sbx.status = if res.exit_code == 0 {
                        SandboxStatus::Completed
                    } else {
                        SandboxStatus::Failed
                    };
                }
                Ok(res)
            }
            Err(e) => {
                if let Some(sbx) = lock(&self.sandboxes).get_mut(sandbox_id) {
                    sbx.status = SandboxStatus::Failed;
                }
                Err(e)
            }
        }
    }

    async fn destroy(&self, sandbox_id: &str) -> Result<(), SandboxError> {
        let mut sandboxes = lock(&self.sandboxes);
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
        lock(&self.sandboxes)
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
        lock(&self.sandboxes).get(sandbox_id).map(|s| s.status)
    }
}

/// Hashes `bytes` and returns the cached compiled module, compiling on a miss.
fn compile_cached(
    engine: &Engine,
    cache: &Mutex<LruCache<[u8; 32], Module>>,
    bytes: &[u8],
) -> Result<Module, SandboxError> {
    // shortcut: keyed on bytes alone, so create latency shows whether this
    // runtime already compiled identical bytes. Harmless while a node serves one
    // tenant (ADR 0008); key on the tenant too once a runtime serves several.
    let key: [u8; 32] = Sha256::digest(bytes).into();
    if let Some(module) = lock(cache).get(&key).cloned() {
        return Ok(module);
    }
    let module = Module::new(engine, bytes)
        .map_err(|e| SandboxError::Create(format!("failed to compile module: {e}")))?;
    lock(cache).put(key, module.clone());
    Ok(module)
}

/// Converts an execution timeout into whole engine epoch ticks, plus one: the
/// first tick can land anywhere within its period.
fn epoch_ticks(timeout: Duration) -> u64 {
    let ticks = timeout.as_nanos().div_ceil(EPOCH_TICK.as_nanos()) + 1;
    u64::try_from(ticks).unwrap_or(u64::MAX)
}

fn execution_error(error: wasmtime::Error, timeout: Duration, context: &str) -> SandboxError {
    if matches!(error.downcast_ref::<Trap>(), Some(Trap::Interrupt)) {
        SandboxError::Timeout(timeout)
    } else {
        SandboxError::Run(format!("{context}: {error}"))
    }
}

/// Synchronous WASM execution. Runs in a blocking thread.
fn run_wasm(
    engine: &Engine,
    module: &Module,
    input: &[u8],
    limits: RunLimits,
    epoch_deadline: u64,
) -> Result<SandboxResult, SandboxError> {
    let RunLimits {
        fuel: fuel_limit,
        memory: memory_limit,
        timeout,
        output: max_output,
    } = limits;
    // `memory_size` caps each memory separately, so also cap how many memories,
    // tables and instances a tenant module may declare.
    let limits = StoreLimitsBuilder::new()
        .memory_size(memory_limit)
        .memories(1)
        .tables(1)
        .table_elements(MAX_TABLE_ELEMENTS)
        .instances(1)
        .trap_on_grow_failure(true)
        .build();
    let mut store = Store::new(engine, limits);
    store.limiter(|limits| limits);
    store.set_epoch_deadline(epoch_deadline);

    store
        .set_fuel(fuel_limit)
        .map_err(|e| SandboxError::Run(format!("failed to set fuel: {e}")))?;

    let instance = Instance::new(&mut store, module, &[])
        .map_err(|e| execution_error(e, timeout, "failed to instantiate"))?;

    let run_func = instance.get_typed_func::<i32, i32>(&mut store, "run").ok();
    let start_func = instance.get_typed_func::<(), ()>(&mut store, "_start").ok();

    let mut stdout = String::new();
    let mut exit_code = 0i32;

    if let Some(run) = run_func {
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| SandboxError::Run("module must export `memory`".into()))?;

        let input_len = input.len() as i32;
        memory
            .write(&mut store, INPUT_OFFSET, input)
            .map_err(|e| SandboxError::Run(format!("failed to write input to memory: {e}")))?;

        exit_code = run
            .call(&mut store, input_len)
            .map_err(|e| execution_error(e, timeout, "execution failed"))?;

        let mut len_buf = [0u8; 4];
        memory
            .read(&store, OUTPUT_LEN_OFFSET, &mut len_buf)
            .map_err(|e| SandboxError::Run(format!("failed to read output length: {e}")))?;
        let output_len = i32::from_le_bytes(len_buf);
        let output_len = usize::try_from(output_len)
            .map_err(|_| SandboxError::Run(format!("negative output length {output_len}")))?;
        if output_len > max_output {
            return Err(SandboxError::Run(format!(
                "output is {output_len} bytes, limit is {max_output}"
            )));
        }
        // Borrow guest memory rather than allocate a buffer the guest sized.
        let output = memory
            .data(&store)
            .get(OUTPUT_DATA_OFFSET..OUTPUT_DATA_OFFSET + output_len)
            .ok_or_else(|| {
                SandboxError::Run(format!("output of {output_len} bytes exceeds guest memory"))
            })?;
        stdout = String::from_utf8_lossy(output).into_owned();
    } else if let Some(start) = start_func {
        start
            .call(&mut store, ())
            .map_err(|e| execution_error(e, timeout, "execution failed"))?;
    } else {
        return Err(SandboxError::Run(
            "module must export `run` or `_start`".into(),
        ));
    }

    // Unmetered runs start at u64::MAX, so the subtraction still reports real use.
    let remaining = store
        .get_fuel()
        .map_err(|e| SandboxError::Run(format!("failed to read fuel: {e}")))?;
    let fuel_consumed = fuel_limit.saturating_sub(remaining);

    Ok(SandboxResult {
        stdout,
        stderr: String::new(),
        exit_code,
        fuel_consumed,
        duration: Duration::ZERO,
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

    #[tokio::test]
    async fn epoch_deadline_stops_unmetered_infinite_loop() {
        let rt = WasmtimeRuntime::new().unwrap();
        let module =
            wat::parse_str(r#"(module (func (export "_start") (loop $forever br $forever)))"#)
                .unwrap();
        let spec = SandboxSpec {
            module,
            fuel_limit: None,
            timeout: Some(Duration::from_millis(30)),
            ..Default::default()
        };
        let id = rt.create(&spec).await.unwrap();

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(1), rt.run(&id, b""))
            .await
            .expect("epoch interruption must release the blocking task");

        assert!(matches!(result, Err(SandboxError::Timeout(_))));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn store_limits_memory_growth() {
        let module = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "run") (param i32) (result i32)
                    i32.const 1
                    memory.grow
                    drop
                    i32.const 0))"#,
        )
        .unwrap();
        let mut spec = spec_with(module);
        spec.memory_limit = Some(64 * 1024);

        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt.create(&spec).await.unwrap();

        assert!(matches!(rt.run(&id, b"").await, Err(SandboxError::Run(_))));
    }

    async fn run_error(wat: &str) -> SandboxError {
        let rt = WasmtimeRuntime::new().unwrap();
        let id = rt
            .create(&spec_with(wat::parse_str(wat).unwrap()))
            .await
            .unwrap();
        rt.run(&id, b"").await.unwrap_err()
    }

    #[tokio::test]
    async fn store_limits_reject_second_memory() {
        let error = run_error(
            r#"(module
                (memory (export "memory") 1)
                (memory 1)
                (func (export "run") (param i32) (result i32) i32.const 0))"#,
        )
        .await;
        assert!(matches!(error, SandboxError::Run(_)), "{error:?}");
    }

    #[tokio::test]
    async fn store_limits_reject_oversized_table() {
        let error = run_error(&format!(
            r#"(module
                (memory (export "memory") 1)
                (table {} funcref)
                (func (export "run") (param i32) (result i32) i32.const 0))"#,
            MAX_TABLE_ELEMENTS + 1
        ))
        .await;
        assert!(matches!(error, SandboxError::Run(_)), "{error:?}");
    }

    #[tokio::test]
    async fn create_rejects_oversized_module() {
        let rt = WasmtimeRuntime::new().unwrap();
        let spec = spec_with(vec![0; MAX_MODULE_BYTES + 1]);
        let error = rt.create(&spec).await.unwrap_err();
        assert!(error.to_string().contains("limit"), "{error}");
    }

    #[tokio::test]
    async fn unmetered_run_reports_fuel_used() {
        let rt = WasmtimeRuntime::new().unwrap();
        let mut spec = spec_with(echo_wat());
        spec.fuel_limit = None;
        let id = rt.create(&spec).await.unwrap();
        assert!(rt.run(&id, b"hello").await.unwrap().fuel_consumed > 0);
    }

    #[tokio::test]
    async fn same_module_reuses_cached_compilation() {
        let rt = WasmtimeRuntime::new().unwrap();
        rt.create(&spec_with(echo_wat())).await.unwrap();
        rt.create(&spec_with(echo_wat())).await.unwrap();

        assert_eq!(rt.module_cache.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn create_rejects_limits_over_node_maximum() {
        let rt = WasmtimeRuntime::new().unwrap();
        let over = [
            SandboxSpec {
                memory_limit: Some(MAX_MEMORY_LIMIT_BYTES + 1),
                ..spec_with(echo_wat())
            },
            SandboxSpec {
                timeout: Some(MAX_TIMEOUT + Duration::from_secs(1)),
                ..spec_with(echo_wat())
            },
            SandboxSpec {
                output_limit: Some(MAX_OUTPUT_LIMIT_BYTES + 1),
                ..spec_with(echo_wat())
            },
        ];
        for spec in &over {
            let error = rt.create(spec).await.unwrap_err();
            assert!(error.to_string().contains("node maximum"), "{error}");
        }
        assert_eq!(rt.sandbox_count(), 0);
    }

    /// A `run` that reports `len` output bytes without writing them.
    fn output_len_wat(len: i32) -> Vec<u8> {
        wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1)
                (func (export "run") (param i32) (result i32)
                    i32.const 0
                    i32.const {len}
                    i32.store
                    i32.const 0))"#
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn run_rejects_bad_output_length() {
        let rt = WasmtimeRuntime::new().unwrap();
        // Negative, over `output_limit`, and past the 64 KiB guest memory.
        for (len, output_limit) in [(-1, 1024), (2048, 1024), (128 * 1024, 1024 * 1024)] {
            let mut spec = spec_with(output_len_wat(len));
            spec.output_limit = Some(output_limit);
            let id = rt.create(&spec).await.unwrap();
            let error = rt.run(&id, b"").await.unwrap_err();
            assert!(matches!(error, SandboxError::Run(_)), "{len}: {error:?}");
        }
    }

    #[tokio::test]
    async fn create_rejects_beyond_sandbox_cap() {
        let rt = WasmtimeRuntime::new().unwrap();
        for _ in 0..MAX_SANDBOXES {
            rt.create(&spec_with(echo_wat())).await.unwrap();
        }
        let error = rt.create(&spec_with(echo_wat())).await.unwrap_err();
        assert!(error.to_string().contains("maximum"), "{error}");
    }

    #[tokio::test]
    async fn queue_wait_counts_toward_timeout() {
        let rt = WasmtimeRuntime::new().unwrap();
        let mut spec = spec_with(echo_wat());
        spec.timeout = Some(Duration::from_millis(50));
        let id = rt.create(&spec).await.unwrap();
        let slots = rt.run_slots.available_permits() as u32;
        let _held = rt.run_slots.acquire_many(slots).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), rt.run(&id, b"")).await;
        assert!(matches!(result, Ok(Err(SandboxError::Timeout(_)))));
    }

    #[tokio::test]
    async fn in_flight_run_keeps_epoch_ticker_alive() {
        let rt = WasmtimeRuntime::new().unwrap();
        let module =
            wat::parse_str(r#"(module (func (export "_start") (loop $forever br $forever)))"#)
                .unwrap();
        let spec = SandboxSpec {
            module,
            fuel_limit: None,
            timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let id = rt.create(&spec).await.unwrap();
        let slots = Arc::clone(&rt.run_slots);
        let total = slots.available_permits();

        // Abandon the run, then drop the runtime while the guest still spins.
        let _ = tokio::time::timeout(Duration::from_millis(20), rt.run(&id, b"")).await;
        assert_eq!(slots.available_permits(), total - 1);
        drop(rt);

        let start = Instant::now();
        while slots.available_permits() < total {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "guest never stopped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn epoch_deadline_adds_one_tick() {
        assert_eq!(epoch_ticks(EPOCH_TICK), 2);
        assert_eq!(epoch_ticks(Duration::ZERO), 1);
    }
}
