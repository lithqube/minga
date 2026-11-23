# QA Report — Issue #139 · migrate-v1 admin token via HTTP_PROXY

**Branch:** issue-139-migrate-v1-proxy  
**Head commit:** 9d9fc88  
**Merge base:** f7a2fc3  
**QA version:** v1  
**Result:** ✅ PASS

---

## Acceptance Criteria

### AC1: With HTTP_PROXY set, migrate-v1 sends no request to the proxy

**Status:** ✅ PASS  
**Test:** `tests::migrate_v1_does_not_use_http_proxy` (`crates/minga-cli/src/main.rs:3721`)  
**Method:** Binds a non-blocking TCP listener as a "proxy" sentinel. Sets `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` to point at that listener. Runs `run_migrate_v1` against a loopback mock API. After the call, asserts `proxy.accept()` returns `WouldBlock` — no connection was made to the proxy.

### AC2: migrate-v1 still works against a loopback API

**Status:** ✅ PASS  
**Test:** `tests::migrate_v1_works_against_loopback_api` (`crates/minga-cli/src/main.rs:3765`)  
**Method:** Spins up an axum mock server on a loopback port, runs both dry-run and apply paths, asserts `ExitCode::SUCCESS` for both.

---

## Fix Analysis

**File:** `crates/minga-cli/src/main.rs:830–841`

The fix adds `.no_proxy()` and `.redirect(reqwest::redirect::Policy::none())` to the migrate-v1 `ClientBuilder`.

### Does `.no_proxy()` cover HTTP_PROXY, HTTPS_PROXY, and ALL_PROXY?

Yes. Verified against `reqwest-0.12.23/src/async_impl/client.rs:1375–1379`:

```rust
pub fn no_proxy(mut self) -> ClientBuilder {
    self.config.proxies.clear();     // clears any explicitly-added proxies
    self.config.auto_sys_proxy = false;  // prevents ProxyMatcher::system() from being added
    self
}
```

`ProxyMatcher::system()` is what reads `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` from the environment at request time. Setting `auto_sys_proxy = false` prevents it from being consulted at all, covering both upper- and lower-case variants (the system proxy reader normalises case). `ALL_PROXY` is also read by `ProxyMatcher::system()` — not a separate path. All three env vars are blocked.

### Do redirects break any legitimate migrate-v1 behaviour?

No. `migrate-v1` makes exactly two targeted requests: `GET /minga/managers` (list voters) and `POST /minga/managers/migrate-v1` (apply). Both go to `args.api_url` which is a fixed operator-supplied address. No redirect is expected or desirable. Disabling redirects removes a potential token-forwarding vector on 3xx responses.

---

## Verification Commands

```
# Format
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check
# → EXIT:0

# Clippy
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings
# → EXIT:0

# Tests (head 9d9fc88)
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-cli
# → 57 passed; 0 failed

# Red-first check (merge-base f7a2fc3)
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-cli -- migrate_v1_does_not_use_http_proxy migrate_v1_works_against_loopback_api
# → 0 tests ran (filtered out — tests not present on base): confirms tests are new to this PR
```

---

## Summary

- fmt ✅ · clippy ✅ · 57/57 tests ✅
- Both AC1 and AC2 tests pass on head; neither test exists on merge-base.
- Fix is complete: `.no_proxy()` on reqwest `ClientBuilder` suppresses all proxy env vars (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, both cases) by clearing the proxy list and disabling `ProxyMatcher::system()`.
- No legitimate behaviour removed: migrate-v1 makes two fixed-target requests; redirects are neither expected nor safe.
