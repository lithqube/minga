# QA Report — issue #199 · v2

**Branch:** issue-199-release-upgrade-notes-single-host  
**Head:** 405dc20fabc62568eaf808713eb7f91c35dd6057  
**Merge-base:** 1d4d0af  
**Run:** 2026-11-24 (round 2 — post-Builder rebuild)  
**Result:** ✅ PASS — all 4 ACs verified

---

## Build checks

| Check | Result |
|-------|--------|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| `cargo test -p minga-cli` | ✅ 79/79 (7 suites) |

> Builder quoted "9/9" — that covered only the integration-test binary.  
> Full crate suite (`src/main.rs` unit tests + all files under `tests/`) is **79 tests**.

---

## AC2 — command correctness audit

Every shell command in `docs/release-1.3.0.md` §"Upgrading a v1.2.0 single-host layout" verified against clap source.

### Step 1
```bash
minga state export -o /root/minga-state.json --api-url http://127.0.0.1:2375
```
- **Subcommand:** `StateCommand::Export` — `crates/minga-cli/src/main.rs:119`
- **`-o`/`--output`:** `#[arg(short, long)]` — `main.rs:124`
- **`--api-url`:** `StateApiArgs::api_url`, `#[arg(long, env="MINGA_API_URL")]` — `main.rs:143`; default `http://127.0.0.1:2375`
- **Verdict:** ✅

### Step 5
```bash
minga manager \
  --node-id <YOUR_NODE_ID> \
  --bind 127.0.0.1:50051 \
  --api-bind 0.0.0.0:2375 \
  --api-token <YOUR_TOKEN> \
  --data-dir /var/lib/minga
```
- **`--node-id`:** `NodeArgs::node_id` — `main.rs:331`
- **`--bind`:** `NodeArgs::bind` — `main.rs:336`
- **`--api-bind`:** `NodeArgs::api_bind` — `main.rs:476`
- **`--api-token`:** `NodeArgs::api_token`, `#[arg(long, env="MINGA_API_TOKEN")]` — `main.rs:479`
- **`--data-dir`:** `NodeArgs::data_dir`, default `/var/lib/minga` — `main.rs:340`
- **`--wg-endpoint` absent:** correct; the guard at `main.rs:1692–1698` fires only when `--peer` or `--join` is set
- **Verdict:** ✅

### Step 6
```bash
curl -s http://127.0.0.1:2375/readyz
MINGA_CTL_ENDPOINT=http://127.0.0.1:2375 minga-ctl upgrade preflight --to v1.3.0
```
- **`MINGA_CTL_ENDPOINT`:** read at `crates/minga-ctl/src/config.rs:405` — `std::env::var("MINGA_CTL_ENDPOINT")`; overrides context endpoint
- **`upgrade preflight`:** `UpgradeCommand::Preflight(PreflightArgs)` — `crates/minga-ctl/src/cmd/upgrade.rs:71`
- **`--to`:** `PreflightArgs::to: String`, `#[arg(long)]`, **required** (no default) — `upgrade.rs:78`
- **No `--api-url`:** correct; minga-ctl has no such flag; endpoint only via `MINGA_CTL_ENDPOINT` or `--context`
- **Verdict:** ✅ (R1 from round 1 is fixed)

### Step 7
```bash
minga state import /root/minga-state.json --api-url http://127.0.0.1:2375
```
- **Subcommand:** `StateCommand::Import` — `main.rs:132`
- **Positional `file`:** `file: PathBuf` — `main.rs:133`
- **`--api-url`:** `StateApiArgs::api_url` — `main.rs:143`
- **Verdict:** ✅

### Step 8
```bash
MINGA_CTL_ENDPOINT=http://127.0.0.1:2375 minga-ctl stack ls
```
- **`StackCommand::Ls`:** unit variant — `crates/minga-ctl/src/cmd/stack.rs:54`; no arguments
- **No `--api-url`:** correct; minga-ctl has no such flag
- **Verdict:** ✅ (R2 from round 1 is fixed)

---

## AC2 — step ordering

Layout: host running `minga manager` + `minga api` (both with `--peer`).

| # | Action | Precondition met? |
|---|--------|-------------------|
| 1 | Export desired state from running 1.2.0 | Cluster up ✅ |
| 2 | Back up data directory (while both processes running) | 1.2.0 still running ✅ |
| 3 | Stop both processes, remove `--peer` and `minga api` unit | Backup complete ✅ |
| 4 | Install 1.3.0 binary | Old processes stopped ✅ |
| 5 | Start manager without `--peer`, with `--api-bind` | 1.3.0 binary installed ✅ |
| 6 | Confirm manager becomes leader (`/readyz` + `minga-ctl upgrade preflight`) | Manager running ✅ |
| 7 | Import desired state | Manager leader, store empty ✅ |
| 8 | Verify result (`minga-ctl stack ls`) | State imported ✅ |

Ordering is safe for the described layout. ✅

---

## AC1 — release workflow

`.github/workflows/release.yml:230–246` — "Resolve release notes" step copies `docs/release-1.3.0.md`; `body_path` + `generate_release_notes: true`. Verified in round 1; unchanged.  ✅

## AC3 — `--wg-endpoint` error text

- Site 1 (`main.rs:1698`): `"--peer is set … remove --peer to run as a single host"` ✅
- Site 2 (`main.rs:1774`): `"for a single-host install, remove --peer and restart"` ✅

## AC4 — CLI regression test

**File:** `crates/minga-cli/tests/peer_wg_endpoint_error.rs`  
**Test:** `manager_with_peer_and_no_wg_endpoint_names_peer_in_error`  
**Green on HEAD:** ✅ (included in 79/79 run above)  
**Red on merge-base `1d4d0af`:** `1d4d0af` emits `"managers in a multi-node cluster require --wg-endpoint"` — contains neither `"--peer"` nor `"single host"`, so both assertions at lines 38 and 42 would fail. File did not exist at `1d4d0af`; added in `3b825ca`. ✅

---

## PR body marker check

Blocks present (one each, no duplicates): `sb:status`, `sb:problem`, `sb:solution`, `sb:ac`, `sb:history`, `sb:risk`. Will rewrite `sb:status`, tick `sb:ac`, append `sb:history` on exit.

---

## Round 1 findings resolved

| ID | Finding | Status |
|----|---------|--------|
| R1 | step 6: `--api-url` flag on `minga-ctl`, `--to` missing | ✅ fixed — `405dc20` |
| R2 | step 8: `--api-url` flag on `minga-ctl stack ls` | ✅ fixed — `405dc20` |

---

**Local tests:** `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-cli`  
**Evidence path:** `docs/super-board/runs/issue-199-qa-v2/`
