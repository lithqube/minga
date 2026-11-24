# QA Report — Issue #199 · v1

**PR:** #212 — `🐛 [fix] release: give a v1.2.0 single-host upgrade steps`
**Branch:** `issue-199-release-upgrade-notes-single-host`
**Head:** `3b825ca`
**Run date:** Nov 24, 01:20 CET
**Result:** ✅ PASS — all 4 ACs verified

---

## Build verification

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| `cargo test -p minga-cli` | ✅ 79 tests, 7 suites, 0 failed |

Command: `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-cli`

---

## AC1 — Release workflow publishes upgrade notes above PR list

**Result:** ✅ Pass

`.github/workflows/release.yml:230–246`:
- "Resolve release notes" step derives `docs/release-${major_minor}.0.md` from the tag (e.g. `v1.3.0-rc.1 → docs/release-1.3.0.md`) and copies it to `release-notes-body.md` (empty file if none exists).
- `softprops/action-gh-release` is called with both `body_path: release-notes-body.md` and `generate_release_notes: true`. The action prepends the `body_path` content above the auto-generated PR list.
- `docs/release-1.3.0.md` exists in the repo (21 836 bytes, committed on branch).

---

## AC2 — `docs/release-1.3.0.md` numbered steps for v1.2.0 single-host layout

**Result:** ✅ Pass

Section at line 179: "Upgrading a v1.2.0 single-host layout (manager + api + `--peer`)". 8 numbered steps. Each command and flag verified against the codebase:

| Step command / flag | Source location | Status |
|---|---|---|
| `minga state export -o` | `crates/minga-cli/src/main.rs:124` — `#[arg(short, long)] output: Option<PathBuf>` | ✅ |
| `--api-url` on export/import | `crates/minga-cli/src/state.rs:43` `ApiOptions` | ✅ |
| `minga state import` | `crates/minga-cli/src/main.rs:773` | ✅ |
| `minga manager --api-bind` | `crates/minga-cli/src/main.rs:476` | ✅ |
| `minga manager --api-token` | `crates/minga-cli/src/main.rs:479` | ✅ |
| `minga manager --data-dir` | `crates/minga-cli/src/main.rs:340` | ✅ |
| `minga manager --node-id` / `--bind` | `scripts/minga.service` ExecStart | ✅ |
| `minga-ctl upgrade preflight` | `crates/minga-ctl/src/cmd/upgrade.rs:164` | ✅ |
| `minga-ctl stack ls` | `crates/minga-ctl/src/cmd/stack.rs:577` | ✅ |
| `minga api` systemd unit (generic name) | `scripts/minga.service` uses `${MINGA_ROLE}` — docs refer generically, no incorrect filename | ✅ |

---

## AC3 — Both `--peer` error sites updated

**Result:** ✅ Pass

**Site 1** (`crates/minga-cli/src/main.rs:1698`) — triggered when `--peer` is set, no `--wg-endpoint`:
```
error: --peer is set, which makes this a multi-node cluster; either add --wg-endpoint or remove --peer to run as a single host
```
Names `--peer` as trigger ✅. Says single host should start without it ✅.

**Site 2** (`crates/minga-cli/src/main.rs:1774`) — triggered when other nodes already registered in store:
```
error: other nodes are registered in this cluster; either add --wg-endpoint or, for a single-host install, remove --peer and restart
```
Names `--peer` as the thing to remove ✅. Says single-host install ✅.

Old messages at both sites: `error: managers in a multi-node cluster require --wg-endpoint` (no mention of `--peer`).

---

## AC4 — CLI test asserts new error text; fails on merge-base

**Result:** ✅ Pass

Test file: `crates/minga-cli/tests/peer_wg_endpoint_error.rs`
Test name: `manager_with_peer_and_no_wg_endpoint_names_peer_in_error`

Assertions:
- exit non-zero ✅
- output contains `"--peer"` ✅
- output contains `"single host"` or `"without --peer"` ✅

**Red-first on merge-base `1d4d0af`:**
```
thread 'manager_with_peer_and_no_wg_endpoint_names_peer_in_error' panicked at crates/minga-cli/tests/peer_wg_endpoint_error.rs:38:5:
error must name --peer as the trigger:
error: managers in a multi-node cluster require --wg-endpoint
test result: FAILED. 0 passed; 1 failed
```
Test correctly fails on merge-base (old error text lacks `--peer`), passes on PR head. ✅

---

## Test count note

Builder reported "15/15". Actual count on PR head: **79 tests across 7 suites** (57 unit + 22 integration). All pass. The discrepancy is a counting method difference; no tests were removed.
