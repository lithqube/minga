# QA Report — Issue #205 — v1

**PR:** https://github.com/lithqube/minga/pull/214  
**Branch:** issue-205-operator-networking-troubleshooting-docs  
**Head at QA start:** b70528f  
**QA date:** 2026-11-24

---

## AC1 — networking.md statements verified against code

| Statement | Source location | Result |
|---|---|---|
| DEFAULT_SUPERNET `10.100.0.0/16` | `crates/minga-net/src/cidr.rs:170` | ✅ |
| Bridge name `minga-<8 hex>` from `bridge_name()` | `crates/minga-node/src/netns.rs:641-643` | ✅ |
| DNS server on bridge gateway IP | `crates/minga-node/src/netns.rs:660` | ✅ |
| DNS names registered: `<service>`, `tasks.<service>`, `<stack>.<service>`, `<service>.<stack>.minga.local`, `<task-id>.<service>.<stack>.minga.local` | `crates/minga-node/src/supervisor.rs:239-253` | ✅ |
| Bare service name works without search domains (musl) | `crates/minga-node/src/supervisor.rs:236-239` (comment + code), pinned by test at `supervisor.rs:2372` and `crates/minga-cli/tests/deploy_path_probe.rs:344` | ✅ |
| resolv.conf: `nameserver <gw>`, `options ndots:0`, `search minga.local` always; `<stack>.minga.local` prepended when `MINGA_STACK` set | `crates/minga-node/src/netns.rs:1761-1768`; search built at `crates/minga-node/src/containerd/runtime.rs:682-690` | ✅ |
| PortMode: Ingress (default) / Host | `crates/minga-state/src/types.rs:197-205` | ✅ |
| DNAT rule with `--dst-type LOCAL` at `netns.rs:545` | `crates/minga-node/src/netns.rs:545` | ✅ |
| ip_forward=1 set at bridge creation (`netns.rs:1354`) | `crates/minga-node/src/netns.rs:1354` | ✅ |
| WireGuard mesh subnet `10.200.0.0/16`; `mesh_node_ip` from node ID | `crates/minga-net/src/overlay.rs:233-247` | ✅ |
| DEFAULT_WG_PORT = 51820 | `crates/minga-net/src/cidr.rs:215` | ✅ |
| isolation_rules(): drop inter-bridge unless DNAT, allow same-bridge | `crates/minga-node/src/netns.rs:591-609` | ✅ |
| Egress REFUSED for confined tasks at dns_server.rs | `crates/minga-net/src/dns_server.rs:290-304` (doc cites 285-295 — minor line drift, behavior correct) | ✅ |

**AC1: PASS.** All statements verified against specific code locations. One minor line-number drift in dns_server.rs citation (285-295 vs actual 290-304); behavior description is accurate.

---

## AC2 — troubleshooting.md commands verified against code

| Command / claim | Source location | Result |
|---|---|---|
| `minga-ctl service inspect <stack>.<service>` | `crates/minga-ctl/src/cmd/service.rs:36-48` (Inspect variant) | ✅ |
| `minga-ctl service inspect --tail N` | `crates/minga-ctl/src/cmd/service.rs:44-47` (`--tail u64`) | ✅ |
| `minga-ctl service ps <stack>.<service>` | `crates/minga-ctl/src/cmd/service.rs:31-35` (Ps variant) | ✅ |
| `minga-ctl service logs <stack>.<service>` with `--tail` and `-f` | `crates/minga-ctl/src/cmd/service.rs:49-63` (Logs variant) | ✅ |
| `minga logs <task-id>` with `--tail` and `-f` | `crates/minga-cli/src/main.rs:97-110` (Logs command) | ✅ |
| Task ID pattern `<stack>.<service>.<replica>` | `crates/minga-cli/src/main.rs:98` ("Task ID (e.g. plant-7.web.1)") | ✅ |
| `journalctl -u minga` / systemd unit name `minga` | `scripts/minga.service` exists; service installed as `/etc/systemd/system/minga.service` | ✅ |
| MINGA_TASK_SOURCE, MINGA_WG_FORWARD chain names | `crates/minga-node/src/netns.rs:163-164` | ✅ |
| DNS REFUSED for egress-confined tasks | `crates/minga-net/src/dns_server.rs:290-304` | ✅ |

**AC2: PASS.** All CLI commands, flags, unit names, and chain names verified.

---

## AC3 — links and nav

| Check | Result |
|---|---|
| `docs/networking.md` exists | ✅ |
| `docs/troubleshooting.md` exists | ✅ |
| `docs/README.md` links both (`networking.md`, `troubleshooting.md`) | ✅ lines 48-49 |
| `site/src/data/docsNav.ts` entries for `/docs/networking/` and `/docs/troubleshooting/` | ✅ lines 17-18 |
| `site/src/pages/docs/[...slug].astro` slug `networking` → `docs/networking.md` | ✅ line 17 |
| `site/src/pages/docs/[...slug].astro` slug `troubleshooting` → `docs/troubleshooting.md` | ✅ line 18 |
| **BUG FOUND & FIXED:** PR removed `design` slug → `/docs/design/` 404 regression. `design.md` still in nav (`docsNav.ts:38`). Restored in `[...slug].astro` | ✅ fixed, line 19 added |

**AC3: PASS** (after fix).

---

## AC4 — site build

Command: `cd site && npm run build`  
Result: **32 pages built, exit 0** (31 before fix; +1 from restored design page)  
Pages include: `/docs/networking/index.html`, `/docs/troubleshooting/index.html`, `/docs/design/index.html`

**AC4: PASS.**

---

## Cargo checks

| Check | Command | Result |
|---|---|---|
| fmt | `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check` | ✅ exit 0 |
| clippy | `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings` | ✅ exit 0 |

---

## Fix committed

`site/src/pages/docs/[...slug].astro`: restored `design` slug that was accidentally dropped when the Builder replaced it with `networking`+`troubleshooting`. The nav entry in `docsNav.ts` for `/docs/design/` was never removed, so the page would have 404'd without the fix.

---

## Local tests

```
cd site && npm run build   # 32 pages, exit 0
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings
```
