# Troubleshooting

Diagnostic steps for common problems. Commands assume you have `minga-ctl` configured with a context pointing at your manager.

## Task will not start

A failed task records its last error and exit code in state. Retrieve them before reading logs.

**Step 1 — Check task status and error:**

```bash
minga-ctl service inspect <stack>.<service>
```

The output shows the last error (e.g. `exited with code 1`, `killed by signal 9`, or a start error) and the number of running versus desired replicas.

```bash
minga-ctl service ps <stack>.<service>
```

The `ERROR` column in `service ps` shows the same recorded reason.

**Step 2 — Read task logs:**

```bash
minga-ctl service logs <stack>.<service>
```

This calls `minga logs` on the node via the context's `--nats-url`. Use `--tail N` to limit output or `-f` to follow.

You can also call `minga logs` directly if you know the task ID:

```bash
minga logs <stack>.<service>.<n>
```

Task IDs follow the pattern `<stack>.<service>.<replica-number>`. Task output is retained in JetStream; a task that exited can still be read.

**Step 3 — Common exit codes:**

| Exit code / signal | Likely cause |
|---|---|
| `exited with code 1` | Application startup failure; read logs |
| `killed by signal 9` (SIGKILL) | Out-of-memory, or the supervisor stopped the task |
| `exited with code 127` | The entrypoint or command was not found in the image |
| Start error (agent task) | WASM module failed to load; `service inspect` shows the reason |

**Step 4 — Image not found:**

If the error mentions a missing image, the task could not be pulled. Verify the image is available in the `minga` containerd namespace:

```bash
ctr -n minga images list | grep <image>
```

On air-gapped sites, import it first:

```bash
ctr -n minga images import <image>.tar
```

## Name does not resolve

**Check the DNS names the resolver knows:**

The embedded DNS server listens on the bridge gateway IP (the first address of the network's `/24` subnet). Task `resolv.conf` points at it.

The registered names for a running service are (`crates/minga-node/src/supervisor.rs:237-258`):

- `<service>` — bare name, works in musl-based images without search domains
- `tasks.<service>` — all replicas
- `<stack>.<service>` — compat form
- `<service>.<stack>.minga.local` — FQDN

From inside a task, test resolution:

```bash
# on a glibc image (curl, wget, dig available):
getent hosts <service>
# on a musl/Alpine image:
nslookup <service>
# or:
wget -O- http://<service>:<port>/
```

**Check the bridge is up:**

```bash
ip link show type bridge | grep minga-
```

Each network's bridge is named `minga-<8 hex>`. If the bridge is missing, the DNS server for that network cannot be running.

**Check the task's resolv.conf:**

```bash
ip netns exec minga-<task-id> cat /etc/resolv.conf
```

It should contain a `nameserver` line with the bridge gateway IP. If it is empty or points elsewhere, the task was set up without networking.

**DNS only answers local names for tasks under a required egress policy:**

Tasks with `x-minga-egress` have their DNS confined: external names return `REFUSED` at the resolver, so they never reach upstream DNS (`crates/minga-net/src/dns_server.rs:285-295`). This is intentional. Use the egress proxy for external HTTP.

## iptables or kernel errors

**`net.bridge.bridge-nf-call-iptables` must be 1:**

Minga sets this sysctl when the first bridge is created. If it was reset (e.g. by a system restart before Minga ran), tasks on different bridges cannot be isolated correctly. Check:

```bash
sysctl net.bridge.bridge-nf-call-iptables
# should be: net.bridge.bridge-nf-call-iptables = 1
```

If it is 0, `modprobe br_netfilter` and `sysctl -w net.bridge.bridge-nf-call-iptables=1` will restore it. Minga sets both on the next bridge creation.

**`net.ipv4.ip_forward` must be 1:**

Minga enables packet forwarding when it creates network bridges (`crates/minga-node/src/netns.rs:1354`). Verify:

```bash
sysctl net.ipv4.ip_forward
# should be: net.ipv4.ip_forward = 1
```

**`this host cannot run task networking` at start:**

The kernel rejected an iptables match the node needs. The message names the match, the kernel module, `iptables -V`, the kernel release and the failing command. See [host requirements](networking.md#host-requirements).

**A task will not start: `active task namespace minga-... has no state file`:**

A process still runs in a task namespace whose state file is gone. Stop that process, then `ip netns delete minga-...`. Namespaces nothing runs in are removed by the node at start.

**iptables backend mismatch:**

Minga uses `iptables` and `iptables-restore`. If the system uses only `nftables` and the `iptables-nft` shim is absent, rules cannot be installed. Check:

```bash
iptables --version
# should include "nf_tables" or "legacy"
```

Confirm that `iptables-restore` is available:

```bash
which iptables-restore
```

**Inspect Minga's iptables chains:**

```bash
iptables -t nat -L PREROUTING -n -v
iptables -t filter -L MINGA_TASK_SOURCE -n -v
iptables -t filter -L MINGA_WG_FORWARD -n -v
```

`MINGA_TASK_SOURCE` enforces per-task source IP. `MINGA_WG_FORWARD` controls mesh traffic. Missing chains suggest Minga's network setup did not complete.

**nftables egress tables (for `x-minga-egress` services):**

```bash
nft list tables | grep minga
```

A stale egress table from a task that did not clean up can be removed by restarting the task or by running `nft delete table ip minga-egress-<task-id>`.

## Where node logs are

**Under systemd (plain Linux):**

The `minga` process logs to stderr; systemd captures it in the journal.

```bash
# Follow live:
journalctl -u minga -f

# Last 200 lines:
journalctl -u minga -n 200

# Since a specific time:
journalctl -u minga --since "2026-11-24 10:00"
```

The service file is at `scripts/minga.service`; install it to `/etc/systemd/system/minga.service`. See the [systemd unit](../scripts/minga.service) and [environment example](../scripts/minga.env.example) for configuration.

**On Talos Linux:**

Talos has no systemd. The Minga container logs go to the Talos log subsystem. Retrieve them with:

```bash
talosctl logs minga-manager
talosctl logs minga-worker
```

See [Talos deployment](talos-deployment.md) for the container names used in `ContainerConfig`.

**Node-local structured logs:**

Minga writes JSON logs to stderr. Pipe through `jq` for structured inspection:

```bash
journalctl -u minga -o cat | jq 'select(.level == "ERROR")'
```

Fields include `level`, `message`, `task`, `service`, `tenant`, and the failing command's `stderr` for networking errors.
