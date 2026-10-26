# Deploying Minga on Talos Linux

**Status:** Recommended deployment target
**Version:** Talos v1.14+

Talos Linux is an immutable, API-driven Linux distribution purpose-built for Kubernetes — but it runs **any containerd workload**. Since Minga targets containerd directly via gRPC (no kubelet), Talos is an ideal host: single-binary OS, atomic upgrades, no SSH, no package manager, secure by default.

---

## Why Talos for Minga?

| Property | Benefit for Minga |
|----------|-------------------|
| **Immutable OS** | No config drift; Minga binary + systemd unit is the only mutable state |
| **Atomic upgrades** | `talosctl upgrade` → reboot; rollback is one command |
| **No SSH / no shell** | Reduced attack surface; all ops via `talosctl` API |
| **Containerd built-in** | `/run/containerd/containerd.sock` exposed by default — Minga connects directly |
| **WireGuard native** | Talos `machine` config supports WireGuard peers; Minga overlay works unmodified |
| **Edge/air-gap ready** | ISO/PXE install; no registry pull at boot if images pre-loaded |

---

## Quick Start: 3-Node Cluster

### Prerequisites
- `talosctl` ≥ 1.14
- 3 x86_64 or arm64 machines (bare metal, VMs, or cloud)
- Minga binary (`minga-linux-amd64` or `minga-linux-arm64`)

### 1. Generate Talos Config
```bash
talosctl gen config minga-cluster https://<control-plane-vip>:6443 \
  --output-dir ./talos-config \
  --config-patch @./talos-patches/containerd.yaml \
  --config-patch @./talos-patches/wireguard.yaml
```
- `<control-plane-vip>`: a stable IP/VIP for the control plane (keepalived, metalLB, or cloud LB)
- Patches below enable containerd socket access and WireGuard

### 2. Containerd Patch (`talos-patches/containerd.yaml`)
```yaml
machine:
  containerd:
    # Expose containerd socket to host (default in Talos 1.14+)
    # No extra config needed for Minga to connect at unix:///run/containerd/containerd.sock
    # If you need remote containerd access:
    # config:
    #   grpc:
    #     address: /run/containerd/containerd.sock
    #     uid: 0
    #     gid: 0
```

### 3. WireGuard Patch (`talos-patches/wireguard.yaml`) — optional, for Minga overlay
```yaml
machine:
  network:
    wireguard:
      - name: minga-overlay
        privateKey: <base64-private-key>
        listenPort: 51820
        peers:
          - publicKey: <base64-peer-pubkey>
            endpoint: <peer-ip>:51820
            allowedIPs: ["10.200.0.0/16"]
```

### 4. Apply Config to Nodes
```bash
# Control plane node 1 (bootstrap)
talosctl apply-config --insecure --nodes <cp1-ip> --file ./talos-config/controlplane.yaml

# Control plane nodes 2-3
talosctl apply-config --insecure --nodes <cp2-ip>,<cp3-ip> --file ./talos-config/controlplane.yaml

# Worker nodes (if any)
talosctl apply-config --insecure --nodes <worker1-ip>,<worker2-ip> --file ./talos-config/worker.yaml
```

### 5. Bootstrap etcd (once, on CP1)
```bash
talosctl bootstrap --nodes <cp1-ip> --endpoints <cp1-ip>
```

### 6. Deploy Minga Binary
```bash
# On each node (via talosctl, no SSH needed):
talosctl --nodes <node-ip> upload minga-linux-amd64 /opt/minga/minga
talosctl --nodes <node-ip> upload minga.service /etc/systemd/system/minga.service
talosctl --nodes <node-ip> run --command "systemctl daemon-reload && systemctl enable --now minga"
```

**`minga.service`** (systemd unit):
```ini
[Unit]
Description=Minga Node Supervisor
After=network-online.target containerd.service
Wants=network-online.target containerd.service

[Service]
Type=notify
ExecStart=/opt/minga/minga node \
  --containerd-socket unix:///run/containerd/containerd.sock \
  --nats-url nats://<nats-ip>:4222 \
  --node-id <unique-node-id>
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

### 7. Verify Cluster
```bash
# Check Minga nodes
minga node ls

# Check containerd connectivity
minga ctr version

# Check metrics
curl http://<node-ip>:2375/metrics | head -20

# Health endpoints
curl http://<node-ip>:2375/healthz
curl http://<node-ip>:2375/readyz
```

---

## Single-Node (Dev / Edge) Setup

For a single machine running both control plane + worker:

```bash
# 1. Generate single-node config
talosctl gen config minga-single https://<node-ip>:6443 \
  --output-dir ./talos-config \
  --config-patch @./talos-patches/containerd.yaml

# 2. Apply
talosctl apply-config --insecure --nodes <node-ip> --file ./talos-config/controlplane.yaml

# 3. Bootstrap (single node = self-bootstrap)
talosctl bootstrap --nodes <node-ip> --endpoints <node-ip>

# 4. Deploy Minga (same as step 6 above)

# 5. Deploy a test stack
minga stack deploy -f docker-compose.yml
```

---

## Air-Gapped / Offline Install

1. **Build Talos ISO with pre-loaded images**:
   ```bash
   # Using talosctl image factory or siderolabs/imager
   talosctl image factory \
     --output iso \
     --arch amd64 \
     --install-disk /dev/sda \
     --extra-registry <local-registry> \
     --extra-image <local-registry>/minga:latest
   ```

2. **Boot nodes from ISO** (PXE, USB, or virt-install)

3. **Apply config** (same as above, but `--insecure` not needed if certs pre-provisioned)

4. **Pre-load Minga image** into containerd:
   ```bash
   # On each node (via talosctl):
   talosctl --nodes <node-ip> images load minga.tar
   ```

---

## Minga-Specific Talos Considerations

| Area | Detail |
|------|--------|
| **Containerd socket** | Minga expects `unix:///run/containerd/containerd.sock` (Talos default). No extra config. |
| **NATS** | Run NATS as a Minga sidecar (`minga sidecar nats`) or external. Talos doesn't manage app processes — use Minga supervisor or systemd. |
| **WireGuard** | If using Minga's built-in overlay, ensure Talos `wireguard` module is loaded (default). Minga manages peers via IPAM. |
| **Persistent data** | `/var/lib/minga` (Raft, logs) and `/var/lib/containerd` survive Talos upgrades. Bind-mount in systemd unit if needed. |
| **Certificate rotation** | Talos rotates its own certs. Minga's NATS connection uses its own certs (configure via `--nats-tls-*` flags). |
| **Upgrades** | `talosctl upgrade --nodes <ip> --image factory.talos.dev/siderolabs/talos:<version>` — Minga systemd unit restarts automatically. |

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `minga: dial unix:///run/containerd/containerd.sock: permission denied` | Ensure Minga runs as root (systemd unit default) or add `UID=0` to containerd socket config |
| `readyz returns 503` | Check `talosctl logs -n <node> -l app=minga` — usually Raft not elected or NATS unreachable |
| `metrics endpoint empty` | Verify `minga-telemetry` crate is linked and `METRICS.gather()` called |
| `WireGuard handshake fails` | Check Talos firewall (`machine.network.firewall`) allows UDP 51820; verify peer `allowedIPs` |

---

## References

- [Talos Documentation](https://docs.siderolabs.com/talos/v1.14/)
- [Minga Design Doc](https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575)
- [ADR 0004: Observability](../docs/adr/0004-observability.md)
- [Wave 9 Plan](Wave-9-PLAN.md)