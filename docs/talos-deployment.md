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

## Hetzner Cloud (hcloud) Deployment

**Recommended for cloud deployments** — Hetzner provides Talos as a Public ISO and first-class `hcloud` CLI/Terraform support.

### Why hcloud for Minga?

| Feature | Benefit |
|---------|---------|
| **Talos Public ISO** | Pre-built ISO with qemu-guest-agent (schematic `ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515`) |
| **Native Load Balancer** | Stable VIP for control plane (`hcloud load-balancer`) — no keepalived needed |
| **Private Networks** | Isolated cluster traffic via Hetzner Cloud Networks |
| **Terraform Modules** | `hcloud-talos/talos/hcloud`, `hcloud-k8s/kubernetes/hcloud` for reproducible clusters |
| **Cost-effective** | CPX22 (2 vCPU, 4 GB) ≈ €4.50/mo per node |

### Quick Start: 3-Node Cluster on hcloud

#### Prerequisites
- `hcloud` CLI ≥ 1.45 (`brew install hcloud` or download from GitHub)
- `talosctl` ≥ 1.14
- Hetzner Cloud API token (`HCLOUD_TOKEN`)

#### 1. Create Control Plane Load Balancer (provides VIP)
```bash
# Create LB for control plane API (port 6443)
hcloud load-balancer create --name minga-cp --network-zone eu-central --type lb11 --label 'type=controlplane'
# Note the LB IPv4 address (e.g., 49.12.x.x)
hcloud load-balancer add-service minga-cp --listen-port 6443 --destination-port 6443 --protocol tcp
LB_IP=$(hcloud load-balancer describe minga-cp -o json | jq -r '.public_net.ipv4.ip')
```

#### 2. Generate Talos Config (use LB IP as endpoint)
```bash
talosctl gen config minga-cluster https://${LB_IP}:6443 \
  --output-dir ./talos-config \
  --config-patch @./talos-patches/containerd.yaml \
  --config-patch @./talos-patches/wireguard.yaml
# --with-examples=false --with-docs=false  # Avoid 32KB userdata limit
```

#### 3. Get Talos Public ISO Image ID
```bash
# List available images, find Talos (schematic ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515)
hcloud image list --type iso --selector 'os_flavor=talos'
# Or use the known schematic via factory.talos.dev:
TALOS_ISO_ID=$(hcloud image list --type iso -o json | jq -r '.[] | select(.description | contains("talos")) | .id' | head -1)
```

#### 4. Create Control Plane Nodes (3 nodes for HA)
```bash
for i in 1 2 3; do
  hcloud server create \
    --name "minga-cp${i}" \
    --type cpx22 \
    --image "${TALOS_ISO_ID}" \
    --user-data-from-file ./talos-config/controlplane.yaml \
    --label type=controlplane \
    --location nbg1 \
    --ssh-key <your-ssh-key>  # Only for rescue/debug; Talos has no SSH
done
```

#### 5. Add Control Plane Nodes to Load Balancer
```bash
hcloud load-balancer add-target minga-cp --label-selector 'type=controlplane'
```

#### 6. (Optional) Create Worker Nodes
```bash
for i in 1 2; do
  hcloud server create \
    --name "minga-worker${i}" \
    --type cpx22 \
    --image "${TALOS_ISO_ID}" \
    --user-data-from-file ./talos-config/worker.yaml \
    --label type=worker \
    --location nbg1
done
```

#### 7. Bootstrap etcd (on first control plane node)
```bash
CP1_IP=$(hcloud server ip minga-cp1)
talosctl bootstrap --nodes "${CP1_IP}" --endpoints "${CP1_IP}"
```

#### 8. Deploy Minga Binary (via talosctl, no SSH)
```bash
for NODE in minga-cp1 minga-cp2 minga-cp3 minga-worker1 minga-worker2; do
  NODE_IP=$(hcloud server ip "${NODE}")
  talosctl --nodes "${NODE_IP}" upload ./bin/minga-linux-amd64 /opt/minga/minga
  talosctl --nodes "${NODE_IP}" upload ./scripts/minga.service /etc/systemd/system/minga.service
  talosctl --nodes "${NODE_IP}" run --command "systemctl daemon-reload && systemctl enable --now minga"
done
```

#### 9. Verify
```bash
# Health checks
for NODE in minga-cp1 minga-cp2 minga-cp3; do
  IP=$(hcloud server ip "${NODE}")
  echo "=== ${NODE} (${IP}) ==="
  curl -sf "http://${IP}:2375/healthz" && echo "healthz: OK"
  curl -sf "http://${IP}:2375/readyz" && echo "readyz: OK"
  curl -s  "http://${IP}:2375/metrics" | head -5
done

# Deploy a stack
minga stack deploy -f docker-compose.yml
```

### Single-Node on hcloud (Dev/Test)

```bash
# 1. Create LB (single node still needs stable endpoint)
hcloud load-balancer create --name minga-single --network-zone eu-central --type lb11
hcloud load-balancer add-service minga-single --listen-port 6443 --destination-port 6443 --protocol tcp
LB_IP=$(hcloud load-balancer describe minga-single -o json | jq -r '.public_net.ipv4.ip')

# 2. Generate config
talosctl gen config minga-single https://${LB_IP}:6443 --output-dir ./talos-config --config-patch @./talos-patches/containerd.yaml

# 3. Create server
TALOS_ISO_ID=$(hcloud image list --type iso --selector 'os_flavor=talos' -o json | jq -r '.[0].id')
hcloud server create --name minga-single --type cpx22 --image "${TALOS_ISO_ID}" --user-data-from-file ./talos-config/controlplane.yaml --label type=controlplane

# 4. Add to LB
hcloud load-balancer add-target minga-single --label-selector 'type=controlplane'

# 5. Bootstrap (self)
IP=$(hcloud server ip minga-single)
talosctl bootstrap --nodes "${IP}" --endpoints "${IP}"

# 6. Deploy Minga (same as step 8 above)
```

### Terraform Alternative (Reproducible Clusters)

```hcl
# terraform/main.tf
terraform {
  required_version = ">= 1.5"
  required_providers {
    hcloud = { source = "hetznercloud/hcloud", version = "~> 1.45" }
  }
}

provider "hcloud" { token = var.hcloud_token }

# Talos image (use factory.talos.dev URL or local snapshot)
data "hcloud_image" "talos" {
  type = "iso"
  selectors = { os_flavor = "talos" }
  most_recent = true
}

# Load balancer for control plane
resource "hcloud_load_balancer" "controlplane" {
  name            = "minga-cp"
  load_balancer_type = "lb11"
  network_zone    = "eu-central"
  target_labels   = { type = "controlplane" }
  service {
    listen_port      = 6443
    destination_port = 6443
    protocol         = "tcp"
  }
}

# Control plane nodes (3 for HA)
resource "hcloud_server" "controlplane" {
  count               = 3
  name                = "minga-cp${count.index + 1}"
  server_type         = "cpx22"
  image               = data.hcloud_image.talos.id
  user_data           = file("${path.module}/talos-config/controlplane.yaml")
  labels              = { type = "controlplane" }
  location            = "nbg1"
  ssh_keys            = [var.ssh_key_name]
}

# Worker nodes
resource "hcloud_server" "worker" {
  count     = 2
  name      = "minga-worker${count.index + 1}"
  server_type = "cpx22"
  image       = data.hcloud_image.talos.id
  user_data   = file("${path.module}/talos-config/worker.yaml")
  labels      = { type = "worker" }
  location    = "nbg1"
}

output "controlplane_ips" { value = hcloud_server.controlplane[*].ipv4_address }
output "lb_ip" { value = hcloud_load_balancer.controlplane.ipv4_address }
```

```bash
# Deploy
terraform init && terraform apply
# Then bootstrap:
talosctl bootstrap --nodes $(terraform output -raw controlplane_ips | jq -r '.[0]') --endpoints $(terraform output -raw lb_ip)
```

### Minga-Specific hcloud Notes

| Area | Detail |
|------|--------|
| **Containerd socket** | `unix:///run/containerd/containerd.sock` — works out of box |
| **WireGuard** | Use Talos `machine.network.wireguard` config OR Minga's built-in overlay (both work) |
| **Private networking** | Create `hcloud network` + attach servers; Minga overlay uses private IPs |
| **NATS** | Run as Minga sidecar (`minga sidecar nats`) on one node, or external |
| **Persistent volumes** | Hetzner CSI driver available if needed; Minga local volumes work on local disk |
| **Upgrades** | `talosctl upgrade --nodes <ip> --image factory.talos.dev/siderolabs/talos:v1.14.x` — zero-downtime with HA |
| **Backups** | `hcloud server create-snapshot` before upgrades for instant rollback |

### Cost Estimate (3 CP + 2 Worker, CPX22, Nuremberg)

| Resource | Monthly (EUR) |
|----------|---------------|
| 5 × CPX22 | ~€22.50 |
| 1 × LB11 | ~€5.50 |
| Network | Free (private) |
| **Total** | **~€28/mo** |

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