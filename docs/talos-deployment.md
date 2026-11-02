# Deploying Minga on Talos Linux

**Status:** Deployment target — **not yet validated on a Talos host.**
**Version:** Talos v1.14+

> **Read this before following the guide.**
>
> **Not validated:** nothing here has been run against a Talos host.
>
> **The Minga install step can't work as previously written.** Talos has no shell, no systemd, and no API to copy a binary in and start it: `talosctl` has no `upload` or `run`. Running Minga on Talos needs a **Talos system extension** that ships `minga` and `nats-server`, and that extension doesn't exist yet. Step 6 describes what it must run.
>
> **The Talos-side steps are standard but unverified here:** `talosctl gen config`, `apply-config`, `bootstrap`, and the hcloud commands.
>
> The verified way to run Minga today is a plain Linux host; see the [README quick start](../README.md#quick-start-one-linux-host). For current limits, see [architecture.md](architecture.md).

Talos Linux is an immutable, API-driven Linux distribution purpose-built for Kubernetes — but it runs **any containerd workload**. Since Minga targets containerd directly via gRPC (no kubelet), Talos is an ideal host: single-binary OS, atomic upgrades, no SSH, no package manager, secure by default.

---

## Why Talos for Minga?

| Property | Benefit for Minga |
|----------|-------------------|
| **Immutable OS** | No config drift; Minga would arrive as a system extension (not built yet) |
| **Atomic upgrades** | `talosctl upgrade` → reboot; rollback is one command |
| **No SSH / no shell** | Reduced attack surface; all ops via `talosctl` API |
| **Containerd built-in** | `/run/containerd/containerd.sock` exposed by default — Minga connects directly |
| **WireGuard native** | Talos can run WireGuard itself. Minga's own mesh isn't applied yet, so cross-node container traffic doesn't work either way |
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
  --config-patch @./talos-patches/containerd.yaml
```
- `<control-plane-vip>`: a stable IP/VIP for the control plane (keepalived, metalLB, or cloud LB)
- The containerd patch below is the only one Minga needs. To use Talos's own WireGuard (section 3), add `--config-patch @./talos-patches/wireguard.yaml` to this command.

### 2. Containerd Patch (`talos-patches/containerd.yaml`)
```yaml
machine:
  containerd:
    # Expose containerd socket to host (default in Talos 1.14+)
    # Minga connects to the path /run/containerd/containerd.sock (unverified on Talos)
    # If you need remote containerd access:
    # config:
    #   grpc:
    #     address: /run/containerd/containerd.sock
    #     uid: 0
    #     gid: 0
```

### 3. WireGuard Patch (`talos-patches/wireguard.yaml`) — optional, not used by Minga yet

> Minga doesn't apply its WireGuard mesh in `v0.1.0`, so this patch doesn't give containers cross-node connectivity. You can skip it; the `gen config` commands in this guide leave it out.

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

### 6. Run Minga (open problem: needs a system extension)

Talos can't install a binary and a systemd unit, so Minga has to be packaged as a Talos system extension with an extension service. Until that exists, this step can't be done. Each node's service must run one role:

```bash
# Every manager and worker uses the same external TLS NATS server (certificate-only;
# --nats-token is ignored over TLS). A manager without --nats-url would spawn a
# private loopback sidecar and split the cluster's bus.
export MINGA_NATS_TLS_CA=… MINGA_NATS_TLS_CERT=… MINGA_NATS_TLS_KEY=… MINGA_NATS_TLS_SERVER_NAME=…

# Managers: scheduler + supervisor
minga manager --node-id <n> --bind <node-ip>:7001 --peer <id>=<ip>:7001 [--peer …] \
  --nats-url tls://<nats-host>:4222 --data-dir /var/lib/minga --runtime containerd \
  --containerd-socket /run/containerd/containerd.sock --metrics-bind <node-ip>:9100

# Workers: supervisor only
minga worker --node-id <n> --bind <node-ip>:7001 --peer … --nats-url tls://<nats-host>:4222 \
  --data-dir /var/lib/minga --runtime containerd --metrics-bind <node-ip>:9100

# Docker API: today it is its own Raft voter, so give it an id, list it in every --peer set,
# and give it its own data directory (it must not share the manager's Raft databases)
MINGA_API_TOKEN=… minga api --node-id <n> --bind <node-ip>:7002 --peer … \
  --data-dir /var/lib/minga-api --api-bind <private-ip>:2375
```

What the extension service must provide:
- root with network admin rights (netns, bridges, veth, iptables)
- the containerd socket
- persistent `/var/lib/minga`
- SIGINT on stop (Minga's graceful shutdown)
- workload images preloaded in containerd namespace `minga`

Raft membership is fixed at first start, so start every node together with the full `--peer` list. Use `minga tls init-ca` and `minga tls issue-node` for `--raft-tls-*`.

### 7. Verify

```bash
# Health (node metrics endpoint from --metrics-bind)
curl -s http://<node-ip>:9100/readyz
curl -s http://<node-ip>:9100/metrics | grep '^minga_'

# Docker API (bearer token required when MINGA_API_TOKEN is set)
curl -s -H "Authorization: Bearer $MINGA_API_TOKEN" http://<api-ip>:2375/v1.41/nodes
# The docker CLI sends the token via "HttpHeaders" in ~/.docker/config.json:
#   {"HttpHeaders": {"Authorization": "Bearer <token>"}}
docker -H tcp://<api-ip>:2375 node ls
```

> Keep `:2375` and the Raft ports on a private network or firewall them. The API token is the only thing between the network and container creation.

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

# 4. Run Minga: see step 6 above (needs the system extension)

# 5. Deploy a test stack. `minga deploy` writes the stack through the
# Docker-compatible API and reports placement per service. Point it at the
# API (and pass the token if --api-token is set):
minga deploy -c docker-compose.yml minga-test --api-url http://<node-ip>:2375
# The Docker CLI works against the same API:
docker stack deploy -c docker-compose.yml minga-test --host tcp://<node-ip>:2375
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
  --config-patch @./talos-patches/containerd.yaml
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

#### 8. Run Minga

Same open problem as [step 6](#6-run-minga-open-problem-needs-a-system-extension): it needs a Talos system extension. Put the Raft (`7001`/`7002`), NATS (`4222`), metrics (`9100`) and API (`2375`) ports on the hcloud private network only.

#### 9. Verify
```bash
for NODE in minga-cp1 minga-cp2 minga-cp3; do
  IP=$(hcloud server ip "${NODE}")   # use private IPs if ports are firewalled
  echo "=== ${NODE} (${IP}) ==="
  curl -sf "http://${IP}:9100/healthz" && echo "healthz: OK"
  curl -sf "http://${IP}:9100/readyz" && echo "readyz: OK"
done

# Deploy a stack through the Docker-compatible API on the node running `minga api`:
MINGA_API_TOKEN=… minga deploy -c docker-compose.yml minga-test --api-url http://<api-ip>:2375
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
| **Containerd socket** | Path `/run/containerd/containerd.sock` (no `unix://` prefix); unverified on Talos |
| **WireGuard** | Minga's mesh isn't applied yet; containers on different nodes can't reach each other |
| **Private networking** | Create `hcloud network` + attach servers; Minga overlay uses private IPs |
| **NATS** | A manager without `--nats-url` spawns a loopback-only sidecar. Multi-node: point every manager and worker at one external TLS NATS (`--nats-url tls://…`, `MINGA_NATS_TLS_*`, certificate-only) |
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

4. **Workload images.** Minga isn't a container image. It needs the system extension from step 6. The images your stacks use must be present in containerd namespace `minga`; there is no registry pull yet. How to import them on Talos hasn't been worked out.

---

## Minga-Specific Talos Considerations

| Area | Detail |
|------|--------|
| **Containerd socket** | Minga's default is the path `/run/containerd/containerd.sock`, with no `unix://` prefix. Check which containerd instance Talos exposes there before relying on it. |
| **NATS** | A manager without `--nats-url` spawns `nats-server` (bundled in the release archive) on 127.0.0.1:4222. Multi-node: run one external TLS NATS server and set `--nats-url tls://…` plus `MINGA_NATS_TLS_CA/CERT/KEY/SERVER_NAME` on **every** manager and worker. Over TLS, `--nats-token` is ignored. |
| **WireGuard** | Not used by Minga yet. |
| **Raft** | Static membership: every node must be in every `--peer` list and start together. Use `--raft-tls-ca/cert/key` from `minga tls`. |
| **Persistent data** | `/var/lib/minga` (Raft log, state, secrets, volumes) must persist across Talos upgrades. |
| **Shutdown** | Minga shuts down gracefully on SIGINT and marks the node `Left`; the service must send SIGINT, not SIGTERM. |

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `cannot connect to containerd` | Pass a filesystem path to `--containerd-socket`, not a `unix://` URL. Run as root. |
| `/readyz` returns 503 | The body says why: `raft not ready` (no leader; check that every `--peer` is up) or `bus not connected` (check `--nats-url`, TLS env, token). |
| Deploy succeeds, no tasks placed | No eligible node. Check `docker node ls` for `Ready`/`active`, constraints, and capacity. Unplaceable services get HTTP 409. |
| Containers on different nodes can't talk | Expected in `v0.1.0`: there's no cross-node mesh yet. |

---

## References

- [Talos Documentation](https://docs.siderolabs.com/talos/v1.14/)
- [Minga Design Doc](https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575)
- [ADR 0004: Observability](../docs/adr/0004-observability.md)
- [Architecture notes](architecture.md)