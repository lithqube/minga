# Deploying Minga on Talos Linux

**Status:** **Installable; validated through workload start.** On docker-provisioned Talos v1.14.5: the config assembled and validated (`talosctl validate`), both Minga containers pulled from a registry mirror and ran, the manager booted healthy (NATS sidecar, containerd, scheduler/supervisor loops), `/readyz` answered, the API served with token auth, the preload Job pulled an image into the `minga` namespace with the bundled `ctr`, and an nginx stack was placed, started and listened in its netns (veth, IP, routes, DNAT verified; task→host ICMP worked). **Not yet reproduced:** host→task packet delivery in that nested environment (packets never left the node stack; bare-metal validation pending — the same Minga networking is verified on plain Linux hosts). Bare-metal and hcloud multi-node are not hardware-validated.
**Version:** Talos v1.14+ (older versions lack `ContainerConfig`)

> **How Minga runs on Talos.** Talos v1.14 runs arbitrary OCI images directly via
> [`ContainerConfig`](https://docs.siderolabs.com/talos/v1.14/reference/configuration/container/containerconfig)
> machine-config documents — no system extension, no custom boot asset, no imager.
> Talos pulls `ghcr.io/lithqube/minga` (multi-arch; bundles `nats-server` and `ctr`)
> with CRI and starts the containers privileged with host networking. State lives on
> `UserVolumeConfig` volumes under `/var/mnt`.

---

## Why Talos for Minga?

| Property | Benefit for Minga |
|----------|-------------------|
| **Immutable OS** | No config drift; Minga arrives as two `ContainerConfig` documents in the machine config |
| **Atomic upgrades** | `talosctl upgrade` → reboot; rollback is one command. The Minga image tag survives upgrades |
| **No SSH / no shell** | Reduced attack surface; all ops via `talosctl` API |
| **Containerd built-in** | `/run/containerd/containerd.sock` exposed by default — Minga connects directly |
| **Kubernetes included** | Used exactly once here: the image-preload Job below. Minga workloads don't touch kubelet |
| **Edge/air-gap ready** | ISO/PXE install; the image can come from any registry mirror (`machine.registries`) |

---

## Single-Node Quick Start

The scripted path (`scripts/setup-talos-minga.sh`) does all of this; the manual
steps are documented below it.

### Prerequisites
- `talosctl` ≥ 1.14, `curl`, `openssl`
- One x86_64 or arm64 machine booted into Talos **maintenance mode** (fresh ISO/PXE boot)
- Network access from the node to `ghcr.io` (or to your registry mirror)

### Scripted

```bash
./scripts/setup-talos-minga.sh --node <node-ip>
```

The script:
1. `talosctl gen config` for a single-node cluster named `minga-cluster`.
2. Appends the Minga documents from [`talos/single-node.yaml`](../talos/single-node.yaml)
   (two `UserVolumeConfig`s + two `ContainerConfig`s) to `controlplane.yaml`,
   substituting the image reference (`--image`) and a generated API token (`--api-token`).
3. Validates the assembled config (`talosctl validate --mode metal`).
4. `apply-config --insecure`, waits for the API, bootstraps etcd.
5. Waits for both containers in `talosctl get containerinstancestatus`, then
   checks `http://<node-ip>:9100/readyz` from your host.
6. Fetches the kubeconfig and prints the API token, the preload step and the
   deploy command. Keep the printed workdir path: re-applying that
   `controlplane.yaml` (with any edits) is how you change Minga's config later.

### Manual equivalent

```bash
talosctl gen config minga-cluster https://<node-ip>:6443 --output-dir ./talos-config
# Substitute __MINGA_IMAGE__ and __MINGA_API_TOKEN__ in talos/single-node.yaml,
# append the result to talos-config/controlplane.yaml as extra YAML documents, then:
talosctl validate --mode metal --config ./talos-config/controlplane.yaml
talosctl apply-config --insecure --nodes <node-ip> --file ./talos-config/controlplane.yaml
talosctl bootstrap --nodes <node-ip>
talosctl get containerinstancestatus   # minga-manager, minga-api → running
curl -s http://<node-ip>:9100/readyz   # OK
```

What runs where:

| Container | Role | Why privileged / host network | State |
|-----------|------|-------------------------------|-------|
| `minga-manager` | `manager` (scheduler + supervisor + NATS sidecar) | Creates netns, bridges, veth pairs, iptables DNAT in the host netns | `minga-state` volume → `/var/mnt/minga-state` |
| `minga-api` | `api` (Docker Engine API on `:2375`) | `restricted` profile is enough; host net so Raft peers on loopback work | `minga-api-state` volume → `/var/mnt/minga-api-state` |

Two path rules follow from running Minga in a container while runc runs on
the host. First, `--data-dir` must be the **same path** in the container and
on the host (hence the `/var/mnt/…` destinations above): the OCI specs Minga
writes — per-task `resolv.conf`, staged secrets, named-volume binds — are
resolved by runc against the host filesystem. Second, Compose **bind** mounts
name host paths: they only work if you mount that host path into the manager
container at the identical destination. Named volumes (under the data dir)
need nothing extra.

The API bearer token lives in the machine config — which already contains the
cluster CA keys, so treat it as sensitive regardless. Firewall `:2375`, the Raft
ports (`7001`/`7002`) and `:9100` to your management network.

### Preload workload images

A task pulls a missing image itself with the bundled `ctr` (anonymous access,
default containerd socket; this path is not validated on Talos). Preloading is
still the route for private registries and air-gapped nodes. Talos has no `ctr`
on the host, so preload via the
Kubernetes every Talos cluster already has — the Job uses the `ctr` bundled in
the Minga image:

```bash
talosctl kubeconfig   # the script already did this
# edit the image list in talos/preload-images.yaml, then:
kubectl apply -f talos/preload-images.yaml
kubectl -n kube-system logs job/minga-preload-images
```

Air-gapped: put image tarballs on a `UserVolume` and switch the Job's args to
`["-n", "minga", "images", "import", "/images/<name>.tar"]` with the volume mounted.

### Provision secrets

Minga takes no secret values over its API: a `secrets:` entry with
`external: true` reads a file from the node's store, and a task whose secret
is missing fails to start. Talos has no shell, so
[`talos/provision-secret.yaml`](../talos/provision-secret.yaml) writes the file
with `minga secret put`. The secret ID is `<stack>_<compose secret key>`; the
file's header has the commands. **Not yet run on Talos.**

### Deploy

```bash
MINGA_API_TOKEN=<token> minga deploy -c compose.yaml shop --api-url http://<node-ip>:2375
docker -H tcp://<node-ip>:2375 service ls   # same API (token via HttpHeaders in config.json)
```

---

## Multi-Node

Everything from single-node applies, plus the three standing multi-node rules
(from the [README](../README.md#deployment-topologies)): fixed Raft membership at
first start (`--peer` every node, start together), no cross-node container mesh
yet, and **one external TLS NATS server or cluster for every manager and
worker** — a manager left without `--nats-url` spawns a private loopback sidecar
and splits the bus, and over TLS the connection is certificate-only
(`--nats-token` ignored). For a 3-server NATS cluster, list all three in
`--nats-url` (`tls://nats-1:4222,tls://nats-2:4222,tls://nats-3:4222`), name
all three in `MINGA_NATS_TLS_SERVER_NAME` (`nats-1,nats-2,nats-3`), and add
`--nats-replicas=3` on the managers so losing one NATS server loses no bus state.

Per node, append a `ContainerConfig` (image, `args`, `environment` adjusted) and
a `UserVolumeConfig` to that node's machine config:

```yaml
---
apiVersion: v1alpha1
kind: UserVolumeConfig
name: minga-state
volumeType: directory
---
apiVersion: v1alpha1
kind: ContainerConfig
name: minga-manager   # or minga-worker with the worker role in args
image: ghcr.io/lithqube/minga:vX.Y.Z   # pin by digest
args:
  - manager
  - --node-id=<n>
  - --bind=<node-ip>:7001
  - --peer=1=<ip1>:7001
  - --peer=2=<ip2>:7001            # … every node, ids unique
  - --nats-url=tls://<nats-host>:4222
  # Must be identical in the container and on the host: runc resolves the
  # OCI bind sources minga writes (resolv.conf, secrets, volume binds).
  - --data-dir=/var/mnt/minga-state
  - --runtime=containerd
  - --containerd-socket=/run/containerd/containerd.sock
  - --metrics-bind=<node-ip>:9100
environment:
  - MINGA_NATS_TLS_CA=/etc/minga/nats-ca.pem
  - MINGA_NATS_TLS_CERT=/etc/minga/nats-client.pem
  - MINGA_NATS_TLS_KEY=/etc/minga/nats-client-key.pem
  - MINGA_NATS_TLS_SERVER_NAME=<nats-host>
network:
  mode: host
security:
  profile: privileged
mounts:
  - userVolume:
      name: minga-state
      destination: /var/mnt/minga-state
  - hostPath:
      source: /run/containerd/containerd.sock
      destination: /run/containerd/containerd.sock
  # Netns handles must be visible to runc on the host (shared propagation).
  - hostPath:
      source: /run/netns
      destination: /var/run/netns
      options:
        - rbind
        - rshared
  # The NATS CA/client certificates referenced above. Bake them into an
  # ExtensionServiceConfig-mounted configFile, or place them on the volume.
dependsOn:
  networks:
    - addresses
```

Issue Raft (`--raft-tls-*`) and NATS certificates with `minga tls init-ca` and
`minga tls issue-node`. Preload workload images on **every** node: convert the
preload Job to a DaemonSet.

### Changing Minga's configuration later

Re-`apply-config` the full `controlplane.yaml` (with the documents) after edits.
Do **not** `talosctl patch machineconfig` a `ContainerConfig`: strategic merge
concatenates list fields (mounts, args) instead of replacing them. A config
change restarts the affected container; image pulls retry with backoff.

---

## Hetzner Cloud (hcloud) Deployment

**Not hardware-validated.** The Talos/hcloud steps are standard; the Minga part
is the single/multi-node config above, with one hcloud-specific note: cloud-init
user data has a 32 KiB limit — generate configs with `--with-docs=false --with-examples=false`
so the appended Minga documents fit.

### Why hcloud for Minga?

| Feature | Benefit |
|---------|---------|
| **Talos Public ISO** | Pre-built ISO with qemu-guest-agent (schematic `ce4c980550dd2ab1b17bbf2b08801c7eb59418eafe8f279833297925d67c7515`) |
| **Native Load Balancer** | Stable VIP for the control plane API — no keepalived needed |
| **Private Networks** | Raft (`7001`/`7002`), NATS (`4222`), metrics (`9100`) and the API (`2375`) go here only |
| **Terraform Modules** | `hcloud-talos/talos/hcloud` for reproducible clusters |
| **Cost-effective** | CPX22 (2 vCPU, 4 GB) ≈ €4.50/mo per node |

### Quick Start: 3-Node Cluster on hcloud

#### Prerequisites
- `hcloud` CLI ≥ 1.45, `talosctl` ≥ 1.14, `HCLOUD_TOKEN`

#### 1. Control-plane Load Balancer (VIP)
```bash
hcloud load-balancer create --name minga-cp --network-zone eu-central --type lb11 --label 'type=controlplane'
hcloud load-balancer add-service minga-cp --listen-port 6443 --destination-port 6443 --protocol tcp
LB_IP=$(hcloud load-balancer describe minga-cp -o json | jq -r '.public_net.ipv4.ip')
```

#### 2. Generate Talos config (LB IP as endpoint)
```bash
talosctl gen config minga-cluster https://${LB_IP}:6443 \
  --output-dir ./talos-config --with-docs=false --with-examples=false
# Append the per-node Minga documents (multi-node section above) to
# controlplane.yaml / worker.yaml here, then talosctl validate --mode metal.
```

#### 3. Talos Public ISO image id
```bash
TALOS_ISO_ID=$(hcloud image list --type iso --selector 'os_flavor=talos' -o json | jq -r '.[0].id')
```

#### 4. Control-plane nodes (3 for HA)
```bash
for i in 1 2 3; do
  hcloud server create --name "minga-cp${i}" --type cpx22 --image "${TALOS_ISO_ID}" \
    --user-data-from-file ./talos-config/controlplane.yaml \
    --label type=controlplane --location nbg1
done
hcloud load-balancer add-target minga-cp --label-selector 'type=controlplane'
```

#### 5. Workers (optional)
```bash
for i in 1 2; do
  hcloud server create --name "minga-worker${i}" --type cpx22 --image "${TALOS_ISO_ID}" \
    --user-data-from-file ./talos-config/worker.yaml --label type=worker --location nbg1
done
```

#### 6. Bootstrap etcd (first control-plane node)
```bash
CP1_IP=$(hcloud server ip minga-cp1)
talosctl bootstrap --nodes "${CP1_IP}" --endpoints "${CP1_IP}"
```

#### 7. Preload images and verify
```bash
talosctl kubeconfig
kubectl apply -f talos/preload-images.yaml   # DaemonSet form for multi-node
for NODE in minga-cp1 minga-cp2 minga-cp3; do
  IP=$(hcloud server ip "${NODE}")
  echo "=== ${NODE} (${IP}) ==="
  curl -sf "http://${IP}:9100/readyz" && echo " readyz: OK"
done
MINGA_API_TOKEN=… minga deploy -c docker-compose.yml minga-test --api-url http://<api-ip>:2375
```

### Single-node on hcloud (dev/test)

Same flow with one server and the single-node documents; an LB is still useful
for a stable endpoint. Use `talos/single-node.yaml` via the setup script's
manual equivalent.

---

## Operations on Talos

| Task | Command |
|------|---------|
| Container status | `talosctl get containerinstancestatus` (resources in the `containers` namespace) |
| Image pull status | `talosctl get containerimagestatus` |
| Volumes | `talosctl get volumestatus` (`u-minga-state`, `u-minga-api-state`) |
| Logs | `talosctl logs --namespace taloscontainers minga-manager` (or `minga-api`) |
| Config change | edit + re-`apply-config` the full machine config (not `patch machineconfig`) |
| Minga upgrade | bump the image tag/digest in the documents, re-`apply-config`; every node before relying on mixed versions |

Notes:

- `ContainerConfig` containers are **not** Talos services: `talosctl service`
  doesn't apply. Talos restarts them automatically 5s after exit.
- A stop/restart reaches Minga as SIGTERM/SIGKILL, not the SIGINT its graceful
  node-leave path expects, so a restarted node can linger in `active` until its
  lease expires (~30s) — drain first if you need a clean handoff.
- Talos upgrades (`talosctl upgrade`) keep the machine config: the Minga
  containers come back with the same image reference. This is why digest
  pinning is recommended for production.

## Known limits (unchanged by this guide)

- No cross-node container mesh (WireGuard) yet; published ports are host-local.
- Image pull is a `ctr` subprocess: anonymous registry access only, and it ignores `--containerd-socket`. Preload images from private registries.
- Placement pauses if a worker wins the Raft election.
- Talos compliance certifications (FIPS, SOC 2, VS-NfD, NATO) are **not assessed** — which is not the same as not applicable.

## Troubleshooting

| Symptom | Cause → Fix |
|---------|-------------|
| `failed to start container: network setup: ip … No such file or directory` | The image lacks `iproute2`/`iptables`/`procps` — the release image bundles them; a custom-built image must too |
| `failed to fulfil mount request: open /var/… no such file or directory` | Path identity broken: `--data-dir` (or a Compose bind source) differs between container and host. Use the `/var/mnt/<volume>` layout from the examples |
| `namespace path: lstat /var/run/netns/…: no such file or directory` | `/run/netns` not mounted into the manager container with `rshared` — runc on the host can't see the handle |
| `ctr pull <ref> failed` | The auto-pull could not reach the registry or needs credentials. Preload it (`talos/preload-images.yaml`). Minga normalizes short refs to `docker.io/library/…`; the preload stores exactly that |
| Container stuck `pulling` in `containerimagestatus` | Node can't reach the registry. For a private/mirror registry configure `machine.registries.mirrors` (and `tls.insecureSkipVerify` only if you must) |
| `cannot connect to containerd` | Pass a filesystem path to `--containerd-socket`, not a `unix://` URL; the mount in the document must match |
| `/readyz` returns 503 | The body says why: `raft not ready` (no leader; check that every `--peer` is up) or `bus not connected` (check `--nats-url`, TLS env) |
| Deploy succeeds, no tasks placed | No eligible node. Check `/v1.41/nodes` for `Ready`/`active`, constraints, and capacity. Unplaceable services get HTTP 409 |
| Containers on different nodes can't talk | Expected for now: there's no cross-node mesh yet |

---

## References

- [Talos `ContainerConfig`](https://docs.siderolabs.com/talos/v1.14/reference/configuration/container/containerconfig), [`UserVolumeConfig`](https://docs.siderolabs.com/talos/v1.14/reference/configuration/block/uservolumeconfig), [registry mirrors](https://docs.siderolabs.com/talos/v1.14/configure-your-talos-cluster/images-container-runtime/pull-through-cache)
- [Hetzner Talos](https://docs.siderolabs.com/talos/v1.14/talos-guides/install/cloud-platforms/hetzner)
- [Architecture notes](architecture.md)
