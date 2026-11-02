#!/usr/bin/env bash
# setup-talos-minga.sh — install Minga (manager + API) on a single Talos node.
#
# The node must be booted into Talos maintenance mode (fresh ISO/PXE boot).
# Minga runs as two Talos ContainerConfig containers pulled from a registry;
# no system extension or custom boot asset is involved. Requires Talos v1.14+.
#
# Usage:
#   ./setup-talos-minga.sh --node 10.0.0.5
#   ./setup-talos-minga.sh --node 10.0.0.5 --image ghcr.io/lithqube/minga:v0.1.1
#
# Flags:
#   --node IP          node address (required; maintenance mode)
#   --image REF        minga image (default: ghcr.io/lithqube/minga:latest;
#                      pin a digest for production)
#   --api-token TOK    Docker-API bearer token (default: generated, printed)
#   --cluster-name N   Talos cluster name (default: minga-cluster)
#   --talos-version V  talosctl gen config version (default: talosctl's own)
#   --skip-kubeconfig  do not fetch the kubeconfig at the end
#
# Multi-node is out of scope here: every manager and worker must share one
# external TLS NATS server and a fixed peer list — see docs/talos-deployment.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

NODE_IP=""
IMAGE="ghcr.io/lithqube/minga:latest"
API_TOKEN=""
CLUSTER_NAME="minga-cluster"
TALOS_VERSION=""
FETCH_KUBECONFIG=1

usage() { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; }

err() { echo "error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --node) NODE_IP="${2:?--node needs an IP}"; shift 2 ;;
        --image) IMAGE="${2:?--image needs a reference}"; shift 2 ;;
        --api-token) API_TOKEN="${2:?--api-token needs a value}"; shift 2 ;;
        --cluster-name) CLUSTER_NAME="${2:?--cluster-name needs a value}"; shift 2 ;;
        --talos-version) TALOS_VERSION="${2:?--talos-version needs a value}"; shift 2 ;;
        --skip-kubeconfig) FETCH_KUBECONFIG=0; shift ;;
        -h | --help) usage; exit 0 ;;
        *) err "unknown argument: $1 (try --help)" ;;
    esac
done

[[ -n "$NODE_IP" ]] || err "--node is required"
command -v talosctl >/dev/null 2>&1 || err "talosctl not found (>= v1.14 required)"
command -v curl >/dev/null 2>&1 || err "curl not found"
if [[ -z "$API_TOKEN" ]]; then
    API_TOKEN="$(openssl rand -hex 16)"
fi

WORKDIR="$(mktemp -d /tmp/minga-talos.XXXXXX)"
echo "workdir: $WORKDIR"

# 1. Base cluster config. The preload Job tolerates the control-plane
#    NoSchedule taint itself, so no patches are needed.
gen_args=(gen config "$CLUSTER_NAME" "https://$NODE_IP:6443"
    --output-dir "$WORKDIR")
if [[ -n "$TALOS_VERSION" ]]; then
    gen_args+=(--talos-version "$TALOS_VERSION")
fi
talosctl "${gen_args[@]}" >/dev/null

# 2. Append the Minga documents (UserVolumes + ContainerConfigs).
sed -e "s|__MINGA_IMAGE__|${IMAGE}|g" \
    -e "s|__MINGA_API_TOKEN__|${API_TOKEN}|g" \
    "${REPO_ROOT}/talos/single-node.yaml" > "$WORKDIR/minga-docs.yaml"
{
    echo "---"
    cat "$WORKDIR/minga-docs.yaml"
} >> "$WORKDIR/controlplane.yaml"

# 3. Validate the assembled config before touching the node.
talosctl validate --mode metal --config "$WORKDIR/controlplane.yaml"

# 4. Apply (maintenance mode is unauthenticated: --insecure), then use the
#    generated talosconfig for everything after.
talosctl apply-config --insecure --nodes "$NODE_IP" --file "$WORKDIR/controlplane.yaml"
export TALOSCONFIG="$WORKDIR/talosconfig"
talosctl config endpoint "$NODE_IP" >/dev/null
talosctl config node "$NODE_IP" >/dev/null

echo "waiting for the Talos API (node reboots into the applied config)..."
for _ in $(seq 1 60); do
    if talosctl version --short >/dev/null 2>&1; then
        break
    fi
    sleep 5
done
talosctl version --short >/dev/null 2>&1 || err "Talos API never came up"

# 5. Bootstrap Kubernetes (also what the preload Job needs).
if ! talosctl get member --namespace cluster 2>/dev/null | grep -q .; then
    talosctl bootstrap --nodes "$NODE_IP" || true
fi

# 6. Wait for the two Minga containers.
echo "waiting for minga-manager and minga-api (first pull can take a minute)..."
for _ in $(seq 1 60); do
    count=$(talosctl get containerstatus 2>/dev/null | grep -E "minga-(manager|api)" | grep -ci "running" || true)
    if [ "$count" = "2" ]; then
        break
    fi
    sleep 5
done
talosctl get containerstatus || true

# 7. Ready check from the host: the manager's metrics endpoint is on 9100.
for _ in $(seq 1 30); do
    if curl -fsS "http://$NODE_IP:9100/readyz" >/dev/null 2>&1; then
        break
    fi
    sleep 5
done
if ! curl -fsS "http://$NODE_IP:9100/readyz"; then
    err "manager /readyz did not come up; check: talosctl logs --namespace taloscontainers minga-manager"
fi
echo

if [[ "$FETCH_KUBECONFIG" = "1" ]]; then
    talosctl kubeconfig --nodes "$NODE_IP" --force
fi

cat <<EOF

Minga is installed on $NODE_IP.
  manager:   readyz http://$NODE_IP:9100/readyz  (metrics on :9100)
  Docker API: http://$NODE_IP:2375  token: $API_TOKEN

Next:
  1. Preload workload images: edit talos/preload-images.yaml, then
     kubectl apply -f talos/preload-images.yaml
  2. Deploy:  MINGA_API_TOKEN=$API_TOKEN minga deploy -c compose.yaml <stack> --api-url http://$NODE_IP:2375
  3. Logs:    talosctl logs --namespace taloscontainers minga-manager   (or minga-api)
EOF
