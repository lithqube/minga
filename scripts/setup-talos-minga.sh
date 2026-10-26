#!/usr/bin/env bash
# setup-talos-minga.sh
# Automated Talos + Minga cluster setup for 3-node or single-node deployments.
#
# Usage:
#   ./setup-talos-minga.sh --cluster-size 3 --nodes "10.0.0.1,10.0.0.2,10.0.0.3" --vip 10.0.0.100
#   ./setup-talos-minga.sh --cluster-size 1 --nodes "10.0.0.5" --vip 10.0.0.5
#
# Requirements:
#   - talosctl >= 1.14
#   - minga binary in ./bin/minga-linux-<arch>
#   - SSH access to nodes (for initial Talos install) OR PXE/ISO boot

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="${SCRIPT_DIR}/talos-workdir"
MINGA_BIN="${SCRIPT_DIR}/bin/minga-linux-amd64"  # or arm64
CLUSTER_SIZE=3
NODE_IPS=""
VIP=""
TALOS_VERSION="v1.14.0"
CLUSTER_NAME="minga-cluster"
OUTPUT_DIR="${WORKDIR}/talos-config"
PATCH_DIR="${SCRIPT_DIR}/talos-patches"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err() { echo -e "${RED}[ERR]${NC} $*" >&2; }

usage() {
  cat <<EOF
Usage: $0 [options]
  --cluster-size N    Number of nodes (1 or 3) [default: 3]
  --nodes "ip1,ip2"   Comma-separated node IPs
  --vip IP            Control plane VIP (for 3-node) or node IP (for 1-node)
  --talos-version V   Talos version [default: v1.14.0]
  --cluster-name N    Cluster name [default: minga-cluster]
  --minga-bin PATH    Path to minga binary [default: ./bin/minga-linux-amd64]
  --workdir DIR       Working directory [default: ./talos-workdir]
  -h, --help          Show this help
EOF
  exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --cluster-size) CLUSTER_SIZE="$2"; shift 2 ;;
    --nodes) NODE_IPS="$2"; shift 2 ;;
    --vip) VIP="$2"; shift 2 ;;
    --talos-version) TALOS_VERSION="$2"; shift 2 ;;
    --cluster-name) CLUSTER_NAME="$2"; shift 2 ;;
    --minga-bin) MINGA_BIN="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) err "Unknown option: $1"; usage ;;
  esac
done

# Validate
if [[ -z "$NODE_IPS" || -z "$VIP" ]]; then
  err "Missing required arguments: --nodes and --vip"
  usage
fi

if [[ ! -f "$MINGA_BIN" ]]; then
  err "Minga binary not found at $MINGA_BIN"
  exit 1
fi

if ! command -v talosctl &>/dev/null; then
  err "talosctl not found in PATH"
  exit 1
fi

IFS=',' read -ra NODES <<< "$NODE_IPS"
if [[ ${#NODES[@]} -ne $CLUSTER_SIZE ]]; then
  err "Number of nodes (${#NODES[@]}) doesn't match cluster-size ($CLUSTER_SIZE)"
  exit 1
fi

# Create directories
mkdir -p "$WORKDIR" "$OUTPUT_DIR" "$PATCH_DIR"

# Generate patches
cat > "$PATCH_DIR/containerd.yaml" <<'EOF'
machine:
  containerd:
    # Expose containerd socket to host (default in Talos 1.14+)
    # Minga connects at unix:///run/containerd/containerd.sock
EOF

cat > "$PATCH_DIR/wireguard.yaml" <<'EOF'
machine:
  network:
    wireguard:
      - name: minga-overlay
        privateKey: GENERATE_AT_RUNTIME
        listenPort: 51820
        peers: []
EOF

log "Generating Talos config for $CLUSTER_NAME..."
talosctl gen config "$CLUSTER_NAME" "https://$VIP:6443" \
  --output-dir "$OUTPUT_DIR" \
  --config-patch @"$PATCH_DIR/containerd.yaml" \
  --config-patch @"$PATCH_DIR/wireguard.yaml" \
  --talos-version "$TALOS_VERSION"

# Install Talos on nodes
log "Installing Talos on nodes..."
for NODE in "${NODES[@]}"; do
  log "  Installing on $NODE..."
  if [[ $CLUSTER_SIZE -eq 1 ]]; then
    talosctl apply-config --insecure --nodes "$NODE" --file "$OUTPUT_DIR/controlplane.yaml"
  else
    # First node gets controlplane, others get worker (will be promoted)
    if [[ "$NODE" == "${NODES[0]}" ]]; then
      talosctl apply-config --insecure --nodes "$NODE" --file "$OUTPUT_DIR/controlplane.yaml"
    else
      talosctl apply-config --insecure --nodes "$NODE" --file "$OUTPUT_DIR/worker.yaml"
    fi
  fi
done

# Wait for nodes to come up
log "Waiting for nodes to be ready..."
sleep 30

# Bootstrap etcd (first control plane node)
log "Bootstrapping etcd on ${NODES[0]}..."
talosctl bootstrap --nodes "${NODES[0]}" --endpoints "${NODES[0]}"

# Wait for cluster to stabilize
log "Waiting for cluster to stabilize..."
sleep 20

# Deploy Minga binary and systemd unit
log "Deploying Minga to all nodes..."
for NODE in "${NODES[@]}"; do
  log "  Deploying to $NODE..."
  talosctl --nodes "$NODE" upload "$MINGA_BIN" /opt/minga/minga
  
  # Generate systemd unit with node-specific config
  cat > /tmp/minga.service <<EOF
[Unit]
Description=Minga Node Supervisor
After=network-online.target containerd.service
Wants=network-online.target containerd.service

[Service]
Type=notify
ExecStart=/opt/minga/minga node \\
  --containerd-socket unix:///run/containerd/containerd.sock \\
  --nats-url nats://${NODES[0]}:4222 \\
  --node-id $(echo "$NODE" | tr '.' '-')
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF
  talosctl --nodes "$NODE" upload /tmp/minga.service /etc/systemd/system/minga.service
  talosctl --nodes "$NODE" run --command "systemctl daemon-reload && systemctl enable --now minga"
done

# Wait for Minga to start
log "Waiting for Minga to start..."
sleep 10

# Verify
log "Verifying cluster..."
for NODE in "${NODES[@]}"; do
  log "  Checking $NODE..."
  if talosctl --nodes "$NODE" run --command "curl -sf http://localhost:2375/healthz" >/dev/null; then
    log "    healthz: OK"
  else
    warn "    healthz: FAILED"
  fi
  if talosctl --nodes "$NODE" run --command "curl -sf http://localhost:2375/readyz" >/dev/null; then
    log "    readyz: OK"
  else
    warn "    readyz: FAILED (may need time for Raft election)"
  fi
done

log "Cluster setup complete!"
log "Next steps:"
log "  1. Deploy a stack: minga stack deploy -f docker-compose.yml"
log "  2. Check nodes: minga node ls"
log "  3. View metrics: curl http://${NODES[0]}:2375/metrics"