#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
old_server="${MINGA_NATS_SERVER_21022_BIN:-}"
new_server="${MINGA_NATS_SERVER_2153_BIN:-}"

require_server() {
  local binary="$1" expected="$2"
  if [[ -z "$binary" || ! -x "$binary" ]]; then
    echo "expected an executable nats-server $expected binary; set the matching MINGA_NATS_SERVER_*_BIN variable" >&2
    exit 1
  fi
  local version
  version="$("$binary" --version 2>&1)"
  if [[ "$version" != *"v$expected"* ]]; then
    echo "expected nats-server v$expected, got: $version" >&2
    exit 1
  fi
}

require_server "$old_server" "2.10.22"
require_server "$new_server" "2.15.3"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/minga-nats-rolling.XXXXXX")"
server_pids=()
cleanup() {
  for pid in "${server_pids[@]}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$work_dir"
}
trap cleanup EXIT

read -r -a ports < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(9):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
print(*(sock.getsockname()[1] for sock in sockets))
PY
)
client_ports=("${ports[@]:0:3}")
monitor_ports=("${ports[@]:3:3}")
route_ports=("${ports[@]:6:3}")
node_names=("minga-roll-$$-1" "minga-roll-$$-2" "minga-roll-$$-3")
route_urls=()
seed_urls=()
for index in 0 1 2; do
  route_urls+=("nats://127.0.0.1:${route_ports[$index]}")
  seed_urls+=("nats://127.0.0.1:${client_ports[$index]}")
done
routes_csv="$(IFS=,; printf '%s' "${route_urls[*]}")"
MINGA_TEST_NATS_URL="$(IFS=,; printf '%s' "${seed_urls[*]}")"
export MINGA_TEST_NATS_URL
export MINGA_REQUIRE_NATS=1

start_node() {
  local index="$1" binary="$2"
  mkdir -p "$work_dir/store-$index"
  "$binary" -js \
    -a 127.0.0.1 \
    -p "${client_ports[$index]}" \
    -m "${monitor_ports[$index]}" \
    -n "${node_names[$index]}" \
    --cluster_name minga-rolling-upgrade \
    --cluster "nats://127.0.0.1:${route_ports[$index]}" \
    --routes "$routes_csv" \
    --store_dir "$work_dir/store-$index" \
    >"$work_dir/server-$index.log" 2>&1 &
  server_pids[index]=$!
}

stop_node() {
  local index="$1" pid="${server_pids[$1]}"
  kill "$pid"
  wait "$pid" 2>/dev/null || true
  server_pids[index]=""
}

wait_for_meta_group() {
  python3 - "${monitor_ports[@]}" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request
ports = sys.argv[1:]
deadline = time.monotonic() + 60
last = []
while time.monotonic() < deadline:
    last = []
    for port in ports:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/jsz", timeout=1) as response:
                jsz = json.load(response)
            last.append(jsz.get("meta_cluster", {}))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            last.append(None)
    if all(meta and meta.get("cluster_size") == 3 and meta.get("leader") for meta in last):
        sys.exit(0)
    time.sleep(0.25)
raise SystemExit(f"JetStream meta group did not reach three peers: {last}")
PY
}

for index in 0 1 2; do
  start_node "$index" "$old_server"
done
wait_for_meta_group
cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade rolling_seed -- --ignored --exact

for index in 0 1 2; do
  echo "Replacing NATS server ${node_names[$index]}: 2.10.22 -> 2.15.3"
  stop_node "$index"
  start_node "$index" "$new_server"
  wait_for_meta_group
  cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade rolling_check -- --ignored --exact
done

cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade rolling_complete -- --ignored --exact
echo "NATS rolling upgrade 2.10.22 -> 2.15.3 verified"
