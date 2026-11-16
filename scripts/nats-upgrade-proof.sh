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

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/minga-nats-upgrade.XXXXXX")"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

read -r client_port monitor_port < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(2):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
print(*(sock.getsockname()[1] for sock in sockets))
PY
)
export MINGA_TEST_NATS_URL="nats://127.0.0.1:${client_port}"
export MINGA_REQUIRE_NATS=1
store_dir="$work_dir/store"
mkdir -p "$store_dir"

start_server() {
  local binary="$1" log="$2"
  "$binary" -js -sd "$store_dir" -a 127.0.0.1 -p "$client_port" -m "$monitor_port" >"$log" 2>&1 &
  server_pid=$!
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:${monitor_port}/healthz?js-enabled-only=true" >/dev/null; then
      return
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      cat "$log" >&2
      echo "nats-server exited before becoming ready" >&2
      return 1
    fi
    sleep 1
  done
  cat "$log" >&2
  echo "nats-server did not become ready" >&2
  return 1
}

stop_server() {
  kill "$server_pid"
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
}

start_server "$old_server" "$work_dir/nats-2.10.22.log"
cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade seed -- --ignored --exact
stop_server

start_server "$new_server" "$work_dir/nats-2.15.3.log"
cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade verify -- --ignored --exact
stop_server

if start_server "$old_server" "$work_dir/nats-2.10.22-downgrade.log"; then
  if cargo test --manifest-path "$repo_root/Cargo.toml" -p minga-bus --test nats_upgrade downgrade_check -- --ignored --exact; then
    echo "NATS downgrade 2.15.3 -> 2.10.22 reopened the store and preserved data"
  else
    echo "NATS downgrade 2.15.3 -> 2.10.22 started, but Minga could not verify the data" >&2
  fi
  stop_server
else
  echo "NATS downgrade 2.15.3 -> 2.10.22 could not reopen the store"
fi

echo "NATS store upgrade 2.10.22 -> 2.15.3 verified"
