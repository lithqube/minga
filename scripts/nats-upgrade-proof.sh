#!/usr/bin/env bash
# Upgrade proof for Phase 14 plan 14-04: a JetStream store written by an
# older nats-server keeps Minga's data when the server binary is replaced.
#
#   scripts/nats-upgrade-proof.sh OLD_NATS_SERVER NEW_NATS_SERVER [single|cluster|all]
#
# single:  seed a store on OLD, restart it on NEW, probe and verify, then try
#          a downgrade back to OLD and report the result (not a failure).
# cluster: three OLD servers with R3 streams, replaced by NEW one at a time,
#          with a probe after each replacement and a verify at the end.
# The tests it drives are the #[ignore]d ones in
# crates/minga-bus/tests/nats_upgrade.rs. Set KEEP=1 to keep the store dirs.
set -euo pipefail

OLD=${1:?old nats-server binary}
NEW=${2:?new nats-server binary}
MODE=${3:-all}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/nats-upgrade.XXXXXX")

cleanup() {
  for pidfile in "$WORK"/*.pid; do
    [[ -e "$pidfile" ]] && kill "$(cat "$pidfile")" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ "${KEEP:-0}" == 1 ]]; then echo "kept $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

echo "old: $("$OLD" --version)   new: $("$NEW" --version)"
cargo test -q -p minga-bus --test nats_upgrade --no-run

# run_test NAME URL [REPLICAS]
run_test() {
  MINGA_TEST_NATS_URL=$2 MINGA_UPGRADE_REPLICAS=${3:-1} \
    cargo test -q -p minga-bus --test nats_upgrade -- --ignored --exact "$1"
}

# start NAME BIN INDEX [cluster]: server INDEX uses client port 14400+INDEX,
# route port 14500+INDEX, monitor port 14600+INDEX, and $WORK/NAME as store.
start() {
  local name=$1 bin=$2 i=$3
  local args=(-js -n "$name" -p $((14400 + i)) -m $((14600 + i)) -sd "$WORK/$name")
  if [[ "${4:-}" == cluster ]]; then
    args+=(--cluster_name minga-upgrade --cluster "nats://127.0.0.1:$((14500 + i))"
      --routes "nats://127.0.0.1:14501,nats://127.0.0.1:14502,nats://127.0.0.1:14503")
  fi
  mkdir -p "$WORK/$name"
  "$bin" "${args[@]}" >>"$WORK/$name.log" 2>&1 &
  echo $! >"$WORK/$name.pid"
}

# healthy NAME INDEX: wait until the server reports itself healthy, or fail.
healthy() {
  local pid
  pid=$(cat "$WORK/$1.pid")
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$((14600 + $2))/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$1 exited:"; tail -20 "$WORK/$1.log"; return 1
    fi
    sleep 0.5
  done
  echo "$1 not healthy after 60s:"; tail -20 "$WORK/$1.log"; return 1
}

stop() {
  local pid
  pid=$(cat "$WORK/$1.pid")
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -f "$WORK/$1.pid"
}

single() {
  local url=nats://127.0.0.1:14401
  echo "== single server: seed on old"
  start s1 "$OLD" 1 && healthy s1 1
  run_test upgrade_seed "$url"
  stop s1

  echo "== single server: restart on new"
  start s1 "$NEW" 1 && healthy s1 1
  run_test upgrade_probe "$url"
  run_test upgrade_verify "$url"
  stop s1

  echo "== single server: downgrade back to old (reported, not required)"
  start s1 "$OLD" 1
  if healthy s1 1 && run_test upgrade_probe "$url"; then
    echo "DOWNGRADE RESULT: old server opened the upgraded store and the probe passed"
  else
    echo "DOWNGRADE RESULT: failed; server log:"; tail -20 "$WORK/s1.log"
  fi
  stop s1
}

cluster() {
  echo "== cluster: three old servers, R3 streams"
  for i in 1 2 3; do start "c$i" "$OLD" "$i" cluster; done
  for i in 1 2 3; do healthy "c$i" "$i"; done
  run_test upgrade_seed nats://127.0.0.1:14401 3

  for i in 1 2 3; do
    echo "== cluster: replace c$i with new"
    local started=$SECONDS
    stop "c$i"
    start "c$i" "$NEW" "$i" cluster && healthy "c$i" "$i"
    echo "c$i healthy on new after $((SECONDS - started))s"
    run_test upgrade_probe "nats://127.0.0.1:$((14400 + i))" 3
  done
  run_test upgrade_verify nats://127.0.0.1:14401 3
  for i in 1 2 3; do stop "c$i"; done
}

case "$MODE" in
  single) single ;;
  cluster) cluster ;;
  all) single; cluster ;;
  *) echo "unknown mode $MODE"; exit 2 ;;
esac
echo "UPGRADE PROOF PASSED ($MODE)"
