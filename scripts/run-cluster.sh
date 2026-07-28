#!/usr/bin/env bash
# Start all nodes defined in CLUSTER config (default: config/cluster.local.toml).
# Usage: ./scripts/run-cluster.sh
#        CLUSTER_CONFIG=config/cluster.local.toml ./scripts/run-cluster.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CLUSTER_CONFIG="${CLUSTER_CONFIG:-config/cluster.local.toml}"
mapfile -t IDS < <(cargo run --quiet --bin raft-node -- --list-nodes)

echo "cluster: $CLUSTER_CONFIG"
echo "nodes: ${IDS[*]}"
echo "Press Ctrl+C to stop."

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

export RUST_LOG="${RUST_LOG:-info}"
for id in "${IDS[@]}"; do
  NODE_ID="$id" cargo run --bin raft-node &
  pids+=($!)
  sleep 0.3
done

wait
