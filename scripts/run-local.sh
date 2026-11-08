#!/usr/bin/env bash
# Starts a 3-node cluster and the dashboard on localhost.
#   RPC:       127.0.0.1:7001-7003
#   HTTP:      127.0.0.1:8001-8003  (/status, /metrics, /kv/<key>)
#   Dashboard: http://127.0.0.1:8080
# Ctrl-C stops everything. Data lives in ./data.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
CLUSTER=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait' EXIT
for i in 1 2 3; do
  # Restart a node automatically after a dashboard "crash" (it exits 1).
  ( while true; do bin/kvserver -id $i -cluster $CLUSTER -http 127.0.0.1:800$i -data data/$i || true; sleep 1; done ) &
  pids+=($!)
done
bin/kvdash -listen 127.0.0.1:8080 -nodes 1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003 &
pids+=($!)
echo "dashboard: http://127.0.0.1:8080"
wait
