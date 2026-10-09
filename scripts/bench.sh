#!/usr/bin/env bash
# Thread-scaling benchmark of the agent. Usage: scripts/bench.sh [files] [file_kb]
# Env: STORVAULT_ENDPOINT / _ACCESS_KEY / _SECRET_KEY / _BUCKET (defaults: local MinIO)
set -euo pipefail
FILES=${1:-500}; KB=${2:-256}
export STORVAULT_ENDPOINT=${STORVAULT_ENDPOINT:-http://localhost:9000}
export STORVAULT_ACCESS_KEY=${STORVAULT_ACCESS_KEY:-minioadmin} STORVAULT_SECRET_KEY=${STORVAULT_SECRET_KEY:-minioadmin}
export STORVAULT_BUCKET=${STORVAULT_BUCKET:-bench}
AGENT=${AGENT:-agent/build/storvault-agent}
D=$(mktemp -d); trap 'rm -rf "$D"' EXIT
for i in $(seq 1 "$FILES"); do head -c $((KB*1024)) /dev/urandom > "$D/f$i"; done
echo "dataset: $FILES files x ${KB}KB   endpoint: $STORVAULT_ENDPOINT"
printf "%-8s %-10s %-12s\n" threads seconds MB/s
for t in 1 2 4 8 16; do
  $AGENT backup --src "$D" --prefix "bench-$t-$RANDOM/" --threads "$t" --create-bucket |
    python3 -c "import sys,json;d=json.load(sys.stdin);print('%-8s %-10.2f %-12.1f'%($t,d['duration_s'],d['throughput_mb_s']))"
done
