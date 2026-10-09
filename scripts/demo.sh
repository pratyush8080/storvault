#!/usr/bin/env bash
# End-to-end demo against the docker-compose stack (API on :8080).
set -euo pipefail
API=${API:-http://localhost:8080}
j() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }
wait_job() { while :; do s=$(curl -s $API/jobs/$1 | j "['status']"); [[ $s == success || $s == failed ]] && { curl -s $API/jobs/$1 | python3 -m json.tool; return; }; sleep 1; done; }
echo "== backup";  id=$(curl -s -X POST $API/jobs/backup -H 'content-type: application/json' -d '{"source_path":"/source"}' | j "['job_id']"); wait_job $id
SNAP=$(curl -s $API/snapshots | j "[0]['id']"); echo "snapshot: $SNAP"
echo "== deep verify"; id=$(curl -s -X POST $API/jobs/verify -H 'content-type: application/json' -d "{\"snapshot_id\":\"$SNAP\",\"deep\":true}" | j "['job_id']"); wait_job $id
echo "== tier to cold (evict hot)"; id=$(curl -s -X POST $API/jobs/tier -H 'content-type: application/json' -d "{\"snapshot_id\":\"$SNAP\",\"evict_hot\":true}" | j "['job_id']"); wait_job $id
echo "== restore from cold"; id=$(curl -s -X POST $API/jobs/restore -H 'content-type: application/json' -d "{\"snapshot_id\":\"$SNAP\",\"dest\":\"/restore/$SNAP\"}" | j "['job_id']"); wait_job $id
echo "== snapshots"; curl -s $API/snapshots | python3 -m json.tool
