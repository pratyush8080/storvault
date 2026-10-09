#!/usr/bin/env bash
# Manual failure injection against the compose stack: stop MinIO mid-backup, then bring it back.
set -euo pipefail
API=${API:-http://localhost:8080}; C="docker compose -f deploy/docker-compose.yml"
curl -s -X POST $API/jobs/backup -H 'content-type: application/json' -d '{"source_path":"/source"}'; echo
sleep 1; echo ">> stopping hot tier for 10s"; $C stop minio; sleep 10; echo ">> starting hot tier"; $C start minio
echo "Watch: curl $API/jobs | python3 -m json.tool   (attempts>1 and status=success => recovered)"
echo "Network faults without docker: tests/chaos_proxy.py   |   with tc: sudo tc qdisc add dev eth0 root netem loss 10% delay 100ms"
