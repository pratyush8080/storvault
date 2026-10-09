# StorVault

Linux backup & recovery platform: a **C++ multithreaded backup agent** + **Python orchestration service** on
**S3-compatible object storage** (MinIO hot tier, Ceph RGW cold tier), with checksum verification, retry/backoff,
restore, retention, tiering, failure-injection tests, and Prometheus/Grafana observability. Docker + Kubernetes ready.

```
 FastAPI orchestrator ──► catalog (SQLite/Postgres)         APScheduler: per-policy cron + maintenance
        │ spawns (subprocess, JSON summary, retry+backoff)
        ▼
 C++ agent: thread pool · multipart (parallel parts) · streaming SHA-256 · SigV4 over libcurl · retry+jitter
        │ S3 API
        ▼
 MinIO (hot) ── orchestrator tiering / lifecycle rules ──► Ceph RGW (cold)
 Prometheus ◄── /metrics ── Grafana dashboard + alert rules
```

## Install (Ubuntu 22.04/24.04 or WSL2)
```bash
sudo apt install -y build-essential cmake ninja-build libssl-dev libcurl4-openssl-dev python3 python3-venv python3-pip
python3 -m venv .venv && source .venv/bin/activate && pip install -r orchestrator/requirements-dev.txt
# for the full stack:  Docker + Docker Compose;  for k8s:  kubectl + kind
```
No AWS SDK needed: the agent implements SigV4 itself (libcurl + OpenSSL).

## Run the tests (no Docker needed — uses `moto` S3 servers + a chaos proxy)
```bash
make test        # builds the agent, runs C++ unit tests + 19 pytest integration/failure-injection/API tests
make tsan        # ThreadSanitizer build of the unit tests
```

## Run the full stack
```bash
make up                                   # MinIO + cold tier + StorVault + Prometheus + Grafana
scripts/demo.sh                           # backup → deep verify → tier to cold → restore from cold
open http://localhost:8080/docs           # API     |  :3000 Grafana (admin/admin)  |  :9090 Prometheus
scripts/chaos.sh                          # stop MinIO mid-backup, watch it recover
scripts/bench.sh                          # thread-scaling benchmark (put numbers in your README!)
```
Real Ceph RGW as cold tier: `docker compose -f deploy/docker-compose.yml --profile ceph up -d ceph`, then set
`SV_COLD_ENDPOINT=http://host.docker.internal:8000 SV_COLD_ACCESS_KEY=cephadmin SV_COLD_SECRET_KEY=cephadmin123`.

## Kubernetes (kind)
```bash
make k8s        # builds image, loads into kind, applies deploy/k8s/*, creates monitoring configmaps
kubectl -n storvault delete pod -l app=storvault    # pod comes back; catalog persists on the PVC
```

## Agent CLI
```
storvault-agent backup  --src DIR --prefix snap-1/ [--base-manifest prev.tsv] [--threads 8] [--part-size-mb 8]
storvault-agent verify  --manifest-key snap-1/manifest.tsv [--deep]
storvault-agent restore --manifest-key snap-1/manifest.tsv --dest DIR
storvault-agent list    --prefix snap-1/
# creds via --endpoint/--bucket/--access-key/--secret-key or STORVAULT_* env; JSON summary on stdout
```

## API
`POST /jobs/{backup,restore,verify,tier,retention}` → 202 `{job_id}` · `GET /jobs[/{id}]` · `GET /snapshots` ·
`DELETE /snapshots/{id}` · `POST/GET/DELETE /policies` · `/healthz` `/readyz` `/metrics`

```bash
curl -X POST localhost:8080/policies -H 'content-type: application/json' \
  -d '{"name":"nightly","source_path":"/source","cron":"0 2 * * *","keep_last":7,"tier_after_days":3}'
```

## Layout
```
agent/          C++17: s3client (SigV4), threadpool, retry/backoff, metrics, main (backup/verify/restore) + tests
orchestrator/   Python: api, service (core logic), db (catalog), lifecycle, metrics
deploy/         Dockerfile (multi-stage), docker-compose, k8s manifests, Prometheus rules, Grafana dashboard
tests/          pytest: unit, integration, failure injection (chaos_proxy.py), API
scripts/        demo, chaos, bench, MinIO tiering, k8s-up      docs/   design.md, STUDY_GUIDE.md
```
