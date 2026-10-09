# Study guide — resume bullet → code → what to be able to explain

Read in this order: `agent/src/util.*` → `s3client.*` → `threadpool.hpp` → `main.cpp` → `orchestrator/storvault/service.py` → tests.

## Bullet 1 — "C++ multithreaded backup agent + Python orchestration + S3 storage; backup, checksum verification, retry, restore, retention"
| Claim | Where | Be ready to explain |
|---|---|---|
| Multithreaded agent | `threadpool.hpp`, `cmd_backup` in `main.cpp` | Queue + condvar; why `wait_idle` checks `active_` as well as queue; why per-slot writes need no lock |
| Parallel multipart | `upload_file` (atomic part counter) | S3 min part 5 MiB, ≤10,000 parts; CreateMultipart→UploadPart→Complete; abort on failure; memory bound |
| S3 API | `s3client.cpp::perform` | SigV4: canonical request → string-to-sign → HMAC key chain (date→region→service→"aws4_request"); why headers are sorted |
| Checksums | `util.cpp::sha256_file`, manifest, `cmd_verify` | Streaming hash; shallow vs deep verify; why ETag ≠ checksum |
| Restore | `cmd_restore` | tmp file + atomic rename; path-traversal guard; hash before publish |
| Incremental | `--base-manifest` branch | size+mtime quick check (rsync-style); trade-offs (touch, clock skew) |
| Orchestration | `service.py::backup`, `api.py` | Subprocess + JSON contract; exit codes 0/1/2; creds via env not argv |
| Retention | `apply_retention`, `_drop_from_tier` | keep_last / max_age; ref-counting shared objects; never delete the newest |

## Bullet 2 — "MinIO + Ceph RGW via S3 API, lifecycle policies, hot→cold transitions; Docker; Kubernetes with probes, PVCs, config"
| Claim | Where | Explain |
|---|---|---|
| Endpoint-agnostic | `Tier` in `config.py`, `S3Client` | Path-style addressing; why same code runs on MinIO/Ceph/AWS/moto |
| Lifecycle policies | `lifecycle.py`, `scripts/setup_minio_tier.sh` | Expiration, noncurrent versions, abort-incomplete-MPU, Transition rule + remote tier |
| Hot→cold | `service.tier_snapshot` | Copy → deep-verify → evict; why verify *before* evicting; idempotent copy |
| Docker | `deploy/Dockerfile` | Multi-stage build; runtime image has no compiler; non-root user; tests run during build |
| K8s | `deploy/k8s/*.yaml` | liveness (`/healthz`: process alive) vs readiness (`/readyz`: DB+S3 reachable) vs startup probe; PVC for catalog; ConfigMap vs Secret; `Recreate` + single replica because SQLite/RWO; CronJob |

## Bullet 3 — "Exponential backoff, checksum validation, failure injection tests; Prometheus + Grafana"
| Claim | Where | Explain |
|---|---|---|
| Backoff | `util.hpp::backoff_ms`, `service.backoff_delay` | Full jitter formula; thundering herd; cap; retryable vs fatal (`Result::retryable`) |
| Failure injection | `tests/chaos_proxy.py`, `test_integration.py` | Outage mid-backup, connection reset mid-upload, silent corruption, missing manifest (fatal ⇒ not retried), crash recovery |
| Metrics | `metrics.hpp`, `metrics.py`, `deploy/monitoring/` | Counter vs gauge vs histogram; `histogram_quantile` for p95; why last-success timestamp + `time()-x` alert |
| Short-lived agent metrics | `emit_json` + `record_agent` | Agent is a batch process → returns a summary; orchestrator folds it into long-lived metrics (alt: Pushgateway/textfile — `--metrics-file`) |

## Interview questions you should now be able to answer
1. A multipart upload succeeded but the checksum differs at restore — what are the possible causes? (source changed mid-read, bit-rot, bad RAM, proxy corruption, wrong part ordering)
2. Why jitter? Why cap? What's the worst-case total wait for 5 attempts with base 200 ms?
3. How does your thread pool avoid data races? What does TSan check and what can't it catch?
4. Orchestrator crashes mid-job — what state is left and how do you recover?
5. Why can't you trust the S3 ETag as an integrity check?
6. Two incremental snapshots share an object; the older is deleted. What prevents data loss?
7. Liveness vs readiness: what happens if `/healthz` depended on S3 being up?
8. How would you scale this to 1000 hosts? (see "What I'd do at scale" in design.md)
9. What did the thread-scaling benchmark show and why does it flatten? (network/disk bound, per-request latency, server limits)

## Try-it exercises (best way to learn it)
- Run `scripts/bench.sh` against real MinIO; plot threads vs MB/s; add to README.
- Break things: `proxy.set_latency(0.5)` in a test; corrupt a part; kill the agent with `kill -9` mid-run and see `recover_interrupted`.
- Add a feature: client-side AES-GCM encryption in the agent; or Postgres catalog; or `--bwlimit`.
