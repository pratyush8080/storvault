"""Prometheus metrics for the orchestrator (served on /metrics)."""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

JOBS = Counter("storvault_jobs_total", "Jobs finished", ["type", "status"], registry=REGISTRY)
JOB_DURATION = Histogram("storvault_job_duration_seconds", "Job wall time", ["type"],
                         buckets=(1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600), registry=REGISTRY)
JOB_RETRIES = Counter("storvault_job_retries_total", "Orchestrator-level job retries", ["type"], registry=REGISTRY)
BYTES = Counter("storvault_bytes_total", "Bytes moved by agent", ["direction"], registry=REGISTRY)
FILES = Counter("storvault_files_total", "Files processed by agent", ["result"], registry=REGISTRY)
AGENT_ERRORS = Counter("storvault_agent_errors_total", "Agent errors by class", ["type"], registry=REGISTRY)
AGENT_RETRIES = Counter("storvault_agent_retries_total", "S3 call retries inside the agent", registry=REGISTRY)
THROUGHPUT = Gauge("storvault_last_throughput_mb_s", "Throughput of last agent run", ["command"], registry=REGISTRY)
OBJ_LATENCY = Histogram("storvault_object_latency_ms", "Per-object agent latency (ms)", ["command"],
                        buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000), registry=REGISTRY)
LAST_SUCCESS = Gauge("storvault_last_successful_backup_timestamp_seconds", "Unix time of last good backup",
                     registry=REGISTRY)
STORAGE = Gauge("storvault_storage_bytes", "Logical bytes referenced per tier", ["tier"], registry=REGISTRY)
SNAPSHOTS = Gauge("storvault_snapshots", "Snapshots by tier presence", ["tier"], registry=REGISTRY)
VERIFY_FAIL = Gauge("storvault_snapshots_failing_verification", "Snapshots whose last verify failed",
                    registry=REGISTRY)


def record_agent(command: str, r: dict):
    """Fold one agent JSON summary into the metrics."""
    BYTES.labels("up").inc(r.get("bytes_uploaded", 0))
    BYTES.labels("down").inc(r.get("bytes_downloaded", 0))
    FILES.labels("ok").inc(r.get("files_ok", 0))
    FILES.labels("unchanged").inc(r.get("files_unchanged", 0))
    FILES.labels("failed").inc(r.get("files_failed", 0))
    AGENT_RETRIES.inc(r.get("retries", 0))
    for t in ("retryable", "fatal", "checksum"):
        AGENT_ERRORS.labels(t).inc(r.get(f"errors_{t}", 0))
    THROUGHPUT.labels(command).set(r.get("throughput_mb_s", 0))
    # Re-observe the agent's histogram: each bucket's count is observed just under its upper bound.
    bounds, counts = r.get("latency_bounds_ms", []), r.get("latency_buckets", [])
    h = OBJ_LATENCY.labels(command)
    for i, n in enumerate(counts):
        v = (bounds[i] * 0.99) if i < len(bounds) else (bounds[-1] * 1.5 if bounds else 0)
        for _ in range(int(n)):
            h.observe(v)


def render() -> bytes:
    return generate_latest(REGISTRY)
