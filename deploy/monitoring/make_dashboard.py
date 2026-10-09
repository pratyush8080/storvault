#!/usr/bin/env python3
"""Generates grafana/dashboards/storvault.json  (python3 make_dashboard.py)."""
import json, os

DS = {"type": "prometheus", "uid": "prom"}
panels, pid = [], 0


def panel(title, exprs, x, y, w=8, h=8, kind="timeseries", unit=None, legends=None, thresholds=None):
    global pid
    pid += 1
    p = {"id": pid, "type": kind, "title": title, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"refId": chr(65 + i), "expr": e, "datasource": DS, **({"legendFormat": legends[i]} if legends else {})}
                     for i, e in enumerate(exprs)],
         "fieldConfig": {"defaults": {**({"unit": unit} if unit else {}),
                                      **({"thresholds": thresholds} if thresholds else {})}, "overrides": []}}
    panels.append(p)


R = "5m"
panel("Time since last successful backup", ["time() - storvault_last_successful_backup_timestamp_seconds"], 0, 0, 6, 5, "stat", "s",
      thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 43200}, {"color": "red", "value": 86400}]})
panel("Snapshots (hot / cold)", ["storvault_snapshots"], 6, 0, 6, 5, "stat", legends=["{{tier}}"])
panel("Snapshots failing verification", ["storvault_snapshots_failing_verification"], 12, 0, 6, 5, "stat",
      thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]})
panel("Last agent throughput", ["storvault_last_throughput_mb_s"], 18, 0, 6, 5, "stat", "MBs", legends=["{{command}}"])

panel("Throughput (bytes/s)", [f"rate(storvault_bytes_total[{R}])"], 0, 5, 8, 8, unit="Bps", legends=["{{direction}}"])
panel("Object latency p50 / p95 / p99 (ms)",
      [f"histogram_quantile({q}, sum by (le) (rate(storvault_object_latency_ms_bucket[{R}])))" for q in (0.5, 0.95, 0.99)],
      8, 5, 8, 8, unit="ms", legends=["p50", "p95", "p99"])
panel("Errors by class (per s)", [f"rate(storvault_agent_errors_total[{R}])"], 16, 5, 8, 8, legends=["{{type}}"])

panel("Agent S3 retries (per s)", [f"rate(storvault_agent_retries_total[{R}])", f"rate(storvault_job_retries_total[{R}])"], 0, 13, 8, 8,
      legends=["S3 call retries", "job retries {{type}}"])
panel("Jobs finished", [f"sum by (type, status) (increase(storvault_jobs_total[1h]))"], 8, 13, 8, 8, legends=["{{type}} {{status}}"])
panel("Job duration p95 (s)", [f"histogram_quantile(0.95, sum by (le, type) (rate(storvault_job_duration_seconds_bucket[1h])))"], 16, 13, 8, 8,
      unit="s", legends=["{{type}}"])

panel("Storage referenced by tier", ["storvault_storage_bytes"], 0, 21, 12, 8, unit="bytes", legends=["{{tier}}"])
panel("Files processed (per s)", [f"rate(storvault_files_total[{R}])"], 12, 21, 12, 8, legends=["{{result}}"])

d = {"uid": "storvault", "title": "StorVault", "schemaVersion": 39, "version": 1, "refresh": "10s",
     "time": {"from": "now-1h", "to": "now"}, "tags": ["storvault"], "panels": panels}
out = os.path.join(os.path.dirname(__file__), "grafana", "dashboards", "storvault.json")
json.dump(d, open(out, "w"), indent=2)
print("wrote", out)
