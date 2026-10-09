"""Core orchestration logic: runs the C++ agent, records results in the catalog, tiers and prunes snapshots."""
import datetime as dt
import json
import logging
import os
import random
import subprocess
import tempfile
import time
import uuid

from sqlalchemy import func

from . import metrics, s3util
from .config import Settings
from .db import Job, Policy, Snapshot, SnapshotObject, make_session_factory, utcnow

log = logging.getLogger("storvault")


def backoff_delay(attempt: int, base: float, cap: float, rng=random) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2**attempt))."""
    return rng.uniform(0, min(cap, base * (2 ** attempt)))


class AgentError(Exception):
    def __init__(self, msg, retryable=True, result=None):
        super().__init__(msg)
        self.retryable, self.result = retryable, result


class Service:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.Session, self.engine = make_session_factory(self.s.db_url)
        os.makedirs(self.s.work_dir, exist_ok=True)

    # ------------------------------------------------------------------ agent
    def _agent_cmd(self, command: str, tier_name: str, extra: list[str]) -> tuple[list[str], dict]:
        t = self.s.tier(tier_name)
        cmd = [self.s.agent_bin, command, "--endpoint", t.endpoint, "--bucket", t.bucket,
               "--region", t.region, "--threads", str(self.s.agent_threads)] + extra
        env = dict(os.environ, STORVAULT_ACCESS_KEY=t.access_key, STORVAULT_SECRET_KEY=t.secret_key)  # keys never on argv
        return cmd, env

    def run_agent_once(self, command: str, tier_name: str, extra: list[str]) -> dict:
        cmd, env = self._agent_cmd(command, tier_name, extra)
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=self.s.agent_timeout_s)
        except subprocess.TimeoutExpired:
            raise AgentError("agent timed out", retryable=True)
        except FileNotFoundError:
            raise AgentError(f"agent binary not found: {self.s.agent_bin}", retryable=False)
        out = p.stdout.strip().splitlines()
        result = None
        if out:
            try:
                result = json.loads(out[-1])
            except json.JSONDecodeError:
                pass
        if p.returncode == 0 and result:
            return result
        if p.returncode == 2:
            raise AgentError(f"agent usage error: {p.stderr.strip()[:300]}", retryable=False)
        errs = (result or {}).get("errors", [])
        # Fatal if the agent saw only non-retryable problems (e.g. bad credentials, checksum mismatch).
        retryable = bool(result is None or result.get("errors_retryable", 0) > 0)
        if result and result.get("errors_checksum", 0) > 0 and command in ("verify", "restore"):
            retryable = False
        raise AgentError(f"agent rc={p.returncode}: {errs[:3] or p.stderr.strip()[:300]}", retryable, result)

    def run_agent(self, command: str, tier_name: str, extra: list[str], job_id: int | None = None) -> dict:
        """Run the agent with orchestrator-level retry (exponential backoff + jitter)."""
        last: AgentError | None = None
        for attempt in range(self.s.job_max_attempts):
            if job_id:
                self._job_update(job_id, attempts=attempt + 1)
            try:
                res = self.run_agent_once(command, tier_name, extra)
                metrics.record_agent(command, res)
                return res
            except AgentError as e:
                last = e
                if e.result:
                    metrics.record_agent(command, e.result)
                if not e.retryable or attempt + 1 >= self.s.job_max_attempts:
                    break
                metrics.JOB_RETRIES.labels(command).inc()
                d = backoff_delay(attempt, self.s.backoff_base_s, self.s.backoff_cap_s)
                log.warning("%s failed (%s); retry %d in %.1fs", command, e, attempt + 1, d)
                time.sleep(d)
        raise last  # type: ignore[misc]

    # ------------------------------------------------------------------ jobs
    def create_job(self, type_: str, params: dict, snapshot_id: str | None = None) -> int:
        with self.Session() as s:
            j = Job(type=type_, params=json.dumps(params), snapshot_id=snapshot_id)
            s.add(j); s.commit()
            return j.id

    def _job_update(self, job_id: int, **kw):
        with self.Session() as s:
            j = s.get(Job, job_id)
            for k, v in kw.items():
                setattr(j, k, json.dumps(v) if k == "result" else v)
            s.commit()

    def execute_job(self, job_id: int):
        with self.Session() as s:
            j = s.get(Job, job_id)
            type_, params = j.type, json.loads(j.params or "{}")
        self._job_update(job_id, status="running", started_at=utcnow())
        t0 = time.time()
        try:
            fn = {"backup": self.backup, "restore": self.restore, "verify": self.verify,
                  "tier": self.tier_snapshot, "retention": self.apply_retention}[type_]
            result = fn(job_id=job_id, **params)
            self._job_update(job_id, status="success", result=result, finished_at=utcnow())
            metrics.JOBS.labels(type_, "success").inc()
        except Exception as e:  # noqa: BLE001
            log.exception("job %s failed", job_id)
            self._job_update(job_id, status="failed", error=str(e)[:2000], finished_at=utcnow())
            metrics.JOBS.labels(type_, "failed").inc()
        finally:
            metrics.JOB_DURATION.labels(type_).observe(time.time() - t0)
            self.refresh_storage_metrics()

    def recover_interrupted(self) -> list[int]:
        """Crash recovery: jobs left 'running' by a dead orchestrator are marked interrupted; their
        half-written snapshots are marked failed (the agent writes the manifest last, so a snapshot
        without a manifest is never restorable and never trusted)."""
        ids = []
        with self.Session() as s:
            for j in s.query(Job).filter(Job.status.in_(["running", "queued"])).all():
                j.status, j.error, j.finished_at = "interrupted", "orchestrator restarted mid-job", utcnow()
                ids.append(j.id)
            for sn in s.query(Snapshot).filter(Snapshot.status == "running").all():
                sn.status = "failed"
            s.commit()
        return ids

    # ------------------------------------------------------------------ backup
    def backup(self, source_path: str, policy_name: str | None = None, verify: bool = True,
               job_id: int | None = None) -> dict:
        if not os.path.isdir(source_path):
            raise ValueError(f"source_path not a directory: {source_path}")
        snap_id = f"snap-{utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        prefix = snap_id + "/"
        hot = self.s.hot
        s3c = s3util.client(hot)
        s3util.ensure_bucket(s3c, hot.bucket)

        # incremental: base on the newest complete snapshot of this source that still lives in hot
        base_file = None
        with self.Session() as s:
            prev = (s.query(Snapshot).filter_by(source_path=source_path, status="complete", hot_present=True)
                    .order_by(Snapshot.created_at.desc()).first())
            s.add(Snapshot(id=snap_id, policy_name=policy_name, source_path=source_path, status="running"))
            s.commit()
            if job_id:
                s.get(Job, job_id).snapshot_id = snap_id; s.commit()
        if prev:
            try:
                text, _ = s3util.read_manifest(s3c, hot.bucket, prev.manifest_key)
                base_file = os.path.join(self.s.work_dir, f"base-{snap_id}.tsv")
                with open(base_file, "w") as f:
                    f.write(text)
            except Exception as e:  # noqa: BLE001
                log.warning("cannot load base manifest, doing full backup: %s", e)

        extra = ["--src", source_path, "--prefix", prefix, "--create-bucket"]
        if base_file:
            extra += ["--base-manifest", base_file]
        try:
            res = self.run_agent("backup", "hot", extra, job_id)
        except Exception:
            with self.Session() as s:
                s.get(Snapshot, snap_id).status = "failed"; s.commit()
            raise
        finally:
            if base_file and os.path.exists(base_file):
                os.remove(base_file)

        manifest_key = res["manifest_key"]
        _, entries = s3util.read_manifest(s3c, hot.bucket, manifest_key)
        with self.Session() as s:
            sn = s.get(Snapshot, snap_id)
            sn.status, sn.manifest_key, sn.hot_present = "complete", manifest_key, True
            sn.file_count = len(entries)
            sn.bytes_total = sum(e["size"] for e in entries)
            sn.bytes_uploaded = res.get("bytes_uploaded", 0)
            keys = {e["key"]: e["size"] for e in entries}
            keys[manifest_key] = 0
            s.bulk_save_objects([SnapshotObject(snapshot_id=snap_id, key=k, size=v) for k, v in keys.items()])
            s.commit()
        metrics.LAST_SUCCESS.set(time.time())
        out = {"snapshot_id": snap_id, "files": res["files_total"], "uploaded": res["files_ok"],
               "unchanged": res["files_unchanged"], "bytes_uploaded": res["bytes_uploaded"],
               "throughput_mb_s": res["throughput_mb_s"], "retries": res["retries"]}
        if verify:
            out["verify"] = self.verify(snap_id, deep=False, tier="hot", job_id=job_id)
        return out

    # ------------------------------------------------------------------ verify / restore
    def _tier_for(self, sn: Snapshot, requested: str | None) -> str:
        if requested:
            return requested
        return "hot" if sn.hot_present else "cold"

    def verify(self, snapshot_id: str, deep: bool = False, tier: str | None = None, job_id: int | None = None) -> dict:
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            if not sn or sn.status != "complete":
                raise ValueError("snapshot not found or not complete")
            tier = self._tier_for(sn, tier)
            mk = sn.manifest_key
        extra = ["--manifest-key", mk] + (["--deep"] if deep else [])
        try:
            res, ok = self.run_agent("verify", tier, extra, job_id), True
        except AgentError as e:
            if e.result is None:
                raise
            res, ok = e.result, False
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            sn.last_verified_at, sn.last_verify_ok = utcnow(), ok
            s.commit()
        if not ok:
            raise AgentError(f"verification FAILED on {tier}: {res.get('errors', [])[:5]}", retryable=False, result=res)
        return {"snapshot_id": snapshot_id, "tier": tier, "deep": deep, "files_ok": res["files_ok"]}

    def restore(self, snapshot_id: str, dest: str, tier: str | None = None, job_id: int | None = None) -> dict:
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            if not sn or sn.status != "complete":
                raise ValueError("snapshot not found or not complete")
            tier = self._tier_for(sn, tier)
            mk = sn.manifest_key
        res = self.run_agent("restore", tier, ["--manifest-key", mk, "--dest", dest], job_id)
        return {"snapshot_id": snapshot_id, "tier": tier, "dest": dest, "files": res["files_ok"],
                "bytes": res["bytes_downloaded"], "throughput_mb_s": res["throughput_mb_s"]}

    # ------------------------------------------------------------------ tiering (hot -> cold)
    def _copy_object(self, src, dst, sb, db, key):
        """Stream src->dst, preserving user metadata; verify size afterwards."""
        head = src.head_object(Bucket=sb, Key=key)
        try:   # skip if the cold copy already matches (idempotent / resumable)
            h2 = dst.head_object(Bucket=db, Key=key)
            if h2["ContentLength"] == head["ContentLength"] and h2.get("Metadata", {}).get("sha256") == head.get("Metadata", {}).get("sha256"):
                return 0
        except Exception:  # noqa: BLE001
            pass
        body = src.get_object(Bucket=sb, Key=key)["Body"]
        dst.upload_fileobj(body, db, key, ExtraArgs={"Metadata": head.get("Metadata", {})})
        h2 = dst.head_object(Bucket=db, Key=key)
        if h2["ContentLength"] != head["ContentLength"]:
            raise AgentError(f"tier copy size mismatch for {key}", retryable=False)
        return head["ContentLength"]

    def tier_snapshot(self, snapshot_id: str, evict_hot: bool | None = None, job_id: int | None = None) -> dict:
        evict = self.s.evict_hot_after_tiering if evict_hot is None else evict_hot
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            if not sn or sn.status != "complete" or not sn.hot_present:
                raise ValueError("snapshot must be complete and present in hot tier")
            keys = [o.key for o in sn.objects]
        hot, cold = self.s.hot, self.s.cold
        hc, cc = s3util.client(hot), s3util.client(cold)
        s3util.ensure_bucket(cc, cold.bucket)
        moved = 0
        for k in keys:
            for attempt in range(self.s.job_max_attempts):
                try:
                    moved += self._copy_object(hc, cc, hot.bucket, cold.bucket, k); break
                except AgentError:
                    raise
                except Exception:  # noqa: BLE001
                    if attempt + 1 >= self.s.job_max_attempts:
                        raise
                    time.sleep(backoff_delay(attempt, self.s.backoff_base_s, self.s.backoff_cap_s))
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id); sn.cold_present, sn.cold_at = True, utcnow(); s.commit()
        # Integrity gate BEFORE evicting hot copies: deep-verify the cold copy.
        v = self.verify(snapshot_id, deep=True, tier="cold", job_id=job_id)
        evicted = 0
        if evict:
            evicted = self._drop_from_tier(snapshot_id, "hot")
        return {"snapshot_id": snapshot_id, "bytes_copied": moved, "cold_verified": v["files_ok"], "hot_objects_evicted": evicted}

    def _drop_from_tier(self, snapshot_id: str, tier_name: str) -> int:
        """Delete objects of a snapshot from one tier, but keep keys still referenced by other snapshots there."""
        flag = Snapshot.hot_present if tier_name == "hot" else Snapshot.cold_present
        t = self.s.tier(tier_name); c = s3util.client(t)
        with self.Session() as s:
            keys = [o.key for o in s.query(SnapshotObject).filter_by(snapshot_id=snapshot_id)]
            still = {r[0] for r in s.query(SnapshotObject.key)
                     .join(Snapshot, Snapshot.id == SnapshotObject.snapshot_id)
                     .filter(SnapshotObject.snapshot_id != snapshot_id, Snapshot.status == "complete", flag.is_(True),
                             SnapshotObject.key.in_(keys)).all()} if keys else set()
        n = 0
        for k in keys:
            if k not in still:
                c.delete_object(Bucket=t.bucket, Key=k); n += 1
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            if sn:
                if tier_name == "hot": sn.hot_present = False
                else: sn.cold_present = False
                s.commit()
        return n

    # ------------------------------------------------------------------ retention
    def delete_snapshot(self, snapshot_id: str) -> dict:
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id)
            if not sn:
                raise ValueError("no such snapshot")
            had_hot, had_cold = sn.hot_present, sn.cold_present
        dropped = 0
        # NOTE: a snapshot's own row must still exist while _drop_from_tier runs (it excludes itself by id)
        if had_hot: dropped += self._drop_from_tier(snapshot_id, "hot")
        if had_cold: dropped += self._drop_from_tier(snapshot_id, "cold")
        with self.Session() as s:
            sn = s.get(Snapshot, snapshot_id); s.delete(sn); s.commit()
        return {"snapshot_id": snapshot_id, "objects_deleted": dropped}

    def apply_retention(self, job_id: int | None = None) -> dict:
        """Per policy: keep newest K and drop anything older than max_age_days (never drops the newest)."""
        deleted, tiered = [], []
        with self.Session() as s:
            policies = s.query(Policy).all()
        now = utcnow()
        for p in policies:
            with self.Session() as s:
                snaps = (s.query(Snapshot).filter_by(policy_name=p.name, status="complete")
                         .order_by(Snapshot.created_at.desc()).all())
            doomed = []
            for i, sn in enumerate(snaps):
                too_many = p.keep_last is not None and i >= p.keep_last
                too_old = p.max_age_days is not None and i > 0 and sn.created_at < now - dt.timedelta(days=p.max_age_days)
                if too_many or too_old:
                    doomed.append(sn.id)
            for sid in doomed:
                self.delete_snapshot(sid); deleted.append(sid)
            if p.tier_after_days is not None:
                for sn in snaps:
                    if sn.id in doomed or not sn.hot_present or sn.cold_present:
                        continue
                    if sn.created_at < now - dt.timedelta(days=p.tier_after_days):
                        self.tier_snapshot(sn.id); tiered.append(sn.id)
        # failed/abandoned snapshots: remove their catalog rows (their orphan objects are cleaned by lifecycle rules)
        with self.Session() as s:
            s.query(Snapshot).filter(Snapshot.status == "failed", Snapshot.created_at < now - dt.timedelta(days=1)).delete()
            s.commit()
        return {"deleted": deleted, "tiered": tiered}

    # ------------------------------------------------------------------ metrics
    def refresh_storage_metrics(self):
        with self.Session() as s:
            for tier, flag in (("hot", Snapshot.hot_present), ("cold", Snapshot.cold_present)):
                # bytes referenced = sum over DISTINCT keys referenced by snapshots present in this tier
                sub = (s.query(SnapshotObject.key, func.max(SnapshotObject.size).label("sz"))
                       .join(Snapshot, Snapshot.id == SnapshotObject.snapshot_id)
                       .filter(flag.is_(True), Snapshot.status == "complete").group_by(SnapshotObject.key).subquery())
                total = s.query(func.coalesce(func.sum(sub.c.sz), 0)).scalar()
                metrics.STORAGE.labels(tier).set(int(total or 0))
                metrics.SNAPSHOTS.labels(tier).set(s.query(Snapshot).filter(flag.is_(True), Snapshot.status == "complete").count())
            metrics.VERIFY_FAIL.set(s.query(Snapshot).filter(Snapshot.last_verify_ok.is_(False)).count())
