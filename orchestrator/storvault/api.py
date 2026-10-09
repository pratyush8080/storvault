"""FastAPI app: REST API, health probes, /metrics, cron scheduler."""
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from . import metrics, s3util
from .config import Settings
from .db import Job, Policy, Snapshot
from .service import Service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("storvault.api")


class BackupReq(BaseModel):
    source_path: str
    verify: bool = True


class RestoreReq(BaseModel):
    snapshot_id: str
    dest: str
    tier: str | None = Field(None, pattern="^(hot|cold)$")


class VerifyReq(BaseModel):
    snapshot_id: str
    deep: bool = False
    tier: str | None = Field(None, pattern="^(hot|cold)$")


class TierReq(BaseModel):
    snapshot_id: str
    evict_hot: bool | None = None


class PolicyReq(BaseModel):
    name: str
    source_path: str
    cron: str = "0 2 * * *"
    keep_last: int | None = 7
    max_age_days: int | None = None
    tier_after_days: int | None = None
    verify_after_backup: bool = True
    enabled: bool = True


def create_app(settings: Settings | None = None) -> FastAPI:
    svc = Service(settings)
    pool = ThreadPoolExecutor(max_workers=svc.s.max_parallel_jobs)
    sched = BackgroundScheduler(timezone="UTC")

    def submit(type_: str, params: dict) -> int:
        jid = svc.create_job(type_, params, params.get("snapshot_id"))
        pool.submit(svc.execute_job, jid)
        return jid

    def sync_schedule():
        """(Re)build cron jobs from policies in the catalog."""
        for j in sched.get_jobs():
            if j.id.startswith("policy:"):
                j.remove()
        with svc.Session() as s:
            for p in s.query(Policy).filter_by(enabled=True).all():
                sched.add_job(lambda p=p: submit("backup", {"source_path": p.source_path, "policy_name": p.name,
                                                              "verify": bool(p.verify_after_backup)}),
                              CronTrigger.from_crontab(p.cron, timezone="UTC"), id=f"policy:{p.name}",
                              replace_existing=True, misfire_grace_time=3600, coalesce=True)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        recovered = svc.recover_interrupted()
        if recovered:
            log.warning("marked interrupted jobs: %s", recovered)
        svc.refresh_storage_metrics()
        if svc.s.scheduler_enabled:
            sched.add_job(lambda: submit("retention", {}), CronTrigger.from_crontab(svc.s.maintenance_cron, timezone="UTC"),
                          id="maintenance", replace_existing=True, coalesce=True)
            sync_schedule()
            sched.start()
        yield
        if sched.running:
            sched.shutdown(wait=False)
        pool.shutdown(wait=False, cancel_futures=True)

    app = FastAPI(title="StorVault", version="0.1.0", lifespan=lifespan)
    app.state.svc = svc

    # ---- probes ----
    @app.get("/healthz")
    def healthz():            # liveness: process is up and event loop responsive
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():             # readiness: catalog DB + hot tier reachable
        try:
            with svc.Session() as s:
                s.query(Policy).first()
            s3util.client(svc.s.hot).list_buckets()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(503, f"not ready: {type(e).__name__}")
        return {"status": "ready"}

    @app.get("/metrics")
    def prom():
        svc.refresh_storage_metrics()
        return Response(metrics.render(), media_type="text/plain; version=0.0.4")

    # ---- jobs ----
    @app.post("/jobs/backup", status_code=202)
    def job_backup(r: BackupReq):
        return {"job_id": submit("backup", r.model_dump())}

    @app.post("/jobs/restore", status_code=202)
    def job_restore(r: RestoreReq):
        return {"job_id": submit("restore", r.model_dump())}

    @app.post("/jobs/verify", status_code=202)
    def job_verify(r: VerifyReq):
        return {"job_id": submit("verify", r.model_dump())}

    @app.post("/jobs/tier", status_code=202)
    def job_tier(r: TierReq):
        return {"job_id": submit("tier", r.model_dump(exclude_none=True))}

    @app.post("/jobs/retention", status_code=202)
    def job_retention():
        return {"job_id": submit("retention", {})}

    @app.get("/jobs")
    def jobs(limit: int = 50):
        with svc.Session() as s:
            return [j.to_dict() for j in s.query(Job).order_by(Job.id.desc()).limit(min(limit, 500))]

    @app.get("/jobs/{job_id}")
    def job(job_id: int):
        with svc.Session() as s:
            j = s.get(Job, job_id)
            if not j:
                raise HTTPException(404, "no such job")
            return j.to_dict()

    # ---- snapshots ----
    @app.get("/snapshots")
    def snapshots(source_path: str | None = None):
        with svc.Session() as s:
            q = s.query(Snapshot).order_by(Snapshot.created_at.desc())
            if source_path:
                q = q.filter_by(source_path=source_path)
            return [{"id": x.id, "policy": x.policy_name, "source_path": x.source_path, "created_at": x.created_at,
                     "status": x.status, "files": x.file_count, "bytes_total": x.bytes_total,
                     "bytes_uploaded": x.bytes_uploaded, "hot": x.hot_present, "cold": x.cold_present,
                     "last_verified_at": x.last_verified_at, "last_verify_ok": x.last_verify_ok} for x in q.all()]

    @app.delete("/snapshots/{snapshot_id}")
    def del_snapshot(snapshot_id: str):
        try:
            return svc.delete_snapshot(snapshot_id)
        except ValueError as e:
            raise HTTPException(404, str(e))

    # ---- policies ----
    @app.post("/policies", status_code=201)
    def create_policy(p: PolicyReq):
        with svc.Session() as s:
            if s.query(Policy).filter_by(name=p.name).first():
                raise HTTPException(409, "policy exists")
            s.add(Policy(**p.model_dump())); s.commit()
        try:
            CronTrigger.from_crontab(p.cron)
        except ValueError:
            with svc.Session() as s:
                s.query(Policy).filter_by(name=p.name).delete(); s.commit()
            raise HTTPException(422, "invalid cron expression")
        if sched.running:
            sync_schedule()
        return p

    @app.get("/policies")
    def list_policies():
        with svc.Session() as s:
            return [{c.name: getattr(x, c.name) for c in Policy.__table__.columns} for x in s.query(Policy).all()]

    @app.delete("/policies/{name}")
    def del_policy(name: str):
        with svc.Session() as s:
            n = s.query(Policy).filter_by(name=name).delete(); s.commit()
        if not n:
            raise HTTPException(404, "no such policy")
        if sched.running:
            sync_schedule()
        return {"deleted": name}

    return app


def app_factory() -> FastAPI:   # uvicorn --factory storvault.api:app_factory
    return create_app()
