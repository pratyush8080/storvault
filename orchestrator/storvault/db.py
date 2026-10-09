"""Catalog database (SQLAlchemy). SQLite by default, Postgres via SV_DB_URL."""
import datetime as dt
import json

from sqlalchemy import (Boolean, Column, DateTime, ForeignKey, Index, Integer, BigInteger, String, Text,
                        create_engine)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


class Policy(Base):
    __tablename__ = "policies"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    source_path = Column(String(1024), nullable=False)
    cron = Column(String(100), nullable=False, default="0 2 * * *")   # nightly 02:00
    keep_last = Column(Integer, default=7)             # retention: keep newest K snapshots
    max_age_days = Column(Integer, nullable=True)      # retention: delete older than N days
    tier_after_days = Column(Integer, nullable=True)   # copy to cold tier after N days
    verify_after_backup = Column(Boolean, default=True)
    enabled = Column(Boolean, default=True)


class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(String(64), primary_key=True)
    policy_name = Column(String(100), nullable=True)
    source_path = Column(String(1024), nullable=False)
    created_at = Column(DateTime, default=utcnow, index=True)
    status = Column(String(20), default="running")     # running | complete | failed
    manifest_key = Column(String(256))
    file_count = Column(Integer, default=0)
    bytes_total = Column(BigInteger, default=0)        # logical size of the snapshot
    bytes_uploaded = Column(BigInteger, default=0)     # new bytes (incremental delta)
    hot_present = Column(Boolean, default=False)
    cold_present = Column(Boolean, default=False)
    cold_at = Column(DateTime, nullable=True)
    last_verified_at = Column(DateTime, nullable=True)
    last_verify_ok = Column(Boolean, nullable=True)
    objects = relationship("SnapshotObject", cascade="all, delete-orphan", back_populates="snapshot")


class SnapshotObject(Base):
    """Which object keys a snapshot references (incremental snapshots share keys => ref counting)."""
    __tablename__ = "snapshot_objects"
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(String(64), ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    key = Column(String(1024), nullable=False)
    size = Column(BigInteger, default=0)
    snapshot = relationship("Snapshot", back_populates="objects")


Index("ix_snapobj_key", SnapshotObject.key)


class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    type = Column(String(20), nullable=False)          # backup | restore | verify | tier | retention
    status = Column(String(20), default="queued", index=True)  # queued|running|success|failed|interrupted
    snapshot_id = Column(String(64), nullable=True)
    params = Column(Text, default="{}")
    attempts = Column(Integer, default=0)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    def to_dict(self):
        return {"id": self.id, "type": self.type, "status": self.status, "snapshot_id": self.snapshot_id,
                "params": json.loads(self.params or "{}"), "attempts": self.attempts,
                "result": json.loads(self.result) if self.result else None, "error": self.error,
                "created_at": self.created_at, "started_at": self.started_at, "finished_at": self.finished_at}


def make_session_factory(url: str):
    kw = {"connect_args": {"check_same_thread": False}} if url.startswith("sqlite") else {"pool_pre_ping": True}
    engine = create_engine(url, **kw)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False), engine
