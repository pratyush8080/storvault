"""End-to-end tests: real C++ agent + orchestrator logic + two real S3 servers."""
import json
import os
import subprocess
import threading
import time

import pytest

from conftest import trees_equal
from storvault import metrics, s3util
from storvault.db import Job, Snapshot
from storvault.service import AgentError


def test_backup_verify_restore_roundtrip(svc, tree, tmp_path):
    r = svc.backup(tree)
    assert r["files"] == 24 and r["verify"]["files_ok"] == 24
    out = str(tmp_path / "restored")
    svc.restore(r["snapshot_id"], out)
    assert trees_equal(tree, out)            # backup -> restore -> diff == identical


def test_incremental_uploads_only_changes(svc, tree):
    first = svc.backup(tree)
    assert first["unchanged"] == 0
    time.sleep(1.1)                           # mtime granularity is 1s
    open(os.path.join(tree, "docs", "a.txt"), "w").write("changed\n")
    second = svc.backup(tree)
    assert second["uploaded"] == 1 and second["unchanged"] == 23
    assert second["bytes_uploaded"] < 100


def test_incremental_snapshot_restores_completely(svc, tree, tmp_path):
    svc.backup(tree)
    time.sleep(1.1)
    open(os.path.join(tree, "new.txt"), "w").write("new file\n")
    snap2 = svc.backup(tree)["snapshot_id"]
    svc.restore(snap2, str(tmp_path / "r2"))
    assert trees_equal(tree, str(tmp_path / "r2"))


def test_retention_keep_last_preserves_shared_objects(svc, tree, tmp_path):
    from storvault.db import Policy
    with svc.Session() as s:
        s.add(Policy(name="p", source_path=tree, keep_last=2)); s.commit()
    ids = []
    for i in range(4):
        time.sleep(1.1)
        open(os.path.join(tree, f"v{i}.txt"), "w").write(f"version {i}\n")
        ids.append(svc.backup(tree, policy_name="p", verify=False)["snapshot_id"])
    res = svc.apply_retention()
    assert set(res["deleted"]) == set(ids[:2])        # oldest two pruned
    # Newest snapshot still restores fully even though it shares objects with deleted ones
    svc.restore(ids[3], str(tmp_path / "r"))
    assert trees_equal(tree, str(tmp_path / "r"))
    with svc.Session() as s:
        assert s.query(Snapshot).filter_by(status="complete").count() == 2


def test_tier_to_cold_then_restore_from_cold(svc, tree, tmp_path):
    sid = svc.backup(tree)["snapshot_id"]
    r = svc.tier_snapshot(sid, evict_hot=True)
    assert r["cold_verified"] == 24 and r["hot_objects_evicted"] > 0
    with svc.Session() as s:
        sn = s.get(Snapshot, sid)
        assert sn.cold_present and not sn.hot_present
    # hot copy is really gone
    hot = svc.s.hot
    keys = s3util.client(hot).list_objects_v2(Bucket=hot.bucket).get("KeyCount", 0)
    assert keys == 0
    svc.restore(sid, str(tmp_path / "from_cold"))      # auto-selects cold tier
    assert trees_equal(tree, str(tmp_path / "from_cold"))


def test_deep_verify_detects_silent_corruption(svc, tree):
    sid = svc.backup(tree)["snapshot_id"]
    hot = svc.s.hot
    c = s3util.client(hot)
    key = f"{sid}/f3.dat"
    size = c.head_object(Bucket=hot.bucket, Key=key)["ContentLength"]
    c.put_object(Bucket=hot.bucket, Key=key, Body=b"\x00" * size)      # same size, bad bytes, no metadata
    with pytest.raises(AgentError) as e:
        svc.verify(sid, deep=True)
    assert "CHECKSUM MISMATCH" in str(e.value)
    with svc.Session() as s:
        assert s.get(Snapshot, sid).last_verify_ok is False


def test_restore_refuses_to_publish_corrupt_file(svc, tree, tmp_path):
    sid = svc.backup(tree, verify=False)["snapshot_id"]
    hot = svc.s.hot
    c = s3util.client(hot)
    size = c.head_object(Bucket=hot.bucket, Key=f"{sid}/f5.dat")["ContentLength"]
    c.put_object(Bucket=hot.bucket, Key=f"{sid}/f5.dat", Body=b"x" * size)
    out = tmp_path / "bad"
    with pytest.raises(AgentError):
        svc.restore(sid, str(out))
    assert not (out / "f5.dat").exists()               # corrupt data never lands at the final path
    assert not list(out.glob("*.sv-part"))             # temp files cleaned


def test_fatal_error_is_not_retried(svc, tree):
    sid = svc.backup(tree, verify=False)["snapshot_id"]
    hot = svc.s.hot
    with svc.Session() as s:
        mk = s.get(Snapshot, sid).manifest_key
    s3util.client(hot).delete_object(Bucket=hot.bucket, Key=mk)        # manifest => 404 (fatal)
    before = metrics.REGISTRY.get_sample_value("storvault_job_retries_total", {"type": "restore"}) or 0
    t = time.time()
    with pytest.raises(AgentError):
        svc.restore(sid, "/tmp/never")
    after = metrics.REGISTRY.get_sample_value("storvault_job_retries_total", {"type": "restore"}) or 0
    assert after == before and time.time() - t < 3


def test_network_outage_mid_backup_is_survived(svc_proxied, tree, proxy):
    """Chaos: the hot tier disappears for ~1.5s while a backup is running. Retries/backoff must carry it through."""
    svc = svc_proxied

    def total_retries():
        return ((metrics.REGISTRY.get_sample_value("storvault_agent_retries_total") or 0) +
                (metrics.REGISTRY.get_sample_value("storvault_job_retries_total", {"type": "backup"}) or 0))

    def outage():
        time.sleep(0.05); proxy.down(); time.sleep(1.5); proxy.up()
    r0 = total_retries()
    th = threading.Thread(target=outage); th.start()
    res = svc.backup(tree, verify=False)
    th.join()
    assert total_retries() > r0, "expected retries during the outage"
    assert svc.verify(res["snapshot_id"], deep=True)["files_ok"] == 24     # data intact despite the outage


def test_connection_reset_mid_upload_is_retried(svc_proxied, tree, proxy):
    proxy.reset_after(200_000)                          # kill one connection after 200 KB uploaded
    res = svc_proxied.backup(tree, verify=False)
    assert svc_proxied.verify(res["snapshot_id"], deep=True)["files_ok"] == 24


def test_recover_interrupted_jobs_after_crash(svc):
    with svc.Session() as s:
        s.add(Job(type="backup", status="running"))
        s.add(Snapshot(id="snap-dead", source_path="/x", status="running")); s.commit()
    svc.recover_interrupted()
    with svc.Session() as s:
        assert s.query(Job).first().status == "interrupted"
        assert s.get(Snapshot, "snap-dead").status == "failed"


def test_failed_backup_never_becomes_a_usable_snapshot(svc, tmp_path):
    from storvault.db import Snapshot
    with pytest.raises(ValueError):
        svc.backup(str(tmp_path / "does-not-exist"))
    with svc.Session() as s:
        assert s.query(Snapshot).filter_by(status="complete").count() == 0
