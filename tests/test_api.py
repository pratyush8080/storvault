import time

import pytest
from fastapi.testclient import TestClient

from conftest import trees_equal


@pytest.fixture()
def client(svc):
    from storvault.api import create_app
    from storvault.config import Settings
    app = create_app(Settings())
    with TestClient(app) as c:
        yield c


def wait_job(client, jid, timeout=60):
    t = time.time()
    while time.time() - t < timeout:
        j = client.get(f"/jobs/{jid}").json()
        if j["status"] in ("success", "failed", "interrupted"):
            return j
        time.sleep(0.3)
    raise TimeoutError


def test_probes_and_metrics(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").status_code == 200
    body = client.get("/metrics").text
    for name in ("storvault_jobs_total", "storvault_storage_bytes", "storvault_job_duration_seconds"):
        assert name in body


def test_api_backup_restore_flow(client, tree, tmp_path):
    jid = client.post("/jobs/backup", json={"source_path": tree}).json()["job_id"]
    j = wait_job(client, jid)
    assert j["status"] == "success", j
    sid = j["result"]["snapshot_id"]
    snaps = client.get("/snapshots").json()
    assert snaps[0]["id"] == sid and snaps[0]["hot"] and snaps[0]["last_verify_ok"]
    out = str(tmp_path / "r")
    assert wait_job(client, client.post("/jobs/restore", json={"snapshot_id": sid, "dest": out}).json()["job_id"])["status"] == "success"
    assert trees_equal(tree, out)
    m = client.get("/metrics").text
    assert 'storvault_jobs_total{status="success",type="backup"} 1.0' in m
    assert "storvault_last_successful_backup_timestamp_seconds" in m


def test_policy_crud_and_validation(client, tree):
    r = client.post("/policies", json={"name": "nightly", "source_path": tree, "cron": "0 2 * * *", "keep_last": 3})
    assert r.status_code == 201
    assert client.post("/policies", json={"name": "nightly", "source_path": tree}).status_code == 409
    assert client.post("/policies", json={"name": "bad", "source_path": tree, "cron": "not a cron"}).status_code == 422
    assert [p["name"] for p in client.get("/policies").json()] == ["nightly"]
    assert client.delete("/policies/nightly").status_code == 200


def test_unknown_job_404(client):
    assert client.get("/jobs/99999").status_code == 404
