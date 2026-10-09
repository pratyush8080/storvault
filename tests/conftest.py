import os
import shutil
import subprocess
import sys
import time

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "orchestrator"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

AGENT_BIN = os.path.join(ROOT, "agent", "build", "storvault-agent")
HOT_PORT, COLD_PORT, HOT_PROXY_PORT = 5601, 5602, 5701


def _wait(url, timeout=20):
    t = time.time()
    while time.time() - t < timeout:
        try:
            requests.get(url, timeout=1); return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


@pytest.fixture(scope="session")
def agent_bin():
    if not os.path.exists(AGENT_BIN):
        subprocess.check_call(["cmake", "-S", f"{ROOT}/agent", "-B", f"{ROOT}/agent/build", "-DCMAKE_BUILD_TYPE=Release"])
        subprocess.check_call(["cmake", "--build", f"{ROOT}/agent/build", "-j4"])
    return AGENT_BIN


@pytest.fixture(scope="session")
def s3_servers():
    """Two independent S3 servers (moto): 'hot' (MinIO stand-in) and 'cold' (Ceph RGW stand-in)."""
    procs = [subprocess.Popen(["moto_server", "-p", str(p)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
             for p in (HOT_PORT, COLD_PORT)]
    for p in (HOT_PORT, COLD_PORT):
        _wait(f"http://127.0.0.1:{p}/")
    yield
    for p in procs:
        p.terminate()


@pytest.fixture(scope="session")
def proxy(s3_servers):
    from chaos_proxy import ChaosProxy
    px = ChaosProxy(HOT_PROXY_PORT, ("127.0.0.1", HOT_PORT)).start()
    yield px
    px.stop()


def _make_service(tmp_path, agent_bin, monkeypatch, hot_port):
    from storvault.config import Settings
    from storvault.service import Service
    uid = tmp_path.name.lower().replace("_", "-")[-30:]
    env = {"SV_DB_URL": f"sqlite:///{tmp_path}/cat.db", "SV_AGENT_BIN": agent_bin, "SV_WORK_DIR": str(tmp_path / "w"),
           "SV_HOT_ENDPOINT": f"http://127.0.0.1:{hot_port}", "SV_HOT_BUCKET": f"hot-{uid}",
           "SV_COLD_ENDPOINT": f"http://127.0.0.1:{COLD_PORT}", "SV_COLD_BUCKET": f"cold-{uid}",
           "SV_HOT_ACCESS_KEY": "t", "SV_HOT_SECRET_KEY": "t", "SV_COLD_ACCESS_KEY": "t", "SV_COLD_SECRET_KEY": "t",
           "SV_BACKOFF_BASE_S": "0.1", "SV_BACKOFF_CAP_S": "0.5", "SV_JOB_MAX_ATTEMPTS": "4", "SV_AGENT_THREADS": "4",
           "SV_SCHEDULER": "0"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return Service(Settings())


@pytest.fixture()
def svc(tmp_path, agent_bin, s3_servers, monkeypatch):
    """A fresh Service: own sqlite catalog + unique buckets, fast backoff. Talks to hot/cold directly."""
    return _make_service(tmp_path, agent_bin, monkeypatch, HOT_PORT)


@pytest.fixture()
def svc_proxied(tmp_path, agent_bin, proxy, monkeypatch):
    """Same, but the hot tier is reached through the chaos proxy (for failure injection)."""
    proxy.up(); proxy.set_latency(0); proxy.reset_after(None)
    return _make_service(tmp_path, agent_bin, monkeypatch, HOT_PROXY_PORT)


@pytest.fixture()
def tree(tmp_path):
    """A small source tree: nested dirs, odd filenames, an empty file, one multipart-sized file."""
    src = tmp_path / "src"
    (src / "docs" / "deep").mkdir(parents=True)
    (src / "docs" / "a.txt").write_text("hello\n")
    (src / "docs" / "deep" / "my file (1) & more.txt").write_text("spaces and symbols\n")
    (src / "empty.bin").write_bytes(b"")
    for i in range(20):
        (src / f"f{i}.dat").write_bytes(os.urandom(5000 + i))
    (src / "big.bin").write_bytes(os.urandom(18 * 1024 * 1024))   # > 16 MiB threshold => multipart
    return str(src)


def trees_equal(a: str, b: str) -> bool:
    return subprocess.run(["diff", "-r", a, b], capture_output=True).returncode == 0
