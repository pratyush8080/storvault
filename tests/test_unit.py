import random
import subprocess

from conftest import ROOT


def test_cpp_unit_tests(agent_bin):
    """Runs the C++ test binary (sha256 vectors, hmac, backoff bounds, retry logic, thread pool)."""
    out = subprocess.run([f"{ROOT}/agent/build/sv_tests"], capture_output=True, text=True)
    assert out.returncode == 0, out.stdout


def test_backoff_bounded_and_jittered():
    from storvault.service import backoff_delay
    rng = random.Random(7)
    ds = [backoff_delay(3, base=2, cap=60, rng=rng) for _ in range(500)]
    assert all(0 <= d <= 16 for d in ds)              # 2 * 2^3
    assert max(ds) - min(ds) > 5                      # jittered, not constant
    assert all(backoff_delay(30, 2, 60, rng) <= 60 for _ in range(100))   # capped


def test_manifest_parse_roundtrip():
    from storvault.s3util import parse_manifest
    rows = parse_manifest("docs/my%20file.txt\tsnap-1/docs/my%20file.txt\t12\tabc\t1700000000\n")
    assert rows[0]["path"] == "docs/my file.txt" and rows[0]["size"] == 12
