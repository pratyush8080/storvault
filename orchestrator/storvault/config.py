"""Settings, read from environment variables (12-factor; ConfigMap/Secret friendly in Kubernetes)."""
import os
from dataclasses import dataclass, field


def _e(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass
class Tier:
    name: str
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"


@dataclass
class Settings:
    db_url: str = field(default_factory=lambda: _e("SV_DB_URL", "sqlite:///./storvault.db"))
    agent_bin: str = field(default_factory=lambda: _e("SV_AGENT_BIN", "storvault-agent"))
    agent_threads: int = field(default_factory=lambda: int(_e("SV_AGENT_THREADS", "8")))
    agent_timeout_s: int = field(default_factory=lambda: int(_e("SV_AGENT_TIMEOUT_S", "3600")))
    work_dir: str = field(default_factory=lambda: _e("SV_WORK_DIR", "/tmp/storvault"))
    # orchestrator-level retry (agent also retries each S3 call internally)
    job_max_attempts: int = field(default_factory=lambda: int(_e("SV_JOB_MAX_ATTEMPTS", "4")))
    backoff_base_s: float = field(default_factory=lambda: float(_e("SV_BACKOFF_BASE_S", "2")))
    backoff_cap_s: float = field(default_factory=lambda: float(_e("SV_BACKOFF_CAP_S", "60")))
    max_parallel_jobs: int = field(default_factory=lambda: int(_e("SV_MAX_PARALLEL_JOBS", "2")))
    maintenance_cron: str = field(default_factory=lambda: _e("SV_MAINTENANCE_CRON", "*/30 * * * *"))
    scheduler_enabled: bool = field(default_factory=lambda: _e("SV_SCHEDULER", "1") == "1")
    evict_hot_after_tiering: bool = field(default_factory=lambda: _e("SV_EVICT_HOT", "1") == "1")

    @property
    def hot(self) -> Tier:
        return Tier("hot", _e("SV_HOT_ENDPOINT", "http://localhost:9000"), _e("SV_HOT_BUCKET", "storvault-hot"),
                    _e("SV_HOT_ACCESS_KEY", "minioadmin"), _e("SV_HOT_SECRET_KEY", "minioadmin"))

    @property
    def cold(self) -> Tier:
        return Tier("cold", _e("SV_COLD_ENDPOINT", "http://localhost:8000"), _e("SV_COLD_BUCKET", "storvault-cold"),
                    _e("SV_COLD_ACCESS_KEY", "cephadmin"), _e("SV_COLD_SECRET_KEY", "cephadmin"))

    def tier(self, name: str) -> Tier:
        return self.hot if name == "hot" else self.cold
