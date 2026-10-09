"""Bucket lifecycle rules via the S3 API (works on MinIO and Ceph RGW).

Rules applied to the HOT bucket:
  * abort incomplete multipart uploads after N days  (cleans up crashed/killed agent runs)
  * expire noncurrent object versions after N days    (if versioning is enabled)
  * optional hard expiration safety net (e.g. 400 days)
  * optional native transition rule to a *named remote tier* (MinIO: `mc admin tier add`, see scripts/)
The orchestrator-driven tiering in service.tier_snapshot is the portable path that keeps objects directly
readable in the cold bucket; native transition is the "storage-system-managed" path.
"""
from . import s3util
from .config import Settings


def apply_lifecycle(settings: Settings | None = None, abort_mpu_days=1, noncurrent_days=7,
                    expire_days: int | None = None, transition_days: int | None = None,
                    transition_tier: str | None = None, tier_name: str = "hot") -> dict:
    s = settings or Settings()
    t = s.tier(tier_name)
    c = s3util.client(t)
    s3util.ensure_bucket(c, t.bucket)
    rules = [
        {"ID": "abort-stale-multipart", "Status": "Enabled", "Filter": {"Prefix": ""},
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": abort_mpu_days}},
        {"ID": "expire-noncurrent", "Status": "Enabled", "Filter": {"Prefix": ""},
         "NoncurrentVersionExpiration": {"NoncurrentDays": noncurrent_days}},
    ]
    if expire_days:
        rules.append({"ID": "hard-expire", "Status": "Enabled", "Filter": {"Prefix": ""},
                      "Expiration": {"Days": expire_days}})
    if transition_days and transition_tier:
        rules.append({"ID": "hot-to-cold-transition", "Status": "Enabled", "Filter": {"Prefix": ""},
                      "Transitions": [{"Days": transition_days, "StorageClass": transition_tier}]})
    c.put_bucket_lifecycle_configuration(Bucket=t.bucket, LifecycleConfiguration={"Rules": rules})
    return {"bucket": t.bucket, "rules": [r["ID"] for r in rules]}


if __name__ == "__main__":
    import json, sys
    print(json.dumps(apply_lifecycle(transition_days=int(sys.argv[1]) if len(sys.argv) > 1 else None,
                                     transition_tier=sys.argv[2] if len(sys.argv) > 2 else None)))
