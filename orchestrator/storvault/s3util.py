"""boto3 helpers: one client per tier (endpoint-agnostic: MinIO, Ceph RGW, AWS, moto)."""
import boto3
from botocore.config import Config

from .config import Tier


def client(t: Tier):
    return boto3.client(
        "s3", endpoint_url=t.endpoint, aws_access_key_id=t.access_key, aws_secret_access_key=t.secret_key,
        region_name=t.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=5, read_timeout=60))


def ensure_bucket(c, bucket: str):
    try:
        c.head_bucket(Bucket=bucket)
    except Exception:
        try:
            c.create_bucket(Bucket=bucket)
        except Exception as e:  # race / already owned
            if "BucketAlreadyOwnedByYou" not in str(e) and "BucketAlreadyExists" not in str(e):
                raise


def parse_manifest(text: str):
    """TSV: percent-encoded-path, key, size, sha256, mtime  -> list of dicts"""
    from urllib.parse import unquote
    out = []
    for line in text.splitlines():
        f = line.split("\t")
        if len(f) >= 5:
            out.append({"path": unquote(f[0]), "key": f[1], "size": int(f[2]), "sha256": f[3], "mtime": f[4]})
    return out


def read_manifest(c, bucket: str, key: str):
    body = c.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    return body, parse_manifest(body)
