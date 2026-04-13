import boto3
from utils.logger import get_logger

log = get_logger(__name__)
_s3 = boto3.client("s3")


def upload(bucket: str, key: str, body: bytes, content_type: str = "application/json") -> str:
    """Upload bytes to S3. Idempotent — overwrites existing object."""
    _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    s3_path = f"s3://{bucket}/{key}"
    log.info(f"Uploaded {s3_path} ({len(body)} bytes)")
    return s3_path


def download(bucket: str, key: str) -> bytes:
    """Download object from S3 as bytes."""
    response = _s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def list_keys(bucket: str, prefix: str) -> list[str]:
    """List all object keys under a prefix."""
    paginator = _s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def raw_prefix(date: str, provider: str) -> str:
    return f"raw/{date}/{provider}/"


def consolidated_key(date: str, provider: str) -> str:
    return f"consolidated/{date}/{provider}/prices.json"


def parquet_key(date: str, provider: str) -> str:
    return f"parquet/{date}/{provider}/prices.parquet"
