"""Bronze storage, on S3-compatible object storage or on local disk.

Object storage is optional. Without credentials everything still runs and writes to `data/`
under the same partition layout, so the project is runnable by anyone who clones it. Requiring a
cloud account to execute a DAG would make the repository unreviewable.

The layout is identical in both places — `bronze/klines_1m/dt=…/symbol=…/part-0.parquet` — which
means a local run and a cloud run produce paths that differ only in prefix, and DuckDB reads
either with the same glob.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_REQUIRED = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")


class ObjectStoreError(RuntimeError):
    pass


def missing_settings() -> list[str]:
    return [k for k in _REQUIRED if not (os.getenv(k) or "").strip()]


def is_configured() -> bool:
    return not missing_settings()


def bucket() -> str:
    return os.environ["R2_BUCKET"]


def endpoint() -> str:
    """Derived from the account id, so it is not a separate secret to keep in step.

    Cloudflare's dashboard shows the endpoint with the bucket already appended. Passing that
    whole URL produces a 404 that reads exactly like a missing bucket, which is a slow thing to
    debug for a five-character mistake.
    """
    return f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"


def client():
    if not is_configured():
        raise ObjectStoreError(f"object store not configured; missing {missing_settings()}")
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint(),
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def put_bytes(key: str, payload: bytes) -> None:
    """Write one object, overwriting whatever was there.

    Overwrite is the correct behaviour and the reason a re-run is idempotent: a partition is
    identified by date and symbol, so writing it again with the same source data replaces it with
    an identical file rather than appending a second copy.
    """
    client().put_object(Bucket=bucket(), Key=key, Body=payload)
    logger.info("wrote s3://%s/%s (%.0f KB)", bucket(), key, len(payload) / 1024)


def get_bytes(key: str) -> bytes:
    return client().get_object(Bucket=bucket(), Key=key)["Body"].read()


def exists(key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client().head_object(Bucket=bucket(), Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def list_keys(prefix: str = ""):
    """Every key under a prefix, following pagination.

    A plain list_objects_v2 stops at 1000 keys without saying so. Twenty symbols a day reaches
    that in under two months, and the missing keys would look like missing history.
    """
    paginator = client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket(), Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def check_writable() -> None:
    """Prove the credentials can write, not merely read.

    An R2 token is read-only by default, and a read-only token lists a bucket perfectly well — so
    a connectivity check passes and the first upload fails. Inside a scheduled DAG that means a
    run's work is lost on a worker that is about to be destroyed.
    """
    key = "_healthcheck/writable"
    payload = b"bmp write check"
    c = client()
    try:
        c.put_object(Bucket=bucket(), Key=key, Body=payload)
    except Exception as exc:
        raise ObjectStoreError(
            f"cannot write to s3://{bucket()}/ — {type(exc).__name__}: {exc}. "
            f"An R2 API token needs 'Object Read & Write'; a read-only one still lists."
        ) from exc
    back = c.get_object(Bucket=bucket(), Key=key)["Body"].read()
    c.delete_object(Bucket=bucket(), Key=key)
    if back != payload:
        raise ObjectStoreError("wrote to the bucket but read back different bytes")


def write_partition(key: str, payload: bytes, local_path: Path) -> str:
    """Write Bronze wherever this environment can. Returns a human-readable destination.

    Falls back to local disk rather than failing, because a contributor without credentials
    should still be able to run the DAG end to end and see real files appear.
    """
    if is_configured():
        put_bytes(key, payload)
        return f"s3://{bucket()}/{key}"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(payload)
    logger.info("object store not configured — wrote %s", local_path)
    return str(local_path)
