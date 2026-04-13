import json
from storage import s3
from utils.logger import get_logger

log = get_logger(__name__)


def consolidate(provider: str, date: str, bucket: str) -> str:
    """
    Merge all raw files from Camada 1 into a single file in Camada 2.
    Returns s3_path of consolidated file.
    Idempotent — overwrites if already exists.
    """
    prefix = s3.raw_prefix(date, provider)
    keys = s3.list_keys(bucket, prefix)

    if not keys:
        raise RuntimeError(f"No raw files found for provider={provider} date={date}")

    if len(keys) == 1:
        # GCP case: single file — copy directly
        log.info(f"Single raw file for provider={provider} — copying directly (no-op merge)")
        content = s3.download(bucket, keys[0])
    else:
        # AWS / Azure / Oracle: merge N files into one array
        log.info(f"Merging {len(keys)} files for provider={provider}")
        content = _merge(provider, bucket, keys)

    dest_key = s3.consolidated_key(date, provider)
    path = s3.upload(bucket, dest_key, content)
    log.info(f"Consolidated provider={provider} → {path}")
    return path


def _merge(provider: str, bucket: str, keys: list[str]) -> bytes:
    """Merge strategy per provider."""
    if provider == "azure":
        return _merge_azure(bucket, keys)
    return _merge_generic(bucket, keys)


def _merge_azure(bucket: str, keys: list[str]) -> bytes:
    """Azure: concatenate 'items' arrays from all pages."""
    all_items = []
    for key in sorted(keys):
        data = json.loads(s3.download(bucket, key))
        all_items.extend(data.get("items", []))
    return json.dumps({"items": all_items}, ensure_ascii=False).encode()


def _merge_generic(bucket: str, keys: list[str]) -> bytes:
    """AWS / Oracle: concatenate top-level arrays or wrap in list."""
    all_records = []
    for key in sorted(keys):
        data = json.loads(s3.download(bucket, key))
        if isinstance(data, list):
            all_records.extend(data)
        else:
            all_records.append(data)
    return json.dumps(all_records, ensure_ascii=False).encode()
