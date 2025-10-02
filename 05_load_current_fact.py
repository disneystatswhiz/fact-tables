#!/usr/bin/env python3
"""
06_load_current_fact.py
---------------------------------
Purpose:
1) Download the current Parquet fact table from S3 → work/ (no reading/counting).
2) If it does not exist on S3 but a local delta exists, promote it to current by raw copy.
3) Optionally initialize an empty current Parquet if neither exists.

Outputs (JSON):
  {
    "exists_on_s3": bool,
    "local": "work/fact_table/current_fact.parquet",
    "bytes": int|null,
    "action": "downloaded" | "promoted_delta_to_current" | "created_empty_current"
  }

Notes:
- Avoids *all* Parquet reads to prevent pyarrow native crashes on Windows.
- Normalization/dedup happens later in 07; here we just ensure a current file exists.
"""
import argparse
import json
import os
import shutil
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Optional: only used for --init empty file
try:
    import pandas as pd  # noqa
    _HAVE_PANDAS = True
except Exception:
    _HAVE_PANDAS = False

SCHEMA = ["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]


# ---------- helpers ---------------------------------------------------------

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"Bad S3 URI: {uri}")
    no_scheme = uri[5:]
    bucket, key = no_scheme.split("/", 1)
    return bucket, key


def head_object(bucket: str, key: str):
    s3 = boto3.client("s3")
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as ce:
        code = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in (403, 404):
            return None
        raise


def download_object(bucket: str, key: str, dest: Path) -> bool:
    s3 = boto3.client("s3")
    ensure_dir(dest.parent)
    try:
        s3.download_file(bucket, key, str(dest))
        return True
    except ClientError as ce:
        code = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in (403, 404):
            return False
        raise


def file_size_or_none(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except Exception:
        return None


def create_empty_parquet(dest: Path):
    """Best-effort empty writer (requires pandas/pyarrow)."""
    if not _HAVE_PANDAS:
        raise RuntimeError("Cannot --init empty parquet: pandas/pyarrow not available.")
    import pyarrow as pa  # local import to avoid module import if unused
    import pyarrow.parquet as pq
    ensure_dir(dest.parent)
    table = pa.table({
        "entity_code": pa.array([], type=pa.string()),
        "observed_at": pa.array([], type=pa.string()),
        "wait_time_minutes": pa.array([], type=pa.int32()),
        "wait_time_type": pa.array([], type=pa.string()),
    })
    pq.write_table(table, str(dest), compression="snappy")


def promote_by_copy(delta_path: Path, current_path: Path) -> int | None:
    """Copy delta file to current without reading it. Returns bytes or None."""
    ensure_dir(current_path.parent)
    if not delta_path.is_file():
        return None
    shutil.copy2(delta_path, current_path)
    return file_size_or_none(current_path)


# ---------- main ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Ensure work/current_fact.parquet exists (download or promote).")
    ap.add_argument("--s3", dest="s3_uri",
                    default=os.getenv("S3_FACT_URI",
                                      "s3://touringplans_stats/stats_work/fact_table/wait_time_fact_table.parquet"))
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--delta", dest="delta_parquet", default=None,
                    help="Optional: explicit path to local delta parquet "
                         "(defaults to work/fact_table/delta_minimal.parquet; "
                         "fallback to delta_converted.parquet if needed)")
    ap.add_argument("--init", dest="init_empty", action="store_true",
                    help="If no S3 object and no delta, create an empty current parquet.")
    args = ap.parse_args()

    bkt, key = parse_s3_uri(args.s3_uri)
    out_dir = Path(args.loc_work) / "fact_table"
    ensure_dir(out_dir)

    current_path = out_dir / "current_fact.parquet"

    # Prefer delta_minimal; fallback to delta_converted if present
    default_delta = out_dir / "delta_minimal.parquet"
    fallback_delta = out_dir / "delta_converted.parquet"
    delta_path = Path(args.delta_parquet) if args.delta_parquet else (
        default_delta if default_delta.is_file() else fallback_delta
    )

    action = None
    bytes_written = None

    # 1) Try to download current from S3 (no read)
    head = head_object(bkt, key)
    exists_on_s3 = head is not None

    if exists_on_s3:
        ok = download_object(bkt, key, current_path)
        if ok:
            action = "downloaded"
            bytes_written = file_size_or_none(current_path)
        else:
            exists_on_s3 = False  # disappeared between HEAD/GET

    # 2) If not on S3, promote delta → current by raw copy
    if not exists_on_s3 and action is None:
        bytes_written = promote_by_copy(delta_path, current_path)
        if bytes_written is not None and bytes_written > 0:
            action = "promoted_delta_to_current"
        else:
            # 3) Still nothing; optionally initialize empty parquet
            if args.init_empty:
                create_empty_parquet(current_path)
                bytes_written = file_size_or_none(current_path)
                action = "created_empty_current"
            else:
                # Create a 0-byte placeholder so downstream has a path, but mark empty
                ensure_dir(current_path.parent)
                try:
                    current_path.touch(exist_ok=True)
                except Exception:
                    pass
                bytes_written = file_size_or_none(current_path)
                action = "created_empty_current"

    payload = {
        "exists_on_s3": bool(exists_on_s3),
        "local": str(current_path),
        "bytes": bytes_written,
        "action": action
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
