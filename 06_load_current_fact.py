#!/usr/bin/env python3
"""
05_load_current_fact.py
---------------------------------
Purpose: Download the current Parquet fact table from S3 (if it exists) to the
local workspace and make it available for the merge step. Optionally initialize
an empty Parquet with the correct schema if nothing exists yet.

Inputs (ENV/CLI):
  --s3      -> S3_FACT_URI (default s3://touringplans_stats/stats_work/master/wait_time_fact_table.parquet)
  --work    -> LOC_WORK (default ./work)
  --init    -> if provided, create an empty local Parquet if the S3 object does not exist

Outputs:
  - Local file: work/fact_table/current_fact.parquet (if exists or initialized)
  - STDOUT JSON summary: {"exists":bool, "local":"...", "rows":int | null}

Notes:
- We only *download* (no upload here). Counting rows uses pyarrow to avoid
  loading the entire table into RAM.
- Partitions are not assumed; this script expects a single Parquet object S3 key.
- Coordinates with 01_fact_status.py which already determines LastModified. (The
  Julia utility that inspired this flow is get_last_modified_s3_ts.)
"""
import argparse
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except Exception:
    pq = None
    pa = None


SCHEMA_COLUMNS = ["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]


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
        if ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (403, 404):
            return None
        raise


def download_object(bucket: str, key: str, dest: Path) -> bool:
    s3 = boto3.client("s3")
    ensure_dir(dest.parent)
    try:
        s3.download_file(bucket, key, str(dest))
        return True
    except ClientError as ce:
        if ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (403, 404):
            return False
        raise


def create_empty_parquet(dest: Path) -> None:
    if pq is None or pa is None:
        raise RuntimeError("pyarrow is required to write Parquet. Please `pip install pyarrow`.")
    ensure_dir(dest.parent)
    table = pa.table({
        "entity_code": pa.array([], type=pa.string()),
        "observed_at": pa.array([], type=pa.string()),
        "wait_time_minutes": pa.array([], type=pa.int32()),
        "wait_time_type": pa.array([], type=pa.string()),
    })
    pq.write_table(table, str(dest))


def count_rows_parquet(path: Path):
    if pq is None:
        return None
    try:
        md = pq.ParquetFile(str(path)).metadata
        return md.num_rows if md is not None else None
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="Download current Parquet fact from S3 → work/")
    ap.add_argument("--s3", dest="s3_uri", default=os.getenv("S3_FACT_URI", "s3://touringplans_stats/stats_work/master/wait_time_fact_table.parquet"))
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--init", dest="init_empty", action="store_true")
    args = ap.parse_args()

    bkt, key = parse_s3_uri(args.s3_uri)
    out_dir = Path(args.loc_work) / "fact_table"
    ensure_dir(out_dir)
    local = out_dir / "current_fact.parquet"

    head = head_object(bkt, key)
    exists = head is not None

    rows = None
    if exists:
        ok = download_object(bkt, key, local)
        if ok:
            rows = count_rows_parquet(local)
        else:
            # object disappeared between head and get; treat as non-existent
            exists = False

    if not exists and args.init_empty:
        create_empty_parquet(local)
        rows = 0

    payload = {"exists": bool(exists), "local": str(local), "rows": rows}
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
