#!/usr/bin/env python3
"""
01_fact_status.py
---------------------------------
Purpose: Check whether the Parquet wait_time_fact_table exists on S3 and, if so,
fetch its LastModified timestamp (UTC). Emits a single JSON object to STDOUT and
writes the same JSON to work/fact_table/fact_status.json.

Inputs (ENV or CLI overrides):
  - S3_FACT_URI   (e.g., s3://touringplans_stats/stats_work/master/wait_time_fact_table.parquet)
  - LOC_WORK      (default: ./work)

CLI overrides win over ENV values.

Output JSON schema:
  {
    "exists": bool,
    "bucket": str,
    "key": str,
    "last_modified_utc": "YYYY-MM-DDTHH:MM:SS" | null
  }

Notes:
- Uses boto3 HeadObject to obtain LastModified in UTC.
- Mirrors the intent of the Julia helper `get_last_modified_s3_ts` you already
  use elsewhere, but implemented in Python with boto3.  
- If credentials/region are not configured, rely on standard AWS env/credential chain.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception as e:  # pragma: no cover
    print(json.dumps({
        "error": "Missing boto3 dependency. Please `pip install boto3`.",
        "detail": str(e)
    }))
    sys.exit(1)


# ------------------------- helpers -------------------------

def parse_s3_uri(uri: str):
    """Parse s3://bucket/key into (bucket, key)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Bad S3 URI: {uri}")
    p = urlparse(uri)
    bucket = p.netloc
    key = p.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Bad S3 URI: {uri}")
    return bucket, key


def head_s3_object(bucket: str, key: str):
    """Return HeadObject dict or None if not found."""
    s3 = boto3.client("s3")
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as ce:  # 404, 403, etc.
        if ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (403, 404):
            return None
        raise


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


# --------------------------- main --------------------------

def main():
    parser = argparse.ArgumentParser(description="Report Parquet fact status on S3")
    parser.add_argument("--s3", dest="s3_uri", default=os.getenv("S3_FACT_URI", "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet"))
    parser.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    args = parser.parse_args()

    bucket, key = parse_s3_uri(args.s3_uri)

    head = head_s3_object(bucket, key)
    exists = head is not None

    last_modified_str = None
    if exists:
        # boto3 returns a timezone-aware datetime in UTC by default
        lm: datetime = head["LastModified"]
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=timezone.utc)
        lm_utc = lm.astimezone(timezone.utc)
        last_modified_str = lm_utc.strftime("%Y-%m-%dT%H:%M:%S")

    payload = {
        "exists": exists,
        "bucket": bucket,
        "key": key,
        "last_modified_utc": last_modified_str,
    }

    out_dir = Path(args.loc_work) / "fact_table"
    ensure_dir(out_dir)
    out_file = out_dir / "fact_status.json"

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
