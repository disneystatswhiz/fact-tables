#!/usr/bin/env python3
"""
Builds stats_work/fact_tables/latest_obs_report.csv on S3 with two columns:
  - entity_code
  - latest_observation_date  (YYYY-MM-DD, per entity_code)

Skips work if the latest `needs_processing_*.csv` is empty (same behavior as update.py).

Reads ONLY the needed columns (entity_code, observed_at) from the Parquet on S3,
processing row groups incrementally for low memory use.
"""

from __future__ import annotations

import argparse
import csv
import glob
import sys
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def parse_args():
    ap = argparse.ArgumentParser(
        description="Create latest_obs_report.csv directly from S3 Parquet (skips if needs_processing is empty)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("--bucket", default="touringplans_stats", help="S3 bucket")
    ap.add_argument(
        "--fact-key",
        default="stats_work/fact_tables/wait_time_fact_table.parquet",
        help="S3 key of the Parquet fact table",
    )
    ap.add_argument(
        "--out-key",
        default="stats_work/fact_tables/latest_obs_report.csv",
        help="S3 key for the output CSV",
    )
    ap.add_argument(
        "--list-csv",
        help="Local CSV path from report.py listing keys needing processing (to mirror update.py's skip logic)."
    )
    return ap.parse_args()


def download_s3_to_file(s3, bucket: str, key: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(dst))


def upload_file_to_s3(s3, src: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(src), bucket, key)


def read_needs_list(csv_path: str) -> list[str]:
    """Return list of s3_key values (may be empty)."""
    keys: list[str] = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "s3_key" not in reader.fieldnames:
            raise ValueError(f"{csv_path} missing required column 's3_key'")
        for row in reader:
            k = (row.get("s3_key") or "").strip()
            if k:
                keys.append(k)
    return keys


def main():
    args = parse_args()

    # --- Auto-detect latest needs_processing CSV if none given (same as update.py)
    list_csv = args.list_csv
    if not list_csv:
        candidates = sorted(glob.glob(str(Path("work") / "needs_processing_*.csv")), reverse=True)
        if candidates:
            list_csv = candidates[0]
            print(f"[INFO] latest.py auto-detected list-csv: {list_csv}")
        else:
            print("[WARN] latest.py: No --list-csv provided and no work/needs_processing_*.csv found.")
            print("      Skipping latest_obs_report.csv build.")
            return

    # --- Skip if needs list is empty (mirror update.py)
    try:
        keys = read_needs_list(list_csv)
    except Exception as e:
        print(f"[WARN] latest.py: Failed to read '{list_csv}': {e}")
        print("      Skipping latest_obs_report.csv build.")
        return

    if not keys:
        print(f"[INFO] latest.py: No keys to process; '{list_csv}' is empty. Skipping latest_obs_report.csv.")
        return

    s3 = boto3.client("s3")

    # 1) Download the Parquet to a temp location (single file Parquet)
    tmpdir = Path(tempfile.mkdtemp(prefix="latest_obs_report_"))
    local_parquet = tmpdir / "wait_time_fact_table.parquet"
    local_csv = tmpdir / "latest_obs_report.csv"

    try:
        download_s3_to_file(s3, args.bucket, args.fact_key, local_parquet)
        print(f"[INFO] downloaded s3://{args.bucket}/{args.fact_key}")
    except ClientError as e:
        raise SystemExit(f"[ERROR] Could not download fact table: {e}")

    # 2) Iterate row groups, read only required columns
    pf = pq.ParquetFile(str(local_parquet))
    required_cols = ["entity_code", "observed_at"]

    latest_by_entity: dict[str, str] = {}

    for i in range(pf.num_row_groups):
        tbl = pf.read_row_group(i, columns=required_cols)
        df = tbl.to_pandas(types_mapper=pd.ArrowDtype)

        if df.empty:
            continue

        df["entity_code"] = df["entity_code"].astype("string").str.upper().str.strip()
        df["observed_at"] = df["observed_at"].astype("string").str.strip()
        df = df.dropna(subset=["entity_code", "observed_at"])
        if df.empty:
            continue

        # observed_at is already local-with-offset; take the local date portion
        df["obs_date"] = df["observed_at"].str.slice(0, 10)  # 'YYYY-MM-DD'

        chunk_max = df.groupby("entity_code", as_index=False)["obs_date"].max()
        for row in chunk_max.itertuples(index=False):
            ec = str(row.entity_code)
            d = str(row.obs_date)
            prev = latest_by_entity.get(ec)
            if prev is None or d > prev:
                latest_by_entity[ec] = d

        print(f"[INFO] processed row group {i+1}/{pf.num_row_groups} (unique entities so far: {len(latest_by_entity):,})")

    if not latest_by_entity:
        # Table present but empty -> publish empty CSV (harmless)
        pd.DataFrame(columns=["entity_code", "latest_observation_date"]).to_csv(local_csv, index=False)
        upload_file_to_s3(s3, local_csv, args.bucket, args.out_key)
        print(f"[SUCCESS] Wrote empty latest_obs_report to s3://{args.bucket}/{args.out_key}")
        return

    out = (
        pd.DataFrame(
            {
                "entity_code": list(latest_by_entity.keys()),
                "latest_observation_date": list(latest_by_entity.values()),
            }
        )
        .sort_values("entity_code")
        .reset_index(drop=True)
    )
    assert out["entity_code"].is_unique, "Duplicate entity_code rows produced unexpectedly."

    out.to_csv(local_csv, index=False)
    upload_file_to_s3(s3, local_csv, args.bucket, args.out_key)
    print(f"[SUCCESS] Wrote {len(out):,} rows to s3://{args.bucket}/{args.out_key}")


if __name__ == "__main__":
    main()
