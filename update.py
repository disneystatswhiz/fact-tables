#!/usr/bin/env python3
"""
09_update_from_list.py
----------------------
STEP 2/2: Incrementally update the Wait Time FACT TABLE using a CSV produced by 08_report_daily.py.

Process
-------
1) Read local CSV of "needs processing" S3 keys (from 08_report_daily.py).
2) Download the current fact-table parquet from S3 and stream its rows into a new parquet,
   indexing PKs in a disk-backed SQLite DB to enforce append-only de-dup.
3) Parse each listed S3 source file into the 4-column schema:
     - entity_code (UPPER)
     - observed_at (string with timezone offset)
     - wait_time_type ("POSTED" | "ACTUAL" | "PRIORITY")
     - wait_time_minutes (int; PRIORITY uses 8888 for sellout)
   Apply identical rules used by the main builder (compact-time guard for PRIORITY, tz from path).
4) Write only *new* rows (not already present in the existing parquet) to the new parquet.
5) Atomically upload the merged parquet back to S3 (optional backup).

Assumes:
- boto3, pandas, pyarrow, sqlite3 available; AWS creds configured.

Usage example:
python 09_update_from_list.py \
  --bucket touringplans_stats \
  --fact-key stats_work/fact_tables/wait_time_fact_table.parquet \
  --list-csv work/needs_processing_YYYYMMDD.csv \
  --atomic true \
  --backup true
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import random
import re
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo

# ---------- Logging: line-buffered (Git Bash friendly)
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

# ---------- Defaults
CHUNKSIZE = 250_000
SAMPLE_K = 1000  # small sanity sample written next to temp parquet (optional)

# ---------- Simple data class
@dataclass
class S3ObjRef:
    key: str
    last_modified_utc: Optional[str] = None
    size_bytes: Optional[int] = None

# ---------- S3 helpers
def parse_s3_uri(uri: str) -> Tuple[str, str]:
    assert uri.startswith("s3://"), "S3 URI must start with s3://"
    _, rest = uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    return bucket, key

def download_s3_to_file(s3, bucket: str, key: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(local_path))

def upload_file_to_s3(s3, local_path: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(local_path), bucket, key)

def copy_on_s3(s3, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
    s3.copy_object(Bucket=dst_bucket, Key=dst_key, CopySource={"Bucket": src_bucket, "Key": src_key})

# ---------- CSV list loader
def read_needs_list(csv_path: str) -> List[str]:
    """
    Read the CSV produced by 08_report_daily.py and return the list of S3 keys.
    Expects at least a 's3_key' column.
    """
    keys: List[str] = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "s3_key" not in reader.fieldnames:
            raise ValueError(f"{csv_path} missing required column 's3_key'")
        for row in reader:
            k = (row.get("s3_key") or "").strip()
            if k:
                keys.append(k)
    return keys

# ---------- TZ utilities (derive from key path)
_TZ_REGEX_HAS_OFFSET = re.compile(r"(?:Z|[+\-]\d{2}:?\d{2})\Z", re.IGNORECASE)

def _prop_from_key(key: str) -> str:
    parts = key.split("/")
    for marker in ("wait_times", "fastpass_times"):
        if marker in parts:
            i = parts.index(marker)
            if i + 1 < len(parts):
                return parts[i + 1].lower().strip()
    return ""

def _zone_from_key(key: str) -> ZoneInfo:
    prop = _prop_from_key(key)
    if prop == "dlr":
        return ZoneInfo("America/Los_Angeles")
    if prop == "tdr":
        return ZoneInfo("Asia/Tokyo")
    # wdw / uor / ush / unknown → Orlando
    return ZoneInfo("America/New_York")

def ensure_observed_at_has_offset(df: pd.DataFrame, tz: ZoneInfo) -> pd.DataFrame:
    if df.empty or "observed_at" not in df.columns:
        return df
    s = df["observed_at"].astype("string").str.strip()
    missing_tz_mask = ~s.fillna("").str.contains(_TZ_REGEX_HAS_OFFSET, regex=True, na=False)
    if missing_tz_mask.any():
        to_parse = s[missing_tz_mask]
        parsed = pd.to_datetime(to_parse, errors="coerce")
        ok = parsed.notna()
        if ok.any():
            localized = parsed[ok].dt.tz_localize(
                tz, nonexistent="shift_forward", ambiguous="infer"
            )
            formatted = localized.dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
                r"([+\-]\d{2})(\d{2})$", r"\1:\2", regex=True
            )
            df.loc[formatted.index, "observed_at"] = formatted
    return df

# ---------- Dedupe SQLite
def ensure_sqlite(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dedupe_keys (
            entity_code TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            wait_time_type TEXT NOT NULL,
            wait_time_minutes INTEGER NOT NULL,
            PRIMARY KEY (entity_code, observed_at, wait_time_type, wait_time_minutes)
        )
    """)
    conn.commit()

def insert_new_mask(conn: sqlite3.Connection, df: pd.DataFrame) -> pd.Series:
    cur = conn.cursor()
    cur.execute("BEGIN")
    mask = []
    # Ensure type for PK column stability
    ints = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
    for t in zip(
        df["entity_code"].astype(str),
        df["observed_at"].astype(str),
        df["wait_time_type"].astype(str),
        ints.fillna(-999999).astype(int),  # placeholder but rows w/ NA should be dropped earlier
    ):
        cur.execute(
            "INSERT OR IGNORE INTO dedupe_keys(entity_code,observed_at,wait_time_type,wait_time_minutes) VALUES (?,?,?,?)",
            t
        )
        mask.append(cur.rowcount == 1)
    conn.commit()
    return pd.Series(mask, index=df.index)

# ---------- STANDBY parsing (same as builder)
def parse_standby_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    lower = {c: c.lower().strip() for c in chunk.columns}
    df = chunk.rename(columns=lower)

    out_frames = []

    if "submitted_posted_time" in df.columns:
        p = df[["entity_code", "observed_at", "submitted_posted_time"]].dropna()
        if not p.empty:
            p = p.rename(columns={"submitted_posted_time": "wait_time_minutes"}).copy()
            p["wait_time_minutes"] = pd.to_numeric(p["wait_time_minutes"], errors="coerce").round().astype("Int64")
            p = p[(p["wait_time_minutes"] >= 0) & (p["wait_time_minutes"] <= 1000)]
            p["wait_time_type"] = "POSTED"
            out_frames.append(p)

    if "submitted_actual_time" in df.columns:
        a = df[["entity_code", "observed_at", "submitted_actual_time"]].dropna()
        if not a.empty:
            a = a.rename(columns={"submitted_actual_time": "wait_time_minutes"}).copy()
            a["wait_time_minutes"] = pd.to_numeric(a["wait_time_minutes"], errors="coerce").round().astype("Int64")
            a = a[(a["wait_time_minutes"] >= 0) & (a["wait_time_minutes"] <= 1000)]
            a["wait_time_type"] = "ACTUAL"
            out_frames.append(a)

    if not out_frames:
        return pd.DataFrame(columns=["entity_code","observed_at","wait_time_type","wait_time_minutes"])

    out = pd.concat(out_frames, ignore_index=True)
    out["entity_code"] = out["entity_code"].astype("string").str.upper().str.strip()
    out["observed_at"] = out["observed_at"].astype("string").str.strip()
    return out[["entity_code","observed_at","wait_time_type","wait_time_minutes"]]

# ---------- PRIORITY parsing (same as builder)
PRIO_COLS = ["FATTID","FDAY","FMONTH","FYEAR","FHOUR","FMIN","FWINHR","FWINMIN"]

def _split_hhmm_or_hhmmss_to_hour_min(x: pd.Series) -> tuple[pd.Series, pd.Series]:
    v = pd.to_numeric(x, errors="coerce")
    h = pd.Series(pd.NA, index=v.index, dtype="Int64")
    m = pd.Series(pd.NA, index=v.index, dtype="Int64")
    hhmmss = v >= 10000
    hhmm   = (v >= 100) & (v < 10000)
    hour   = v < 100
    if hhmmss.any():
        vv = v.where(hhmmss)
        h.loc[hhmmss] = (vv // 10000).astype("Int64")
        m.loc[hhmmss] = ((vv % 10000) // 100).astype("Int64")
    if hhmm.any():
        vv = v.where(hhmm)
        h.loc[hhmm] = (vv // 100).astype("Int64")
        m.loc[hhmm] = (vv % 100).astype("Int64")
    if hour.any():
        vv = v.where(hour)
        h.loc[hour] = vv.astype("Int64")
    return h, m

def _normalize_priority_compact_times(df: pd.DataFrame) -> pd.DataFrame:
    if "FHOUR" in df.columns:
        h_obs, m_obs = _split_hhmm_or_hhmmss_to_hour_min(df["FHOUR"])
        if "FMIN" in df.columns:
            m_obs = m_obs.fillna(pd.to_numeric(df["FMIN"], errors="coerce").astype("Int64"))
        df["FHOUR"] = h_obs.fillna(0).astype("Int64")
        df["FMIN"]  = m_obs.fillna(0).astype("Int64")
    if "FWINHR" in df.columns:
        h_ret, m_ret = _split_hhmm_or_hhmmss_to_hour_min(df["FWINHR"])
        if "FWINMIN" in df.columns:
            m_ret = m_ret.fillna(pd.to_numeric(df["FWINMIN"], errors="coerce").astype("Int64"))
        df["FWINHR"]  = h_ret.fillna(0).astype("Int64")
        df["FWINMIN"] = m_ret.fillna(0).astype("Int64")
    return df

def _priority_rows_to_minutes(df: pd.DataFrame) -> pd.Series:
    df = _normalize_priority_compact_times(df.copy())
    Y = pd.to_numeric(df["FYEAR"],  errors="coerce").astype("Int64")
    M = pd.to_numeric(df["FMONTH"], errors="coerce").astype("Int64")
    D = pd.to_numeric(df["FDAY"],   errors="coerce").astype("Int64")
    h_obs = pd.to_numeric(df["FHOUR"],   errors="coerce").fillna(0).clip(0, 23).astype("Int64")
    m_obs = pd.to_numeric(df["FMIN"],    errors="coerce").fillna(0).clip(0, 59).astype("Int64")
    h_ret = pd.to_numeric(df["FWINHR"],  errors="coerce").fillna(0).astype("Int64")
    m_ret = pd.to_numeric(df["FWINMIN"], errors="coerce").fillna(0).astype("Int64")
    sellout_mask = pd.to_numeric(df["FWINHR"], errors="coerce") >= 8000
    obs = pd.to_datetime(dict(year=Y, month=M, day=D, hour=h_obs, minute=m_obs), errors="coerce")
    ret = pd.to_datetime(dict(year=Y, month=M, day=D,
                              hour=h_ret.clip(0, 23), minute=m_ret.clip(0, 59)), errors="coerce")
    ret = ret.mask(sellout_mask, pd.Timestamp(year=2099, month=12, day=31))
    rollover = (ret - obs) < pd.Timedelta(minutes=-15)
    ret = ret + pd.to_timedelta(rollover.fillna(False).astype(int), unit="D")
    minutes = ((ret - obs) / pd.Timedelta(minutes=1)).round().astype("Int64")
    minutes = minutes.mask(sellout_mask, 8888)
    return minutes

def _format_priority(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_priority_compact_times(df.copy())
    minutes = _priority_rows_to_minutes(df)
    Y = pd.to_numeric(df["FYEAR"],  errors="coerce").astype("Int64")
    M = pd.to_numeric(df["FMONTH"], errors="coerce").astype("Int64")
    D = pd.to_numeric(df["FDAY"],   errors="coerce").astype("Int64")
    h_obs = pd.to_numeric(df["FHOUR"], errors="coerce").fillna(0).clip(0, 23).astype("Int64")
    m_obs = pd.to_numeric(df["FMIN"],  errors="coerce").fillna(0).clip(0, 59).astype("Int64")
    obs = pd.to_datetime(dict(year=Y, month=M, day=D, hour=h_obs, minute=m_obs), errors="coerce") \
             .dt.strftime("%Y-%m-%dT%H:%M:%S")
    out = pd.DataFrame({
        "entity_code": df["FATTID"].astype(str).str.upper().str.strip(),
        "observed_at": obs,
        "wait_time_type": "PRIORITY",
        "wait_time_minutes": minutes
    })
    out = out.dropna(subset=["entity_code","observed_at","wait_time_minutes"])
    return out[["entity_code","observed_at","wait_time_type","wait_time_minutes"]]

def parse_priority_chunk_headered(chunk: pd.DataFrame) -> pd.DataFrame:
    df = chunk.rename(columns={c: c.upper().strip() for c in chunk.columns})
    minimal = {"FATTID","FDAY","FMONTH","FYEAR","FHOUR","FWINHR"}
    if not minimal.issubset(df.columns):
        return pd.DataFrame(columns=["entity_code","observed_at","wait_time_type","wait_time_minutes"])
    keep = [c for c in PRIO_COLS if c in df.columns]
    df = df[keep].copy()
    return _format_priority(df)

def parse_priority_chunk_headerless(chunk: pd.DataFrame) -> pd.DataFrame:
    df = chunk.iloc[:, :8].copy()
    df.columns = PRIO_COLS
    return _format_priority(df)

def s3_text_stream(s3, bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return io.TextIOWrapper(obj["Body"], encoding="utf-8", errors="replace", newline="")

def parse_priority_stream(s3, bucket: str, key: str) -> Iterable[pd.DataFrame]:
    is_new = False
    try:
        probe = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-8192")["Body"].read()
        head = pd.read_csv(io.BytesIO(probe), nrows=1)
        headered = set(c.upper().strip() for c in head.columns)
        is_new = set(PRIO_COLS).issubset(headered)
    except Exception:
        is_new = False
    stream = s3_text_stream(s3, bucket, key)
    if is_new:
        reader = pd.read_csv(stream, chunksize=CHUNKSIZE, low_memory=False)
        for chunk in reader:
            out = parse_priority_chunk_headered(chunk)
            if not out.empty:
                yield out
    else:
        reader = pd.read_csv(stream, chunksize=CHUNKSIZE, low_memory=False, header=None, skiprows=1)
        for chunk in reader:
            out = parse_priority_chunk_headerless(chunk)
            if not out.empty:
                yield out

def collapse_priority_dupes_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(["entity_code", "observed_at"])
    df = df.drop_duplicates(subset=["entity_code", "observed_at"], keep="last")
    return df

# ---------- Parquet merge helpers
def write_existing_parquet_into_new(conn: sqlite3.Connection, src_parquet: Path, writer: pq.ParquetWriter, schema: pa.Schema) -> int:
    """
    Stream the existing parquet row groups, index keys into SQLite, and write them to the new parquet.
    Returns number of rows written.
    """
    rows = 0
    if not src_parquet.exists() or src_parquet.stat().st_size == 0:
        return 0
    pf = pq.ParquetFile(str(src_parquet))
    for i in range(pf.num_row_groups):
        tbl = pf.read_row_group(i)
        df = tbl.to_pandas(types_mapper=pd.ArrowDtype).astype({
            "entity_code":"string","observed_at":"string","wait_time_type":"string","wait_time_minutes":"Int64"
        }, errors="ignore")
        # Index keys (existing rows shouldn't be filtered out; they're the base)
        _ = insert_new_mask(conn, df)
        writer.write_table(pa.Table.from_pandas(df, preserve_index=False, schema=schema, safe=False))
        rows += len(df)
        print(f"[INFO] Copied existing row group {i+1}/{pf.num_row_groups} ({len(df):,} rows)")
    return rows

# ---------- Main
def parse_args():
    ap = argparse.ArgumentParser(
        description="STEP 2: Update fact table from list of S3 files needing processing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("--bucket", default="touringplans_stats", help="S3 bucket name")
    ap.add_argument("--fact-key",
                    default="stats_work/fact_tables/wait_time_fact_table.parquet",
                    help="Key to the current fact parquet")
    ap.add_argument("--list-csv", help="Local CSV path from 08_report_daily.py")
    ap.add_argument("--atomic", type=lambda x: str(x).lower() in {"1","true","yes"},
                    default=True, help="Atomic upload via temp+copy")
    ap.add_argument("--backup", type=lambda x: str(x).lower() in {"1","true","yes"},
                    default=True, help="Copy existing parquet to .bak before replacing")
    ap.add_argument("--chunksize", type=int, default=CHUNKSIZE)

    args = ap.parse_args()

    # --- Auto-detect latest needs_processing CSV if none given ---
    if not args.list_csv:
        import glob
        candidates = sorted(glob.glob(os.path.join("work", "needs_processing_*.csv")), reverse=True)
        if candidates:
            args.list_csv = candidates[0]
            print(f"[INFO] Auto-detected latest list-csv: {args.list_csv}")
        else:
            print("[WARN] No --list-csv provided and no work/needs_processing_*.csv found.")
            print("       Please run 08_report_daily.py first to generate it.")
            sys.exit(0)

    return args

def atomic_upload_parquet(s3, local_parquet: Path, bucket: str, key: str, atomic: bool):
    if not atomic:
        upload_file_to_s3(s3, local_parquet, bucket, key)
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tmp_key = f"{key}.tmp.{ts}"
    upload_file_to_s3(s3, local_parquet, bucket, tmp_key)
    copy_on_s3(s3, bucket, tmp_key, bucket, key)
    try:
        s3.delete_object(Bucket=bucket, Key=tmp_key)
    except ClientError:
        pass

def main():
    args = parse_args()
    random.seed(1234)

    s3 = boto3.client("s3")
    bucket = args.bucket
    fact_key = args.fact_key

    # Load list
    keys = read_needs_list(args.list_csv)
    if not keys:
        print(f"[INFO] No keys to process; '{args.list_csv}' is empty.")
        return

    # Prepare temp workspace
    tmp_dir = Path(tempfile.mkdtemp(prefix="wait_fact_update_"))
    tmp_existing_parquet = tmp_dir / "existing.parquet"
    tmp_new_parquet = tmp_dir / "merged.parquet"
    tmp_sqlite = tmp_dir / "dedupe.sqlite"
    tmp_sample = tmp_dir / "update_sample.csv"

    # Download existing parquet (if exists)
    fact_exists = True
    try:
        download_s3_to_file(s3, bucket, fact_key, tmp_existing_parquet)
        print(f"[INFO] Downloaded existing parquet: s3://{bucket}/{fact_key} -> {tmp_existing_parquet}")
    except ClientError as e:
        fact_exists = False
        print(f"[WARN] Could not download existing parquet (treating as new table): {e}")

    # Schema + writer for merged output
    schema = pa.schema([
        ("entity_code", pa.string()),
        ("observed_at", pa.string()),
        ("wait_time_type", pa.string()),
        ("wait_time_minutes", pa.int64()),
    ])
    writer = pq.ParquetWriter(str(tmp_new_parquet), schema=schema, compression="zstd")

    # Deduper
    conn = sqlite3.connect(tmp_sqlite)
    ensure_sqlite(conn)

    kept_rows = 0
    # First, stream existing parquet into new parquet + index keys
    if fact_exists:
        kept_rows += write_existing_parquet_into_new(conn, tmp_existing_parquet, writer, schema)

    # Reservoir sample
    sample_rows: List[pd.Series] = []
    seen = 0
    def reservoir_update(reservoir: List[pd.Series], row: pd.Series, k: int, seen_n: int):
        if len(reservoir) < k:
            reservoir.append(row)
        else:
            import random as _rnd
            j = _rnd.randint(0, seen_n)
            if j < k:
                reservoir[j] = row

    # Process each S3 source in the list
    total = len(keys)
    for i, key in enumerate(keys, 1):
        print(f"[INFO] ({i}/{total}) {key}")
        try:
            tz = _zone_from_key(key)

            if "/wait_times/" in key:
                # STANDBY
                stream = s3_text_stream(s3, bucket, key)
                reader = pd.read_csv(stream, chunksize=args.chunksize, low_memory=False)
                for chunk in reader:
                    df = parse_standby_chunk(chunk)
                    if df.empty:
                        continue
                    df = ensure_observed_at_has_offset(df, tz)
                    df = df.dropna(subset=["entity_code","observed_at","wait_time_minutes"]).copy()
                    df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
                    df = df.dropna(subset=["wait_time_minutes"])
                    new_mask = insert_new_mask(conn, df)
                    new_df = df.loc[new_mask]
                    if new_df.empty:
                        continue
                    writer.write_table(pa.Table.from_pandas(new_df, preserve_index=False, schema=schema, safe=False))
                    kept_rows += len(new_df)
                    for _, r in new_df.iterrows():
                        reservoir_update(sample_rows, r, SAMPLE_K, seen)
                        seen += 1

            elif "/fastpass_times/" in key:
                # PRIORITY
                for out in parse_priority_stream(s3, bucket, key):
                    if out.empty:
                        continue
                    before = len(out)
                    out = collapse_priority_dupes_keep_last(out)
                    collapsed = before - len(out)
                    if collapsed:
                        print(f"[INFO] collapsed {collapsed} duplicate PRIORITY timestamps in {key}")
                    out = ensure_observed_at_has_offset(out, tz)
                    out = out.dropna(subset=["entity_code","observed_at","wait_time_minutes"]).copy()
                    out["wait_time_minutes"] = pd.to_numeric(out["wait_time_minutes"], errors="coerce").astype("Int64")
                    out = out.dropna(subset=["wait_time_minutes"])
                    new_mask = insert_new_mask(conn, out)
                    new_df = out.loc[new_mask]
                    if new_df.empty:
                        continue
                    writer.write_table(pa.Table.from_pandas(new_df, preserve_index=False, schema=schema, safe=False))
                    kept_rows += len(new_df)
                    for _, r in new_df.iterrows():
                        reservoir_update(sample_rows, r, SAMPLE_K, seen)
                        seen += 1
            else:
                print(f"[WARN] Unrecognized key family (skipping): {key}")
                continue

        except ClientError as e:
            print(f"[WARN] S3 error for {key}: {e}")
            continue
        except Exception as e:
            print(f"[WARN] Error processing {key}: {e}")
            continue

    # Finish parquet
    writer.close()
    print(f"[INFO] Merged parquet rows (existing + new): {kept_rows:,}")

    # Optional sample dump for quick inspection
    if sample_rows:
        sample_df = pd.DataFrame(sample_rows).sample(frac=1.0, random_state=42).reset_index(drop=True)
    else:
        sample_df = pd.DataFrame(columns=["entity_code","observed_at","wait_time_type","wait_time_minutes"])
    sample_df.to_csv(tmp_sample, index=False)
    print(f"[INFO] Wrote update sample CSV: {tmp_sample} ({len(sample_df):,} rows)")

    # Backup existing parquet (optional)
    if fact_exists and args.backup:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bak_key = f"{fact_key}.bak.{ts}"
        try:
            copy_on_s3(s3, bucket, fact_key, bucket, bak_key)
            print(f"[INFO] Backed up existing parquet to s3://{bucket}/{bak_key}")
        except ClientError as e:
            print(f"[WARN] Backup copy failed: {e}")

    # Atomic upload of new parquet
    atomic_upload_parquet(s3, tmp_new_parquet, bucket, fact_key, args.atomic)
    print(f"[SUCCESS] Published updated parquet to s3://{bucket}/{fact_key}")

if __name__ == "__main__":
    main()
