#!/usr/bin/env python3
"""
Build & publish the Wait Time FACT TABLE from S3 (standby + priority).

Outputs (exact 4 columns):
  - entity_code (str, upper)
  - observed_at (ISO-like string WITH timezone offset; naive inputs treated as local)
  - wait_time_type ("POSTED" | "ACTUAL" | "PRIORITY")
  - wait_time_minutes (int; PRIORITY uses 8888 for sellout)

What it does:
  1) Lists all standby CSVs under s3://touringplans_stats/export/wait_times/{prop}/
  2) Lists all priority CSVs under s3://touringplans_stats/export/fastpass_times/{prop}/
  3) Parses to the 4-column long form (04_parse rules), with compact-time guard for priority.
     Also ensures observed_at always has a timezone offset based on S3 path:
       - DLR → America/Los_Angeles
       - TDR → Asia/Tokyo
       - else → America/New_York (Orlando)
  4) De-dupes across all 4 columns via SQLite PK.
  5) Streams Parquet + uploads to s3://touringplans_stats/stats_work/fact_tables/
     and uploads a 1,000-row sample CSV to the same folder.

Assumes: boto3, pandas, pyarrow installed; AWS creds available.
"""

from __future__ import annotations
import argparse
import io
import random
import re
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo

# Make stdout/stderr line-buffered so logs appear immediately (Git Bash, etc.)
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

# ------------------------------- Defaults -------------------------------
DEFAULT_PROPS = ["wdw", "dlr", "uor", "ush", "tdr"]
S3_BUCKET = "touringplans_stats"
S3_STANDBY_PREFIX_FMT = "export/wait_times/{prop}/"
S3_PRIORITY_PREFIX_FMT = "export/fastpass_times/{prop}/"

S3_PARQUET_OUT = "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet"
S3_SAMPLE_OUT  = "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table_sample.csv"

SAMPLE_K  = 1000
CHUNKSIZE = 250_000

# ------------------------------- S3 utils -------------------------------
def list_s3_csvs(s3, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".csv"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    keys.sort()
    return keys

def s3_text_stream(s3, bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return io.TextIOWrapper(obj["Body"], encoding="utf-8", errors="replace", newline="")

def upload_file(s3, local_path: Path, s3_uri: str):
    assert s3_uri.startswith("s3://")
    _, rest = s3_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    s3.upload_file(str(local_path), bucket, key)

# ------------------------------- Dedupe (SQLite PK) -------------------------------
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
    for t in zip(df["entity_code"], df["observed_at"], df["wait_time_type"], df["wait_time_minutes"].astype(int)):
        cur.execute(
            "INSERT OR IGNORE INTO dedupe_keys(entity_code,observed_at,wait_time_type,wait_time_minutes) VALUES (?,?,?,?)",
            t
        )
        mask.append(cur.rowcount == 1)
    conn.commit()
    return pd.Series(mask, index=df.index)

# ------------------------------- Parsing: STANDBY (04 rules) -------------------------------
def parse_standby_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Keep: entity_code, observed_at, submitted_posted_time, submitted_actual_time
    Emit POSTED / ACTUAL rows; 0..1000 range for minutes.
    """
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
    out = out[["entity_code","observed_at","wait_time_type","wait_time_minutes"]]
    return out

# ------------------------------- Parsing: PRIORITY (04 rules + compact-time guard) -------------------------------
PRIO_COLS = ["FATTID","FDAY","FMONTH","FYEAR","FHOUR","FMIN","FWINHR","FWINMIN"]

def _split_hhmm_or_hhmmss_to_hour_min(x: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Accept ints/strings like 23, 2318, 231801 → (hour, minute).
    Non-numeric → NaN.
    """
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
        h.loc[hour] = vv.astype("Int64")  # minutes left NA

    return h, m

def _normalize_priority_compact_times(df: pd.DataFrame) -> pd.DataFrame:
    # Split FHOUR/FMIN and FWINHR/FWINMIN if compact times present.
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
    """
    Compute minutes_until_return with:
      • sellout if FWINHR >= 8000 → 8888
      • rollover +1 day if (return - observed) < -15 minutes
    Works with compact FHOUR/FMIN, FWINHR/FWINMIN after normalization.
    """
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
    # keep only cols we use; tolerate missing FMIN/FWINMIN (filled by compact-time splitting)
    keep = [c for c in PRIO_COLS if c in df.columns]
    df = df[keep].copy()
    return _format_priority(df)

def parse_priority_chunk_headerless(chunk: pd.DataFrame) -> pd.DataFrame:
    # Headerless with first 8 columns in PRIO_COLS order; skip the first row upstream.
    df = chunk.iloc[:, :8].copy()
    df.columns = PRIO_COLS
    return _format_priority(df)

def parse_priority_stream(s3, bucket: str, key: str) -> Iterable[pd.DataFrame]:
    # Probe a small slice to detect headered vs headerless
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

# ------------------------------- Reservoir sample -------------------------------
def reservoir_update(reservoir: List[pd.Series], row: pd.Series, k: int, seen_n: int):
    if len(reservoir) < k:
        reservoir.append(row)
    else:
        j = random.randint(0, seen_n)
        if j < k:
            reservoir[j] = row

# ------------------------------- TZ normalization (from S3 path) ---------------
_TZ_REGEX_HAS_OFFSET = re.compile(r"(?:Z|[+\-]\d{2}:?\d{2})\Z", re.IGNORECASE)

def _prop_from_key(key: str) -> str:
    """Extract property (wdw/dlr/uor/ush/tdr) from an S3 key path."""
    parts = key.split("/")
    for marker in ("wait_times", "fastpass_times"):
        if marker in parts:
            i = parts.index(marker)
            if i + 1 < len(parts):
                return parts[i + 1].lower().strip()
    return ""

def _zone_from_key(key: str) -> ZoneInfo:
    """Map property → ZoneInfo (DLR=LA, TDR=Tokyo, else Orlando)."""
    prop = _prop_from_key(key)
    if prop == "dlr":
        return ZoneInfo("America/Los_Angeles")
    if prop == "tdr":
        return ZoneInfo("Asia/Tokyo")
    # wdw / uor / ush / unknown → Orlando
    return ZoneInfo("America/New_York")

def ensure_observed_at_has_offset(df: pd.DataFrame, tz: ZoneInfo) -> pd.DataFrame:
    """
    Ensure observed_at strings always have a timezone offset.
    For rows already containing a tz suffix (Z or ±HH:MM or ±HHMM), leave as-is.
    For naive rows, treat them as local time in `tz` (DST-aware) and format with ±HH:MM.
    """
    if df.empty or "observed_at" not in df.columns:
        return df

    s = df["observed_at"].astype("string").str.strip()
    # True where NO tz suffix is present
    missing_tz_mask = ~s.fillna("").str.contains(_TZ_REGEX_HAS_OFFSET, regex=True, na=False)

    if missing_tz_mask.any():
        to_parse = s[missing_tz_mask]
        parsed = pd.to_datetime(to_parse, errors="coerce")  # naive datetimes expected
        ok = parsed.notna()
        if ok.any():
            # Localize to zone; handle DST gaps/folds
            localized = parsed[ok].dt.tz_localize(
                tz, nonexistent="shift_forward", ambiguous="infer"
            )
            formatted = localized.dt.strftime("%Y-%m-%dT%H:%M:%S%z") \
                                   .str.replace(r"([+\-]\d{2})(\d{2})$", r"\1:\2", regex=True)
            df.loc[formatted.index, "observed_at"] = formatted

    return df

# ------------------------------- NEW: collapse duplicate PRIORITY timestamps ----
def collapse_priority_dupes_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    """
    Option A: For PRIORITY rows, ensure unique (entity_code, observed_at) by keeping the last.
    """
    if df.empty:
        return df
    df = df.sort_values(["entity_code", "observed_at"])
    df = df.drop_duplicates(subset=["entity_code", "observed_at"], keep="last")
    return df

# ------------------------------- Orchestration -------------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Wait Time FACT TABLE builder (standby + priority)")
    ap.add_argument("--props", default=",".join(DEFAULT_PROPS),
                    help="Comma list of properties (wdw,dlr,uor,ush,tdr)")
    ap.add_argument("--s3-parquet", default=S3_PARQUET_OUT, help="S3 URI for Parquet output")
    ap.add_argument("--s3-sample",  default=S3_SAMPLE_OUT,  help="S3 URI for 1,000-row sample CSV")
    ap.add_argument("--atomic", action="store_true", help="Upload parquet atomically via temp key + copy")
    ap.add_argument("--chunksize", type=int, default=CHUNKSIZE, help="CSV chunksize")
    ap.add_argument("--sample-k", type=int, default=SAMPLE_K, help="Sample size")
    return ap.parse_args()

def atomic_upload_parquet(s3, local_parquet: Path, dest_uri: str, atomic: bool):
    assert dest_uri.startswith("s3://")
    _, rest = dest_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    if not atomic:
        upload_file(s3, local_parquet, dest_uri)
        return
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tmp_key = f"{key}.tmp.{ts}"
    s3.upload_file(str(local_parquet), bucket, tmp_key)
    s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": tmp_key})
    try:
        s3.delete_object(Bucket=bucket, Key=tmp_key)
    except ClientError:
        pass

def main():
    args = parse_args()
    props = [p.strip() for p in args.props.split(",") if p.strip()]

    s3 = boto3.client("s3")

    # temp outputs
    tmp_dir = Path(tempfile.mkdtemp(prefix="wait_fact_build_"))
    tmp_parquet = tmp_dir / "wait_time_fact_table.parquet"
    tmp_sample  = tmp_dir / "wait_time_fact_table_sample.csv"
    tmp_sqlite  = tmp_dir / "dedupe.sqlite"

    # parquet writer with fixed schema
    schema = pa.schema([
        ("entity_code", pa.string()),
        ("observed_at", pa.string()),
        ("wait_time_type", pa.string()),
        ("wait_time_minutes", pa.int64()),
    ])
    writer = pq.ParquetWriter(str(tmp_parquet), schema=schema, compression="zstd")

    # sqlite deduper
    conn = sqlite3.connect(tmp_sqlite)
    ensure_sqlite(conn)

    # sample reservoir
    reservoir: List[pd.Series] = []
    seen = 0
    kept_rows = 0

    try:
        # 1) Iterate properties, list keys for both families
        all_keys: List[Tuple[str, str]] = []
        for prop in props:
            standby_prefix  = S3_STANDBY_PREFIX_FMT.format(prop=prop)
            priority_prefix = S3_PRIORITY_PREFIX_FMT.format(prop=prop)
            skeys = list_s3_csvs(s3, S3_BUCKET, standby_prefix)
            pkeys = list_s3_csvs(s3, S3_BUCKET, priority_prefix)
            print(f"[INFO] {prop}: standby={len(skeys)} priority={len(pkeys)}")
            all_keys.extend([("standby", k) for k in skeys])
            all_keys.extend([("priority", k) for k in pkeys])

        # 2) Stream-parse everything and write deduped parquet
        total = len(all_keys)
        for idx, (kind, key) in enumerate(all_keys, 1):
            print(f"[INFO] ({idx}/{total}) {kind}: {key}")
            try:
                tz = _zone_from_key(key)  # determine zone once per file

                if kind == "standby":
                    stream = s3_text_stream(s3, S3_BUCKET, key)
                    reader = pd.read_csv(stream, chunksize=args.chunksize, low_memory=False)
                    for chunk in reader:
                        df = parse_standby_chunk(chunk)
                        if df.empty:
                            continue
                        # ensure tz suffix
                        df = ensure_observed_at_has_offset(df, tz)
                        # basic cleaning
                        df = df.dropna(subset=["entity_code","observed_at","wait_time_minutes"]).copy()
                        df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
                        df = df.dropna(subset=["wait_time_minutes"])
                        # dedupe across the 4 columns
                        new_mask = insert_new_mask(conn, df)
                        new_df = df.loc[new_mask]
                        if new_df.empty:
                            continue
                        writer.write_table(pa.Table.from_pandas(new_df, preserve_index=False, schema=schema, safe=False))
                        kept_rows += len(new_df)
                        for _, r in new_df.iterrows():
                            reservoir_update(reservoir, r, args.sample_k, seen)
                            seen += 1

                else:  # priority
                    for out in parse_priority_stream(s3, S3_BUCKET, key):
                        if out.empty:
                            continue
                        # Option A: collapse duplicate timestamps per entity (keep last)
                        before = len(out)
                        out = collapse_priority_dupes_keep_last(out)
                        collapsed = before - len(out)
                        if collapsed:
                            print(f"[INFO] collapsed {collapsed} duplicate PRIORITY timestamps in {key}")

                        # ensure tz suffix
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
                            reservoir_update(reservoir, r, args.sample_k, seen)
                            seen += 1

            except ClientError as e:
                print(f"[WARN] skipping {key}: {e}")
                continue
            except Exception as e:
                print(f"[WARN] error in {key}: {e}")
                continue

        writer.close()
        print(f"[INFO] Deduped rows written: {kept_rows:,}")

        # 3) Upload parquet (atomic optional)
        atomic_upload_parquet(s3, tmp_parquet, args.s3_parquet, args.atomic)

        # 4) Upload sample CSV
        if reservoir:
            sample_df = pd.DataFrame(reservoir).sample(frac=1.0, random_state=42).reset_index(drop=True)
        else:
            sample_df = pd.DataFrame(columns=["entity_code","observed_at","wait_time_type","wait_time_minutes"])
        sample_df.to_csv(tmp_sample, index=False)
        upload_file(s3, tmp_sample, args.s3_sample)

        print("[SUCCESS] Published parquet + sample to S3.")

    finally:
        conn.close()
        # Keep tmp_dir for debugging if you want:
        # import shutil; shutil.rmtree(tmp_dir, ignore_errors=True)

if __name__ == "__main__":
    random.seed(1234)
    main()
