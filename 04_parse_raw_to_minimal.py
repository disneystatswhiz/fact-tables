#!/usr/bin/env python3
"""
04_parse_raw_to_minimal.py
---------------------------------
Purpose: Parse a list of locally synced raw CSV files into a *minimal* delta
DataFrame for **all entities**, keeping only these columns:
  - entity_code (str, uppercase in source conventions)
  - observed_at (ISO8601 naive string, e.g., 2025-08-01T13:05:00)
  - wait_time_minutes (int)
  - wait_time_type ("POSTED" | "ACTUAL" | "PRIORITY")

Inputs (ENV/CLI):
  --work      -> LOC_WORK  (default ./work)
  --input     -> LOC_INPUT (default ./input)  [not strictly required here]
  --list      -> path to newline-delimited changed-files list from step 03
                 (default: work/fact_table/changed_files.txt)
  --out       -> output parquet path (default: work/fact_table/delta_minimal.parquet)
  --sample-csv -> optional CSV preview path (default: work/fact_table/delta_minimal_sample.csv)
  --sample-n   -> number of preview rows to write (default: 50)

Notes:
- Mirrors the logic of your Julia loaders for standby/new-fastpass/old-fastpass
  but operates in "all-entities" mode (no entity filter). See `CustomLoaders`.
- Priority files: compute minutes until return, with rollover (+1 day) if the
  computed return time is >15 minutes earlier than observed time (sold-out rows
  use 8888 sentinel). Standby files: split to POSTED and ACTUAL with simple
  validation (0 ≤ minutes ≤ 1000 for normal rows).
- Output is a single Parquet file for efficient downstream merging, plus a tiny
  CSV preview when requested.
"""
import argparse
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

CSV_EXT = ".csv"


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def is_priority_path(p: Path) -> bool:
    # Heuristic matches your foldering: .../wait_times/priority/<prop>/...
    return "/priority/" in str(p).replace("\\", "/")


# ----------------------------- Standby ---------------------------------

def parse_standby_file(path: Path) -> pd.DataFrame:
    # Keep only the columns we need. Use dtype=str for observed_at to avoid
    # pandas auto-parsing; we format explicitly.
    usecols = [
        "entity_code", "observed_at",
        "submitted_posted_time", "submitted_actual_time",
    ]
    dtype = {
        "entity_code": "string",
        "observed_at": "string",
        "submitted_posted_time": "float64",
        "submitted_actual_time": "float64",
    }
    try:
        df = pd.read_csv(path, usecols=usecols, dtype=dtype)
    except Exception:
        # If the file doesn't have these columns, return empty
        return pd.DataFrame(columns=["entity_code","observed_at","wait_time_minutes","wait_time_type"])  # empty

    out_frames = []

    if "submitted_posted_time" in df.columns:
        p = df[["entity_code", "observed_at", "submitted_posted_time"]].dropna()
        if not p.empty:
            p = p.rename(columns={"submitted_posted_time": "wait_time_minutes"})
            p["wait_time_minutes"] = p["wait_time_minutes"].round().astype("Int64", errors="ignore")
            p = p[(p["wait_time_minutes"] >= 0) & (p["wait_time_minutes"] <= 1000)]
            p["wait_time_type"] = "POSTED"
            out_frames.append(p)

    if "submitted_actual_time" in df.columns:
        a = df[["entity_code", "observed_at", "submitted_actual_time"]].dropna()
        if not a.empty:
            a = a.rename(columns={"submitted_actual_time": "wait_time_minutes"})
            a["wait_time_minutes"] = a["wait_time_minutes"].round().astype("Int64", errors="ignore")
            a = a[(a["wait_time_minutes"] >= 0) & (a["wait_time_minutes"] <= 1000)]
            a["wait_time_type"] = "ACTUAL"
            out_frames.append(a)

    if not out_frames:
        return pd.DataFrame(columns=["entity_code","observed_at","wait_time_minutes","wait_time_type"])  # empty

    out = pd.concat(out_frames, ignore_index=True)
    out = out[["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]]
    return out


# ----------------------------- Priority (New + Old) ---------------------

def _rows_to_minutes(df: pd.DataFrame) -> pd.Series:
    # Compute minutes until return (with sellout sentinel 8888). Apply rollover
    # correction when return time appears >15 minutes earlier than observed.
    obs = pd.to_datetime(pd.to_datetime(dict(year=df["FYEAR"], month=df["FMONTH"], day=df["FDAY"],
                                            hour=df["FHOUR"], minute=df["FMIN"]))
                       .dt.strftime("%Y-%m-%d %H:%M:%S"))

    ret = pd.to_datetime(pd.to_datetime(dict(year=df["FYEAR"], month=df["FMONTH"], day=df["FDAY"],
                                            hour=df["FWINHR"].clip(lower=0), minute=df["FWINMIN"].fillna(0)))
                        .dt.strftime("%Y-%m-%d %H:%M:%S"))

    # Sellout rows (FWINHR >= 8000)
    sellout_mask = df["FWINHR"] >= 8000
    ret = ret.mask(sellout_mask, pd.Timestamp(year=2099, month=12, day=31))

    # Rollover: if return is more than 15 minutes earlier than observed, add a day
    rollover = (ret - obs) < pd.Timedelta(minutes=-15)
    ret = ret + pd.to_timedelta(rollover.astype(int), unit="D")

    minutes = ((ret - obs) / pd.Timedelta(minutes=1)).round().astype("Int64")
    minutes = minutes.mask(sellout_mask, 8888)
    return minutes


def _format_priority(df: pd.DataFrame) -> pd.DataFrame:
    minutes = _rows_to_minutes(df)
    obs = pd.to_datetime(dict(year=df["FYEAR"], month=df["FMONTH"], day=df["FDAY"],
                              hour=df["FHOUR"], minute=df["FMIN"]))
    out = pd.DataFrame({
        "entity_code": df["FATTID"].astype("string"),
        "observed_at": obs.dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "wait_time_minutes": minutes,
        "wait_time_type": "PRIORITY",
    })
    return out


def parse_priority_file(path: Path) -> pd.DataFrame:
    # Try to detect based on header names; if headerless, use index-based read
    try:
        probe = pd.read_csv(path, nrows=1)
        lower = set(c.lower() for c in probe.columns)
    except Exception:
        lower = set()

    cols_needed = ["FATTID","FDAY","FMONTH","FYEAR","FHOUR","FMIN","FWINHR","FWINMIN"]

    if {c.lower() for c in cols_needed}.issubset(lower):
        # New Fastpass with headers
        df = pd.read_csv(path, usecols=cols_needed)
        return _format_priority(df)
    else:
        # Old Fastpass: headerless; read specific indices and rename
        usecols = [0,1,2,3,4,5,6,7]
        try:
            df = pd.read_csv(path, header=None, skiprows=1, usecols=usecols)
        except Exception:
            return pd.DataFrame(columns=["entity_code","observed_at","wait_time_minutes","wait_time_type"])  # empty
        df.columns = cols_needed
        return _format_priority(df)


# ----------------------------- Dispatcher --------------------------------

def parse_any_wait_file(path: Path) -> pd.DataFrame:
    if is_priority_path(path):
        return parse_priority_file(path)
    else:
        return parse_standby_file(path)


def main():
    ap = argparse.ArgumentParser(description="Parse raw CSVs to minimal delta parquet")
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--input", dest="loc_input", default=os.getenv("LOC_INPUT", str(Path.cwd() / "input")))
    ap.add_argument("--list", dest="list_file", default=None)
    ap.add_argument("--out", dest="out_parquet", default=None)
    ap.add_argument("--sample-csv", dest="sample_csv", default=None)
    ap.add_argument("--sample-n", dest="sample_n", type=int, default=int(os.getenv("DELTA_SAMPLE_N", 50)))
    args = ap.parse_args()

    loc_work = Path(args.loc_work)
    out_dir = loc_work / "fact_table"
    ensure_dir(out_dir)

    list_path = Path(args.list_file) if args.list_file else (out_dir / "changed_files.txt")
    out_parquet = Path(args.out_parquet) if args.out_parquet else (out_dir / "delta_minimal.parquet")
    sample_csv = Path(args.sample_csv) if args.sample_csv else (out_dir / "delta_minimal_sample.csv")
    sample_n = max(0, int(args.sample_n))

    if not list_path.is_file():
        # Write empty outputs for downstream stability
        empty = pd.DataFrame(columns=["entity_code","observed_at","wait_time_minutes","wait_time_type"])
        empty.to_parquet(out_parquet, index=False)
        empty.head(0).to_csv(sample_csv, index=False)
        print(json.dumps({"count": 0, "out": str(out_parquet), "sample_csv": str(sample_csv), "sample_rows": 0}))
        return

    # Stream over files, concat at end to keep memory reasonable
    frames = []
    with list_path.open("r", encoding="utf-8") as f:
        for line in f:
            p = Path(line.strip())
            if not p.suffix.lower() == CSV_EXT or not p.is_file():
                continue
            try:
                df = parse_any_wait_file(p)
                if not df.empty:
                    frames.append(df)
            except Exception:
                # best-effort parse; skip bad files
                continue

    if not frames:
        # Write empty outputs for downstream stability
        empty = pd.DataFrame(columns=["entity_code","observed_at","wait_time_minutes","wait_time_type"])
        empty.to_parquet(out_parquet, index=False)
        empty.head(0).to_csv(sample_csv, index=False)
        print(json.dumps({"count": 0, "out": str(out_parquet), "sample_csv": str(sample_csv), "sample_rows": 0}))
        return

    delta = pd.concat(frames, ignore_index=True)
    # Standardize dtypes
    delta["entity_code"] = delta["entity_code"].astype("string")
    delta["observed_at"] = delta["observed_at"].astype("string")
    delta["wait_time_minutes"] = pd.to_numeric(delta["wait_time_minutes"], errors="coerce").astype("Int64")
    delta["wait_time_type"] = delta["wait_time_type"].astype("string")

    ensure_dir(out_parquet.parent)
    # Requires pyarrow or fastparquet
    delta.to_parquet(out_parquet, index=False)

    # Write tiny CSV preview for eyeballing
    preview_rows = min(sample_n, len(delta))
    if preview_rows > 0:
        delta.head(preview_rows).to_csv(sample_csv, index=False)
    else:
        delta.head(0).to_csv(sample_csv, index=False)

    print(json.dumps({
        "count": int(len(delta)),
        "out": str(out_parquet),
        "sample_csv": str(sample_csv),
        "sample_rows": int(preview_rows)
    }))


if __name__ == "__main__":
    main()
