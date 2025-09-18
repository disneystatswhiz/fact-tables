#!/usr/bin/env python3
"""
07_merge_and_dedupe.py
---------------------------------
Purpose: Merge the current fact Parquet (from step 06) with the new delta
Parquet (from step 05), then de-duplicate by
(entity_code, observed_at, wait_time_type), keeping the *last* occurrence.
Writes a merged Parquet for the uploader step.

Inputs (ENV/CLI):
  --work         -> LOC_WORK (default ./work)
  --current      -> current fact parquet   (default work/fact_table/current_fact.parquet)
  --delta        -> delta parquet          (default work/fact_table/delta_converted.parquet)
  --out          -> merged parquet         (default work/fact_table/merged_fact.parquet)
  --sample-csv   -> CSV preview path       (default work/fact_table/merged_fact_sample.csv)
  --sample-n     -> number of preview rows (default 50)
  --check-strict -> warn if any observed_at is not strict ISO8601 with offset
  --compression  -> parquet compression (default: zstd; options: zstd, snappy, gzip, none)

Notes:
- Keeps only minimal schema columns.
- Dedupe policy: keep the last record per key tuple; delta “wins” over current.
- Fast path: if one side is empty, we copy the other Parquet without loading to pandas.
"""
import argparse, json, os, re, shutil
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

SCHEMA = ["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]
KEYS = ["entity_code", "observed_at", "wait_time_type"]
STRICT_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$")

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def parquet_rows(path: Path) -> int:
    try:
        return pq.ParquetFile(str(path)).metadata.num_rows
    except Exception:
        return 0

def load_parquet_safe(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=SCHEMA)
    try:
        df = pd.read_parquet(path)
        for col in SCHEMA:
            if col not in df.columns:
                df[col] = pd.Series(dtype="string" if col != "wait_time_minutes" else "Int64")
        df = df[SCHEMA]
        df["entity_code"] = df["entity_code"].astype("string")
        df["observed_at"] = df["observed_at"].astype("string")
        df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
        df["wait_time_type"] = df["wait_time_type"].astype("string")
        return df
    except Exception:
        return pd.DataFrame(columns=SCHEMA)

def main():
    ap = argparse.ArgumentParser(description="Merge current fact + delta, dedupe, write merged parquet")
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--current", dest="current_parquet", default=None)
    ap.add_argument("--delta", dest="delta_parquet", default=None)
    ap.add_argument("--out", dest="out_parquet", default=None)
    ap.add_argument("--sample-csv", dest="sample_csv", default=None)
    ap.add_argument("--sample-n", dest="sample_n", type=int, default=int(os.getenv("MERGED_SAMPLE_N", 50)))
    ap.add_argument("--check-strict", action="store_true")
    ap.add_argument("--compression", default="zstd", choices=["zstd","snappy","gzip","none"])
    args = ap.parse_args()

    out_dir = Path(args.loc_work) / "fact_table"
    ensure_dir(out_dir)

    current_path = Path(args.current_parquet) if args.current_parquet else (out_dir / "current_fact.parquet")
    delta_path   = Path(args.delta_parquet)   if args.delta_parquet   else (out_dir / "delta_converted.parquet")
    out_path     = Path(args.out_parquet)     if args.out_parquet     else (out_dir / "merged_fact.parquet")
    sample_csv   = Path(args.sample_csv)      if args.sample_csv      else (out_dir / "merged_fact_sample.csv")
    sample_n     = max(0, int(args.sample_n))

    # Fast paths to avoid heavy pandas work on big files
    cur_rows = parquet_rows(current_path) if current_path.is_file() else 0
    delta_rows = parquet_rows(delta_path) if delta_path.is_file() else 0

    if cur_rows == 0 and delta_rows == 0:
        empty = pd.DataFrame(columns=SCHEMA)
        empty.to_parquet(out_path, index=False)
        empty.head(0).to_csv(sample_csv, index=False)
        print(json.dumps({"merged_rows": 0, "current_rows": 0, "delta_rows": 0,
                          "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": 0}))
        return

    if cur_rows == 0 and delta_rows > 0:
        ensure_dir(out_path.parent)
        shutil.copy2(delta_path, out_path)
        # write sample from delta quickly
        pd.read_parquet(delta_path).head(sample_n).to_csv(sample_csv, index=False)
        print(json.dumps({"merged_rows": int(delta_rows), "current_rows": 0, "delta_rows": int(delta_rows),
                          "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": int(min(sample_n, delta_rows))}))
        return

    if delta_rows == 0 and cur_rows > 0:
        ensure_dir(out_path.parent)
        shutil.copy2(current_path, out_path)
        pd.read_parquet(current_path).head(sample_n).to_csv(sample_csv, index=False)
        print(json.dumps({"merged_rows": int(cur_rows), "current_rows": int(cur_rows), "delta_rows": 0,
                          "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": int(min(sample_n, cur_rows))}))
        return

    # Normal path: load both to pandas (fine for daily deltas)
    current = load_parquet_safe(current_path)
    delta   = load_parquet_safe(delta_path)

    if args.check_strict:
        bad_cur = (~current["observed_at"].astype(str).str.match(STRICT_ISO_RE, na=False)).sum()
        bad_del = (~delta["observed_at"].astype(str).str.match(STRICT_ISO_RE, na=False)).sum()
        if bad_cur or bad_del:
            print(f"[warn] non-strict observed_at: current={bad_cur}, delta={bad_del}")

    frames = []
    if not current.empty:
        frames.append(current)
    if not delta.empty:
        frames.append(delta)

    all_df = pd.concat(frames, ignore_index=True, copy=False)

    # Keep the *last* occurrence per key (delta wins)
    all_df = all_df.iloc[::-1].drop_duplicates(subset=KEYS, keep="first").iloc[::-1]

    # Column order/types
    all_df = all_df[SCHEMA]
    all_df["entity_code"] = all_df["entity_code"].astype("string")
    all_df["observed_at"] = all_df["observed_at"].astype("string")
    all_df["wait_time_minutes"] = pd.to_numeric(all_df["wait_time_minutes"], errors="coerce").astype("Int64")
    all_df["wait_time_type"] = all_df["wait_time_type"].astype("string")

    ensure_dir(out_path.parent)
    comp = None if args.compression == "none" else args.compression
    all_df.to_parquet(out_path, index=False, compression=comp)

    preview = min(sample_n, len(all_df))
    (all_df.head(preview)).to_csv(sample_csv, index=False) if preview > 0 else (all_df.head(0).to_csv(sample_csv, index=False))

    print(json.dumps({
        "merged_rows": int(len(all_df)),
        "current_rows": int(len(current)),
        "delta_rows": int(len(delta)),
        "out": str(out_path),
        "sample_csv": str(sample_csv),
        "sample_rows": int(preview)
    }))

if __name__ == "__main__":
    main()
