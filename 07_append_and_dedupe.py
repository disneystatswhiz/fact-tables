#!/usr/bin/env python3
"""
07_append_and_dedupe.py
-------------------------------------------
Purpose: Append new observations from the delta Parquet (from step 05) onto the
current fact Parquet (from step 06) **without overwriting existing keys**.

Append-only policy:
- We NEVER replace an existing (entity_code, observed_at, wait_time_type) that is already
  in the current fact table. If the delta contains a row with a key that already exists,
  we skip it.
- Within the delta itself, if there are multiple rows for the same key, keep the *last*
  occurrence (delta-internal dedupe) and then append only those keys that are not present
  in current.
- Output is a single merged Parquet for the uploader step.

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
- Append-only: existing keys win; delta adds only truly new keys.
- Fast path: if one side is empty, we project to minimal schema and write/copy accordingly.
"""
import argparse, json, os, re
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

SCHEMA = ["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]
KEYS = ["entity_code", "observed_at", "wait_time_type"]
STRICT_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?[+-]\d{2}:\d{2}$")


# ---------- helpers ---------------------------------------------------------

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def parquet_rows(path: Path) -> int:
    try:
        return pq.ParquetFile(str(path)).metadata.num_rows
    except Exception:
        return 0


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string" if col != "wait_time_minutes" else "Int64")
    df = df[SCHEMA]
    df["entity_code"] = df["entity_code"].astype("string")
    df["observed_at"] = df["observed_at"].astype("string")
    df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
    df["wait_time_type"] = df["wait_time_type"].astype("string")
    return df


def load_parquet_min(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return enforce_schema(pd.DataFrame(columns=SCHEMA))
    # Read only needed columns via Arrow to avoid loading everything
    table = ds.dataset(str(path), format="parquet").to_table(columns=SCHEMA)
    df = pd.DataFrame(table.to_pandas(types_mapper=pd.ArrowDtype))
    return enforce_schema(df)


def write_parquet(df: pd.DataFrame, out_path: Path, compression: str):
    ensure_dir(out_path.parent)
    comp = None if compression == "none" else compression
    enforce_schema(df).to_parquet(out_path, index=False, compression=comp)


def write_sample_csv(parquet_path: Path, csv_path: Path, n: int):
    if n <= 0:
        pd.DataFrame(columns=SCHEMA).to_csv(csv_path, index=False)
        return
    dataset = ds.dataset(str(parquet_path), format="parquet")
    # Reading first n rows efficiently
    table_full = dataset.to_table(columns=SCHEMA)
    head = table_full.slice(0, min(n, table_full.num_rows))
    pd.DataFrame(head.to_pandas()).to_csv(csv_path, index=False)


# ---------- main ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Append delta onto current (append-only), write merged parquet")
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

    # Fast paths (project to minimal schema to avoid downstream surprises)
    cur_rows = parquet_rows(current_path) if current_path.is_file() else 0
    delta_rows = parquet_rows(delta_path) if delta_path.is_file() else 0

    if cur_rows == 0 and delta_rows == 0:
        empty = enforce_schema(pd.DataFrame(columns=SCHEMA))
        write_parquet(empty, out_path, args.compression)
        empty.head(0).to_csv(sample_csv, index=False)
        print(json.dumps({
            "merged_rows": 0, "current_rows": 0, "delta_rows": 0,
            "appended_rows": 0, "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": 0
        }))
        return

    if cur_rows == 0 and delta_rows > 0:
        # All rows are new; still enforce schema and delta-internal dedupe
        delta = load_parquet_min(delta_path)
        # Deduplicate within delta on KEYS, keep last occurrence
        delta = delta.iloc[::-1].drop_duplicates(subset=KEYS, keep="first").iloc[::-1]
        write_parquet(delta, out_path, args.compression)
        write_sample_csv(out_path, sample_csv, sample_n)
        merged_rows = parquet_rows(out_path)
        print(json.dumps({
            "merged_rows": int(merged_rows), "current_rows": 0, "delta_rows": int(delta_rows),
            "appended_rows": int(len(delta)), "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": int(min(sample_n, merged_rows))
        }))
        return

    if delta_rows == 0 and cur_rows > 0:
        # Nothing to append; just project current to schema and write
        current = load_parquet_min(current_path)
        write_parquet(current, out_path, args.compression)
        write_sample_csv(out_path, sample_csv, sample_n)
        merged_rows = parquet_rows(out_path)
        print(json.dumps({
            "merged_rows": int(merged_rows), "current_rows": int(cur_rows), "delta_rows": 0,
            "appended_rows": 0, "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": int(min(sample_n, merged_rows))
        }))
        return

    # Normal path: both have rows — perform append-only logic
    current = load_parquet_min(current_path)
    delta   = load_parquet_min(delta_path)

    if args.check_strict:
        bad_cur = (~current["observed_at"].astype(str).str.match(STRICT_ISO_RE, na=False)).sum()
        bad_del = (~delta["observed_at"].astype(str).str.match(STRICT_ISO_RE, na=False)).sum()
        if bad_cur or bad_del:
            print(f"[warn] non-strict observed_at: current={bad_cur}, delta={bad_del}")

    # 1) Dedupe within delta (keep last occurrence)
    delta = delta.iloc[::-1].drop_duplicates(subset=KEYS, keep="first").iloc[::-1]

    # 2) Compute keys present in current
    cur_keys = (
        current[KEYS]
        .astype({"entity_code": "string", "observed_at": "string", "wait_time_type": "string"})
        .apply(lambda r: (r[0], r[1], r[2]), axis=1)
    )
    cur_key_set = set(cur_keys.tolist())

    # 3) Filter delta to only brand-new keys (append-only)
    def to_key_tuple(df: pd.DataFrame):
        return df[KEYS].astype({"entity_code": "string", "observed_at": "string", "wait_time_type": "string"}) \
                 .apply(lambda r: (r[0], r[1], r[2]), axis=1)

    delta_keys = to_key_tuple(delta)
    mask_new = ~delta_keys.isin(cur_key_set)
    delta_new = delta.loc[mask_new]

    skipped_existing = int((~mask_new).sum())

    # 4) Append
    all_df = pd.concat([current, delta_new], ignore_index=True, copy=False)
    all_df = enforce_schema(all_df)

    write_parquet(all_df, out_path, args.compression)

    preview = min(sample_n, len(all_df))
    (all_df.head(preview)).to_csv(sample_csv, index=False) if preview > 0 else (all_df.head(0).to_csv(sample_csv, index=False))

    print(json.dumps({
        "merged_rows": int(len(all_df)),
        "current_rows": int(len(current)),
        "delta_rows": int(len(delta)),
        "appended_rows": int(len(delta_new)),
        "skipped_existing": skipped_existing,
        "out": str(out_path),
        "sample_csv": str(sample_csv),
        "sample_rows": int(preview)
    }))


if __name__ == "__main__":
    main()
