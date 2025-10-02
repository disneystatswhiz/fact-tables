#!/usr/bin/env python3
"""
07_append_and_dedupe.py
-------------------------------------------
Purpose: ALWAYS produce a single upload-ready Parquet at
         work/fact_table/merged_fact.parquet

Scenarios:
1) No current, delta exists         → merged = delta (raw copy; no full read)
2) Current exists, delta exists     → merged = current ⊎ (delta \ keys(current))
3) Current exists, no delta         → merged = current
0) Neither exists                   → merged = empty (schema-only)
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
from pathlib import Path

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pa = None
    pq = None

SCHEMA = ["entity_code", "observed_at", "wait_time_minutes", "wait_time_type"]
KEYS   = ["entity_code", "observed_at", "wait_time_type"]
STRICT_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?[+-]\d{2}:\d{2}$")

# ---------- helpers ---------------------------------------------------------

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def parquet_rows(path: Path) -> int:
    if not path.is_file() or pq is None:
        return 0
    try:
        pf = pq.ParquetFile(str(path), memory_map=True)
        return int(pf.metadata.num_rows)
    except Exception:
        return 0

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string" if col != "wait_time_minutes" else "Int64")
    df = df[SCHEMA]
    df["entity_code"]       = df["entity_code"].astype("string")
    df["observed_at"]       = df["observed_at"].astype("string")
    df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
    df["wait_time_type"]    = df["wait_time_type"].astype("string")
    return df

def normalize_keys_df(df: pd.DataFrame) -> pd.DataFrame:
    df["entity_code"]    = df["entity_code"].astype("string").str.strip()
    df["wait_time_type"] = df["wait_time_type"].astype("string").str.strip().str.lower()
    df["observed_at"] = (
        df["observed_at"].astype("string").str.strip()
          .str.replace(r"\.\d{1,6}(?=[+-]\d{2}:\d{2}$)", "", regex=True)
    )
    return df

def write_sample_csv_streaming(parquet_path: Path, csv_path: Path, n: int):
    if n <= 0 or pq is None or pa is None or not parquet_path.is_file():
        pd.DataFrame(columns=SCHEMA).to_csv(csv_path, index=False)
        return
    try:
        pf = pq.ParquetFile(str(parquet_path), memory_map=True)
        tables, rows = [], 0
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg, columns=SCHEMA)
            tables.append(t)
            rows += int(t.num_rows)
            if rows >= n:
                break
        head = pa.concat_tables(tables).slice(0, n) if tables else pa.table({c: pa.array([], type=pa.string()) for c in SCHEMA})
        pd.DataFrame(head.to_pandas()).to_csv(csv_path, index=False)
    except Exception:
        pd.DataFrame(columns=SCHEMA).to_csv(csv_path, index=False)

def open_sqlite_index(db_path: Path):
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=MEMORY;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS keys(
            entity_code     TEXT NOT NULL,
            observed_at     TEXT NOT NULL,
            wait_time_type  TEXT NOT NULL,
            PRIMARY KEY (entity_code, observed_at, wait_time_type)
        );
    """)
    conn.commit()
    return conn

def insert_keys(conn: sqlite3.Connection, rows: list[tuple]):
    if not rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO keys(entity_code, observed_at, wait_time_type) VALUES (?, ?, ?);",
        rows
    )

def sqlite_anti_join_batch(conn: sqlite3.Connection, batch_keys: list[tuple]) -> set[tuple]:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS temp.batch_keys;")
    cur.execute("CREATE TEMP TABLE batch_keys(entity_code TEXT, observed_at TEXT, wait_time_type TEXT);")
    cur.executemany("INSERT INTO batch_keys VALUES(?, ?, ?);", batch_keys)
    cur.execute("""
        SELECT b.entity_code, b.observed_at, b.wait_time_type
        FROM batch_keys b
        LEFT JOIN keys k
        ON  b.entity_code = k.entity_code
        AND b.observed_at = k.observed_at
        AND b.wait_time_type = k.wait_time_type
        WHERE k.entity_code IS NULL;
    """)
    out = set(cur.fetchall())
    cur.execute("DROP TABLE IF EXISTS temp.batch_keys;")
    return out

def df_to_arrow(df: pd.DataFrame, schema: pa.Schema | None) -> pa.Table:
    if schema is None:
        return pa.Table.from_pandas(df, preserve_index=False)
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)

def same_file(a: Path, b: Path) -> bool:
    try:
        return a.resolve(strict=True) == b.resolve(strict=True)
    except Exception:
        # Fallback comparison
        return a.absolute().as_posix().lower() == b.absolute().as_posix().lower()

# ---------- main ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Append delta onto current and ALWAYS write merged_fact.parquet (streaming, low-memory)")
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--current", dest="current_parquet", default=None)
    ap.add_argument("--delta", dest="delta_parquet", default=None)
    ap.add_argument("--out", dest="out_parquet", default=None)
    ap.add_argument("--sample-csv", dest="sample_csv", default=None)
    ap.add_argument("--sample-n", dest="sample_n", type=int, default=int(os.getenv("MERGED_SAMPLE_N", 50)))
    ap.add_argument("--compression", default=os.getenv("PARQUET_COMPRESSION", "snappy"),
                    choices=["zstd","snappy","gzip","none"])
    ap.add_argument("--index-db", dest="index_db", default=None,
                    help="Optional path for the SQLite key index (defaults to work/fact_table/_keys_index.sqlite)")
    args = ap.parse_args()

    if pq is None or pa is None:
        raise RuntimeError("pyarrow is required for streaming merge; please install pyarrow.")

    out_dir = Path(args.loc_work) / "fact_table"
    ensure_dir(out_dir)

    current_path = Path(args.current_parquet) if args.current_parquet else (out_dir / "current_fact.parquet")
    delta_path   = Path(args.delta_parquet)   if args.delta_parquet   else (out_dir / "delta_minimal.parquet")
    out_path     = Path(args.out_parquet)     if args.out_parquet     else (out_dir / "merged_fact.parquet")
    sample_csv   = Path(args.sample_csv)      if args.sample_csv      else (out_dir / "merged_fact_sample.csv")
    sample_n     = max(0, int(args.sample_n))
    index_db     = Path(args.index_db) if args.index_db else (out_dir / "_keys_index.sqlite")
    compression  = None if args.compression == "none" else args.compression

    # If current and delta point to same file, just copy-as-merged.
    if delta_path.is_file() and current_path.is_file() and same_file(delta_path, current_path):
        ensure_dir(out_path.parent)
        if out_path.resolve() != current_path.resolve():
            shutil.copy2(current_path, out_path)
        write_sample_csv_streaming(out_path, sample_csv, sample_n)
        print(json.dumps({
            "scenario": "identical_current_and_delta",
            "merged_rows": int(parquet_rows(out_path)),
            "current_rows": int(parquet_rows(current_path)),
            "delta_rows": int(parquet_rows(delta_path)),
            "appended_rows": 0, "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv),
            "sample_rows": int(min(sample_n, parquet_rows(out_path)))
        }))
        return

    cur_rows   = parquet_rows(current_path)
    delta_rows = parquet_rows(delta_path)

    # 0) Neither exists → write empty merged
    if cur_rows == 0 and delta_rows == 0:
        empty = pd.DataFrame(columns=SCHEMA)
        t = pa.Table.from_pandas(enforce_schema(empty), preserve_index=False)
        pq.write_table(t, out_path.as_posix(), compression=compression)
        write_sample_csv_streaming(out_path, sample_csv, sample_n)
        print(json.dumps({
            "scenario": "none",
            "merged_rows": 0, "current_rows": 0, "delta_rows": 0,
            "appended_rows": 0, "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv), "sample_rows": 0
        }))
        return

    # 1) No current, delta exists → raw copy (fast)
    if cur_rows == 0 and delta_rows > 0:
        ensure_dir(out_path.parent)
        shutil.copy2(delta_path, out_path)
        write_sample_csv_streaming(out_path, sample_csv, sample_n)
        print(json.dumps({
            "scenario": "bootstrap_copy",
            "merged_rows": int(parquet_rows(out_path)),
            "current_rows": 0, "delta_rows": int(delta_rows),
            "appended_rows": int(delta_rows), "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv),
            "sample_rows": int(min(sample_n, parquet_rows(out_path)))
        }))
        return

    # 3) Current exists, no delta → copy current
    if cur_rows > 0 and delta_rows == 0:
        if current_path.resolve() != out_path.resolve():
            ensure_dir(out_path.parent); shutil.copy2(current_path, out_path)
        write_sample_csv_streaming(out_path, sample_csv, sample_n)
        print(json.dumps({
            "scenario": "no_new_data",
            "merged_rows": int(parquet_rows(out_path)), "current_rows": int(cur_rows), "delta_rows": 0,
            "appended_rows": 0, "skipped_existing": 0,
            "out": str(out_path), "sample_csv": str(sample_csv),
            "sample_rows": int(min(sample_n, parquet_rows(out_path)))
        }))
        return

    # 2) Append-only (current + delta): streaming with SQLite anti-join
    pf_cur   = pq.ParquetFile(current_path.as_posix(), memory_map=True)

    # Try to open delta for row-group streaming; if anything fails, we will fall back to whole-file read.
    try:
        pf_delta = pq.ParquetFile(delta_path.as_posix(), memory_map=True)
        delta_rowgroup_mode = True
    except Exception:
        pf_delta = None
        delta_rowgroup_mode = False

    conn = open_sqlite_index(index_db)
    writer = None
    merged_rows = 0

    try:
        # Stream current → merged and index keys
        for rg in range(pf_cur.num_row_groups):
            t_cur = pf_cur.read_row_group(rg, columns=SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(out_path.as_posix(), t_cur.schema, compression=compression)
            writer.write_table(t_cur)
            merged_rows += int(t_cur.num_rows)

            # Index current keys
            k_tbl = pf_cur.read_row_group(rg, columns=KEYS)
            k_df = pd.DataFrame(k_tbl.to_pandas())
            k_df = normalize_keys_df(k_df)
            insert_keys(conn, list(map(tuple, k_df[KEYS].itertuples(index=False, name=None))))

        appended_rows = 0
        skipped_existing = 0

        if delta_rowgroup_mode:
            # Row-group streaming for delta with safe fallback
            for rg in range(pf_delta.num_row_groups):
                try:
                    t_del = pf_delta.read_row_group(rg, columns=SCHEMA)
                except Exception:
                    # Fallback: read entire delta once and process below
                    delta_rowgroup_mode = False
                    break

                df = enforce_schema(pd.DataFrame(t_del.to_pandas()))
                df = normalize_keys_df(df)
                df = df.iloc[::-1].drop_duplicates(subset=KEYS, keep="first").iloc[::-1]

                batch_keys = list(map(tuple, df[KEYS].itertuples(index=False, name=None)))
                new_key_set = sqlite_anti_join_batch(conn, batch_keys)
                if not new_key_set:
                    skipped_existing += len(batch_keys)
                    continue

                df["__key"] = list(map(tuple, df[KEYS].itertuples(index=False, name=None)))
                df_new = df[df["__key"].isin(new_key_set)].drop(columns="__key")

                t_new = df_to_arrow(df_new, writer.schema if writer is not None else None)
                writer.write_table(t_new)
                merged_rows += int(t_new.num_rows)
                appended_rows += int(t_new.num_rows)

                insert_keys(conn, list(map(tuple, df_new[KEYS].itertuples(index=False, name=None))))
                conn.commit()

        if not delta_rowgroup_mode:
            # Whole-file fallback for delta
            t_all = pq.read_table(delta_path.as_posix(), columns=SCHEMA, memory_map=True)
            df = enforce_schema(pd.DataFrame(t_all.to_pandas()))
            df = normalize_keys_df(df)
            df = df.iloc[::-1].drop_duplicates(subset=KEYS, keep="first").iloc[::-1]

            batch_keys = list(map(tuple, df[KEYS].itertuples(index=False, name=None)))
            new_key_set = sqlite_anti_join_batch(conn, batch_keys)

            if new_key_set:
                df["__key"] = list(map(tuple, df[KEYS].itertuples(index=False, name=None)))
                df_new = df[df["__key"].isin(new_key_set)].drop(columns="__key")
                t_new = df_to_arrow(df_new, writer.schema if writer is not None else None)
                writer.write_table(t_new)
                merged_rows += int(t_new.num_rows)
                appended_rows += int(t_new.num_rows)
                insert_keys(conn, list(map(tuple, df_new[KEYS].itertuples(index=False, name=None))))
                conn.commit()
            else:
                skipped_existing += len(batch_keys)

        if writer is not None:
            writer.close()
            writer = None

        write_sample_csv_streaming(out_path, sample_csv, sample_n)

        print(json.dumps({
            "scenario": "append_only",
            "merged_rows": int(merged_rows),
            "current_rows": int(cur_rows),
            "delta_rows": int(delta_rows),
            "appended_rows": int(appended_rows),
            "skipped_existing": int(skipped_existing),
            "out": str(out_path),
            "sample_csv": str(sample_csv),
            "sample_rows": int(min(sample_n, merged_rows))
        }))

    finally:
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        try:
            index_db.unlink(missing_ok=True)
        except Exception:
            pass

if __name__ == "__main__":
    main()
