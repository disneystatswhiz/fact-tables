#!/usr/bin/env python3
"""
05_convert_datetime.py

Goal:
- Keep rows where `observed_at` is strict ISO8601 with offset:
  YYYY-MM-DDTHH:MM:SS±HH:MM (e.g., 2025-07-16T17:00:03-07:00)
- Rescue rows lacking an explicit offset by parsing and localizing timezone
  based on entity_code:
    DL*, CA*, UH* -> America/Los_Angeles
    TD* (TDL/TDS) -> Asia/Tokyo
    else          -> America/New_York
- Drop anything still unparseable.

Input : Parquet (default: work/fact_table/delta_minimal.parquet)
Output: Parquet (default: work/fact_table/delta_converted.parquet)
Report: CSV sample of dropped rows.

Requires: pandas, pyarrow  (pip install pandas pyarrow)
"""
import argparse, os, re, sys, pathlib
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from zoneinfo import ZoneInfo

# strict ISO with colon offset
STRICT_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$")
# any explicit offset at end (Z, ±HH:MM, ±HHMM) to detect "already offset"
HAS_ANY_OFFSET_RE = re.compile(r"(?:Z|[+-]\d{2}:\d{2}|[+-]\d{4})$")

PACIFIC = ZoneInfo("America/Los_Angeles")
EASTERN = ZoneInfo("America/New_York")
TOKYO   = ZoneInfo("Asia/Tokyo")

def guess_zone(entity_code: str):
    code = (str(entity_code) or "").upper()
    if code.startswith(("DL", "CA", "UH")):
        return PACIFIC
    if code.startswith("TD"):
        return TOKYO
    return EASTERN

def resolve_input(path_str: str) -> str:
    if os.path.exists(path_str):
        return path_str
    default = "work/fact_table/delta_minimal.parquet"
    if os.path.exists(default):
        return default
    raise FileNotFoundError(f"Input not found: {path_str!r} (also tried {default!r})")

def iso_from_ts(series: pd.Series) -> pd.Series:
    """Format tz-aware timestamps to strict ISO with colon in offset."""
    s = series.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return s.str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)

def process_batch(df: pd.DataFrame, report_limit_left: int):
    if df.empty:
        empty = df.iloc[0:0]
        return empty, empty, 0, 0, report_limit_left

    if "observed_at" not in df.columns:
        raise ValueError("Missing required column: observed_at")

    # Prepare
    obs = df["observed_at"].astype("string").str.strip()
    ec  = (df["entity_code"].astype("string") if "entity_code" in df.columns else pd.Series([""]*len(df), index=df.index)).str.upper()

    # 1) Already strict -> keep as-is
    mask_strict = obs.str.match(STRICT_ISO_RE, na=False)
    out_str = pd.Series(pd.NA, index=df.index, dtype="string")
    out_str.loc[mask_strict] = obs.loc[mask_strict]
    kept_strict = int(mask_strict.sum())

    # 2) Has any offset but not strict (e.g., ±HHMM or 'Z') -> parse & normalize
    mask_offset_non_strict = obs.str.contains(HAS_ANY_OFFSET_RE, na=False) & ~mask_strict
    if mask_offset_non_strict.any():
        dt = pd.to_datetime(obs.loc[mask_offset_non_strict], errors="coerce", utc=False)
        ok = dt.notna()
        out_str.loc[mask_offset_non_strict[mask_offset_non_strict].index[ok]] = iso_from_ts(dt.loc[ok])

    # 3) No explicit offset -> parse naive and localize by entity_code (vectorized by groups)
    mask_no_offset = ~obs.str.contains(HAS_ANY_OFFSET_RE, na=False)
    if mask_no_offset.any():
        dt_naive = pd.to_datetime(obs.loc[mask_no_offset], errors="coerce", utc=False)
        parsed_ok = dt_naive.notna()
        if parsed_ok.any():
            idx_ok = dt_naive.index[parsed_ok]
            ec_ok  = ec.loc[idx_ok]

            m_tokyo   = ec_ok.str.startswith("TD")
            m_pacific = ec_ok.str.startswith("DL") | ec_ok.str.startswith("CA") | ec_ok.str.startswith("UH")
            m_eastern = ~(m_tokyo | m_pacific)

            if m_tokyo.any():
                out_str.loc[idx_ok[m_tokyo]]   = iso_from_ts(dt_naive.loc[idx_ok[m_tokyo]].dt.tz_localize(TOKYO))
            if m_pacific.any():
                out_str.loc[idx_ok[m_pacific]] = iso_from_ts(dt_naive.loc[idx_ok[m_pacific]].dt.tz_localize(PACIFIC))
            if m_eastern.any():
                out_str.loc[idx_ok[m_eastern]] = iso_from_ts(dt_naive.loc[idx_ok[m_eastern]].dt.tz_localize(EASTERN))

    # Build outputs
    keep_mask = out_str.notna()
    kept_df = df.loc[keep_mask].copy()
    kept_df["observed_at"] = out_str.loc[keep_mask].astype("string")

    dropped_mask = ~keep_mask
    dropped_count = int(dropped_mask.sum())
    rescued_count = int(keep_mask.sum()) - kept_strict

    # Sample of dropped rows for inspection
    dropped_sample = df.loc[dropped_mask, ["entity_code", "observed_at"]].head(report_limit_left).copy() if dropped_count else df.iloc[0:0]
    report_limit_left = max(0, report_limit_left - len(dropped_sample))

    return kept_df, dropped_sample, kept_strict, rescued_count, report_limit_left

def main():
    ap = argparse.ArgumentParser(description="Normalize/keep ISO datetimes with offset; rescue naive by entity timezone; drop the rest.")
    ap.add_argument("-i", "--input",  default="work/fact_table/delta_minimal.parquet", help="Input Parquet path")
    ap.add_argument("-o", "--output", default="work/fact_table/delta_converted.parquet", help="Output Parquet path")
    ap.add_argument("-r", "--report", default="work/fact_table/delta_converted_dropped_sample.csv", help="CSV sample of dropped rows")
    ap.add_argument("--report-limit", type=int, default=10000, help="Max rows to include in dropped-rows sample")
    ap.add_argument("--batch-rows", type=int, default=2_000_000, help="Approx rows per batch")
    args = ap.parse_args()

    try:
        import pyarrow  # noqa: F401
    except Exception:
        print("ERROR: pyarrow is required. Install with: pip install pyarrow", file=sys.stderr)
        sys.exit(1)

    inp = resolve_input(args.input)
    out_path = args.output
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)

    dataset = ds.dataset(inp, format="parquet")
    writer = None
    total_in = 0
    total_kept = 0
    total_kept_strict = 0
    total_rescued = 0
    dropped_samples = []
    report_limit_left = args.report_limit
    saw_any = False
    last_df_for_schema = None

    try:
        for batch in dataset.to_batches(batch_size=args.batch_rows):
            table = pa.Table.from_batches([batch])
            df = table.to_pandas(types_mapper=None)
            saw_any = True
            if last_df_for_schema is None:
                last_df_for_schema = df

            kept_df, dropped_sample, kept_strict, rescued_count, report_limit_left = process_batch(df, report_limit_left)

            total_in += len(df)
            total_kept += len(kept_df)
            total_kept_strict += kept_strict
            total_rescued += rescued_count
            if not dropped_sample.empty and report_limit_left >= 0:
                dropped_samples.append(dropped_sample)

            if kept_df.empty:
                continue

            tbl_out = pa.Table.from_pandas(kept_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl_out.schema, compression="snappy")
            writer.write_table(tbl_out)
    finally:
        if writer is not None:
            writer.close()

    # If nothing kept, still emit empty parquet with original schema
    if writer is None:
        if saw_any and last_df_for_schema is not None:
            pq.write_table(pa.Table.from_pandas(last_df_for_schema.iloc[0:0], preserve_index=False), out_path)
        else:
            pq.write_table(pa.table({}), out_path)

    # Write dropped sample
    if dropped_samples and args.report_limit > 0:
        pd.concat(dropped_samples, ignore_index=True).to_csv(args.report, index=False)

    total_dropped = total_in - total_kept
    print("✅ 05_convert_datetime complete.")
    print(f"   Rows in:         {total_in:,}")
    print(f"   Kept (strict):   {total_kept_strict:,}")
    print(f"   Rescued (tz):    {total_rescued:,}")
    print(f"   Kept total:      {total_kept:,}")
    print(f"   Dropped:         {total_dropped:,}")
    print(f"   Output:          {pathlib.Path(out_path).as_posix()}")
    if total_dropped and args.report_limit > 0:
        print(f"   Dropped sample:  {pathlib.Path(args.report).as_posix()} (limit {args.report_limit:,})")
    elif total_dropped:
        print("   Dropped rows not sampled (report limit = 0).")

if __name__ == "__main__":
    main()
