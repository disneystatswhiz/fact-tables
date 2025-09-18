#!/usr/bin/env python3
"""
03_list_changed_raw_files.py
---------------------------------
Purpose: List locally synced raw wait-time CSVs whose modification time is
*after* the fact table's LastModified (UTC). Emits a JSON summary to STDOUT and
writes a simple newline-delimited file list for downstream parsing.

Inputs (ENV/CLI):
  --work      -> LOC_WORK  (default ./work)
  --input     -> LOC_INPUT (default ./input)
  --roots     -> comma-separated subpaths under LOC_INPUT to scan
                 (default: "wait_times,wait_times/priority")
  --cutoff    -> override cutoff (ISO UTC, e.g. 2025-08-14T12:34:56). If omitted,
                 read work/fact_table/fact_status.json produced by 01_ script.

Outputs:
  - STDOUT JSON: {"cutoff_utc":"...","count":N,"list_file":"..."}
  - work/fact_table/changed_files.txt: newline-delimited absolute file paths

Notes:
- We rely on local file mtimes (set by `aws s3 sync`) to approximate S3 LastModified.
- Keep it simple: only .csv files; recursive scan under each root.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CSV_EXT = ".csv"
DEFAULT_ROOTS = ["wait_times", "wait_times/priority"]


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def load_cutoff(loc_work: Path, explicit: str | None) -> datetime:
    if explicit:
        # Expect UTC ISO (no tz or with Z); normalize to aware UTC
        ts = explicit.strip()
        if ts.endswith("Z"):
            ts = ts[:-1]
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    # else read fact_status.json from 01_ script
    status_path = loc_work / "fact_table" / "fact_status.json"
    if not status_path.is_file():
        # no fact yet → use epoch start
        return datetime.fromtimestamp(0, tz=timezone.utc)
    with status_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    lm = data.get("last_modified_utc")
    if not lm:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    # lm is UTC without 'Z' per 01_ script
    dt = datetime.fromisoformat(lm)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def scan_changed(loc_input: Path, roots: list[str], cutoff_utc: datetime) -> list[Path]:
    results: list[Path] = []
    for root in roots:
        base = (loc_input / root).resolve()
        if not base.is_dir():
            continue
        for p in base.rglob("*"):
            if p.is_file() and p.suffix.lower() == CSV_EXT:
                try:
                    mt = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                except OSError:
                    continue
                if mt > cutoff_utc:
                    results.append(p)
    # stable order helps with diffs
    results.sort()
    return results


def main():
    ap = argparse.ArgumentParser(description="List changed raw CSVs since fact LastModified")
    ap.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--input", dest="loc_input", default=os.getenv("LOC_INPUT", str(Path.cwd() / "input")))
    ap.add_argument("--roots", dest="roots", default=os.getenv("RAW_ROOTS", ",".join(DEFAULT_ROOTS)))
    ap.add_argument("--cutoff", dest="cutoff", default=None)
    args = ap.parse_args()

    loc_work = Path(args.loc_work)
    loc_input = Path(args.loc_input)
    roots = [r.strip().strip("/") for r in args.roots.split(",") if r.strip()]

    cutoff_utc = load_cutoff(loc_work, args.cutoff)

    changed = scan_changed(loc_input, roots, cutoff_utc)

    out_dir = loc_work / "fact_table"
    ensure_dir(out_dir)
    list_file = out_dir / "changed_files.txt"

    with list_file.open("w", encoding="utf-8") as f:
        for p in changed:
            f.write(str(p) + "\n")

    payload = {
        "cutoff_utc": cutoff_utc.strftime("%Y-%m-%dT%H:%M:%S"),
        "count": len(changed),
        "list_file": str(list_file),
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
