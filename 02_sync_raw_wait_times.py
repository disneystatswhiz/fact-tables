#!/usr/bin/env python3
"""
02_sync_raw_wait_times.py
---------------------------------
Purpose: Ensure local raw wait-time CSVs are synced from S3 (standby + priority)
exactly once per UTC day by default, with an option to --force.

Mirrors the behavior of your Julia s3 sync orchestrator (daily sentinel, per-
property sync of standby and conditionally priority). Writes a compact JSON
summary to STDOUT for downstream scripts.

ENV / CLI:
  --work           -> LOC_WORK (default ./work)          [not required but kept for parity]
  --input          -> LOC_INPUT (default ./input)
  --properties     -> comma-separated (default: wdw,dlr,uor,ush,tdl)
  --enable-uor-priority (flag) -> include UOR priority sync
  --delete         (flag) -> pass --delete to aws s3 sync
  --force          (flag) -> ignore today's sentinel and re-sync

Assumes AWS CLI is installed & configured. Uses local file mtimes afterward
for change detection in later steps.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_PROPS = ["wdw", "dlr", "uor", "ush", "tdr"]


def run(cmd: list) -> bool:
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"Command failed: {' '.join(cmd)}\n{e}\n")
        return False


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def s3_sync_folder(s3path: str, localpath: Path, include_globs=("*.csv",), exclude_globs=(), delete=False) -> bool:
    ensure_dir(localpath)
    cmd = ["aws", "s3", "sync", s3path, str(localpath), "--only-show-errors"]
    for ex in exclude_globs:
        cmd.append(f"--exclude={ex}")
    for inc in include_globs:
        cmd.append(f"--include={inc}")
    if delete:
        cmd.append("--delete")
    return run(cmd)


def sync_property(loc_input: Path, prop: str, enable_priority: bool, delete: bool) -> dict:
    # Standby
    s3_wait = f"s3://touringplans_stats/export/wait_times/{prop}/"
    loc_wait = loc_input / "wait_times" / prop
    ok_wait = s3_sync_folder(s3_wait, loc_wait, include_globs=("*.csv",), delete=delete)

    # Priority
    ok_prio = True
    if enable_priority:
        s3_prio = f"s3://touringplans_stats/export/fastpass_times/{prop}/"
        loc_prio = loc_input / "wait_times" / "priority" / prop
        ok_prio = s3_sync_folder(s3_prio, loc_prio, include_globs=("*.csv",), delete=delete)

    return {"standby": ok_wait, "priority": ok_prio}


def main():
    parser = argparse.ArgumentParser(description="Daily raw wait-time sync from S3 → local")
    parser.add_argument("--work", dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    parser.add_argument("--input", dest="loc_input", default=os.getenv("LOC_INPUT", str(Path.cwd() / "input")))
    parser.add_argument("--properties", dest="properties", default=os.getenv("SYNC_PROPS", ",".join(DEFAULT_PROPS)))
    parser.add_argument("--enable-uor-priority", dest="enable_uor_priority", action="store_true")
    parser.add_argument("--delete", dest="delete", action="store_true")
    parser.add_argument("--force", dest="force", action="store_true")
    args = parser.parse_args()

    loc_input = Path(args.loc_input)
    loc_work = Path(args.loc_work)
    props = [p.strip() for p in args.properties.split(",") if p.strip()]

    # Sentinels under LOC_INPUT to mirror Julia behavior
    sent_dir = loc_input / ".sentinels"
    ensure_dir(sent_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")  # UTC day
    sentinel = sent_dir / f"wait_sync_{stamp}"
    lockfile = sent_dir / f"wait_sync_{stamp}.lock"

    if sentinel.exists() and not args.force:
        payload = {"status": "skipped", "sentinel": str(sentinel)}
        print(json.dumps(payload))
        return

    # crude lock to avoid concurrent runs
    try:
        fh = os.open(str(lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fh)
    except FileExistsError:
        # Wait briefly for the other process (max ~120s) — simplified
        for _ in range(120):
            if sentinel.exists():
                print(json.dumps({"status": "skipped", "sentinel": str(sentinel)}))
                return
        print(json.dumps({"status": "skipped-timeout", "sentinel": str(sentinel)}))
        return

    results = {}
    try:
        for prop in props:
            enable_pri = (prop != "uor") or args.enable_uor_priority
            results[prop] = sync_property(loc_input, prop, enable_pri, args.delete)
        # mark success for the UTC day
        sentinel.touch()
        payload = {"status": "ok", "sentinel": str(sentinel), "results": results}
        print(json.dumps(payload))
    finally:
        try:
            os.remove(str(lockfile))
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
