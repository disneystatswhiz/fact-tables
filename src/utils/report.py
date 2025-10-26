#!/usr/bin/env python3
"""
08_report_daily.py
------------------
STEP 1/2: Create the daily S3 freshness report *and* save the "needs processing"
list locally as a CSV (by default: work/needs_processing_YYYYMMDD.csv).

"Needs processing" = {recent source file | LastModified > fact_table.LastModified}
If the fact table doesn't exist, ALL recent files are considered "needs processing".

Usage:
python 08_report_daily.py \
  --bucket touringplans_stats \
  --props wdw,dlr,uor,ush,tdr \
  --days-back 31 \
  --post-to-slack true
# (optional)
  --save-csv work/needs_processing_custom.csv \
  --save-s3 s3://touringplans_stats/stats_work/fact_tables/needs_processing_YYYYMMDD.csv
"""

from __future__ import annotations
import os
import sys
import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# immediate logs in Git Bash / terminals
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

DEFAULT_PROPS = ["wdw", "dlr", "uor", "ush", "tdr"]
S3_STANDBY_PREFIX_FMT  = "export/wait_times/{prop}/"
S3_PRIORITY_PREFIX_FMT = "export/fastpass_times/{prop}/"
FACT_TABLE_KEY = "stats_work/fact_tables/wait_time_fact_table.parquet"

DEFAULT_DAYS_BACK = 31
DEFAULT_STALE_HOURS = 24
DEFAULT_VERY_STALE_HOURS = 48
DEFAULT_TZ = os.environ.get("TZ", "America/Toronto")


@dataclass
class S3Obj:
    key: str
    last_modified: datetime  # aware (UTC)
    size: int


def _local_tz():
    try:
        from zoneinfo import ZoneInfo  # py3.9+
    except Exception:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return ZoneInfo("America/Toronto")


def fmt_dt_local(dt_utc: datetime, tz) -> str:
    local = dt_utc.astimezone(tz)
    offset = local.utcoffset() or timedelta(0)
    sign = "+" if offset >= timedelta(0) else "-"
    sec = abs(int(offset.total_seconds()))
    hh = sec // 3600
    mm = (sec % 3600) // 60
    return local.strftime(f"%Y-%m-%d %H:%M:%S %Z (UTC{sign}{hh:02d}:{mm:02d})")


def list_objects(s3, bucket: str, prefix: str) -> Iterable[S3Obj]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield S3Obj(
                key=obj["Key"],
                last_modified=obj["LastModified"],  # aware UTC
                size=obj.get("Size", 0),
            )


def head_object(s3, bucket: str, key: str) -> Optional[S3Obj]:
    try:
        meta = s3.head_object(Bucket=bucket, Key=key)
        return S3Obj(key=key, last_modified=meta["LastModified"], size=meta.get("ContentLength", 0))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        print(f"[WARN] head_object failed for s3://{bucket}/{key}: {e}", file=sys.stderr)
        return None


def build_recent_list(s3, bucket: str, prefixes: List[str], days_back: int) -> List[S3Obj]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    items: List[S3Obj] = []
    for p in prefixes:
        for o in list_objects(s3, bucket, p):
            if o.last_modified >= cutoff:
                items.append(o)
    items.sort(key=lambda x: x.last_modified, reverse=True)
    return items


def is_current_non_test(key: str) -> bool:
    k = key.lower()
    return ("current" in k) and ("current_test" not in k)


def compute_needs_processing(recent: List[S3Obj], fact_obj: Optional[S3Obj]) -> List[S3Obj]:
    if not recent:
        return []
    if not fact_obj:
        return recent[:]  # no fact table yet → everything recent needs processing
    return [o for o in recent if o.last_modified > fact_obj.last_modified]


def ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def write_needs_csv(path: str, rows: List[S3Obj], tz) -> None:
    ensure_parent_dir(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["s3_key", "last_modified_utc", "last_modified_local", "size_bytes"])
        for o in rows:
            w.writerow([
                o.key,
                o.last_modified.isoformat(),
                fmt_dt_local(o.last_modified, tz),
                o.size,
            ])


def upload_to_s3(s3, local_path: str, s3_uri: str) -> None:
    if not s3_uri.startswith("s3://"):
        raise ValueError("--save-s3 must start with s3://")
    _, rest = s3_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    s3.upload_file(local_path, bucket, key)


def render_report(
    *,
    bucket: str,
    fact_obj: Optional[S3Obj],
    recent: List[S3Obj],  # filtered to days_back and sorted
    needs: List[S3Obj],
    tz,
    stale_hours: int,
    very_stale_hours: int,
    needs_csv_path: str,
) -> str:
    now_local = datetime.now(timezone.utc).astimezone(tz)
    header = f"*Wait Time FACT TABLE — Daily Freshness Report*\n_{now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}_\n"

    # 1) Fact table
    if fact_obj:
        s1 = (
            f"• *Fact Table Parquet*: `s3://{bucket}/{fact_obj.key}`\n"
            f"  Last modified: `{fmt_dt_local(fact_obj.last_modified, tz)}`"
        )
    else:
        s1 = "• *Fact Table Parquet*: NOT FOUND (treating all recent sources as needing processing)"

    # 2) Unified recent list with conditional “current” staleness flags
    cutoff_24 = datetime.now(timezone.utc) - timedelta(hours=stale_hours)
    cutoff_48 = datetime.now(timezone.utc) - timedelta(hours=very_stale_hours)

    if recent:
        lines = [f"*Recent source files (newest → oldest, window ≤ {len(set([r.last_modified.date() for r in recent]))} day(s))*"]
        for o in recent:
            flag = ""
            if is_current_non_test(o.key):
                if o.last_modified < cutoff_48:
                    flag = " 🔥 *STALE > 48h*"
                elif o.last_modified < cutoff_24:
                    flag = " 🚩 STALE > 24h"
            lines.append(f"  • `s3://{bucket}/{o.key}` — `{fmt_dt_local(o.last_modified, tz)}`{flag}")
        s2 = "\n".join(lines)
    else:
        s2 = "_No source files in the recent window._"

    # 3) Needs-processing summary (and where the CSV was written)
    if needs:
        n_show = min(10, len(needs))
        head_lines = [
            f"*Needs processing*: {len(needs)} file(s) newer than fact table — showing {n_show}",
            f"_Saved full list to_: `{needs_csv_path}`",
        ]
        for o in needs[:n_show]:
            head_lines.append(f"  • `s3://{bucket}/{o.key}` — `{fmt_dt_local(o.last_modified, tz)}`")
        s3_block = "\n".join(head_lines)
    else:
        s3_block = f"_No recent files are newer than the fact table._\n_Saved empty list to_: `{needs_csv_path}`"

    return "\n—\n".join([header, s1, s2, s3_block])


def post_to_slack(text: str, webhook_url: Optional[str]) -> Tuple[bool, Optional[str]]:
    if not webhook_url:
        return False, "SLACK_WEBHOOK_URL not set; skipping Slack post."
    try:
        import json, urllib.request
        data = json.dumps({"text": text}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if 200 <= resp.getcode() < 300:
                return True, None
            return False, f"Slack HTTP {resp.getcode()}"
    except Exception as e:
        return False, f"Slack post failed: {e}"


def default_save_csv_path() -> str:
    today = datetime.now().strftime("%Y%m%d")
    return os.path.join("work", f"needs_processing_{today}.csv")


def parse_args():
    ap = argparse.ArgumentParser(description="STEP 1: Daily S3 freshness report + save needs-processing CSV.")
    ap.add_argument("--bucket", default="touringplans_stats", help="S3 bucket")
    ap.add_argument("--props", default=",".join(DEFAULT_PROPS), help="Comma list: wdw,dlr,uor,ush,tdr")
    ap.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK, help="Recent window in days")
    ap.add_argument("--stale-hours", type=int, default=DEFAULT_STALE_HOURS, help="Stale threshold for 'current' files")
    ap.add_argument("--very-stale-hours", type=int, default=DEFAULT_VERY_STALE_HOURS, help="Very-stale threshold")
    ap.add_argument("--fact-key", default=FACT_TABLE_KEY, help="Key for published parquet")
    ap.add_argument("--post-to-slack", type=lambda x: str(x).lower() in {"1","true","yes"}, default=True)
    # Saving behavior
    ap.add_argument("--save-csv", default=None, help="Local CSV path for needs-processing list (default auto path).")
    ap.add_argument("--save-s3", default=None, help="Optional s3:// URI to upload the CSV.")
    return ap.parse_args()


def main():
    args = parse_args()
    props = [p.strip() for p in args.props.split(",") if p.strip()]
    tz = _local_tz()
    s3 = boto3.client("s3")

    # Fact table
    fact = head_object(s3, args.bucket, args.fact_key)

    # Source prefixes
    prefixes = []
    for p in props:
        prefixes.append(S3_STANDBY_PREFIX_FMT.format(prop=p))
        prefixes.append(S3_PRIORITY_PREFIX_FMT.format(prop=p))

    # Unified recent list within days_back
    recent = build_recent_list(s3, args.bucket, prefixes, args.days_back)

    # Needs-processing list
    needs = compute_needs_processing(recent, fact)

    # Decide save path (default: work/needs_processing_YYYYMMDD.csv)
    save_csv_path = args.save_csv or default_save_csv_path()
    write_needs_csv(save_csv_path, needs, tz)
    print(f"[INFO] Wrote needs-processing CSV: {save_csv_path} ({len(needs)} rows)")

    # Optional S3 upload
    if args.save_s3:
        upload_to_s3(s3, save_csv_path, args.save_s3)
        print(f"[INFO] Uploaded CSV to {args.save_s3}")

    # Render + print + Slack
    text = render_report(
        bucket=args.bucket,
        fact_obj=fact,
        recent=recent,
        needs=needs,
        tz=tz,
        stale_hours=args.stale_hours,
        very_stale_hours=args.very_stale_hours,
        needs_csv_path=save_csv_path,
    )
    print(text)

if __name__ == "__main__":
    main()
