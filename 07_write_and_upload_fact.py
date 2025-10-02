#!/usr/bin/env python3
"""
08_write_and_upload_fact.py
---------------------------------
Purpose: Upload the merged Parquet fact to S3, with optional timestamped backup.
Emits JSON:
 {
   "uploaded": bool,
   "skipped": bool|None,
   "reason": "..."|None,
   "s3": "...",
   "bytes": N|null,
   "etag": "...",
   "version_id": "...",
   "backup_key": "...|null"
 }

Key safeties:
- If the local parquet is missing -> skip (no error).
- If the parquet has 0 rows -> skip (no upload).
- Supports --atomic publish and optional timestamped --backup.

Requires: boto3, pyarrow
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

# ------------------ helpers ------------------

def parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"Bad S3 URI: {uri}")
    p = urlparse(uri)
    bucket = p.netloc
    key = p.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Bad S3 URI: {uri}")
    return bucket, key

def parquet_quick_validate(path: Path) -> bool:
    """Check for PAR1 magic bytes and non-trivial size."""
    try:
        size = path.stat().st_size
        if size < 8:
            return False
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
        return head == b"PAR1" and tail == b"PAR1"
    except Exception:
        return False

def head_object(s3, bucket: str, key: str):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None

def upload_file(local: Path, bucket: str, key: str, *, storage_class: str, concurrency: int,
                sse: str | None, sse_kms_key: str | None):
    s3 = boto3.client("s3")
    cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,    # 64MB
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=max(1, concurrency),
        use_threads=True,
    )
    extra = {
        "StorageClass": storage_class,
        "ContentType": "application/x-parquet",
    }
    if sse:
        extra["ServerSideEncryption"] = sse
        if sse == "aws:kms" and sse_kms_key:
            extra["SSEKMSKeyId"] = sse_kms_key

    s3.upload_file(str(local), bucket, key, ExtraArgs=extra, Config=cfg)
    head = head_object(s3, bucket, key)
    etag = head.get("ETag", "").strip('"') if head else None
    size = int(head.get("ContentLength", 0)) if head else None
    version_id = head.get("VersionId") if head else None
    return etag, size, version_id

def copy_object(bucket: str, src_key: str, dst_key: str, *, storage_class: str,
                sse: str | None, sse_kms_key: str | None):
    s3 = boto3.client("s3")
    extra = {"StorageClass": storage_class, "MetadataDirective": "COPY"}
    if sse:
        extra["ServerSideEncryption"] = sse
        if sse == "aws:kms" and sse_kms_key:
            extra["SSEKMSKeyId"] = sse_kms_key
    s3.copy_object(Bucket=bucket, Key=dst_key, CopySource={"Bucket": bucket, "Key": src_key}, **extra)
    head = head_object(s3, dst_key)
    etag = head.get("ETag", "").strip('"') if head else None
    size = int(head.get("ContentLength", 0)) if head else None
    version_id = head.get("VersionId") if head else None
    return etag, size, version_id

# ------------------ main ------------------

def main():
    ap = argparse.ArgumentParser(description="Upload merged Parquet fact to S3")
    ap.add_argument("--s3",        dest="s3_uri",   default=os.getenv("S3_FACT_URI", "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet"))
    ap.add_argument("--work",      dest="loc_work", default=os.getenv("LOC_WORK", str(Path.cwd() / "work")))
    ap.add_argument("--src",       dest="src_path", default=None)
    ap.add_argument("--backup",    dest="do_backup", action="store_true")
    ap.add_argument("--atomic",    dest="atomic", action="store_true")
    ap.add_argument("--storage-class", default="STANDARD")
    ap.add_argument("--sse", choices=["AES256", "aws:kms"], default=None)
    ap.add_argument("--sse-kms-key", default=None)
    ap.add_argument("--concurrency", type=int, default=max(8, (os.cpu_count() or 4)))
    args = ap.parse_args()

    out_dir = Path(args.loc_work) / "fact_table"
    src = Path(args.src_path) if args.src_path else (out_dir / "merged_fact.parquet")

    # If no local parquet was produced (e.g., delta=0), treat as a clean no-op.
    if not src.is_file():
        print(json.dumps({
            "uploaded": False,
            "skipped": True,
            "reason": f"no local parquet found: {src}",
            "s3": args.s3_uri
        }))
        return

    # Quick signature check
    if not parquet_quick_validate(src):
        print(json.dumps({
            "uploaded": False,
            "skipped": True,
            "reason": f"PAR1 signature/size check failed: {src}",
            "s3": args.s3_uri
        }))
        return

    # Hard check: ensure >0 rows before publish
    try:
        t = pq.read_table(src)
        nrows, ncols = t.num_rows, t.num_columns
        if nrows == 0:
            print(json.dumps({
                "uploaded": False,
                "skipped": True,
                "reason": f"local parquet has 0 rows: {src}",
                "s3": args.s3_uri
            }))
            return
        print(f"[08] Ready to upload {nrows:,} rows × {ncols} cols from {src}")
    except Exception as e:
        print(json.dumps({
            "uploaded": False,
            "skipped": True,
            "reason": f"failed to read parquet: {e}",
            "s3": args.s3_uri
        }))
        return

    bucket, key = parse_s3_uri(args.s3_uri)

    uploaded = False
    etag = size = version_id = None
    backup_key = None

    try:
        if args.atomic:
            # Upload to a temporary key, then server-side copy to final key
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            tmp_key = f"{key}.tmp.{ts}"
            etag, size, version_id = upload_file(
                src, bucket, tmp_key,
                storage_class=args.storage_class,
                concurrency=args.concurrency,
                sse=args.sse,
                sse_kms_key=args.sse_kms_key,
            )
            etag, size, version_id = copy_object(
                bucket, tmp_key, key,
                storage_class=args.storage_class,
                sse=args.sse,
                sse_kms_key=args.sse_kms_key,
            )
            # best-effort cleanup
            try:
                boto3.client("s3").delete_object(Bucket=bucket, Key=tmp_key)
            except ClientError:
                pass
        else:
            etag, size, version_id = upload_file(
                src, bucket, key,
                storage_class=args.storage_class,
                concurrency=args.concurrency,
                sse=args.sse,
                sse_kms_key=args.sse_kms_key,
            )
        uploaded = size is not None and size > 0

        # Optional backup (server-side copy from final object)
        if uploaded and args.do_backup:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            stem = Path(key).stem
            suffix = Path(key).suffix or ".parquet"
            backup_key = str(Path(key).with_name(f"{stem}_{ts}{suffix}"))
            copy_object(
                bucket, key, backup_key,
                storage_class=args.storage_class,
                sse=args.sse,
                sse_kms_key=args.sse_kms_key,
            )

    except ClientError as e:
        print(json.dumps({"uploaded": False, "skipped": False, "s3": args.s3_uri, "error": str(e)}))
        return

    print(json.dumps({
        "uploaded": bool(uploaded),
        "skipped": False,
        "reason": None,
        "s3": args.s3_uri,
        "bytes": size,
        "etag": etag,
        "version_id": version_id,
        "backup_key": backup_key
    }))

if __name__ == "__main__":
    main()
