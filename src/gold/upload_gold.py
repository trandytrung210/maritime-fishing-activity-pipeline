from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.common.manifest import get_run_id, load_gold_changes

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_ROOT = PROJECT_ROOT / "data" / "processed" / "gold"
LOG_ROOT = PROJECT_ROOT / "logs"
AUDIT_LOG = LOG_ROOT / "gold_upload_audit.jsonl"
SUMMARY_FILE = LOG_ROOT / "gold_upload_summary.json"
BUCKET = "fishing-gold"
STAGING_PREFIX = "_staging"
VERIFY_READBACK_AFTER_UPLOAD = True
VERIFY_EXISTING_OBJECTS = True
CHUNK_SIZE = 8 * 1024 * 1024

LOG_ROOT.mkdir(parents=True, exist_ok=True)
load_dotenv(PROJECT_ROOT / ".env")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT]):
    raise ValueError("Missing S3 configuration in .env")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


def calculate_local_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def calculate_remote_sha256(object_key: str) -> str:
    response = s3.get_object(Bucket=BUCKET, Key=object_key)
    body = response["Body"]
    digest = hashlib.sha256()
    try:
        while True:
            chunk = body.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        body.close()
    return digest.hexdigest()


def get_remote_head(object_key: str):
    try:
        return s3.head_object(Bucket=BUCKET, Key=object_key)
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        error_code = error.response.get("Error", {}).get("Code")
        if status_code == 404 or error_code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def write_audit_log(record: dict) -> None:
    with AUDIT_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def list_remote_keys(prefix: str = "") -> set[str]:
    keys: set[str] = set()
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            keys.add(item["Key"])
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return keys


def upload_with_staging(
    file_path: Path,
    final_key: str,
    local_sha256: str,
    run_id: str,
) -> str:
    """Upload to staging, verify, then copy to final key. Never delete final first."""
    staging_key = f"{STAGING_PREFIX}/{run_id}/{final_key}"
    s3.upload_file(
        str(file_path),
        BUCKET,
        staging_key,
        ExtraArgs={"Metadata": {"sha256": local_sha256, "run_id": run_id}},
    )
    staged_head = s3.head_object(Bucket=BUCKET, Key=staging_key)
    if staged_head["ContentLength"] != file_path.stat().st_size:
        raise RuntimeError(f"Staging size mismatch for {staging_key}")
    if staged_head.get("Metadata", {}).get("sha256") != local_sha256:
        raise RuntimeError(f"Staging metadata SHA mismatch for {staging_key}")
    if VERIFY_READBACK_AFTER_UPLOAD:
        remote_sha = calculate_remote_sha256(staging_key)
        if remote_sha != local_sha256:
            raise RuntimeError(f"Staging readback SHA mismatch for {staging_key}")

    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": staging_key},
        Key=final_key,
        MetadataDirective="REPLACE",
        Metadata={"sha256": local_sha256, "run_id": run_id},
    )
    final_head = s3.head_object(Bucket=BUCKET, Key=final_key)
    if final_head["ContentLength"] != file_path.stat().st_size:
        raise RuntimeError(f"Final size mismatch for {final_key}")
    if final_head.get("Metadata", {}).get("sha256") != local_sha256:
        raise RuntimeError(f"Final metadata SHA mismatch for {final_key}")
    if VERIFY_READBACK_AFTER_UPLOAD:
        remote_sha = calculate_remote_sha256(final_key)
        if remote_sha != local_sha256:
            raise RuntimeError(f"Final readback SHA mismatch for {final_key}")

    s3.delete_object(Bucket=BUCKET, Key=staging_key)
    return "FULL_READBACK_SHA256" if VERIFY_READBACK_AFTER_UPLOAD else (
        "HEAD_SIZE_AND_SHA256_METADATA"
    )


def cleanup_stale_objects(local_keys: set[str], summary: dict) -> None:
    """Delete remote gold objects not present locally (never touches staging)."""
    remote_keys = {
        key
        for key in list_remote_keys()
        if not key.startswith(f"{STAGING_PREFIX}/")
    }
    stale = sorted(remote_keys - local_keys)
    summary["stale_candidates"] = len(stale)
    for key in stale:
        s3.delete_object(Bucket=BUCKET, Key=key)
        write_audit_log(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "event_type": "STALE_CLEANUP",
                "bucket": BUCKET,
                "object_key": key,
                "status": "DELETED_STALE",
                "error": None,
            }
        )
        summary["stale_deleted"] += 1
        print(f"Deleted stale object: {key}")


def main() -> None:
    print("=" * 50)
    print("GOLD UPLOAD")
    print("=" * 50)

    run_id = get_run_id()
    changes = load_gold_changes()
    print(f"Run ID: {run_id}")
    print(f"Gold changes run_id: {changes.get('run_id')}")
    print(f"has_silver_changes: {changes.get('has_silver_changes')}")

    if not GOLD_ROOT.exists():
        raise FileNotFoundError(f"Gold root not found: {GOLD_ROOT}")

    parquet_files = sorted(GOLD_ROOT.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {GOLD_ROOT}")

    total_local_bytes = sum(path.stat().st_size for path in parquet_files)
    print(f"Total Parquet files: {len(parquet_files)}")
    print(f"Total local size: {total_local_bytes:,} bytes")

    run_started = datetime.now(UTC)
    start_time = time.perf_counter()
    summary = {
        "run_id": run_id,
        "bucket": BUCKET,
        "started_at": run_started.isoformat(),
        "total_files": len(parquet_files),
        "uploaded_files": 0,
        "skipped_files": 0,
        "failed_files": 0,
        "not_attempted_files": 0,
        "total_local_bytes": total_local_bytes,
        "bytes_uploaded": 0,
        "readback_verified_files": 0,
        "stale_candidates": 0,
        "stale_deleted": 0,
    }
    local_keys: set[str] = set()
    preflight_error = None
    all_uploads_ok = False

    try:
        try:
            s3.head_bucket(Bucket=BUCKET)
        except Exception as error:
            preflight_error = str(error)
            summary["not_attempted_files"] = len(parquet_files)
            write_audit_log(
                {
                    "run_id": run_id,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "event_type": "PREFLIGHT",
                    "bucket": BUCKET,
                    "status": "FAILED_PREFLIGHT",
                    "error": preflight_error,
                }
            )
            raise

        write_audit_log(
            {
                "run_id": run_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "event_type": "PREFLIGHT",
                "bucket": BUCKET,
                "status": "PREFLIGHT_SUCCESS",
                "error": None,
            }
        )

        for index, file_path in enumerate(parquet_files, start=1):
            object_key = file_path.relative_to(GOLD_ROOT).as_posix()
            local_keys.add(object_key)
            file_size = file_path.stat().st_size
            local_sha256 = calculate_local_sha256(file_path)
            timestamp = datetime.now(UTC).isoformat()
            verification = None
            upload_reason = "NEW_OR_CHANGED_OBJECT"
            status = "FAILED"

            print(f"\nProcessing {index}/{len(parquet_files)}")
            print(f"Object key: {object_key}")

            try:
                remote_head = get_remote_head(object_key)
                should_upload = True
                if remote_head is not None:
                    remote_sha = remote_head.get("Metadata", {}).get("sha256")
                    same_size = remote_head["ContentLength"] == file_size
                    same_hash = remote_sha == local_sha256
                    if same_size and same_hash:
                        if VERIFY_EXISTING_OBJECTS:
                            actual = calculate_remote_sha256(object_key)
                            if actual == local_sha256:
                                should_upload = False
                                verification = "FULL_READBACK_SHA256"
                                status = "SKIPPED_UNCHANGED"
                                summary["skipped_files"] += 1
                                summary["readback_verified_files"] += 1
                            else:
                                upload_reason = "REMOTE_CONTENT_MISMATCH"
                        else:
                            should_upload = False
                            verification = "HEAD_SIZE_AND_SHA256_METADATA"
                            status = "SKIPPED_UNCHANGED"
                            summary["skipped_files"] += 1
                    else:
                        upload_reason = "REMOTE_SIZE_OR_METADATA_CHANGED"

                if should_upload:
                    verification = upload_with_staging(
                        file_path,
                        object_key,
                        local_sha256,
                        run_id,
                    )
                    summary["uploaded_files"] += 1
                    summary["bytes_uploaded"] += file_size
                    if verification == "FULL_READBACK_SHA256":
                        summary["readback_verified_files"] += 1
                    status = "UPLOADED_VERIFIED"
                    print("Upload and verification successful.")
                else:
                    print("Unchanged object. Upload skipped.")

                write_audit_log(
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "event_type": "OBJECT_UPLOAD",
                        "bucket": BUCKET,
                        "object_key": object_key,
                        "local_file": str(file_path),
                        "size_bytes": file_size,
                        "sha256": local_sha256,
                        "status": status,
                        "verification": verification,
                        "upload_reason": upload_reason,
                        "error": None,
                    }
                )
            except Exception as error:
                summary["failed_files"] += 1
                print(f"Upload failed: {error}")
                write_audit_log(
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "event_type": "OBJECT_UPLOAD",
                        "bucket": BUCKET,
                        "object_key": object_key,
                        "local_file": str(file_path),
                        "size_bytes": file_size,
                        "sha256": local_sha256,
                        "status": "FAILED",
                        "verification": "FAILED",
                        "error": str(error),
                    }
                )

        if summary["failed_files"] == 0:
            cleanup_stale_objects(local_keys, summary)
            all_uploads_ok = True
            print(
                "Upload verified. Pending gold finalize runs in a "
                "separate Airflow task (finalize_gold_changes)."
            )
    finally:
        summary["finished_at"] = datetime.now(UTC).isoformat()
        summary["duration_seconds"] = round(time.perf_counter() - start_time, 2)
        if preflight_error:
            summary["status"] = "FAILED_PREFLIGHT"
            summary["preflight_error"] = preflight_error
        else:
            summary["status"] = (
                "SUCCESS" if summary["failed_files"] == 0 else "COMPLETED_WITH_ERRORS"
            )
        with SUMMARY_FILE.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=4, ensure_ascii=False)

        print("\n========== GOLD UPLOAD SUMMARY ==========")
        print(f"Total files: {summary['total_files']}")
        print(f"Uploaded: {summary['uploaded_files']}")
        print(f"Skipped: {summary['skipped_files']}")
        print(f"Failed: {summary['failed_files']}")
        print(f"Stale deleted: {summary['stale_deleted']}")
        print(f"Bytes uploaded: {summary['bytes_uploaded']}")
        print(f"Read-back verified: {summary['readback_verified_files']}")
        print(f"Duration: {summary['duration_seconds']} seconds")
        print(f"Summary file: {SUMMARY_FILE}")
        print(f"Audit log: {AUDIT_LOG}")
        print("=========================================")

    if summary["failed_files"] > 0 or not all_uploads_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
