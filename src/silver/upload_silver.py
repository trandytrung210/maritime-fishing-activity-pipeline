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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)
LOG_ROOT = PROJECT_ROOT / "logs"
AUDIT_LOG = LOG_ROOT / "silver_upload_audit.jsonl"
SUMMARY_FILE = LOG_ROOT / "silver_upload_summary.json"
BUCKET = "fishing-silver"
VERIFY_READBACK_AFTER_UPLOAD = True
VERIFY_EXISTING_OBJECTS = True
CHUNK_SIZE = 8 * 1024 * 1024
LOG_ROOT.mkdir(parents=True, exist_ok=True)
load_dotenv(PROJECT_ROOT / ".env")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
if not all([
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_ENDPOINT
]):
    raise ValueError("Missing S3 configuration in .env")
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    )
)
def calculate_local_sha256(file_path):

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:

        while True:

            chunk = file.read(CHUNK_SIZE)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def calculate_remote_sha256(object_key):

    response = s3.get_object(
        Bucket=BUCKET,
        Key=object_key
    )

    body = response["Body"]
    sha256 = hashlib.sha256()

    try:

        while True:

            chunk = body.read(CHUNK_SIZE)

            if not chunk:
                break

            sha256.update(chunk)

    finally:
        body.close()

    return sha256.hexdigest()
def get_remote_head(object_key):
    try:
        return s3.head_object(
            Bucket=BUCKET,
            Key=object_key
        )
    except ClientError as error:
        status_code = error.response.get(
            "ResponseMetadata", {}
        ).get("HTTPStatusCode")
        error_code = error.response.get(
            "Error", {}
        ).get("Code")
        if status_code == 404 or error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):
            return None
        raise
audit_failed_file_names = set()
def safe_write_audit(record,summary,audit_failed_file_names):
    try:
        write_audit_log(record)
        return True
    except Exception as error:
        summary["audit_failed_records"] += 1

        file_ref = (
            record.get("object_key")
            or record.get("local_file")
        )

        if file_ref:
            audit_failed_file_names.add(file_ref)

            summary["audit_failed_files"] = len(
                audit_failed_file_names
            )

        print(
            f"[AUDIT_FAILED] "
            f"{file_ref or record.get('event_type', 'UNKNOWN')}: "
            f"{error}"
        )

        return False
def write_audit_log(record):
    with AUDIT_LOG.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(record, ensure_ascii=False) + "\n"
        )
        file.flush()
        os.fsync(file.fileno())
parquet_files = sorted(
    SILVER_ROOT.rglob("*.parquet")
)
total_local_bytes = sum(
    file_path.stat().st_size
    for file_path in parquet_files
)

print(f"Total local size: {total_local_bytes:,} bytes")
if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found in {SILVER_ROOT}"
    )

print(f"Total Parquet files: {len(parquet_files)}")

run_started = datetime.now(UTC)
run_id = run_started.strftime("%Y%m%dT%H%M%S%fZ")
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
    "audit_failed_files": 0,
    "audit_failed_records": 0
}
preflight_error = None
try:
    try:
    #Check access
        s3.head_bucket(Bucket=BUCKET)
    except Exception as error:
        preflight_error = str(error)
        summary["not_attempted_files"] = len(parquet_files)
        audit_record = ({
            "run_id": run_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": "PREFLIGHT",
            "bucket": BUCKET,
            "status": "FAILED_PREFLIGHT",
            "verification": "BUCKET_ACCESS_CHECK",
            "error": preflight_error
        })
        write_audit_log(audit_record)
        raise
    else:
        write_audit_log({
            "run_id": run_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": "PREFLIGHT",
            "bucket": BUCKET,
            "status": "PREFLIGHT_SUCCESS",
            "verification": "BUCKET_ACCESS_CHECK",
            "error": None
        })
    if preflight_error is None:

        for index, file_path in enumerate(parquet_files, start=1):
            object_key = None
            file_size = None
            local_sha256 = None
            timestamp = datetime.now(UTC).isoformat()
            try:
                object_key = (
                    file_path
                    .relative_to(SILVER_ROOT)
                    .as_posix()
                    )

                file_size = file_path.stat().st_size
                local_sha256 = calculate_local_sha256(file_path)
                print(
                        f"\nProcessing {index}/{len(parquet_files)}"
                    )
                print(f"Object key: {object_key}")

                remote_head = get_remote_head(object_key)
                remote_sha256 = None
                if remote_head is not None:
                        remote_sha256 = (
                            remote_head.get("Metadata", {})
                            .get("sha256")
                        )
                #idempotency
                should_upload = True
                upload_reason = "NEW_OR_CHANGED_OBJECT"
                if remote_head is not None:
                    same_size = (
                        remote_head["ContentLength"] == file_size
                    )
                    same_metadata_hash = (
                        remote_sha256 == local_sha256
                    )
                    if same_size and same_metadata_hash:
                        if VERIFY_EXISTING_OBJECTS:
                            # Verify actual remote content
                            actual_remote_sha = calculate_remote_sha256(
                                object_key
                            )
                            if actual_remote_sha == local_sha256:
                                should_upload = False
                                verification = "FULL_READBACK_SHA256"
                                summary["readback_verified_files"] += 1
                                summary["skipped_files"] += 1
                                status = "SKIPPED_UNCHANGED"
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
                        s3.upload_file(
                            str(file_path),
                            BUCKET,
                            object_key,
                            ExtraArgs={
                                "Metadata": {
                                    "sha256": local_sha256
                                }
                            }
                        )
                        summary["bytes_uploaded"] += file_size
                        uploaded_head = s3.head_object(
                            Bucket=BUCKET,
                            Key=object_key
                        )
                        size_matches = (
                            uploaded_head["ContentLength"] == file_size
                        )
                        hash_metadata_matches = (
                            uploaded_head.get("Metadata", {}).get("sha256")
                            == local_sha256
                        )
                        if not size_matches or not hash_metadata_matches:
                            raise RuntimeError(
                                "Remote size or SHA-256 metadata verification failed"
                            )
                        verification = "HEAD_SIZE_AND_SHA256_METADATA"
                        if VERIFY_READBACK_AFTER_UPLOAD:
                            actual_remote_sha = calculate_remote_sha256(
                                object_key
                            )
                            if actual_remote_sha != local_sha256:
                                raise RuntimeError(
                                    "Remote file content SHA-256 mismatch"
                                )
                            verification = "FULL_READBACK_SHA256"
                            summary["readback_verified_files"] += 1
                        summary["uploaded_files"] += 1
                        status = "UPLOADED_VERIFIED"
                        print("Upload and verification successful.")
                else:
                        print("Unchanged object. Upload skipped.")
                audit_record = {
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "event_type": "OBJECT_UPLOAD",
                    "bucket": BUCKET,
                    "object_key": object_key,
                    "local_file": str(file_path),                        "size_bytes": file_size,
                    "sha256": local_sha256,
                    "status": status,
                    "verification": verification,
                    "upload_reason": upload_reason,
                    "error": None
                    }
                safe_write_audit(
                    audit_record,
                    summary,
                    audit_failed_file_names
                )
            except Exception as error:
                    summary["failed_files"] += 1
                    print(f"Upload failed: {error}")
                    audit_record = {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "bucket": BUCKET,
                        "object_key": object_key,
                        "local_file": str(file_path),
                        "size_bytes": file_size,
                        "sha256": local_sha256,
                        "status": "FAILED",
                        "verification": "FAILED",
                        "error": str(error)
                    }

                    safe_write_audit(
                        audit_record,
                        summary,
                        audit_failed_file_names
                    )
finally:
    #summary
    run_finished = datetime.now(UTC)
    summary["finished_at"] = run_finished.isoformat()
    summary["duration_seconds"] = round(
        time.perf_counter() - start_time,
        2
    )
    if preflight_error:
        summary["status"] = "FAILED_PREFLIGHT"
        summary["preflight_error"] = preflight_error

    else:
        summary["status"] = (
            "SUCCESS"
            if summary["failed_files"] == 0
            else "COMPLETED_WITH_ERRORS"
        )
    with SUMMARY_FILE.open("w", encoding="utf-8") as file:
        json.dump(
            summary,
            file,
            indent=4,
            ensure_ascii=False
        )
    print("\n========== UPLOAD SUMMARY ==========")
    print(f"Total files: {summary['total_files']}")
    print(f"Uploaded: {summary['uploaded_files']}")
    print(f"Skipped: {summary['skipped_files']}")
    print(f"Failed: {summary['failed_files']}")
    print(f"Bytes uploaded: {summary['bytes_uploaded']}")
    print(
        f"Read-back verified: "
        f"{summary['readback_verified_files']}"
    )
    print(f"Duration: {summary['duration_seconds']} seconds")
    print(f"Summary file: {SUMMARY_FILE}")
    print(f"Audit log: {AUDIT_LOG}")
    print("====================================")
if summary["failed_files"] > 0:
    raise SystemExit(1)

