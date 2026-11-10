import argparse
import hashlib
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
AUDIT_PATH = PROJECT_ROOT / "logs" / "ingestion_audit.jsonl"
BUCKET = "fishing-bronze"


@dataclass
class Outcome:
    status: str
    key: str | None = None
    checksum: str | None = None
    error: str | None = None
    verification: str | None = None
    abort: bool = False


def validate_date(file: Path) -> datetime:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", file.stem):
        raise ValueError(f"Invalid filename: {file.name}")
    try:
        return datetime.strptime(file.stem, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date: {file.name}") from exc


def calculate_checksum(file: Path) -> str:
    digest = hashlib.sha256()
    with file.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def calculate_s3_checksum(s3, bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    digest = hashlib.sha256()
    try:
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            digest.update(chunk)
    finally:
        body.close()
    return digest.hexdigest()


def write_audit(filename, key, status, checksum=None, error=None, verification=None):
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "filename": filename,
        "bucket": BUCKET,
        "object_key": key,
        "status": status,
        "sha256": checksum,
        "verification": verification,
        "error": error,
    }
    with AUDIT_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def make_client():
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError("Missing S3 credentials in environment or .env")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            retries={"mode": "standard", "total_max_attempts": 4},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


def process_file(s3, file: Path, fast_existing=False) -> Outcome:
    """Return exactly one result for a file; don't overwrite existing objects."""
    try:
        event_date = validate_date(file)
        key = (
            f"global-fishing-watch/year={event_date:%Y}/"
            f"month={event_date:%m}/{file.name}"
        )
        checksum = calculate_checksum(file)
    except (ValueError, OSError) as exc:
        return Outcome("FAILED", error=str(exc))

    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if error_code in ("404", "NoSuchKey", "NotFound") or http_status == 404:
            head = None
        else:
            return Outcome("INFRA_ERROR", key, checksum, str(exc), abort=True)
    except BotoCoreError as exc:
        return Outcome("INFRA_ERROR", key, checksum, str(exc), abort=True)

    if head is not None:
        # Fast mode only trusts user metadata. Full mode reads and hashes the object.
        metadata_sha = head.get("Metadata", {}).get("sha256")
        try:
            if fast_existing and metadata_sha:
                remote_sha = metadata_sha
                method = "metadata"
            else:
                remote_sha = calculate_s3_checksum(s3, BUCKET, key)
                method = "content"
        except (ClientError, BotoCoreError, OSError) as exc:
            return Outcome("INFRA_ERROR", key, checksum, str(exc), abort=True)
        status = "SKIPPED" if checksum == remote_sha else "CONFLICT"
        return Outcome(status, key, checksum, verification=method)

    # A missing object is uploaded once; do not delete it if verification fails.
    try:
        s3.upload_file(
            Filename=str(file),
            Bucket=BUCKET,
            Key=key,
            ExtraArgs={"Metadata": {"sha256": checksum}},
        )
    except (S3UploadFailedError, ClientError, BotoCoreError, OSError) as exc:
        return Outcome("UPLOAD_FAILED", key, checksum, str(exc), abort=True)

    try:
        remote_sha = calculate_s3_checksum(s3, BUCKET, key)
    except (ClientError, BotoCoreError, OSError) as exc:
        return Outcome("VERIFICATION_FAILED", key, checksum, str(exc), abort=True)
    if remote_sha != checksum:
        return Outcome(
            "VERIFICATION_FAILED", key, checksum,
            "SHA-256 differs after upload; object retained for investigation",
            verification="content", abort=True,
        )
    return Outcome("UPLOADED", key, checksum, verification="content")


def run_batch(s3, selected_files, fast_existing=False, audit_writer=write_audit):
    counts = Counter()
    audit_failed = 0
    aborted = False
    for file in selected_files:
        try:
            result = process_file(s3, file, fast_existing=fast_existing)
        except Exception as exc:
            # Unknown failures must not silently continue a production-oriented job.
            result = Outcome("UNEXPECTED_ERROR", error=str(exc), abort=True)

        counts[result.status] += 1
        try:
            audit_writer(
                file.name, result.key, result.status, result.checksum,
                result.error, result.verification,
            )
        except Exception as exc:
            audit_failed += 1
            print(f"AUDIT_FAILED: {file.name}: {exc}")
        print(f"{result.status}: {file.name}")
        if result.abort:
            aborted = True
            print("Stopping batch: manual investigation or infrastructure recovery required.")
            break

    print("\n=== INGESTION SUMMARY ===")
    print("Selected:", len(selected_files))
    print("Processed:", sum(counts.values()))
    print("Not processed:", len(selected_files) - sum(counts.values()))
    for status in (
        "UPLOADED", "SKIPPED", "CONFLICT", "FAILED", "UPLOAD_FAILED",
        "VERIFICATION_FAILED", "INFRA_ERROR", "UNEXPECTED_ERROR",
    ):
        print(f"{status}:", counts[status])
    print("AUDIT_FAILED:", audit_failed)
    print("Aborted:", aborted)

    has_problem = any(
        counts[status] for status in (
            "CONFLICT", "FAILED", "UPLOAD_FAILED", "VERIFICATION_FAILED",
            "INFRA_ERROR", "UNEXPECTED_ERROR",
        )
    )
    return 1 if has_problem or audit_failed or aborted else 0


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    parser = argparse.ArgumentParser(description="Global Fishing Watch Bronze ingestion")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--limit", type=int, default=3, help="Number of CSVs (default 3)")
    group.add_argument("--all", action="store_true", help="Process every CSV")
    parser.add_argument(
        "--fast-existing", action="store_true",
        help="Trust SHA-256 in object metadata for existing objects when available",
    )
    args = parser.parse_args()
    if not args.all and args.limit <= 0:
        parser.error("--limit must be greater than 0")
    if not RAW_DIR.is_dir():
        print(f"ERROR: Raw directory is missing: {RAW_DIR}")
        return 1
    files = sorted(RAW_DIR.glob("*.csv"), key=lambda path: path.name)
    if not args.all:
        files = files[:args.limit]
    try:
        s3 = make_client()
    except (ValueError, BotoCoreError) as exc:
        print(f"CONFIG_ERROR: {exc}")
        return 1
    return run_batch(s3, files, fast_existing=args.fast_existing)


if __name__ == "__main__":
    raise SystemExit(main())
