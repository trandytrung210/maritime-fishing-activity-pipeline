"""SeaweedFS S3 integration checks (requires live gateway on S3_ENDPOINT)."""

from __future__ import annotations

import hashlib
import os
import uuid

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError

pytestmark = pytest.mark.integration

BUCKET = f"ci-maritime-{uuid.uuid4().hex[:8]}"
OBJECT_KEY = "integration/hello.txt"
PAYLOAD = b"maritime-fishing-ci-seaweedfs-check\n"


@pytest.fixture(scope="module", autouse=True)
def _require_seaweedfs():
    """Skip locally when S3 is down; fail in CI (CI=true / REQUIRE_SEAWEEDFS=1)."""
    try:
        _client().list_buckets()
    except Exception as error:
        message = f"SeaweedFS S3 not available at {_endpoint()}: {error}"
        require = (
            os.getenv("CI", "").lower() in {"1", "true", "yes"}
            or os.getenv("REQUIRE_SEAWEEDFS", "").lower() in {"1", "true", "yes"}
        )
        if require:
            pytest.fail(message)
        pytest.skip(message)


def _endpoint() -> str:
    return os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333")


def _access_key() -> str:
    return os.getenv("AWS_ACCESS_KEY_ID", "ci_access_key")


def _secret_key() -> str:
    return os.getenv("AWS_SECRET_ACCESS_KEY", "ci_secret_key")


def _client(
    *,
    access_key: str | None = None,
    secret_key: str | None = None,
):
    return boto3.client(
        "s3",
        endpoint_url=_endpoint(),
        aws_access_key_id=access_key or _access_key(),
        aws_secret_access_key=secret_key or _secret_key(),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def test_s3_api_responds():
    """S3 gateway accepts signed requests (list buckets)."""
    response = _client().list_buckets()
    assert "Buckets" in response
    assert isinstance(response["Buckets"], list)


def test_auth_rejects_bad_credentials():
    bad = _client(access_key="wrong_key", secret_key="wrong_secret")
    with pytest.raises(ClientError) as exc_info:
        bad.list_buckets()
    code = exc_info.value.response.get("Error", {}).get("Code", "")
    status = exc_info.value.response.get("ResponseMetadata", {}).get(
        "HTTPStatusCode"
    )
    assert status in {403, 400} or code in {
        "InvalidAccessKeyId",
        "AccessDenied",
        "InvalidSecurity",
        "SignatureDoesNotMatch",
        "403",
    }


def test_auth_accepts_configured_credentials():
    response = _client().list_buckets()
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_create_bucket_upload_download_checksum_delete():
    s3 = _client()
    local_sha = hashlib.sha256(PAYLOAD).hexdigest()

    s3.create_bucket(Bucket=BUCKET)
    buckets = {item["Name"] for item in s3.list_buckets()["Buckets"]}
    assert BUCKET in buckets

    s3.put_object(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
        Body=PAYLOAD,
        Metadata={"sha256": local_sha},
    )

    head = s3.head_object(Bucket=BUCKET, Key=OBJECT_KEY)
    assert head["ContentLength"] == len(PAYLOAD)
    assert head.get("Metadata", {}).get("sha256") == local_sha

    body = s3.get_object(Bucket=BUCKET, Key=OBJECT_KEY)["Body"].read()
    assert body == PAYLOAD
    assert hashlib.sha256(body).hexdigest() == local_sha

    s3.delete_object(Bucket=BUCKET, Key=OBJECT_KEY)
    with pytest.raises(ClientError) as exc_info:
        s3.head_object(Bucket=BUCKET, Key=OBJECT_KEY)
    status = exc_info.value.response.get("ResponseMetadata", {}).get(
        "HTTPStatusCode"
    )
    code = exc_info.value.response.get("Error", {}).get("Code")
    assert status == 404 or code in {"404", "NoSuchKey", "NotFound"}

    # Best-effort bucket cleanup (SeaweedFS may keep empty bucket).
    try:
        s3.delete_bucket(Bucket=BUCKET)
    except ClientError:
        pass
