import io
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from src.silver import ingest


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.fail_auth = False

    def head_object(self, Bucket, Key):
        if self.fail_auth:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"Metadata": self.metadata.get(Key, {})}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.objects[Key] = Path(Filename).read_bytes()
        self.metadata[Key] = (ExtraArgs or {}).get("Metadata", {})
        
def sample_file(tmp_path):
    path = tmp_path / "2020-01-01.csv"
    path.write_bytes(b"date,fishing_hours\n2020-01-01,3\n")
    return path


def test_valid_date(tmp_path):
    assert ingest.validate_date(sample_file(tmp_path)).strftime("%Y-%m-%d") == "2020-01-01"


def test_invalid_date(tmp_path):
    with pytest.raises(ValueError):
        ingest.validate_date(tmp_path / "2020-15-99.csv")


def test_new_object_uploaded(tmp_path):
    result = ingest.process_file(FakeS3(), sample_file(tmp_path))
    assert result.status == "UPLOADED"
    assert result.verification == "content"


def test_matching_object_skipped(tmp_path):
    fake = FakeS3()
    file = sample_file(tmp_path)
    assert ingest.process_file(fake, file).status == "UPLOADED"
    assert ingest.process_file(fake, file).status == "SKIPPED"


def test_different_object_conflict(tmp_path):
    fake = FakeS3()
    file = sample_file(tmp_path)
    initial = ingest.process_file(fake, file)
    fake.objects[initial.key] = b"different contents"
    result = ingest.process_file(fake, file)
    assert result.status == "CONFLICT"
    assert fake.objects[initial.key] == b"different contents"  # not overwritten


def test_s3_auth_failure_aborts(tmp_path):
    fake = FakeS3()
    fake.fail_auth = True
    result = ingest.process_file(fake, sample_file(tmp_path))
    assert result.status == "INFRA_ERROR"
    assert result.abort


def test_missing_credentials(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(ValueError, match="Missing S3 credentials"):
        ingest.make_client()


def test_verification_failed_keeps_object(tmp_path):
    class CorruptS3(FakeS3):
        def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
            super().upload_file(Filename, Bucket, Key, ExtraArgs)
            self.objects[Key] = b"corrupted after upload"

    fake = CorruptS3()
    result = ingest.process_file(fake, sample_file(tmp_path))
    assert result.status == "VERIFICATION_FAILED"
    assert result.abort
    assert result.key in fake.objects  # preserved, not automatically deleted


def test_audit_failure_does_not_change_upload_status(tmp_path):
    def broken_audit(*args):
        raise OSError("disk full")

    fake = FakeS3()
    file = sample_file(tmp_path)
    exit_code = ingest.run_batch(fake, [file], audit_writer=broken_audit)
    assert exit_code == 1  # failed audit is visible to Airflow
    assert len(fake.objects) == 1  # upload was successful
