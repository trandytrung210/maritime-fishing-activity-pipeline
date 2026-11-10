from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import portalocker
from portalocker.exceptions import LockException

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = PROJECT_ROOT / "reports"
RUNS_ROOT = REPORT_ROOT / "runs"

PENDING_GOLD_CHANGES_PATH = REPORT_ROOT / "pending_gold_changes.json"
# Latest pointer only; authoritative file is per run_id under RUNS_ROOT.
GOLD_CHANGES_PATH = REPORT_ROOT / "gold_changes.json"
SILVER_STATE_PATH = REPORT_ROOT / "silver_processing_manifest.json"

PENDING_GOLD_LOCK_PATH = REPORT_ROOT / "pending_gold_changes.lock"
SILVER_STATE_LOCK_PATH = REPORT_ROOT / "silver_processing_manifest.lock"
GOLD_CHANGES_LOCK_PATH = REPORT_ROOT / "gold_changes.lock"

# Canonical affected_months item shape used by Gold builders:
#   {"year": int, "month": "MM"}  # zero-padded month string
AFFECTED_MONTH_YEAR_KEY = "year"
AFFECTED_MONTH_MONTH_KEY = "month"

_LOCK_TIMEOUT_SECONDS = 60.0
_SOURCE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTH_RE = re.compile(r"^(0[1-9]|1[0-2])$")
_YEAR_MIN = 1900
_YEAR_MAX = 2100


class ManifestError(ValueError):
    """Raised when a manifest file is corrupt or has an invalid shape."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def load_json(path: Path, default: Any | None = None) -> Any:
    """Load JSON. Missing file → default. Corrupt / invalid JSON → raise."""
    if not path.exists():
        return {} if default is None else default
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ManifestError(f"Cannot read JSON at {path}: {error}") from error
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise ManifestError(
            f"Corrupt JSON at {path}: {error}. "
            "Refusing to replace with an empty document."
        ) from error


def save_json_atomic(path: Path, data: Any) -> None:
    """Write JSON via a uniquely named temp file in the same directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=f".{uuid.uuid4().hex}.tmp",
        dir=str(path.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


@contextmanager
def _file_lock(
    lock_path: Path,
    *,
    timeout: float = _LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Exclusive lock via portalocker (cross-platform)."""
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.touch(exist_ok=True)
    except OSError as error:
        raise ManifestError(
            f"Cannot create lock file {lock_path}: {error}"
        ) from error

    try:
        # fail_when_locked=True enables timeout-based retry (non-blocking).
        with portalocker.Lock(
            str(lock_path),
            mode="a+",
            timeout=timeout,
            flags=portalocker.LOCK_EX,
            fail_when_locked=True,
        ):
            yield
    except LockException as error:
        raise TimeoutError(
            f"Timed out acquiring lock after {timeout:.0f}s: {lock_path}"
        ) from error
    except OSError as error:
        raise ManifestError(
            f"Lock I/O error for {lock_path}: {error}"
        ) from error


def get_run_id() -> str:
    for key in (
        "AIRFLOW_CTX_DAG_RUN_ID",
        "AIRFLOW_RUN_ID",
        "DAG_RUN_ID",
    ):
        value = os.getenv(key)
        if value:
            return value
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def safe_run_id(run_id: str | None = None) -> str:
    value = run_id or get_run_id()
    return "".join(
        char if char.isalnum() or char in "-_" else "_"
        for char in value
    )


def run_manifest_path(run_id: str) -> Path:
    return RUNS_ROOT / safe_run_id(run_id) / "silver_run_manifest.json"


def gold_changes_path(run_id: str | None = None) -> Path:
    """Per-DAG-run gold changes file (authoritative)."""
    return RUNS_ROOT / safe_run_id(run_id) / "gold_changes.json"


def _parse_source_date(source_date: str) -> datetime:
    """Strict YYYY-MM-DD (zero-padded, calendar-valid, year in range)."""
    if not isinstance(source_date, str):
        raise ManifestError(
            f"source_date must be str, got {type(source_date).__name__}"
        )
    if not _SOURCE_DATE_RE.fullmatch(source_date):
        raise ManifestError(
            f"source_date must match YYYY-MM-DD, got {source_date!r}"
        )
    try:
        parsed = datetime.strptime(source_date, "%Y-%m-%d")
    except ValueError as error:
        raise ManifestError(
            f"Invalid calendar date {source_date!r}: {error}"
        ) from error
    if parsed.strftime("%Y-%m-%d") != source_date:
        raise ManifestError(
            f"source_date {source_date!r} is not a normalized YYYY-MM-DD"
        )
    if not (_YEAR_MIN <= parsed.year <= _YEAR_MAX):
        raise ManifestError(
            f"source_date year must be {_YEAR_MIN}..{_YEAR_MAX}, "
            f"got {parsed.year}"
        )
    return parsed


def _validate_year_month(year: Any, month: Any, *, context: str) -> tuple[int, str]:
    try:
        year_i = int(year)
    except (TypeError, ValueError) as error:
        raise ManifestError(
            f"Invalid year in {context}: {year!r}"
        ) from error
    if not (_YEAR_MIN <= year_i <= _YEAR_MAX):
        raise ManifestError(
            f"year in {context} must be {_YEAR_MIN}..{_YEAR_MAX}, got {year_i}"
        )

    month_s = str(month).zfill(2) if not isinstance(month, str) else month
    if isinstance(month, int):
        month_s = f"{month:02d}"
    if not isinstance(month_s, str) or not _MONTH_RE.fullmatch(month_s):
        raise ManifestError(
            f"month in {context} must be '01'..'12', got {month!r}"
        )
    try:
        datetime.strptime(f"{year_i:04d}-{month_s}-01", "%Y-%m-%d")
    except ValueError as error:
        raise ManifestError(
            f"Invalid year/month in {context}: {year_i}-{month_s}: {error}"
        ) from error
    return year_i, month_s


def _validate_pending_entry(key: str, value: dict) -> None:
    parsed_key = _parse_source_date(key)
    if "source_date" not in value:
        raise ManifestError(
            f"Pending entry {key!r} missing required 'source_date'"
        )
    source_date = value["source_date"]
    parsed_entry = _parse_source_date(source_date)
    if source_date != key:
        raise ManifestError(
            f"Pending key {key!r} does not match source_date {source_date!r}"
        )
    if parsed_key.date() != parsed_entry.date():
        raise ManifestError(f"Pending date mismatch for {key!r}")

    if "year" not in value or "month" not in value:
        raise ManifestError(
            f"Pending entry {key!r} requires 'year' and 'month'"
        )
    year_i, month_s = _validate_year_month(
        value["year"],
        value["month"],
        context=f"pending[{key}]",
    )
    if year_i != parsed_key.year:
        raise ManifestError(
            f"Pending {key!r} year {year_i} != date year {parsed_key.year}"
        )
    expected_month = f"{parsed_key.month:02d}"
    if month_s != expected_month:
        raise ManifestError(
            f"Pending {key!r} month {month_s!r} != date month {expected_month!r}"
        )
    value["year"] = year_i
    value["month"] = month_s

    fingerprint = value.get("raw_fingerprint")
    if fingerprint is not None and not isinstance(fingerprint, dict):
        raise ManifestError(
            f"raw_fingerprint for {key!r} must be a JSON object, "
            f"got {type(fingerprint).__name__}"
        )


def _validate_pending_document(data: Any, path: Path) -> dict:
    if not isinstance(data, dict):
        raise ManifestError(
            f"Pending gold changes at {path} must be a JSON object, "
            f"got {type(data).__name__}"
        )
    changes = data.get("changes", {})
    if changes is None:
        changes = {}
        data["changes"] = changes
    if not isinstance(changes, dict):
        raise ManifestError(
            f"'changes' in {path} must be a JSON object, "
            f"got {type(changes).__name__}"
        )
    for key, value in changes.items():
        if not isinstance(key, str):
            raise ManifestError(
                f"Pending change key must be str, got {type(key).__name__}"
            )
        if not isinstance(value, dict):
            raise ManifestError(
                f"Pending change for {key!r} must be a JSON object, "
                f"got {type(value).__name__}"
            )
        _validate_pending_entry(key, value)
    data.setdefault("version", 1)
    data.setdefault("updated_at", None)
    return data


def _validate_gold_changes_document(data: Any, path: Path) -> dict:
    if not isinstance(data, dict):
        raise ManifestError(
            f"Gold changes at {path} must be a JSON object, "
            f"got {type(data).__name__}"
        )
    changed_dates = data.get("changed_dates", [])
    if changed_dates is None:
        changed_dates = []
        data["changed_dates"] = changed_dates
    if not isinstance(changed_dates, list):
        raise ManifestError(
            f"'changed_dates' in {path} must be a list, "
            f"got {type(changed_dates).__name__}"
        )
    for source_date in changed_dates:
        _parse_source_date(source_date)

    affected = data.get("affected_months", [])
    if affected is None:
        affected = []
        data["affected_months"] = affected
    if not isinstance(affected, list):
        raise ManifestError(
            f"'affected_months' in {path} must be a list, "
            f"got {type(affected).__name__}"
        )
    for item in affected:
        _validate_affected_month_item(item)

    snapshot = data.get("changes_snapshot", {})
    if snapshot is None:
        snapshot = {}
        data["changes_snapshot"] = snapshot
    if not isinstance(snapshot, dict):
        raise ManifestError(
            f"'changes_snapshot' in {path} must be a JSON object, "
            f"got {type(snapshot).__name__}"
        )
    for key, value in snapshot.items():
        if not isinstance(key, str):
            raise ManifestError(
                f"changes_snapshot key must be str, got {type(key).__name__}"
            )
        if not isinstance(value, dict):
            raise ManifestError(
                f"changes_snapshot entry for {key!r} must be a JSON object, "
                f"got {type(value).__name__}"
            )
        _validate_pending_entry(key, value)
    return data


def _validate_affected_month_item(item: Any) -> dict:
    if not isinstance(item, dict):
        raise ManifestError(
            f"affected_months item must be object, got {type(item).__name__}"
        )
    if AFFECTED_MONTH_YEAR_KEY not in item or AFFECTED_MONTH_MONTH_KEY not in item:
        raise ManifestError(
            "affected_months item requires 'year' (int) and 'month' ('MM')"
        )
    year_i, month_s = _validate_year_month(
        item[AFFECTED_MONTH_YEAR_KEY],
        item[AFFECTED_MONTH_MONTH_KEY],
        context="affected_months",
    )
    item[AFFECTED_MONTH_YEAR_KEY] = year_i
    item[AFFECTED_MONTH_MONTH_KEY] = month_s
    return item


def normalize_affected_month(year: int, month: int | str) -> dict:
    """Return canonical month dict shared by Gold builders / gold_changes."""
    year_i, month_s = _validate_year_month(year, month, context="normalize_affected_month")
    return {
        AFFECTED_MONTH_YEAR_KEY: year_i,
        AFFECTED_MONTH_MONTH_KEY: month_s,
    }


def _fingerprint_is_empty(fingerprint: dict) -> bool:
    if not fingerprint:
        return True
    return all(
        value is None or value == "" or value == {} or value == []
        for value in fingerprint.values()
    )


def _fingerprint_token(entry: dict | None) -> tuple:
    """
    Stable identity for a pending entry (prefer raw_sha256).

    Empty / missing fingerprints get dedicated tokens so finalize never
    treats "no identity" as a successful match for deletion.
    """
    if not entry or not isinstance(entry, dict):
        return ("missing_entry",)
    fingerprint = entry.get("raw_fingerprint")
    if fingerprint is None:
        return ("missing_fingerprint", entry.get("source_date"))
    if not isinstance(fingerprint, dict):
        return ("invalid_fingerprint", type(fingerprint).__name__)
    if _fingerprint_is_empty(fingerprint):
        return ("empty_fingerprint", entry.get("source_date"))

    sha = fingerprint.get("raw_sha256")
    if isinstance(sha, str) and sha.strip():
        return ("sha256", sha.strip())

    size = fingerprint.get("raw_size")
    mtime = fingerprint.get("raw_mtime_ns")
    if mtime is None:
        mtime = fingerprint.get("raw_mtime")
    if size is not None and mtime is not None:
        return ("stat", size, mtime)

    return ("fingerprint", json.dumps(fingerprint, sort_keys=True, default=str))


def _fingerprint_is_strong(token: tuple) -> bool:
    return token[0] in {"sha256", "stat", "fingerprint"}


def load_pending_gold_changes() -> dict:
    data = load_json(
        PENDING_GOLD_CHANGES_PATH,
        default={"version": 1, "updated_at": None, "changes": {}},
    )
    return _validate_pending_document(data, PENDING_GOLD_CHANGES_PATH)


def add_pending_gold_change(
    source_date: str,
    fingerprint: dict | None = None,
) -> dict:
    """Upsert a source_date into pending gold changes (no duplicates)."""
    parsed = _parse_source_date(source_date)
    if fingerprint is not None and not isinstance(fingerprint, dict):
        raise ManifestError(
            f"fingerprint must be a dict when provided, "
            f"got {type(fingerprint).__name__}"
        )

    with _file_lock(PENDING_GOLD_LOCK_PATH):
        pending = load_pending_gold_changes()
        changes = pending.setdefault("changes", {})
        entry = {
            "source_date": source_date,
            "year": parsed.year,
            "month": f"{parsed.month:02d}",
            "updated_at": _utc_now(),
        }
        if fingerprint is not None:
            entry["raw_fingerprint"] = fingerprint
        changes[source_date] = entry
        pending["updated_at"] = _utc_now()
        save_json_atomic(PENDING_GOLD_CHANGES_PATH, pending)
        return entry


def load_silver_state() -> dict:
    data = load_json(
        SILVER_STATE_PATH,
        default={"version": 1, "updated_at": None, "files": {}},
    )
    if not isinstance(data, dict):
        raise ManifestError(
            f"Silver state at {SILVER_STATE_PATH} must be a JSON object, "
            f"got {type(data).__name__}"
        )
    files = data.get("files", {})
    if files is None:
        files = {}
        data["files"] = files
    if not isinstance(files, dict):
        raise ManifestError(
            f"'files' in {SILVER_STATE_PATH} must be a JSON object, "
            f"got {type(files).__name__}"
        )
    for key in files:
        if isinstance(key, str):
            try:
                _parse_source_date(key)
            except ManifestError:
                continue
    data.setdefault("version", 1)
    data.setdefault("updated_at", None)
    return data


def _write_silver_state_unlocked(state: dict) -> None:
    if not isinstance(state, dict):
        raise ManifestError("Silver state payload must be a dict")
    state["version"] = state.get("version", 1)
    state["updated_at"] = _utc_now()
    save_json_atomic(SILVER_STATE_PATH, state)


def save_silver_state(state: dict) -> None:
    """Replace silver state under lock (full write)."""
    with _file_lock(SILVER_STATE_LOCK_PATH):
        _write_silver_state_unlocked(state)


def update_silver_state(mutator: Callable[[dict], None]) -> dict:
    """
    Locked Read → Modify → Write for silver_processing_manifest.json.

    The mutator receives the loaded state and may modify it in place.
    """
    if not callable(mutator):
        raise ManifestError("mutator must be callable")
    with _file_lock(SILVER_STATE_LOCK_PATH):
        state = load_silver_state()
        mutator(state)
        _write_silver_state_unlocked(state)
        return state


def load_gold_changes(run_id: str | None = None) -> dict:
    """Load gold changes for a DAG run (defaults to current get_run_id())."""
    rid = run_id or get_run_id()
    path = gold_changes_path(rid)
    default = {
        "run_id": rid,
        "generated_at": None,
        "changed_dates": [],
        "affected_months": [],
        "changes_snapshot": {},
        "has_silver_changes": False,
    }
    # Prefer per-run file; fall back to legacy latest pointer once.
    if path.exists():
        data = load_json(path, default=default)
    elif GOLD_CHANGES_PATH.exists():
        data = load_json(GOLD_CHANGES_PATH, default=default)
    else:
        data = default
    return _validate_gold_changes_document(data, path)


def save_gold_changes(payload: dict, run_id: str | None = None) -> Path:
    """
    Persist gold changes under reports/runs/<run_id>/gold_changes.json.

    Also refreshes the latest pointer at GOLD_CHANGES_PATH for convenience.
    """
    if not isinstance(payload, dict):
        raise ManifestError("Gold changes payload must be a dict")
    rid = run_id or payload.get("run_id") or get_run_id()
    payload["run_id"] = rid
    payload.setdefault("changes_snapshot", {})
    path = gold_changes_path(rid)
    _validate_gold_changes_document(payload, path)
    payload["affected_months"] = [
        normalize_affected_month(item["year"], item["month"])
        for item in payload.get("affected_months") or []
    ]
    with _file_lock(GOLD_CHANGES_LOCK_PATH):
        save_json_atomic(path, payload)
        save_json_atomic(GOLD_CHANGES_PATH, payload)
    return path


def save_run_manifest(run_id: str, payload: dict) -> Path:
    path = run_manifest_path(run_id)
    save_json_atomic(path, payload)
    return path


def dates_to_affected_months(dates: list[str]) -> list[dict]:
    """Map YYYY-MM-DD dates → unique canonical affected_months for Gold."""
    if not isinstance(dates, list):
        raise ManifestError(
            f"dates must be a list, got {type(dates).__name__}"
        )
    months: dict[tuple[int, str], dict] = {}
    for source_date in dates:
        parsed = _parse_source_date(source_date)
        item = normalize_affected_month(parsed.year, parsed.month)
        key = (item["year"], item["month"])
        months[key] = item
    return [months[key] for key in sorted(months)]


def build_changes_snapshot(pending_changes: dict) -> dict:
    """Deep-copy pending entries for gold_changes.json (fingerprint compare)."""
    if not isinstance(pending_changes, dict):
        raise ManifestError(
            f"pending changes must be a dict, "
            f"got {type(pending_changes).__name__}"
        )
    return json.loads(json.dumps(pending_changes))


def finalize_gold_changes(
    changed_dates: list[str] | None = None,
    *,
    run_id: str | None = None,
) -> int:
    """
    Remove consumed dates from pending_gold_changes after gold succeeds.

    Uses the per-run gold_changes snapshot written by detect_gold_changes.
    Never deletes pending when snapshot is missing/empty or fingerprint is weak.
    """
    rid = run_id or get_run_id()
    gold = load_gold_changes(rid)
    file_run_id = gold.get("run_id")
    if file_run_id and file_run_id != rid:
        print(
            f"WARNING: finalize run_id mismatch: "
            f"expected {rid!r}, file has {file_run_id!r}"
        )

    if changed_dates is None:
        changed_dates = list(gold.get("changed_dates") or [])
    if not isinstance(changed_dates, list):
        raise ManifestError(
            f"changed_dates must be a list, got {type(changed_dates).__name__}"
        )
    for source_date in changed_dates:
        _parse_source_date(source_date)

    snapshot = gold.get("changes_snapshot")
    if not isinstance(snapshot, dict) or not snapshot:
        print(
            "WARNING: finalize refusing to clear pending — "
            f"changes_snapshot missing/empty for run_id={rid}"
        )
        return 0

    with _file_lock(PENDING_GOLD_LOCK_PATH):
        pending = load_pending_gold_changes()
        changes = pending.get("changes", {})
        removed = 0
        skipped = 0

        for source_date in changed_dates:
            if source_date not in changes:
                continue

            snap_entry = snapshot.get(source_date)
            if snap_entry is None:
                print(
                    f"WARNING: finalize skip {source_date}: "
                    "missing from changes_snapshot"
                )
                skipped += 1
                continue

            snap_token = _fingerprint_token(snap_entry)
            current_token = _fingerprint_token(changes[source_date])
            if not _fingerprint_is_strong(snap_token) or not _fingerprint_is_strong(
                current_token
            ):
                print(
                    f"WARNING: finalize skip {source_date}: "
                    f"weak fingerprint ({snap_token[0]}/{current_token[0]})"
                )
                skipped += 1
                continue
            if snap_token != current_token:
                print(
                    f"WARNING: finalize skip {source_date}: "
                    "pending fingerprint changed since detect"
                )
                skipped += 1
                continue

            del changes[source_date]
            removed += 1

        pending["changes"] = changes
        pending["updated_at"] = _utc_now()
        save_json_atomic(PENDING_GOLD_CHANGES_PATH, pending)
        if skipped:
            print(f"Finalize skipped (kept pending): {skipped}")
        return removed
