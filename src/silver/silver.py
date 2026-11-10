from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    array_union,
    col,
    count,
    explode,
    isnan,
    lit,
    size,
    sum,
    trim,
    when,
)
from pyspark.sql.functions import filter as array_filter
from pyspark.sql.window import Window

from src.common.manifest import (
    REPORT_ROOT,
    SILVER_STATE_PATH,
    add_pending_gold_change,
    get_run_id,
    load_silver_state,
    save_run_manifest,
    save_silver_state,
    update_silver_state,
)
from src.common.spark_io import write_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DQ_REPORT_PATH = REPORT_ROOT / "silver_dq_report.jsonl"
MANIFEST_PATH = SILVER_STATE_PATH
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
QUARANTINE_ROOT = PROJECT_ROOT / "data" / "processed" / "quarantine"
MANIFEST_VERSION = 1

EXPECTED_COLUMNS = [
    "date",
    "cell_ll_lat",
    "cell_ll_lon",
    "mmsi",
    "hours",
    "fishing_hours",
]
BUSINESS_KEY = [
    "date",
    "mmsi",
    "cell_ll_lat",
    "cell_ll_lon",
]


def get_status_counts(df):
    rows = (
        df.groupBy("dq_status")
        .count()
        .collect()
    )
    return {
        row["dq_status"]: row["count"]
        for row in rows
    }


def get_reason_counts(df, reason_column):
    rows = (
        df
        .select(explode(col(reason_column)).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    return {
        row["reason"]: row["count"]
        for row in rows
    }


def build_silver(df, source_date):
    df_silver = (
        df
        .select(*EXPECTED_COLUMNS)
        .withColumn("_date_raw", trim(col("date")))
        .withColumn("_lat_raw", trim(col("cell_ll_lat")))
        .withColumn("_lon_raw", trim(col("cell_ll_lon")))
        .withColumn("_hours_raw", trim(col("hours")))
        .withColumn("_fishing_hours_raw", trim(col("fishing_hours")))
        .withColumn("_mmsi_raw", trim(col("mmsi")))
        .withColumn("date", col("_date_raw").cast("date"))
        .withColumn("cell_ll_lat", col("_lat_raw").cast("double"))
        .withColumn("cell_ll_lon", col("_lon_raw").cast("double"))
        .withColumn("hours", col("_hours_raw").cast("double"))
        .withColumn("fishing_hours", col("_fishing_hours_raw").cast("double"))
        .withColumn("mmsi", col("_mmsi_raw"))
    )
    df_silver = df_silver.withColumn(
        "date_mismatch",
        col("date").isNotNull()
        & (col("date") != lit(source_date).cast("date")),
    )
    df_silver = df_silver.withColumn(
        "mmsi_status",
        when(col("mmsi").rlike(r"^[0-9]{9}$"), lit("VALID"))
        .otherwise(lit("REVIEW")),
    )
    df_silver = df_silver.withColumn(
        "hours_status",
        when(col("hours") > 24, lit("REVIEW_THRESHOLD"))
        .otherwise(lit("NORMAL")),
    )

    df_silver = df_silver.withColumn(
        "invalid_reason",
        array_filter(
            array(
                when(
                    col("_date_raw").isNotNull()
                    & (col("_date_raw") != "")
                    & col("date").isNull(),
                    lit("INVALID_DATE_FORMAT"),
                ),
                when(
                    col("_lat_raw").isNotNull()
                    & (col("_lat_raw") != "")
                    & (
                        col("cell_ll_lat").isNull()
                        | isnan(col("cell_ll_lat"))
                    ),
                    lit("INVALID_LATITUDE_FORMAT"),
                ),
                when(
                    col("_lon_raw").isNotNull()
                    & (col("_lon_raw") != "")
                    & (
                        col("cell_ll_lon").isNull()
                        | isnan(col("cell_ll_lon"))
                    ),
                    lit("INVALID_LONGITUDE_FORMAT"),
                ),
                when(
                    col("_hours_raw").isNotNull()
                    & (col("_hours_raw") != "")
                    & (col("hours").isNull() | isnan(col("hours"))),
                    lit("INVALID_HOURS_FORMAT"),
                ),
                when(
                    col("_fishing_hours_raw").isNotNull()
                    & (col("_fishing_hours_raw") != "")
                    & (
                        col("fishing_hours").isNull()
                        | isnan(col("fishing_hours"))
                    ),
                    lit("INVALID_FISHING_HOURS_FORMAT"),
                ),
                when(
                    (col("cell_ll_lat") < -90)
                    | (col("cell_ll_lat") > 90),
                    lit("INVALID_LATITUDE"),
                ),
                when(
                    (col("cell_ll_lon") < -180)
                    | (col("cell_ll_lon") > 180),
                    lit("INVALID_LONGITUDE"),
                ),
                when(
                    col("hours") < 0,
                    lit("NEGATIVE_HOURS"),
                ),
                when(
                    col("fishing_hours") < 0,
                    lit("NEGATIVE_FISHING"),
                ),
                when(
                    col("date_mismatch"),
                    lit("DATE_MISMATCH"),
                ),
            ),
            lambda x: x.isNotNull(),
        ),
    )
    df_silver = df_silver.withColumn(
        "null_reason",
        array_filter(
            array(
                when(
                    col("_date_raw").isNull() | (col("_date_raw") == ""),
                    lit("MISSING_DATE"),
                ),
                when(
                    col("_lat_raw").isNull() | (col("_lat_raw") == ""),
                    lit("MISSING_LATITUDE"),
                ),
                when(
                    col("_lon_raw").isNull() | (col("_lon_raw") == ""),
                    lit("MISSING_LONGITUDE"),
                ),
                when(
                    col("_mmsi_raw").isNull() | (col("_mmsi_raw") == ""),
                    lit("MISSING_MMSI"),
                ),
                when(
                    col("_hours_raw").isNull() | (col("_hours_raw") == ""),
                    lit("MISSING_HOURS"),
                ),
                when(
                    col("_fishing_hours_raw").isNull()
                    | (col("_fishing_hours_raw") == ""),
                    lit("MISSING_FISHING_HOURS"),
                ),
            ),
            lambda x: x.isNotNull(),
        ),
    )

    valid_key = (
        col("date").isNotNull()
        & col("mmsi").isNotNull()
        & (trim(col("mmsi")) != "")
        & col("cell_ll_lat").isNotNull()
        & col("cell_ll_lon").isNotNull()
    )
    duplicate_window = Window.partitionBy(*BUSINESS_KEY)

    df_silver = df_silver.withColumn(
        "_business_key_count",
        count(lit(1)).over(duplicate_window),
    )
    df_silver = df_silver.withColumn(
        "duplicate_count",
        when(valid_key, col("_business_key_count")).otherwise(lit(1)),
    )
    df_silver = df_silver.withColumn(
        "duplicate_reason",
        array_filter(
            array(
                when(
                    col("duplicate_count") > 1,
                    lit("DUPLICATE_BUSINESS_KEY"),
                )
            ),
            lambda x: x.isNotNull(),
        ),
    )
    df_silver = df_silver.withColumn(
        "invalid_reason",
        array_union(col("invalid_reason"), col("null_reason")),
    )
    df_silver = df_silver.withColumn(
        "invalid_reason",
        array_union(col("invalid_reason"), col("duplicate_reason")),
    )
    df_silver = df_silver.withColumn(
        "dq_status",
        when(size(col("invalid_reason")) > 0, lit("INVALID"))
        .when(col("mmsi_status") == "REVIEW", lit("REVIEW"))
        .when(col("hours_status") == "REVIEW_THRESHOLD", lit("INFO"))
        .otherwise(lit("VALID")),
    )
    df_silver = df_silver.withColumn(
        "dq_reason",
        array_filter(
            array(
                when(
                    col("mmsi_status") == "REVIEW",
                    lit("INVALID_MMSI_FORMAT"),
                ),
                when(
                    col("hours_status") == "REVIEW_THRESHOLD",
                    lit("HOURS_OVER_24"),
                ),
            ),
            lambda x: x.isNotNull(),
        ),
    )
    df_silver = df_silver.drop(
        "_date_raw",
        "_lat_raw",
        "_lon_raw",
        "_hours_raw",
        "_fishing_hours_raw",
        "_mmsi_raw",
        "_business_key_count",
    )
    return df_silver


def generate_dq_report(df, df_silver, source_date):
    df_silver = df_silver.cache()
    try:
        total_rows = df_silver.count()

        df_valid = df_silver.filter(
            col("dq_status").isin("VALID", "INFO", "REVIEW")
        )
        df_quarantine = df_silver.filter(
            col("dq_status") == "INVALID"
        )

        raw_rows = df.count()
        silver_rows = df_valid.count()
        quarantine_rows = df_quarantine.count()
        status_counts = get_status_counts(df_silver)

        duplicate_groups = (
            df_silver
            .filter(col("duplicate_count") > 1)
            .groupBy(*BUSINESS_KEY)
            .agg((count(lit(1)) - 1).alias("excess_rows"))
        )
        duplicate_member_rows = (
            df_silver
            .filter(col("duplicate_count") > 1)
            .count()
        )
        duplicate_metrics = (
            duplicate_groups
            .agg(
                count(lit(1)).alias("duplicate_group_count"),
                sum("excess_rows").alias("excess_duplicate_rows"),
            )
            .first()
        )

        duplicate_group_count = (
            duplicate_metrics["duplicate_group_count"] or 0
        )
        excess_duplicate_rows = (
            duplicate_metrics["excess_duplicate_rows"] or 0
        )
        date_mismatch_rows = df_silver.filter(
            col("date_mismatch")
        ).count()

        invalid_reason_counts = get_reason_counts(
            df_silver, "invalid_reason"
        )
        null_reason_counts = get_reason_counts(
            df_silver, "null_reason"
        )
        dq_reason_counts = get_reason_counts(
            df_silver, "dq_reason"
        )

        row_reconciliation_ok = (
            raw_rows == silver_rows + quarantine_rows
        )
        if not row_reconciliation_ok:
            raise RuntimeError(
                f"Row reconciliation failed for {source_date}"
            )

        report = {
            "source_date": source_date,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "total_rows": total_rows,
            "raw_rows": raw_rows,
            "silver_rows": silver_rows,
            "quarantine_rows": quarantine_rows,
            "status_counts": status_counts,
            "duplicate_rows": duplicate_member_rows,
            "duplicate_groups": duplicate_group_count,
            "excess_duplicate_rows": excess_duplicate_rows,
            "date_mismatch_rows": date_mismatch_rows,
            "invalid_reason_counts": invalid_reason_counts,
            "null_reason_counts": null_reason_counts,
            "dq_reason_counts": dq_reason_counts,
            "row_reconciliation_ok": row_reconciliation_ok,
        }
        return report
    finally:
        df_silver.unpersist()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def load_manifest() -> dict:
    data = load_silver_state()
    data.setdefault("version", MANIFEST_VERSION)
    data.setdefault("files", {})
    return data


def save_manifest(manifest: dict) -> None:
    manifest["version"] = MANIFEST_VERSION
    save_silver_state(manifest)


def load_dq_reports() -> dict[str, dict]:
    """Load DQ reports keyed by source_date (last entry wins)."""
    reports: dict[str, dict] = {}
    if not DQ_REPORT_PATH.exists():
        return reports
    with DQ_REPORT_PATH.open(encoding="utf-8") as report_file:
        for line_number, line in enumerate(report_file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print(
                    f"WARNING: skip invalid DQ JSONL line {line_number}"
                )
                continue
            source_date = record.get("source_date")
            if source_date:
                reports[source_date] = record
    return reports


def save_dq_reports(reports: dict[str, dict]) -> None:
    """Rewrite DQ JSONL with one record per source_date (sorted)."""
    lines = [
        json.dumps(reports[source_date], ensure_ascii=False)
        for source_date in sorted(reports)
    ]
    content = ("\n".join(lines) + "\n") if lines else ""
    path = DQ_REPORT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def upsert_dq_report(report: dict) -> None:
    reports = load_dq_reports()
    reports[report["source_date"]] = report
    save_dq_reports(reports)


def file_stat_fingerprint(path: Path) -> dict:
    stat = path.stat()
    return {
        "raw_path": str(path.resolve()),
        "raw_size": stat.st_size,
        "raw_mtime_ns": stat.st_mtime_ns,
    }


def file_fingerprint(path: Path) -> dict:
    fingerprint = file_stat_fingerprint(path)
    fingerprint["raw_sha256"] = _sha256_file(path)
    return fingerprint


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def silver_output_path(source_date: str) -> Path:
    year, month, _day = source_date.split("-")
    return (
        SILVER_ROOT
        / f"year={year}"
        / f"month={month}"
        / f"source_date={source_date}"
    )


def quarantine_output_path(source_date: str) -> Path:
    year, month, _day = source_date.split("-")
    return (
        QUARANTINE_ROOT
        / f"year={year}"
        / f"month={month}"
        / f"source_date={source_date}"
    )


def parse_force_reprocess() -> set[str] | None:
    """Return None=no force, empty set via 'all'=force all, else specific dates."""
    raw = os.getenv("SILVER_FORCE_REPROCESS", "").strip()
    if not raw:
        return None
    if raw.lower() in {"1", "true", "all", "*"}:
        return set()  # sentinel: force all
    dates = {part.strip() for part in raw.split(",") if part.strip()}
    return dates


def is_force_reprocess(source_date: str, force_dates: set[str] | None) -> bool:
    if force_dates is None:
        return False
    if len(force_dates) == 0:
        return True
    return source_date in force_dates


def bootstrap_manifest_from_existing(
    raw_files: list[Path],
    manifest: dict,
) -> int:
    """Register existing silver partitions so first incremental run can skip them."""
    files_meta = manifest.setdefault("files", {})
    bootstrapped = 0
    for file in raw_files:
        source_date = file.stem
        if source_date in files_meta:
            continue
        silver_path = silver_output_path(source_date)
        if not silver_path.exists():
            continue
        fingerprint = file_stat_fingerprint(file)
        files_meta[source_date] = {
            "source_date": source_date,
            **fingerprint,
            "raw_sha256": None,
            "status": "success",
            "processed_at": None,
            "bootstrapped": True,
            "silver_path": str(silver_path),
        }
        bootstrapped += 1
    if bootstrapped:
        save_manifest(manifest)
        print(
            f"Bootstrapped {bootstrapped} existing silver partitions into manifest"
        )
    return bootstrapped


def classify_raw_files(
    raw_files: list[Path],
    manifest: dict,
    force_dates: set[str] | None = None,
) -> tuple[list[Path], list[Path]]:
    """Split raw files into (pending, skipped)."""
    pending: list[Path] = []
    skipped: list[Path] = []
    files_meta = manifest.get("files", {})

    for file in raw_files:
        source_date = file.stem
        fingerprint = file_stat_fingerprint(file)
        prior = files_meta.get(source_date)
        silver_path = silver_output_path(source_date)

        force = is_force_reprocess(source_date, force_dates)
        # Fast change detection: size + mtime. SHA256 is stored on process.
        unchanged = (
            prior is not None
            and prior.get("status") == "success"
            and prior.get("raw_size") == fingerprint["raw_size"]
            and prior.get("raw_mtime_ns") == fingerprint["raw_mtime_ns"]
            and silver_path.exists()
        )

        if unchanged and not force:
            skipped.append(file)
        else:
            pending.append(file)

    return pending, skipped


def create_spark_session():
    # Keep Spark scratch on a real Linux FS inside Docker; Windows bind mounts
    # reject Hadoop chmod and break parquet writes.
    spark_local = Path(tempfile.gettempdir()) / "spark_silver_local"
    spark_local.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder
        .appName("Silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.local.dir", str(spark_local))
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2",
        )
        .getOrCreate()
    )


def discover_raw_files():
    all_csv_files = sorted(RAW_DIR.rglob("*.csv"))
    raw_files = [
        file
        for file in all_csv_files
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", file.stem)
    ]
    print(f"Total CSV files discovered: {len(all_csv_files)}")
    print(f"Valid daily GFW files: {len(raw_files)}")
    if not raw_files:
        raise FileNotFoundError(
            f"No daily GFW CSV files found in {RAW_DIR}"
        )
    return raw_files


def process_raw_file(
    spark,
    file: Path,
    index: int,
    total_files: int,
    manifest: dict,
    run_id: str,
    run_processed: list[dict],
) -> dict:
    source_date = file.stem
    datetime.strptime(source_date, "%Y-%m-%d")
    fingerprint = file_fingerprint(file)
    silver_output = silver_output_path(source_date)
    quarantine_output = quarantine_output_path(source_date)

    print(f"\nProcessing file {index}/{total_files}")
    print(f"Source date: {source_date}")
    print(f"Raw file: {file}")

    # 1. Read raw + build silver
    df = spark.read.csv(
        str(file),
        header=True,
        inferSchema=False,
    )
    df_silver = build_silver(df, source_date)

    # 2. Generate DQ report (in-memory first)
    report = generate_dq_report(df, df_silver, source_date)

    df_valid = df_silver.filter(
        col("dq_status").isin("VALID", "INFO", "REVIEW")
    )
    df_quarantine = df_silver.filter(
        col("dq_status") == "INVALID"
    )

    # 3. Write silver parquet successfully
    write_parquet(df_valid, silver_output)

    # 4. Write or delete quarantine successfully
    quarantine_count = int(report["quarantine_rows"])
    if quarantine_count > 0:
        write_parquet(df_quarantine, quarantine_output)
    elif quarantine_output.exists():
        shutil.rmtree(quarantine_output)

    # 5. Upsert DQ report (only after successful writes)
    upsert_dq_report(report)

    # 6. Persistent pending gold changes (upsert by source_date)
    add_pending_gold_change(source_date, fingerprint=fingerprint)
    print(f"Pending gold change upserted: {source_date}")

    # 7. Update silver state manifest SUCCESS
    entry = {
        "source_date": source_date,
        **fingerprint,
        "status": "success",
        "processed_at": _utc_now(),
        "run_id": run_id,
        "silver_path": str(silver_output),
        "quarantine_path": (
            str(quarantine_output) if quarantine_count else None
        ),
        "silver_rows": report["silver_rows"],
        "quarantine_rows": quarantine_count,
        "raw_rows": report["raw_rows"],
        "status_counts": report["status_counts"],
    }
    def _apply_success(state: dict) -> None:
        state.setdefault("files", {})[source_date] = entry

    update_silver_state(_apply_success)
    manifest.setdefault("files", {})[source_date] = entry
    run_processed.append(
        {
            "source_date": source_date,
            "raw_path": fingerprint["raw_path"],
            "silver_path": str(silver_output),
            "raw_sha256": fingerprint["raw_sha256"],
        }
    )

    print(f"Silver output: {silver_output}")
    print(f"Quarantine rows: {quarantine_count}")
    print(f"Silver state SUCCESS: {source_date}")
    (
        df_silver
        .groupBy("dq_status")
        .count()
        .orderBy("dq_status")
        .show()
    )
    return entry


def main():
    print("=" * 50)
    print("SILVER PIPELINE (incremental)")
    print("=" * 50)
    run_id = get_run_id()
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Raw input: {RAW_DIR}")
    print(f"Silver output: {SILVER_ROOT}")
    print(f"Quarantine output: {QUARANTINE_ROOT}")
    print(f"Manifest: {MANIFEST_PATH}")
    print(f"DQ report: {DQ_REPORT_PATH}")
    print(f"Run ID: {run_id}")

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    SILVER_ROOT.mkdir(parents=True, exist_ok=True)
    QUARANTINE_ROOT.mkdir(parents=True, exist_ok=True)

    raw_files = discover_raw_files()
    manifest = load_manifest()
    bootstrap_manifest_from_existing(raw_files, manifest)

    force_dates = parse_force_reprocess()
    if force_dates is not None:
        if len(force_dates) == 0:
            print("Force reprocess: ALL files")
        else:
            print(f"Force reprocess dates: {sorted(force_dates)}")

    pending_files, skipped_files = classify_raw_files(
        raw_files,
        manifest,
        force_dates=force_dates,
    )

    print("\n========== INCREMENTAL SUMMARY ==========")
    print(f"Raw files total: {len(raw_files)}")
    print(f"Pending (to process): {len(pending_files)}")
    print(f"Skipped (unchanged): {len(skipped_files)}")
    if skipped_files:
        preview = ", ".join(file.stem for file in skipped_files[:5])
        more = (
            ""
            if len(skipped_files) <= 5
            else f", ... (+{len(skipped_files) - 5})"
        )
        print(f"Skipped preview: {preview}{more}")

    run_processed: list[dict] = []
    run_manifest = {
        "run_id": run_id,
        "started_at": _utc_now(),
        "pending_count": len(pending_files),
        "skipped_count": len(skipped_files),
        "processed_files": run_processed,
        "status": "RUNNING",
    }

    if not pending_files:
        print("\nNo pending raw files. Silver incremental run is a no-op.")
        print(
            "NOTE: Downstream gold may still have work from "
            "pending_gold_changes / prior runs."
        )
        save_manifest(manifest)
        run_manifest["status"] = "NO_OP"
        run_manifest["finished_at"] = _utc_now()
        path = save_run_manifest(run_id, run_manifest)
        print(f"Run manifest: {path}")
        print("Spark Session not started.")
        return

    spark = create_spark_session()
    try:
        for index, file in enumerate(pending_files, start=1):
            process_raw_file(
                spark,
                file,
                index,
                len(pending_files),
                manifest,
                run_id,
                run_processed,
            )
        run_manifest["status"] = "SUCCESS"
        run_manifest["finished_at"] = _utc_now()
        path = save_run_manifest(run_id, run_manifest)
        print("\n========== INCREMENTAL COMPLETE ==========")
        print(f"Processed this run: {len(pending_files)}")
        print(f"Manifest entries: {len(manifest.get('files', {}))}")
        print(f"DQ report dates: {len(load_dq_reports())}")
        print(f"Run manifest: {path}")
    except Exception:
        run_manifest["status"] = "FAILED"
        run_manifest["finished_at"] = _utc_now()
        save_run_manifest(run_id, run_manifest)
        raise
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
