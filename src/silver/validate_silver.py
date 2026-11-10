import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)
REPORT_ROOT = PROJECT_ROOT / "reports"
REPORT_PATH = (
    REPORT_ROOT
    / "silver_validation_report.json"
)
EXPECTED_SCHEMA = {
    "date": "date",
    "cell_ll_lat": "double",
    "cell_ll_lon": "double",
    "mmsi": "string",
    "hours": "double",
    "fishing_hours": "double",
    "date_mismatch": "boolean",
    "mmsi_status": "string",
    "hours_status": "string",
    "invalid_reason": "array<string>",
    "null_reason": "array<string>",
    "duplicate_count": "long",
    "duplicate_reason": "array<string>",
    "dq_status": "string",
    "dq_reason": "array<string>"
}
def check_data_type(actual_type, expected_type):
    if expected_type == "date":
        return pa.types.is_date32(actual_type)
    elif expected_type == "double":
        return pa.types.is_float64(actual_type)
    elif expected_type == "string":
        return pa.types.is_string(actual_type)
    elif expected_type == "boolean":
        return pa.types.is_boolean(actual_type)
    elif expected_type == "long":
        return pa.types.is_int64(actual_type)
    elif expected_type == "array<string>":
        return (
            (pa.types.is_list(actual_type)
            or 
            pa.types.is_large_list(actual_type)
            )
            and pa.types.is_string(actual_type.value_type)
        )
    return False
def validate_file(file_path):
    parquet_file = pq.ParquetFile(file_path)
    #read parquet
    metadata = parquet_file.metadata
    #check schema
    actual_schema = parquet_file.schema_arrow
    actual_columns = actual_schema.names
    expected_columns = list(EXPECTED_SCHEMA.keys())
    #miss_col
    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    #unexpected col
    extra_columns = [
        column
        for column in actual_columns
        if column not in expected_columns
    ]
    # col order
    column_order_match = (
        actual_columns == expected_columns
    )

    #data types
    type_mismatches = {}
    for column, expected_type in EXPECTED_SCHEMA.items():
        if column not in actual_columns:
            continue
        actual_type = actual_schema.field(column).type
        is_valid = check_data_type(
            actual_type,
            expected_type
        )
        if not is_valid:
            type_mismatches[column] = {
                "expected": expected_type,
                "actual": str(actual_type)
            }
    # Schema validation result
    schema_valid = (
        len(missing_columns) == 0
        and len(extra_columns) == 0
        and column_order_match
        and len(type_mismatches) == 0
    )

    # Record count from metadata
    record_count = metadata.num_rows

    return {
        "record_count": record_count,
        "schema_valid": schema_valid,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "column_order_match": column_order_match,
        "type_mismatches": type_mismatches
    }
def run_validation():
    started_at = datetime.now(UTC)
    parquet_files = sorted(
        SILVER_ROOT.rglob("*.parquet")
    )
    print(
        f"Total Parquet files discovered: {len(parquet_files)}"
    )
    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {SILVER_ROOT}"
        )
    file_results = []
    by_source_date = defaultdict(
        lambda: {
            "file_count": 0,
            "readable_files": 0,
            "unreadable_files": 0,
            "total_records": 0,
            "schema_mismatch_files": 0,
            "zero_record_files": 0
        }
    )
    for index, file_path in enumerate(
        parquet_files,
        start=1
    ):
        relative_path = file_path.relative_to(
            SILVER_ROOT
        ).as_posix()
        # Extract source_date from folder path
        source_date = next(
            (
                part.split("=", 1)[1]
                for part in file_path.parts
                if part.startswith("source_date=")
            ),
            None
        )
        print(
            f"Checking file {index}/{len(parquet_files)}: "
            f"{relative_path}"
        )
        try:
            result = validate_file(file_path)
            record_count = result["record_count"]
            if not result["schema_valid"]:
                status = "SCHEMA_MISMATCH"
            elif record_count == 0:
                status = "WARNING_ZERO_RECORDS"
            else:
                status = "PASS"
            file_result = {
                "file": relative_path,
                "source_date": source_date,
                "record_count": record_count,
                "status": status,
                **result
            }
        except Exception as error:
            file_result = {
                "file": relative_path,
                "source_date": source_date,
                "record_count": None,
                "status": "UNREADABLE",
                "error": str(error)
            }
        file_results.append(file_result)
        
        if source_date is not None:
            date_result = by_source_date[source_date]
            date_result["file_count"] += 1
            if file_result["status"] == "UNREADABLE":
                date_result["unreadable_files"] += 1
            else:
                date_result["readable_files"] += 1
                date_result["total_records"] += (
                    file_result["record_count"]
                )
                if file_result["record_count"] == 0:
                    date_result["zero_record_files"] += 1
                if not file_result["schema_valid"]:
                    date_result["schema_mismatch_files"] += 1
    return (
        parquet_files,
        file_results,
        by_source_date,
        started_at
    )
def main():
    REPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )
    parquet_files, file_results, by_source_date, started_at = run_validation()
    total_files = len(parquet_files)
    unreadable_files = sum(
        result["status"] == "UNREADABLE"
        for result in file_results
    )
    schema_mismatch_files = sum(
        result["status"] == "SCHEMA_MISMATCH"
        for result in file_results
    )
    zero_record_files = sum(
        result["status"] == "WARNING_ZERO_RECORDS"
        for result in file_results
    )
    datatype_mismatch_files = sum(
        bool(result.get("type_mismatches"))
        for result in file_results
    )
    total_records = sum(
        result["record_count"]
        for result in file_results
        if result["record_count"] is not None
    )
    overall_status = (
        "PASS"
        if (
            total_files > 0
            and unreadable_files == 0
            and schema_mismatch_files == 0
        )
        else "FAILED"
    )
    report = {
        "validation_started_at": started_at.isoformat(),
        "validation_finished_at": (
            datetime.now(UTC).isoformat()
        ),
        "overall_status": overall_status,
        "summary": {
            "total_files": total_files,
            "total_records": total_records,
            "unreadable_files": unreadable_files,
            "schema_mismatch_files": schema_mismatch_files,
            "datatype_mismatch_files": datatype_mismatch_files,
            "zero_record_files": zero_record_files
        },
        "by_source_date": {
            date: values
            for date, values in sorted(by_source_date.items())
        },
        "file_details": file_results
    }
    with REPORT_PATH.open(
        "w",
        encoding="utf-8"
    ) as report_file:
        json.dump(
            report,
            report_file,
            ensure_ascii=False,
            indent=4
        )
    print("\n========== SILVER VALIDATION ==========")
    print(f"Overall status: {overall_status}")
    print(f"Total files: {total_files}")
    print(f"Total records: {total_records}")
    print(f"Unreadable files: {unreadable_files}")
    print(f"Schema mismatch: {schema_mismatch_files}")
    print(f"Data type mismatch: {datatype_mismatch_files}")
    print(f"Zero-record files: {zero_record_files}")
    print(f"\nReport saved at: {REPORT_PATH}")
    if overall_status == "FAILED":
        raise SystemExit(1)
if __name__ == "__main__":
    main()