import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import count as spark_count

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAILY_GOLD_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "daily_vessel_activity"
)
DQ_REPORT_ROOT = PROJECT_ROOT / "reports" / "daily_vessel_hours_dq"
REVIEW_ROOT = DQ_REPORT_ROOT / "review_records"
SUMMARY_OUTPUT = DQ_REPORT_ROOT / "summary.json"

# Minimal columns needed for DQ rules. Extra columns are allowed.
DAILY_GOLD_EXPECTED_SCHEMA = {
    "date": "date",
    "mmsi": "string",
    "total_hours": "double",
}

REVIEW_EXPECTED_SCHEMA = {
    "date": "date",
    "mmsi": "string",
    "total_hours": "double",
}

# Must match keys written by build_daily_vessel_hours_dq.py -> summary.json
SUMMARY_METRIC_KEYS = (
    "total_records",
    "normal_records",
    "info_records",
    "review_records",
    "null_hours_records",
    "negative_hours_records",
)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ValidateDailyVesselHoursDQ")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark


def validate_dataframe_schema(df, expected_schema, dataset_name):
    print(f"\n========== {dataset_name} SCHEMA VALIDATION ==========")
    df.printSchema()

    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    missing_columns = [
        column
        for column in expected_schema
        if column not in actual_schema
    ]
    type_mismatches = {
        column: {
            "expected": expected_type,
            "actual": actual_schema[column],
        }
        for column, expected_type in expected_schema.items()
        if column in actual_schema
        and actual_schema[column] != expected_type
    }

    columns_pass = not missing_columns
    types_pass = not type_mismatches
    schema_pass = columns_pass and types_pass

    print(
        "Required columns:",
        "PASS" if columns_pass else f"FAIL missing={missing_columns}",
    )
    if type_mismatches:
        print("Data type mismatches:")
        for column, detail in type_mismatches.items():
            print(
                f"  - {column}: expected={detail['expected']} "
                f"actual={detail['actual']}"
            )
        print("Data types: FAIL")
    else:
        print("Data types: PASS")

    print(f"{dataset_name} schema:", "PASS" if schema_pass else "FAIL")
    return schema_pass


def compute_expected_dq_stats(daily_gold_df):
    """
    Mirror build_daily_vessel_hours_dq classification rules:

    REVIEW_NULL      : total_hours IS NULL
    REVIEW_NEGATIVE  : total_hours < 0
    NORMAL           : 0 <= total_hours <= 24
    INFO             : 24 < total_hours <= 48
    REVIEW           : total_hours > 48
    """
    return (
        daily_gold_df
        .agg(
            spark_count("*").alias("total_records"),
            spark_count(
                when(col("total_hours").isNull(), 1)
            ).alias("null_hours_records"),
            spark_count(
                when(col("total_hours") < 0, 1)
            ).alias("negative_hours_records"),
            spark_count(
                when(
                    col("total_hours").isNotNull()
                    & (col("total_hours") >= 0)
                    & (col("total_hours") <= 24),
                    1,
                )
            ).alias("normal_records"),
            spark_count(
                when(
                    (col("total_hours") > 24)
                    & (col("total_hours") <= 48),
                    1,
                )
            ).alias("info_records"),
            spark_count(
                when(col("total_hours") > 48, 1)
            ).alias("review_records"),
        )
        .first()
        .asDict()
    )


def main():
    print("=" * 50)
    print("DAILY VESSEL HOURS DQ VALIDATION")
    print("=" * 50)

    if not DAILY_GOLD_ROOT.exists():
        raise FileNotFoundError(
            f"Daily Gold not found: {DAILY_GOLD_ROOT}"
        )
    if not REVIEW_ROOT.exists():
        raise FileNotFoundError(
            f"Review output not found: {REVIEW_ROOT}"
        )
    if not SUMMARY_OUTPUT.exists():
        raise FileNotFoundError(
            f"Summary JSON not found: {SUMMARY_OUTPUT}"
        )

    spark = create_spark_session()
    exit_code = 1
    try:
        print("\nSpark Session created: PASS")

        daily_gold_df = spark.read.parquet(str(DAILY_GOLD_ROOT))
        review_df = spark.read.parquet(str(REVIEW_ROOT))
        daily_gold_non_empty = (
            daily_gold_df.limit(1).count() > 0
        )

        print(
            "Daily Gold non-empty:",
            "PASS" if daily_gold_non_empty else "FAIL"
        )

        with SUMMARY_OUTPUT.open("r", encoding="utf-8") as file:
            dq_report = json.load(file)

        # Summary metrics live under dq_report["summary"], not top-level.
        if "summary" not in dq_report or not isinstance(
            dq_report["summary"], dict
        ):
            print("Summary JSON missing nested 'summary' object: FAIL")
            raise SystemExit(1)
        summary_data = dq_report["summary"]

        print("Gold Daily loaded: PASS")
        print("Review Parquet loaded: PASS")
        print("Summary JSON loaded: PASS")

        # ---------- SCHEMA ----------
        daily_gold_schema_pass = validate_dataframe_schema(
            daily_gold_df,
            DAILY_GOLD_EXPECTED_SCHEMA,
            "DAILY GOLD",
        )
        review_schema_pass = validate_dataframe_schema(
            review_df,
            REVIEW_EXPECTED_SCHEMA,
            "REVIEW PARQUET",
        )
        if not daily_gold_schema_pass or not review_schema_pass:
            print(
                "\nSchema validation FAILED."
                "\nStopping remaining DQ validation."
            )
            raise SystemExit(1)

        # ---------- EXPECTED COUNTS FROM GOLD ----------
        expected_stats = compute_expected_dq_stats(daily_gold_df)
        print("\n========== EXPECTED DQ SUMMARY ==========")
        for name, value in expected_stats.items():
            print(f"{name}: {value}")

        # Partition completeness: all rows must fall in exactly one bucket.
        partitioned_total = (
            expected_stats["null_hours_records"]
            + expected_stats["negative_hours_records"]
            + expected_stats["normal_records"]
            + expected_stats["info_records"]
            + expected_stats["review_records"]
        )
        partition_complete_pass = (
            partitioned_total == expected_stats["total_records"]
        )
        print(
            "\nPartition completeness:",
            "PASS" if partition_complete_pass else "FAIL",
            f"(sum buckets={partitioned_total}, "
            f"total={expected_stats['total_records']})",
        )

        # ---------- JSON SUMMARY RECONCILIATION ----------
        print("\n========== JSON SUMMARY VALIDATION ==========")
        summary_checks = {}
        for column_name in SUMMARY_METRIC_KEYS:
            expected_value = int(expected_stats[column_name])
            if column_name not in summary_data:
                print(f"{column_name}: FAIL (missing in summary.json)")
                summary_checks[column_name] = False
                continue
            try:
                reported_value = int(summary_data[column_name])
            except (TypeError,ValueError):
                print(f"{column_name}: FAIL (invalid value in summary.json)")
                summary_checks[column_name] = False
                continue
            summary_checks[column_name] = expected_value == reported_value
            print(
                f"{column_name}: "
                f"expected={expected_value} reported={reported_value} -> "
                f"{'PASS' if summary_checks[column_name] else 'FAIL'}"
            )

        # ---------- REVIEW SET RECONCILIATION ----------
        expected_review_df = (
            daily_gold_df
            .filter(col("total_hours") > 48)
            .select("date", "mmsi", "total_hours")
        )
        actual_review_df = review_df.select("date", "mmsi", "total_hours")

        missing_review_rows = (
            expected_review_df.exceptAll(actual_review_df).count()
        )
        unexpected_review_rows = (
            actual_review_df.exceptAll(expected_review_df).count()
        )
        review_content_pass = (
            missing_review_rows == 0 and unexpected_review_rows == 0
        )

        review_records_count = review_df.count()
        invalid_review_records = (
            review_df
            .filter(
                col("total_hours").isNull()
                | (col("total_hours") <= 48)
            )
            .count()
        )
        null_review_keys = (
            review_df
            .filter(col("date").isNull() | col("mmsi").isNull())
            .count()
        )
        review_duplicate_groups = (
            review_df
            .groupBy("date", "mmsi")
            .agg(spark_count("*").alias("key_count"))
            .filter(col("key_count") > 1)
            .count()
        )
        review_duplicate_pass = review_duplicate_groups == 0

        print("\n========== REVIEW PARQUET VALIDATION ==========")
        print(f"Review records: {review_records_count}")
        print(f"Missing review rows: {missing_review_rows}")
        print(f"Unexpected review rows: {unexpected_review_rows}")
        print(f"Invalid review records (<=48 or null): {invalid_review_records}")
        print(f"Null review keys: {null_review_keys}")
        print(f"Duplicate review key groups: {review_duplicate_groups}")

        review_checks = {
            "Review record count":
                review_records_count
                == int(expected_stats["review_records"]),
            "Review threshold": invalid_review_records == 0,
            "Review key completeness": null_review_keys == 0,
            "Review duplicate keys": review_duplicate_pass,
            "Review content": review_content_pass,
        }
        for name, result in review_checks.items():
            print(f"{name}: {'PASS' if result else 'FAIL'}")

        # ---------- OVERALL ----------
        all_checks = {
            "Daily Gold non-empty": daily_gold_non_empty,
            "Daily Gold schema": daily_gold_schema_pass,
            "Review Parquet schema": review_schema_pass,
            "Partition completeness": partition_complete_pass,
            **{f"Summary {k}": v for k, v in summary_checks.items()},
            **review_checks,
        }

        print("\n========== FINAL DQ VALIDATION ==========")
        for name, result in all_checks.items():
            print(f"{name}: {'PASS' if result else 'FAIL'}")

        overall_status = all(all_checks.values())
        print(
            "\nOVERALL STATUS:",
            "PASS" if overall_status else "FAIL",
        )

        if overall_status:
            print("\nDaily DQ validation PASSED.")
            exit_code = 0
        else:
            print(
                "\nDaily DQ validation FAILED. "
                "Do not continue the pipeline."
            )
            exit_code = 1
    finally:
        spark.stop()
        print("\nSpark Session stopped.")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
