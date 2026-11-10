import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lpad, when

from src.common.manifest import load_gold_changes
from src.common.spark_io import write_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAILY_GOLD_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "daily_vessel_activity"
)
DQ_REPORT_ROOT = (
    PROJECT_ROOT
    / "reports"
    / "daily_vessel_hours_dq"
)
REVIEW_OUTPUT = (
    DQ_REPORT_ROOT
    / "review_records"
)
SUMMARY_OUTPUT = (
    DQ_REPORT_ROOT
    / "summary.json"
)
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("DailyVesselHoursDQ")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark

def main():
    print("=" * 50)
    print("DAILY VESSEL HOURS DQ REPORT")
    print("=" * 50)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Daily Gold input: {DAILY_GOLD_ROOT}")
    print(f"DQ Report output: {DQ_REPORT_ROOT}")
    changes = load_gold_changes()
    affected_months = changes.get("affected_months") or []
    print(f"Affected months: {affected_months}")
    if not affected_months:
        print("No affected months. Daily hours DQ incremental no-op.")
        return
    if not DAILY_GOLD_ROOT.exists():
        raise FileNotFoundError(
            f"Daily Gold not found: {DAILY_GOLD_ROOT}"
        )
    print("\nDaily Gold directory exists: PASS")
    spark = create_spark_session()
    try:
        print("Spark Session created: PASS")
        print(f"Spark version: {spark.version}")
        daily_gold_df = spark.read.parquet(
            str(DAILY_GOLD_ROOT)
        )
        month_filter = None
        for item in affected_months:
            year_i = int(item["year"])
            month_s = str(item["month"]).zfill(2)
            condition = (col("year") == year_i) & (
                lpad(col("month").cast("string"), 2, "0") == month_s
            )
            month_filter = (
                condition if month_filter is None else (month_filter | condition)
            )
        daily_gold_df = daily_gold_df.filter(month_filter)
        print("\nDaily Gold loaded: PASS")

        required_columns = [
            "date",
            "year",
            "month",
            "mmsi",
            "total_hours",
            "total_fishing_hours",
            "active_grid_cells",
            "observation_count"
        ]
        missing_columns = [
            column
            for column in required_columns
            if column not in daily_gold_df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )
        print("Required columns check: PASS")
        print("\n========== GOLD DAILY SCHEMA ==========")
        daily_gold_df.printSchema()
        print("\n========== GOLD DAILY SAMPLE ==========")
        daily_gold_df.select(
            *required_columns
        ).show(
            5,
            truncate=False
        )

        # classify daily hours:
        # REVIEW_NULL / REVIEW_NEGATIVE / NORMAL / INFO / REVIEW
        daily_hours_dq_df = (
            daily_gold_df
            .withColumn(
                "daily_hours_dq_status",
                when(
                    col("total_hours").isNull(),
                    "REVIEW_NULL"
                )
                .when(
                    col("total_hours") < 0,
                    "REVIEW_NEGATIVE"
                )
                .when(
                    col("total_hours") <= 24,
                    "NORMAL"
                )
                .when(
                    col("total_hours") <= 48,
                    "INFO"
                )
                .otherwise("REVIEW")
            )
            # >48h
            .withColumn(
                "excess_hours_over_48",
                when(
                    col("total_hours") > 48,
                    col("total_hours") - lit(48.0)
                ).otherwise(lit(0.0))
            )
            # hour to second
            .withColumn(
                "excess_seconds_over_48",

                col("excess_hours_over_48") * lit(3600.0)
            )
            #create dq report
            .withColumn(
                "daily_hours_dq_reason",
                when(
                    col("total_hours").isNull(),
                    "Total hours is NULL"
                )
                .when(
                    col("total_hours") < 0,
                    "Total hours is negative"
                )
                .when(
                    col("total_hours") <= 24,
                    "Within 24-hour reference"
                )
                .when(
                    col("total_hours") <= 48,
                    "Above 24 hours but within 48 hours"
                )
                .otherwise(
                    "Daily vessel total exceeds 48 hours"
                )
            )
        )
        print("\n========== DAILY HOURS DQ SAMPLE ==========")
        daily_hours_dq_df.select(
            "date",
            "mmsi",
            "total_hours",
            "daily_hours_dq_status",
            "excess_hours_over_48",
            "excess_seconds_over_48",
            "daily_hours_dq_reason"
        ).show(10,truncate=False)

        daily_hours_dq_summary = (
            daily_hours_dq_df
            .groupBy("daily_hours_dq_status")
            .count()
            .orderBy("daily_hours_dq_status")
        )
        print("\n========== DAILY HOURS DQ SUMMARY ==========")
        daily_hours_dq_summary.show(truncate=False)
        # REVIEW RECORDS
        daily_hours_review_df = (
            daily_hours_dq_df
            .filter(
                col("daily_hours_dq_status") == "REVIEW")
            .select(
                "date",
                "year",
                "month",
                "mmsi",
                "total_hours",
                "total_fishing_hours",
                "active_grid_cells",
                "observation_count",
                "excess_hours_over_48",
                "excess_seconds_over_48",
                "daily_hours_dq_reason"
            )
            .orderBy(col("total_hours").desc()
            )
        )
        print("\n========== DAILY HOURS REVIEW RECORDS ==========")
        daily_hours_review_df.show(20,truncate=False)
        print("\n========== WRITE REVIEW RECORDS ==========")
        DQ_REPORT_ROOT.mkdir(
            parents=True,
            exist_ok=True
        )
        write_parquet(daily_hours_review_df, REVIEW_OUTPUT)
        print("Review records write: PASS")
        print(f"Review output: {REVIEW_OUTPUT}")
        print("\n========== CREATE DQ SUMMARY ==========")
        status_rows = daily_hours_dq_summary.collect()
        status_counts = {
            row["daily_hours_dq_status"]: int(row["count"])
            for row in status_rows
        }
        total_records = sum(status_counts.values())
        dq_report = {
            "report_name": "Daily Vessel Hours DQ Report",
            "generated_at_utc": datetime.now(
                UTC
            ).isoformat(),
            "source_layer": "gold_daily",
            "rules": {
                "null": "total_hours IS NULL",
                "negative": "total_hours < 0",
                "normal": "0 <= total_hours <= 24",
                "info": "24 < total_hours <= 48",
                "review": "total_hours > 48"
            },
            "summary": {
                "total_records": total_records,
                "normal_records": status_counts.get(
                    "NORMAL", 0
                ),
                "info_records": status_counts.get(
                    "INFO", 0
                ),
                "review_records": status_counts.get(
                    "REVIEW", 0
                ),
                "null_hours_records": status_counts.get(
                    "REVIEW_NULL", 0
                ),
                "negative_hours_records": status_counts.get(
                    "REVIEW_NEGATIVE", 0
                ),
            }
        }
        #JSON report
        with SUMMARY_OUTPUT.open(
            "w",
            encoding="utf-8"
        ) as file:
            json.dump(
                dq_report,
                file,
                indent=4,
                ensure_ascii=False
            )
        print("DQ Summary write: PASS")
        print(f"Summary output: {SUMMARY_OUTPUT}")
        print("\n========== DQ REPORT SUMMARY ==========")
        print(
            json.dumps(
                dq_report["summary"],
                indent=4
            )
        )
    finally:
        spark.stop()
        print("\nSpark Session stopped.")
if __name__ == "__main__":
    main()
