from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    first,
    lit,
    lpad,
    month,
    struct,
    sum,
    when,
    year,
)

from src.common.manifest import load_gold_changes
from src.common.spark_io import write_parquet_replace_partitions

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "processed" / "gold"
GOLD_OUTPUT = GOLD_ROOT / "daily_vessel_activity"


def main() -> None:
    print("=" * 50)
    print("GOLD DAILY PIPELINE (incremental)")
    print("=" * 50)

    changes = load_gold_changes()
    affected_months = changes.get("affected_months") or []
    print(f"Gold changes run_id: {changes.get('run_id')}")
    print(f"Affected months: {affected_months}")

    if not affected_months:
        print("No affected months. Daily gold incremental no-op.")
        return

    if not SILVER_ROOT.exists():
        raise FileNotFoundError(f"Silver directory not found: {SILVER_ROOT}")

    spark = (
        SparkSession.builder
        .appName("MaritimeFishingGold")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    try:
        df_silver = spark.read.parquet(str(SILVER_ROOT))
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
        df_silver = df_silver.filter(month_filter)
        df_silver = df_silver.select(
            "date",
            "mmsi",
            "mmsi_status",
            "cell_ll_lat",
            "cell_ll_lon",
            "hours",
            "fishing_hours",
            "hours_status",
            "dq_status",
        )

        observation_count = count(lit(1))
        nonnull_status_count = count(col("mmsi_status"))
        distinct_status_count = countDistinct(col("mmsi_status"))
        mmsi_status_consistent = (
            (distinct_status_count == lit(1))
            & (nonnull_status_count == observation_count)
        )
        valid_coordinates = (
            col("cell_ll_lat").isNotNull() & col("cell_ll_lon").isNotNull()
        )
        df_gold = (
            df_silver.groupBy("date", "mmsi").agg(
                when(mmsi_status_consistent, first("mmsi_status"))
                .otherwise(lit("REVIEW"))
                .alias("mmsi_status"),
                distinct_status_count.alias("mmsi_status_distinct_values"),
                when(distinct_status_count > 1, lit(True))
                .otherwise(lit(False))
                .alias("mmsi_status_conflict"),
                sum(
                    when(col("mmsi_status").isNull(), 1).otherwise(0)
                ).alias("mmsi_status_missing_observations"),
                sum("hours").alias("total_hours"),
                sum("fishing_hours").alias("total_fishing_hours"),
                countDistinct(
                    when(
                        valid_coordinates,
                        struct(col("cell_ll_lat"), col("cell_ll_lon")),
                    )
                ).alias("active_grid_cells"),
                observation_count.alias("observation_count"),
                sum(
                    when(~valid_coordinates, 1).otherwise(0)
                ).alias("missing_coordinate_observations"),
                sum(
                    when(col("mmsi_status") == "REVIEW", 1).otherwise(0)
                ).alias("mmsi_review_observations"),
                sum(
                    when(
                        col("hours_status") == "REVIEW_THRESHOLD", 1
                    ).otherwise(0)
                ).alias("hours_threshold_observations"),
            )
        )
        df_gold = (
            df_gold.withColumn("year", year(col("date"))).withColumn(
                "month",
                lpad(month(col("date")).cast("string"), 2, "0"),
            )
        )
        replaced = write_parquet_replace_partitions(
            df_gold,
            GOLD_OUTPUT,
            partition_by=("year", "month"),
        )
        print(f"Replaced partitions: {len(replaced)}")
        print(f"Gold records (affected scope): {df_gold.count():,}")
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
