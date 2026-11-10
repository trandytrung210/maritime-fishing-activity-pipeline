from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    percent_rank,
    when,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

from src.common.manifest import load_gold_changes
from src.common.spark_io import write_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "processed" / "gold"
AREA_GOLD_OUTPUT = GOLD_ROOT / "area_fishing_activity"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("GoldAreaFishingActivity")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark


def build_area_gold(silver_df):
    """Aggregate silver AIS cells into area-level fishing activity metrics."""
    area_source_df = (
        silver_df
        .select(
            "date",
            "year",
            "mmsi",
            "cell_ll_lat",
            "cell_ll_lon",
            "hours",
            "fishing_hours",
        )
        .filter(
            col("cell_ll_lat").isNotNull()
            & col("cell_ll_lon").isNotNull()
        )
    )

    area_gold_df = (
        area_source_df
        .groupBy("cell_ll_lat", "cell_ll_lon")
        .agg(
            countDistinct("mmsi").alias("unique_vessels"),
            countDistinct(
                when(col("fishing_hours") > 0, col("mmsi"))
            ).alias("unique_fishing_vessels"),
            spark_sum("hours").alias("total_hours"),
            spark_sum("fishing_hours").alias("total_fishing_hours"),
            countDistinct("date").alias("active_days"),
            count("*").alias("observation_count"),
        )
        .withColumn(
            "fishing_intensity",
            when(
                col("total_hours") > 0,
                col("total_fishing_hours") / col("total_hours"),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "fishing_hours_per_vessel",
            when(
                col("unique_vessels") > 0,
                col("total_fishing_hours") / col("unique_vessels"),
            ).otherwise(lit(0.0)),
        )
    )
    return area_gold_df


def show_analysis(area_gold_df, top_n=10):
    """Print rankings that answer the key area questions."""
    ranked = (
        area_gold_df
        .withColumn(
            "total_hours",
            spark_round(col("total_hours"), 2),
        )
        .withColumn(
            "total_fishing_hours",
            spark_round(col("total_fishing_hours"), 2),
        )
        .withColumn(
            "fishing_intensity",
            spark_round(col("fishing_intensity"), 4),
        )
        .withColumn(
            "fishing_hours_per_vessel",
            spark_round(col("fishing_hours_per_vessel"), 2),
        )
    )

    vessel_cols = [
        "cell_ll_lat",
        "cell_ll_lon",
        "unique_vessels",
        "unique_fishing_vessels",
        "total_fishing_hours",
        "fishing_intensity",
    ]
    fishing_cols = [
        "cell_ll_lat",
        "cell_ll_lon",
        "total_fishing_hours",
        "unique_vessels",
        "unique_fishing_vessels",
        "total_hours",
        "fishing_intensity",
        "fishing_hours_per_vessel",
    ]
    low_fishing_cols = [
        "cell_ll_lat",
        "cell_ll_lon",
        "unique_vessels",
        "total_hours",
        "total_fishing_hours",
        "fishing_intensity",
        "fishing_hours_per_vessel",
    ]

    print("\n" + "=" * 70)
    print(f"1) TOP {top_n} khu vuc co NHIEU tau xuat hien nhat (unique_vessels)")
    print("=" * 70)
    (
        ranked
        .orderBy(
            col("unique_vessels").desc(),
            col("total_fishing_hours").desc(),
        )
        .select(*vessel_cols)
        .show(top_n, truncate=False)
    )

    print("\n" + "=" * 70)
    print(f"2) TOP {top_n} khu vuc co IT tau xuat hien nhat (unique_vessels)")
    print("=" * 70)
    (
        ranked
        .orderBy(
            col("unique_vessels").asc(),
            col("total_fishing_hours").asc(),
        )
        .select(*vessel_cols)
        .show(top_n, truncate=False)
    )

    print("\n" + "=" * 70)
    print(f"3) TOP {top_n} khu vuc co TONG thoi gian danh ca nhieu nhat")
    print("=" * 70)
    (
        ranked
        .orderBy(
            col("total_fishing_hours").desc(),
            col("unique_vessels").desc(),
        )
        .select(*fishing_cols)
        .show(top_n, truncate=False)
    )

    # High presence, low fishing:
    # - vessel count in top quartile
    # - fishing intensity in bottom quartile
    vessel_window = Window.orderBy(col("unique_vessels").asc())
    intensity_window = Window.orderBy(col("fishing_intensity").asc())

    scored = (
        ranked
        .withColumn("vessel_pct", percent_rank().over(vessel_window))
        .withColumn("intensity_pct", percent_rank().over(intensity_window))
    )

    high_presence_low_fishing = (
        scored
        .filter(
            (col("vessel_pct") >= 0.75)
            & (col("intensity_pct") <= 0.25)
            & (col("unique_vessels") >= 5)
        )
        .orderBy(
            col("unique_vessels").desc(),
            col("fishing_intensity").asc(),
        )
    )

    print("\n" + "=" * 70)
    print(
        f"4) TOP {top_n} khu vuc NHIEU tau xuat hien nhung hoat dong danh ca THAP"
    )
    print("   (unique_vessels top 25% + fishing_intensity bottom 25%)")
    print("=" * 70)
    (
        high_presence_low_fishing
        .select(*low_fishing_cols)
        .show(top_n, truncate=False)
    )

    return high_presence_low_fishing


def main():
    print("=" * 50)
    print("GOLD AREA FISHING ACTIVITY")
    print("=" * 50)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Silver input: {SILVER_ROOT}")
    print(f"Gold Area output: {AREA_GOLD_OUTPUT}")

    changes = load_gold_changes()
    print(f"Gold changes run_id: {changes.get('run_id')}")
    print(f"has_silver_changes: {changes.get('has_silver_changes')}")
    if not changes.get("has_silver_changes"):
        print(
            "No silver changes detected. Area gold full refresh skipped."
        )
        return

    if not SILVER_ROOT.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_ROOT}"
        )
    print("\nSilver directory exists: PASS")

    spark = create_spark_session()
    area_gold_df = None
    try:
        print("Spark Session created: PASS")
        print(f"Spark version: {spark.version}")

        silver_df = spark.read.parquet(str(SILVER_ROOT))
        print("\nSilver loaded: PASS")

        required_columns = [
            "date",
            "year",
            "month",
            "mmsi",
            "cell_ll_lat",
            "cell_ll_lon",
            "hours",
            "fishing_hours",
        ]
        missing_columns = [
            column
            for column in required_columns
            if column not in silver_df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )
        print("Required columns check: PASS")

        area_gold_df = build_area_gold(silver_df).cache()

        print("\n========== AREA GOLD SCHEMA ==========")
        area_gold_df.printSchema()

        print("\n========== AREA GOLD SAMPLE ==========")
        area_gold_df.show(10, truncate=False)

        area_count = area_gold_df.count()
        print(f"\nArea (grid cell) count: {area_count:,}")

        print("\n========== WRITE AREA GOLD ==========")
        write_parquet(area_gold_df, AREA_GOLD_OUTPUT)
        print("Area Gold write: PASS")
        print(f"Output directory: {AREA_GOLD_OUTPUT}")

        show_analysis(area_gold_df, top_n=10)

    finally:
        if area_gold_df is not None:
            area_gold_df.unpersist()
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
