from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, lpad, struct, when
from pyspark.sql.functions import month as spark_month
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import year as spark_year

from src.common.manifest import load_gold_changes
from src.common.spark_io import write_parquet_replace_partitions

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = (PROJECT_ROOT/"data"/"processed"/"silver")
GOLD_ROOT = (PROJECT_ROOT/"data"/"processed"/"gold")
MONTHLY_GOLD_OUTPUT = (GOLD_ROOT/"monthly_vessel_activity")
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("MaritimeFishingGoldMonthly")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark
def main():
    print("=" * 50)
    print("GOLD MONTHLY PIPELINE (incremental)")
    print("=" * 50)
    #check input,output
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Silver input: {SILVER_ROOT}")
    print(f"Gold Monthly output: {MONTHLY_GOLD_OUTPUT}")
    changes = load_gold_changes()
    affected_months = changes.get("affected_months") or []
    print(f"Gold changes run_id: {changes.get('run_id')}")
    print(f"Affected months: {affected_months}")
    if not affected_months:
        print("No affected months. Monthly gold incremental no-op.")
        return
    if not SILVER_ROOT.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_ROOT}"
        )
    print("\nSilver directory exists: PASS")
    spark = create_spark_session()
    try:
        print("Spark Session created: PASS")
        print(f"Spark version: {spark.version}")
        #read silve
        silver_df = spark.read.parquet(str(SILVER_ROOT))
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
        silver_df = silver_df.filter(month_filter)
        print("\nSilver data loaded: PASS")
        required_columns = [
            "date",
            "year",
            "month",
            "mmsi",
            "hours",
            "fishing_hours",
            "cell_ll_lat",
            "cell_ll_lon",
            "mmsi_status",
            "hours_status"
        ]
        missing_columns = [
            column
            for column in required_columns
            if column not in silver_df.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        print("Required columns check: PASS")
        print("\n========== SILVER SCHEMA ==========")
        silver_df.printSchema()
        print("\n========== SILVER SAMPLE ==========")
        silver_df.select(
            *required_columns
        ).show(5, truncate=False)
        #check date
        calendar_mismatch_df = (
            silver_df
            .filter(
                col("date").isNotNull()
                & (
                    (spark_year(col("date")) != col("year"))
                    | (spark_month(col("date")) != col("month"))
                )
            )
            .select(
                "date",
                "source_date",
                "year",
                "month",
                "date_mismatch"
            )
            .limit(10)
        )
        print("\n========== CALENDAR MISMATCH SAMPLE ==========")
        calendar_mismatch_df.show(
            10,
            truncate=False
        )
        #check flag
        flagged_date_df = (
            silver_df
            .filter(col("date_mismatch") == True)
            .select(
                "date",
                "source_date",
                "year",
                "month",
                "date_mismatch"
            )
            .limit(10)
        )
        print("\n========== SILVER DATE MISMATCH FLAG ==========")
        flagged_date_df.show(10,truncate=False)
        #check null
        null_check_df = silver_df.agg(
            count(
                when(col("date").isNull(), True)
            ).alias("null_date"),

            count(
                when(col("year").isNull(), True)
            ).alias("null_year"),

            count(
                when(col("month").isNull(), True)
            ).alias("null_month"),

            count(
                when(col("mmsi").isNull(), True)
            ).alias("null_mmsi")
        )
        print("\n========== NULL DIMENSION CHECK ==========")
        null_check_df.show(truncate=False)
        
        monthly_source_df = silver_df.select(
            "date",
            "year",
            "month",
            "mmsi",
            "hours",
            "fishing_hours",
            "cell_ll_lat",
            "cell_ll_lon",
            "mmsi_status",
            "hours_status"
        )

        print("\n========== MONTHLY SOURCE DATA ==========")

        print(
            f"Selected columns: {monthly_source_df.columns}"
        )

        monthly_source_df.printSchema()

        monthly_source_df.show(
            5,
            truncate=False
        )
        
        # Aggregate Silver data into Monthly Gold

        monthly_gold_df = (
            monthly_source_df
            .groupBy(
                "year",
                "month",
                "mmsi"
            )
            .agg(
                # Number of active days
                countDistinct("date").alias("active_days"),

                # Total hours
                spark_sum("hours").alias("total_hours"),

                # Total fishing hours
                spark_sum("fishing_hours").alias(
                    "total_fishing_hours"
                ),

                # Unique grid cells per month
                countDistinct(
                    when(
                        col("cell_ll_lat").isNotNull()
                        & col("cell_ll_lon").isNotNull(),
                        struct(
                            col("cell_ll_lat"),
                            col("cell_ll_lon")
                        )
                    )
                ).alias("active_grid_cells"),

                # Total observations
                count("*").alias("observation_count"),

                # Observations with missing coordinates
                count(
                    when(
                        col("cell_ll_lat").isNull()
                        | col("cell_ll_lon").isNull(),
                        True
                    )
                ).alias("missing_coordinate_observations"),

                # MMSI observations requiring review
                count(
                    when(
                        col("mmsi_status") == "REVIEW",
                        True
                    )
                ).alias("mmsi_review_observations"),

                # Hours observations requiring review
                count(
                    when(
                        col("hours_status") == "REVIEW_THRESHOLD",
                        True
                    )
                ).alias("hours_threshold_observations")
            )
        )


        print("\n========== WRITE MONTHLY GOLD ==========")

        monthly_gold_df = monthly_gold_df.withColumn(
            "month",
            lpad(col("month").cast("string"), 2, "0"),
        )
        replaced = write_parquet_replace_partitions(
            monthly_gold_df,
            MONTHLY_GOLD_OUTPUT,
            partition_by=("year", "month"),
        )
        print(f"Monthly Gold write: PASS ({len(replaced)} partitions)")
        if not MONTHLY_GOLD_OUTPUT.exists():
            raise RuntimeError(
                "Monthly Gold output directory was not created."
            )
        print(f"Output directory: {MONTHLY_GOLD_OUTPUT}")

    finally:
        spark.stop()
        print("Spark Session stopped.")
if __name__ == "__main__":
    main()
