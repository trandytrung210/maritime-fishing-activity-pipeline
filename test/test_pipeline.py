from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
)
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import sum as spark_sum

# Project path
PROJECT_ROOT = Path(
    r"E:\Marttime_Fishing_Activitiy_Project"
)

# Bronze file
bronze_file = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2020-11-19.csv"
)
silver_root = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)

print("========== BRONZE FILE CHECK ==========")

print("File path:", bronze_file)
print("File exists:", bronze_file.exists())

if not bronze_file.is_file():
    raise FileNotFoundError(
        f"Bronze file not found: {bronze_file}"
    )


# Create Spark Session
spark = (
    SparkSession.builder
    .appName("BronzeSilverInvestigation")
    .master("local[2]")
    .getOrCreate()
)

try:

    # Read Bronze CSV
    bronze_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(bronze_file))
    )

    # 1. Print Bronze schema
    print("\n========== BRONZE SCHEMA ==========")

    bronze_df.printSchema()

    # 2. Display first 5 rows
    print("\n========== BRONZE SAMPLE ==========")

    bronze_df.show(
        5,
        truncate=False
    )
    silver_root = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)

    silver_df = (
        spark.read
        .parquet(str(silver_root))
    )

    print("\n========== SILVER SCHEMA ==========")
    silver_df.printSchema()
    # 3. Filter target MMSI
    if "mmsi" not in bronze_df.columns:
        raise ValueError(
            "Column mmsi does not exist in Bronze"
        )

    target_date = "2020-11-19"
    target_mmsi = "416204220"

    # Filter Bronze
    bronze_target = (
        bronze_df
        .filter(
            (col("date").cast("date") == lit(target_date).cast("date"))
            & (trim(col("mmsi").cast("string")) == target_mmsi)
        )
    )

    # Filter Silver
    silver_target = (
        silver_df
        .filter(
            (col("date").cast("date") == lit(target_date).cast("date"))
            & (trim(col("mmsi").cast("string")) == target_mmsi)
        )
    )
    print("\n========== TARGET MMSI ==========")

    print("Target MMSI: 416204220")

    print(
        "Observation count:",
        bronze_target.count()
    )

    bronze_target.show(
        100,
        truncate=False
    )

    target_date = "2020-11-19"
    target_mmsi = "416204220"

    # Filter Bronze
    bronze_target = (
        bronze_df
        .filter(
            (col("date").cast("date") == lit(target_date).cast("date"))
            & (trim(col("mmsi").cast("string")) == target_mmsi)
        )
    )

    # Filter Silver
    silver_target = (
        silver_df
        .filter(
            (col("date").cast("date") == lit(target_date).cast("date"))
            & (trim(col("mmsi").cast("string")) == target_mmsi)
        )
    )

    # Calculate Bronze statistics

    bronze_stats = (
        bronze_target
        .agg(
            spark_count("*").alias("records"),
            spark_sum("hours").alias("total_hours"),
            spark_sum("fishing_hours").alias("total_fishing_hours")
        )
        .first()
    )

    # Calculate Silver statistics

    silver_stats = (
        silver_target
        .agg(
            spark_count("*").alias("records"),
            spark_sum("hours").alias("total_hours"),
            spark_sum("fishing_hours").alias("total_fishing_hours")
        )
        .first()
    )

    print("\n========== BRONZE VS SILVER ==========")

    print(f"Bronze records: {bronze_stats['records']}")
    print(f"Silver records: {silver_stats['records']}")

    print(f"Bronze hours: {bronze_stats['total_hours']}")
    print(f"Silver hours: {silver_stats['total_hours']}")

    print(
        f"Bronze fishing hours: "
        f"{bronze_stats['total_fishing_hours']}"
    )

    print(
        f"Silver fishing hours: "
        f"{silver_stats['total_fishing_hours']}"
    )

finally:
    spark.stop()
