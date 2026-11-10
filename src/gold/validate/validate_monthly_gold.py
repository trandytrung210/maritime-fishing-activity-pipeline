from math import isclose
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    greatest,
    lit,
    struct,
    when,
)
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import month as spark_month
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import year as spark_year

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
MONTHLY_GOLD_ROOT = (
    PROJECT_ROOT / "data" / "processed" / "gold" / "monthly_vessel_activity"
)

# Grain of monthly gold: one row per (year, month, mmsi)
PRIMARY_KEY = ("year", "month", "mmsi")

# Partition month may be int (legacy) or zero-padded string (month=01).
EXPECTED_SCHEMA = {
    "mmsi": "string",
    "active_days": "bigint",
    "total_hours": "double",
    "total_fishing_hours": "double",
    "active_grid_cells": "bigint",
    "observation_count": "bigint",
    "missing_coordinate_observations": "bigint",
    "mmsi_review_observations": "bigint",
    "hours_threshold_observations": "bigint",
    "year": "int",
    "month": {"int", "string"},
}

SILVER_EXPECTED_SCHEMA = {
    "date": "date",
    "year": {"int", "string"},
    "month": {"int", "string"},
    "mmsi": "string",
    "cell_ll_lat": "double",
    "cell_ll_lon": "double",
    "hours": "double",
    "fishing_hours": "double",
    "mmsi_status": "string",
    "hours_status": "string",
}


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ValidateMaritimeFishingGoldMonthly")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark


def normalize_month(df):
    """Cast month to int so silver/gold joins use the same grain key."""
    return df.withColumn("month", col("month").cast("int"))


def normalize_year(df):
    return df.withColumn("year", col("year").cast("int"))


def type_matches(column_name, actual_type, expected):
    if isinstance(expected, set):
        return actual_type in expected
    return actual_type == expected


def validate_required_schema(df, expected_schema, dataset_name, allow_extra=True):
    print(f"\n========== {dataset_name} SCHEMA / DATA TYPE VALIDATION ==========")
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
    unexpected_columns = []
    if not allow_extra:
        unexpected_columns = [
            column
            for column in actual_schema
            if column not in expected_schema
        ]

    type_mismatches = {
        column: {
            "expected": sorted(expected)
            if isinstance(expected, set)
            else expected,
            "actual": actual_schema[column],
        }
        for column, expected in expected_schema.items()
        if column in actual_schema
        and not type_matches(column, actual_schema[column], expected)
    }

    columns_pass = not missing_columns
    types_pass = not type_mismatches
    extra_pass = not unexpected_columns
    schema_pass = columns_pass and types_pass and extra_pass

    print(
        "Required columns:",
        "PASS" if columns_pass else f"FAIL missing={missing_columns}",
    )
    if not allow_extra:
        print(
            "No unexpected columns:",
            "PASS" if extra_pass else f"FAIL unexpected={unexpected_columns}",
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


def close_enough(left, right):
    return isclose(
        float(left),
        float(right),
        rel_tol=1e-9,
        abs_tol=1e-6,
    )


def float_mismatch(expected_col, actual_col):
    return (
        (col(expected_col).isNull() != col(actual_col).isNull())
        | (
            col(expected_col).isNotNull()
            & col(actual_col).isNotNull()
            & (
                spark_abs(col(expected_col) - col(actual_col))
                > greatest(
                    lit(1e-6),
                    lit(1e-9)
                    * greatest(
                        spark_abs(col(expected_col)),
                        spark_abs(col(actual_col)),
                    ),
                )
            )
        )
    )


def main():
    print("=" * 50)
    print("GOLD MONTHLY VALIDATION")
    print("=" * 50)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Silver input: {SILVER_ROOT}")
    print(f"Gold Monthly input: {MONTHLY_GOLD_ROOT}")

    if not MONTHLY_GOLD_ROOT.exists():
        raise FileNotFoundError(
            f"Gold Monthly directory not found: {MONTHLY_GOLD_ROOT}"
        )
    if not SILVER_ROOT.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_ROOT}"
        )

    spark = create_spark_session()
    exit_code = 1
    try:
        monthly_gold_raw_df = spark.read.parquet(str(MONTHLY_GOLD_ROOT))
        silver_raw_df = spark.read.parquet(str(SILVER_ROOT))

        # Gold must match exact monthly contract (no unexpected columns).
        schema_pass = validate_required_schema(
            monthly_gold_raw_df,
            EXPECTED_SCHEMA,
            "GOLD MONTHLY",
            allow_extra=False,
        )
        # Silver may contain DQ helper columns beyond the required set.
        silver_schema_pass = validate_required_schema(
            silver_raw_df,
            SILVER_EXPECTED_SCHEMA,
            "SILVER",
            allow_extra=True,
        )
        if not schema_pass or not silver_schema_pass:
            print("\nSchema validation FAILED.")
            print("Stopping remaining validation checks.")
            raise SystemExit(1)

        monthly_gold_df = normalize_year(normalize_month(monthly_gold_raw_df))
        silver_df = normalize_year(normalize_month(silver_raw_df))

        # ---------- Calendar partition sanity ----------
        invalid_gold_months = (
            monthly_gold_df
            .filter(col("month").isNull() | ~col("month").between(1, 12))
            .count()
        )
        invalid_silver_months = (
            silver_df
            .filter(col("month").isNull() | ~col("month").between(1, 12))
            .count()
        )
        gold_month_pass = invalid_gold_months == 0
        silver_month_pass = invalid_silver_months == 0

        print("\n========== MONTH RANGE CHECK ==========")
        print(f"Invalid gold months: {invalid_gold_months}")
        print(f"Invalid silver months: {invalid_silver_months}")
        print("Gold month validation:", "PASS" if gold_month_pass else "FAIL")
        print(
            "Silver month validation:",
            "PASS" if silver_month_pass else "FAIL",
        )

        gold_monthly_count = monthly_gold_df.count()
        print(f"\nGold Monthly records: {gold_monthly_count:,}")
        print("\n========== GOLD MONTHLY SAMPLE ==========")
        monthly_gold_df.select(*EXPECTED_SCHEMA.keys()).show(
            10,
            truncate=False,
        )
        # ---------- Calendar consistency ----------
        calendar_mismatch_count = (
            silver_df
            .filter(
                col("date").isNotNull()
                & (
                    (spark_year(col("date")) != col("year"))
                    | (spark_month(col("date")) != col("month"))
                )
            )
            .count()
        )

        calendar_pass = calendar_mismatch_count == 0

        print("\n========== CALENDAR CONSISTENCY CHECK ==========")
        print("Calendar mismatch rows:", calendar_mismatch_count)
        print(
            "Calendar consistency:",
            "PASS" if calendar_pass else "FAIL"
        )

        # ---------- Grain uniqueness (GOLD only) ----------
        # Silver is observation-level: many rows share (year, month, mmsi).
        # Do NOT treat monthly PRIMARY_KEY as unique on silver.
        null_primary_key_count = (
            monthly_gold_df
            .filter(
                col("year").isNull()
                | col("month").isNull()
                | col("mmsi").isNull()
            )
            .count()
        )
        null_primary_key_pass = null_primary_key_count == 0

        silver_null_group_keys = (
            silver_df
            .filter(
                col("year").isNull()
                | col("month").isNull()
                | col("mmsi").isNull()
            )
            .count()
        )
        silver_null_date_count = (
            silver_df
            .filter(col("date").isNull())
            .count()
        )

        silver_date_pass = silver_null_date_count == 0

        print(
            "Null silver dates:",
            silver_null_date_count
        )

        print(
            "Silver date validation:",
            "PASS" if silver_date_pass else "FAIL"
        )
        silver_group_keys_pass = silver_null_group_keys == 0

        duplicate_stats = (
            monthly_gold_df
            .groupBy(*PRIMARY_KEY)
            .agg(spark_count("*").alias("key_count"))
            .filter(col("key_count") > 1)
            .agg(
                spark_count("*").alias("duplicate_groups"),
                coalesce(spark_sum("key_count"), lit(0)).alias(
                    "duplicate_member_rows"
                ),
                (
                    coalesce(spark_sum("key_count"), lit(0))
                    - spark_count("*")
                ).alias("excess_duplicate_rows"),
            )
            .first()
        )
        duplicate_pass = duplicate_stats["duplicate_groups"] == 0

        print("\n========== GRAIN UNIQUENESS CHECK ==========")
        print(f"Gold primary key: {PRIMARY_KEY}")
        print(f"Null gold primary keys: {null_primary_key_count}")
        print(
            "Null gold primary keys:",
            "PASS" if null_primary_key_pass else "FAIL",
        )
        print(f"Null silver group keys: {silver_null_group_keys}")
        print(
            "Null silver group keys:",
            "PASS" if silver_group_keys_pass else "FAIL",
        )
        print(f"Duplicate gold groups: {duplicate_stats['duplicate_groups']}")
        print(
            f"Duplicate gold member rows: "
            f"{duplicate_stats['duplicate_member_rows']}"
        )
        print(
            f"Excess duplicate gold rows: "
            f"{duplicate_stats['excess_duplicate_rows']}"
        )
        print(
            "Duplicate gold primary keys:",
            "PASS" if duplicate_pass else "FAIL",
        )

        # ---------- Aggregate reconciliation ----------
        silver_aggregate = (
            silver_df
            .agg(
                spark_count(lit(1)).alias("silver_records"),
                coalesce(spark_sum("hours"), lit(0.0)).alias(
                    "silver_total_hours"
                ),
                coalesce(spark_sum("fishing_hours"), lit(0.0)).alias(
                    "silver_total_fishing_hours"
                ),
                coalesce(
                    spark_sum(
                        when(col("mmsi_status") == "REVIEW", 1).otherwise(0)
                    ),
                    lit(0),
                ).alias("silver_mmsi_review"),
                coalesce(
                    spark_sum(
                        when(
                            col("hours_status") == "REVIEW_THRESHOLD",
                            1,
                        ).otherwise(0)
                    ),
                    lit(0),
                ).alias("silver_hours_threshold"),
                coalesce(
                    spark_sum(
                        when(
                            col("cell_ll_lat").isNull()
                            | col("cell_ll_lon").isNull(),
                            1,
                        ).otherwise(0)
                    ),
                    lit(0),
                ).alias("silver_missing_coordinates"),
            )
            .first()
        )

        gold_aggregate = (
            monthly_gold_df
            .agg(
                spark_count(lit(1)).alias("gold_records"),
                coalesce(spark_sum("observation_count"), lit(0)).alias(
                    "gold_observation_count"
                ),
                coalesce(spark_sum("total_hours"), lit(0.0)).alias(
                    "gold_total_hours"
                ),
                coalesce(spark_sum("total_fishing_hours"), lit(0.0)).alias(
                    "gold_total_fishing_hours"
                ),
                coalesce(spark_sum("mmsi_review_observations"), lit(0)).alias(
                    "gold_mmsi_review"
                ),
                coalesce(
                    spark_sum("hours_threshold_observations"),
                    lit(0),
                ).alias("gold_hours_threshold"),
                coalesce(
                    spark_sum("missing_coordinate_observations"),
                    lit(0),
                ).alias("gold_missing_coordinates"),
            )
            .first()
        )

        observation_match = (
            silver_aggregate["silver_records"]
            == gold_aggregate["gold_observation_count"]
        )
        total_hours_match = close_enough(
            silver_aggregate["silver_total_hours"],
            gold_aggregate["gold_total_hours"],
        )
        total_fishing_hours_match = close_enough(
            silver_aggregate["silver_total_fishing_hours"],
            gold_aggregate["gold_total_fishing_hours"],
        )
        mmsi_review_match = (
            silver_aggregate["silver_mmsi_review"]
            == gold_aggregate["gold_mmsi_review"]
        )
        hours_threshold_match = (
            silver_aggregate["silver_hours_threshold"]
            == gold_aggregate["gold_hours_threshold"]
        )
        missing_coordinates_match = (
            silver_aggregate["silver_missing_coordinates"]
            == gold_aggregate["gold_missing_coordinates"]
        )

        print("\n========== RECORD COUNTS ==========")
        print(f"Silver observation rows: {silver_aggregate['silver_records']}")
        print(f"Gold monthly rows: {gold_aggregate['gold_records']}")
        print(
            "Note: gold rows << silver rows is expected "
            "(silver is observation-level; gold is monthly grain)."
        )

        print("\n========== OBSERVATION RECONCILIATION ==========")
        print(f"Silver records: {silver_aggregate['silver_records']}")
        print(
            f"Gold sum(observation_count): "
            f"{gold_aggregate['gold_observation_count']}"
        )
        print(
            "Silver = Gold observations:",
            "PASS" if observation_match else "FAIL",
        )

        print("\n========== AGGREGATE RECONCILIATION ==========")
        print(f"Silver total hours: {silver_aggregate['silver_total_hours']}")
        print(f"Gold total hours: {gold_aggregate['gold_total_hours']}")
        print("Total hours match:", "PASS" if total_hours_match else "FAIL")
        print(
            "Silver total fishing hours: "
            f"{silver_aggregate['silver_total_fishing_hours']}"
        )
        print(
            "Gold total fishing hours: "
            f"{gold_aggregate['gold_total_fishing_hours']}"
        )
        print(
            "Total fishing hours match:",
            "PASS" if total_fishing_hours_match else "FAIL",
        )

        print("\n========== QUALITY METRICS RECONCILIATION ==========")
        print(f"Silver MMSI review: {silver_aggregate['silver_mmsi_review']}")
        print(f"Gold MMSI review: {gold_aggregate['gold_mmsi_review']}")
        print(
            "MMSI review observations:",
            "PASS" if mmsi_review_match else "FAIL",
        )
        print(
            "Silver hours threshold: "
            f"{silver_aggregate['silver_hours_threshold']}"
        )
        print(
            f"Gold hours threshold: {gold_aggregate['gold_hours_threshold']}"
        )
        print(
            "Hours threshold observations:",
            "PASS" if hours_threshold_match else "FAIL",
        )
        print(
            "Silver missing coordinates: "
            f"{silver_aggregate['silver_missing_coordinates']}"
        )
        print(
            "Gold missing coordinates: "
            f"{gold_aggregate['gold_missing_coordinates']}"
        )
        print(
            "Missing coordinate observations:",
            "PASS" if missing_coordinates_match else "FAIL",
        )

        # ---------- Per-key expected metrics from Silver ----------
        silver_monthly_expected = (
            silver_df
            .groupBy(*PRIMARY_KEY)
            .agg(
                countDistinct("date").alias("expected_active_days"),
                count("*").alias("expected_observation_count"),
                countDistinct(
                    when(
                        col("cell_ll_lat").isNotNull()
                        & col("cell_ll_lon").isNotNull(),
                        struct(col("cell_ll_lat"), col("cell_ll_lon")),
                    )
                ).alias("expected_active_grid_cells"),
                spark_sum("hours").alias("expected_total_hours"),
                spark_sum("fishing_hours").alias(
                    "expected_total_fishing_hours"
                ),
                count(
                    when(col("mmsi_status") == "REVIEW", True)
                ).alias("expected_mmsi_review_observations"),
                count(
                    when(
                        col("hours_status") == "REVIEW_THRESHOLD",
                        True,
                    )
                ).alias("expected_hours_threshold_observations"),
                count(
                    when(
                        col("cell_ll_lat").isNull()
                        | col("cell_ll_lon").isNull(),
                        True,
                    )
                ).alias("expected_missing_coordinate_observations"),
            )
            .withColumn("_silver_present", lit(1))
        )

        gold_monthly = monthly_gold_df.withColumn("_gold_present", lit(1))
        monthly_comparison_df = silver_monthly_expected.alias("s").join(
            gold_monthly.alias("g"),
            on=list(PRIMARY_KEY),
            how="full_outer",
        )

        matched_keys = (
            col("s._silver_present").isNotNull()
            & col("g._gold_present").isNotNull()
        )
        total_hours_mismatch = float_mismatch(
            "s.expected_total_hours",
            "g.total_hours",
        )
        total_fishing_hours_mismatch = float_mismatch(
            "s.expected_total_fishing_hours",
            "g.total_fishing_hours",
        )

        monthly_key_stats = (
            monthly_comparison_df
            .agg(
                spark_count(
                    when(
                        col("s._silver_present").isNotNull()
                        & col("g._gold_present").isNull(),
                        1,
                    )
                ).alias("missing_gold_keys"),
                spark_count(
                    when(
                        col("s._silver_present").isNull()
                        & col("g._gold_present").isNotNull(),
                        1,
                    )
                ).alias("unexpected_gold_keys"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col("s.expected_active_days").eqNullSafe(
                                col("g.active_days")
                            )
                        ),
                        1,
                    )
                ).alias("active_days_mismatches"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col("s.expected_observation_count").eqNullSafe(
                                col("g.observation_count")
                            )
                        ),
                        1,
                    )
                ).alias("observation_count_mismatches"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col("s.expected_active_grid_cells").eqNullSafe(
                                col("g.active_grid_cells")
                            )
                        ),
                        1,
                    )
                ).alias("active_grid_cells_mismatches"),
                spark_count(
                    when(matched_keys & total_hours_mismatch, 1)
                ).alias("total_hours_mismatches"),
                spark_count(
                    when(matched_keys & total_fishing_hours_mismatch, 1)
                ).alias("total_fishing_hours_mismatches"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col(
                                "s.expected_mmsi_review_observations"
                            ).eqNullSafe(col("g.mmsi_review_observations"))
                        ),
                        1,
                    )
                ).alias("mmsi_review_mismatches"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col(
                                "s.expected_hours_threshold_observations"
                            ).eqNullSafe(
                                col("g.hours_threshold_observations")
                            )
                        ),
                        1,
                    )
                ).alias("hours_threshold_mismatches"),
                spark_count(
                    when(
                        matched_keys
                        & ~(
                            col(
                                "s.expected_missing_coordinate_observations"
                            ).eqNullSafe(
                                col("g.missing_coordinate_observations")
                            )
                        ),
                        1,
                    )
                ).alias("missing_coordinate_mismatches"),
            )
            .first()
        )

        monthly_key_checks = {
            "Missing Gold keys":
                monthly_key_stats["missing_gold_keys"] == 0,
            "Unexpected Gold keys":
                monthly_key_stats["unexpected_gold_keys"] == 0,
            "Active days":
                monthly_key_stats["active_days_mismatches"] == 0,
            "Observation count":
                monthly_key_stats["observation_count_mismatches"] == 0,
            "Active grid cells":
                monthly_key_stats["active_grid_cells_mismatches"] == 0,
            "Total hours (per-key)":
                monthly_key_stats["total_hours_mismatches"] == 0,
            "Total fishing hours (per-key)":
                monthly_key_stats["total_fishing_hours_mismatches"] == 0,
            "MMSI review observations (per-key)":
                monthly_key_stats["mmsi_review_mismatches"] == 0,
            "Hours threshold observations (per-key)":
                monthly_key_stats["hours_threshold_mismatches"] == 0,
            "Missing coordinate observations (per-key)":
                monthly_key_stats["missing_coordinate_mismatches"] == 0,
        }

        print("\n========== PER-KEY VALIDATION STATUS ==========")
        for name, result in monthly_key_checks.items():
            print(f"{name}: {'PASS' if result else 'FAIL'}")
        per_key_pass = all(monthly_key_checks.values())
        print(
            "\nPER-KEY OVERALL STATUS:",
            "PASS" if per_key_pass else "FAIL",
        )

        mismatch_details = (
            monthly_comparison_df
            .filter(
                (
                    col("s._silver_present").isNotNull()
                    & col("g._gold_present").isNull()
                )
                | (
                    col("s._silver_present").isNull()
                    & col("g._gold_present").isNotNull()
                )
                | (
                    matched_keys
                    & ~(
                        col("s.expected_active_days").eqNullSafe(
                            col("g.active_days")
                        )
                    )
                )
                | (
                    matched_keys
                    & ~(
                        col("s.expected_observation_count").eqNullSafe(
                            col("g.observation_count")
                        )
                    )
                )
                | (
                    matched_keys
                    & ~(
                        col("s.expected_active_grid_cells").eqNullSafe(
                            col("g.active_grid_cells")
                        )
                    )
                )
                | (matched_keys & total_hours_mismatch)
                | (matched_keys & total_fishing_hours_mismatch)
                | (
                    matched_keys
                    & ~(
                        col(
                            "s.expected_mmsi_review_observations"
                        ).eqNullSafe(col("g.mmsi_review_observations"))
                    )
                )
                | (
                    matched_keys
                    & ~(
                        col(
                            "s.expected_hours_threshold_observations"
                        ).eqNullSafe(col("g.hours_threshold_observations"))
                    )
                )
                | (
                    matched_keys
                    & ~(
                        col(
                            "s.expected_missing_coordinate_observations"
                        ).eqNullSafe(
                            col("g.missing_coordinate_observations")
                        )
                    )
                )
            )
            .select(
                "year",
                "month",
                "mmsi",
                col("s.expected_active_days").alias("expected_active_days"),
                col("g.active_days").alias("actual_active_days"),
                col("s.expected_observation_count").alias(
                    "expected_observation_count"
                ),
                col("g.observation_count").alias("actual_observation_count"),
                col("s.expected_active_grid_cells").alias(
                    "expected_active_grid_cells"
                ),
                col("g.active_grid_cells").alias("actual_active_grid_cells"),
                col("s.expected_total_hours").alias("expected_total_hours"),
                col("g.total_hours").alias("actual_total_hours"),
                col("s.expected_total_fishing_hours").alias(
                    "expected_total_fishing_hours"
                ),
                col("g.total_fishing_hours").alias(
                    "actual_total_fishing_hours"
                ),
                col("s.expected_mmsi_review_observations").alias(
                    "expected_mmsi_review_observations"
                ),
                col("g.mmsi_review_observations").alias(
                    "actual_mmsi_review_observations"
                ),
                col("s.expected_hours_threshold_observations").alias(
                    "expected_hours_threshold_observations"
                ),
                col("g.hours_threshold_observations").alias(
                    "actual_hours_threshold_observations"
                ),
                col("s.expected_missing_coordinate_observations").alias(
                    "expected_missing_coordinate_observations"
                ),
                col("g.missing_coordinate_observations").alias(
                    "actual_missing_coordinate_observations"
                ),
            )
        )
        print("\n========== MISMATCH DETAILS ==========")
        mismatch_details.show(50, truncate=False)

        checks = {
            "Silver date": silver_date_pass,
            "Calendar consistency": calendar_pass,
            "Gold schema / data types": schema_pass,
            "Silver schema / data types": silver_schema_pass,
            "Gold month range": gold_month_pass,
            "Silver month range": silver_month_pass,
            "Silver non-empty": silver_aggregate["silver_records"] > 0,
            "Gold non-empty": gold_aggregate["gold_records"] > 0,
            "Null gold primary keys": null_primary_key_pass,
            "Null silver group keys": silver_group_keys_pass,
            "Duplicate gold primary keys": duplicate_pass,
            "Observation reconciliation": observation_match,
            "Total hours": total_hours_match,
            "Total fishing hours": total_fishing_hours_match,
            "MMSI review observations": mmsi_review_match,
            "Hours threshold observations": hours_threshold_match,
            "Missing coordinate observations": missing_coordinates_match,
            "Per-key validation": per_key_pass,
        }

        print("\n========== OVERALL MONTHLY GOLD VALIDATION ==========")
        for name, result in checks.items():
            print(f"{name}: {'PASS' if result else 'FAIL'}")

        overall_pass = all(checks.values())
        print(
            "\nOVERALL STATUS:",
            "PASS" if overall_pass else "FAIL",
        )
        if overall_pass:
            print("Validation PASSED — safe to upload.")
            exit_code = 0
        else:
            print(
                "Validation FAILED — do not upload monthly gold "
                "until all checks pass."
            )
            exit_code = 1
    finally:
        spark.stop()
        print("\nSpark Session stopped.")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
