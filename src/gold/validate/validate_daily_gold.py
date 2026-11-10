import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, count, countDistinct, lit, struct, when
from pyspark.sql.functions import sum as spark_sum

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)
GOLD_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "daily_vessel_activity"
)

GOLD_CHANGES_PATH = PROJECT_ROOT / "data" / "processed" / "manifest" / "gold_changes.json"


def _affected_months() -> list[str]:
    if not GOLD_CHANGES_PATH.exists():
        return []
    try:
        payload = json.loads(GOLD_CHANGES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    months = payload.get("affected_months") or []
    return [str(m) for m in months if m]


if __name__ == "__main__":
    months = _affected_months()
    if not months:
        print("No affected_months in gold_changes.json — nothing to validate. PASS.")
        sys.exit(0)

spark = (
    SparkSession.builder
    .appName("GoldDataValidation")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)
try:
    df_silver = spark.read.parquet(str(SILVER_ROOT))
    df_gold = spark.read.parquet(str(GOLD_ROOT))
    print("\n========== GOLD VALIDATION ==========")
    print("Silver schema:")
    df_silver.printSchema()
    print("Gold schema:")
    df_gold.printSchema()
    expected_schema = {
        "date": "date",
        "mmsi": "string",
        "mmsi_status": "string",
        "mmsi_status_distinct_values": "bigint",
        "mmsi_status_conflict": "boolean",
        "mmsi_status_missing_observations": "bigint",
        "total_hours": "double",
        "total_fishing_hours": "double",
        "active_grid_cells": "bigint",
        "observation_count": "bigint",
        "missing_coordinate_observations": "bigint",
        "mmsi_review_observations": "bigint",
        "hours_threshold_observations": "bigint",
        "year": "int",
        "month": "int"
    }
    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in df_gold.schema.fields
    }
    schema_type_pass = (
        actual_schema == expected_schema
    )
    column_order_pass = (
        df_gold.columns == list(expected_schema.keys())
    )
    schema_pass = (
        schema_type_pass
        and column_order_pass
    )
    print("\nSchema validation:","PASS" if schema_pass else "FAIL")
    print("Column order validation:","PASS" if column_order_pass else "FAIL")
    expected_columns = list(expected_schema.keys())
    actual_columns = df_gold.columns
    # 1. Missing columns
    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]
    # 2. Extra columns
    extra_columns = [
        column
        for column in actual_columns
        if column not in expected_columns
    ]
    # 3. Data type mismatches
    type_mismatches = {
        column: {
            "expected": expected_schema[column],
            "actual": actual_schema[column]
        }
        for column in expected_columns
        if column in actual_schema
        and actual_schema[column] != expected_schema[column]
    }
    # 4. Column order
    column_order_pass = (
        actual_columns == expected_columns
    )
    # 5. Schema validation
    schema_pass = (
        len(missing_columns) == 0
        and len(extra_columns) == 0
        and len(type_mismatches) == 0
        and column_order_pass
    )
    # Print diagnostic results
    print("\n========== SCHEMA DIAGNOSTICS ==========")
    print("Missing columns:", missing_columns)
    print("Extra columns:", extra_columns)
    print("Type mismatches:", type_mismatches)
    print(
        "Column order:",
        "PASS" if column_order_pass else "FAIL"
    )
    print("Expected schema:", expected_schema)
    print("Actual schema:", actual_schema)
    print(
        "Schema validation:",
        "PASS" if schema_pass else "FAIL"
    )
    print("========================================")
    gold_metrics = df_gold.agg(
        count(lit(1)).alias("gold_records"),
        coalesce(
            spark_sum("observation_count"),
            lit(0)
        ).alias("gold_observation_count"),
        coalesce(
            spark_sum("total_hours"),
            lit(0.0)
        ).alias("gold_total_hours"),
        coalesce(
            spark_sum("total_fishing_hours"),
            lit(0.0)
        ).alias("gold_total_fishing_hours"),
        coalesce(
            spark_sum(
                when(
                    col("date").isNull() | col("mmsi").isNull(),
                    1
                ).otherwise(0)
            ),
            lit(0)
        ).alias("null_keys")
    ).first().asDict()
    print("\nGold records:", gold_metrics["gold_records"])
    print(
        "Gold observations:",
        gold_metrics["gold_observation_count"]
    )
    print("Null primary keys:", gold_metrics["null_keys"])
    silver_metrics = df_silver.agg(
        count(lit(1)).alias("silver_records"),
        coalesce(
            spark_sum("hours"),
            lit(0.0)
        ).alias("silver_total_hours"),
        coalesce(
            spark_sum("fishing_hours"),
            lit(0.0)
        ).alias("silver_total_fishing_hours"),
        coalesce(
            spark_sum(
                when(
                    col("mmsi_status") == "REVIEW",
                    1
                ).otherwise(0)
            ),
            lit(0)
        ).alias("silver_mmsi_review"),
        coalesce(
            spark_sum(
                when(
                    col("hours_status") == "REVIEW_THRESHOLD",
                    1
                ).otherwise(0)
            ),
            lit(0)
        ).alias("silver_hours_threshold"),
        coalesce(
            spark_sum(
                when(
                    col("cell_ll_lat").isNull()
                    | col("cell_ll_lon").isNull(),
                    1
                ).otherwise(0)
            ),
            lit(0)
        ).alias("silver_missing_coordinates")
    ).first().asDict()
    observation_match = (
        silver_metrics["silver_records"]
        == gold_metrics["gold_observation_count"]
    )
    print("\n========== RECORD RECONCILIATION ==========")
    print("Silver records:", silver_metrics["silver_records"])
    print("Gold observation count:",gold_metrics["gold_observation_count"])
    print("Silver = Gold observations:",observation_match)
    def close_enough(a, b):
        tolerance = max(
            0.000001,
            0.000000001 * max(abs(a), abs(b))
        )
        return abs(a - b) <= tolerance
    hours_match = close_enough(
        silver_metrics["silver_total_hours"],
        gold_metrics["gold_total_hours"]
    )
    fishing_hours_match = close_enough(
        silver_metrics["silver_total_fishing_hours"],
        gold_metrics["gold_total_fishing_hours"]
    )
    mmsi_review_match = (
        silver_metrics["silver_mmsi_review"]
        == df_gold.agg(
            coalesce(
                spark_sum("mmsi_review_observations"),
                lit(0)
            )
        ).first()[0]
    )
    hours_threshold_match = (
        silver_metrics["silver_hours_threshold"]
        == df_gold.agg(
            coalesce(
                spark_sum("hours_threshold_observations"),
                lit(0)
            )
        ).first()[0]
    )
    missing_coordinates_match = (
        silver_metrics["silver_missing_coordinates"]
        == df_gold.agg(
            coalesce(
                spark_sum("missing_coordinate_observations"),
                lit(0)
            )
        ).first()[0]
    )
    print("\n========== AGGREGATE RECONCILIATION ==========")
    print("Total hours match:", hours_match)
    print("Total fishing hours match:", fishing_hours_match)
    print("MMSI review observations match:", mmsi_review_match)
    print("Hours threshold observations match:", hours_threshold_match)
    print("Missing coordinate observations match:", missing_coordinates_match)
    duplicate_groups = 0
    duplicate_member_rows = 0
    excess_duplicate_rows = 0
    periods = (
    df_silver
    .select("year", "month")
    .unionByName(
        df_gold.select("year", "month")
    )
    .distinct()
    .orderBy("year", "month")
    .collect()
)
    for period in periods:
        year = period["year"]
        month = period["month"]
        df_period = df_gold.filter(
            (col("year") == year)
            & (col("month") == month)
        )
        duplicate_stats = (
            df_period
            .groupBy("date", "mmsi")
            .agg(
                count(lit(1)).alias("key_count")
            )
            .filter(col("key_count") > 1)
            .agg(
                count(lit(1)).alias("groups"),
                coalesce(
                    spark_sum("key_count"),
                    lit(0)
                ).alias("members"),
                coalesce(
                    spark_sum(col("key_count") - lit(1)),
                    lit(0)
                ).alias("excess")
            )
            .first()
        )
        duplicate_groups += duplicate_stats["groups"]
        duplicate_member_rows += duplicate_stats["members"]
        excess_duplicate_rows += duplicate_stats["excess"]
    duplicate_pass = excess_duplicate_rows == 0
    print("\n========== DUPLICATE KEY CHECK ==========")
    print("Duplicate groups:", duplicate_groups)
    print("Duplicate member rows:", duplicate_member_rows)
    print("Excess duplicate rows:", excess_duplicate_rows)
    print("Unique date + MMSI:", duplicate_pass)
    advanced_mismatches = {
        "missing_gold_keys": 0,
        "unexpected_gold_keys": 0,
        "mmsi_status_distinct_mismatches": 0,
        "mmsi_status_conflict_mismatches": 0,
        "mmsi_status_missing_mismatches": 0,
        "active_grid_cells_mismatches": 0
    }
    for period in periods:
        year = period["year"]
        month = period["month"]
        print(f"Validating advanced fields: {year}-{month:02d}")
        silver_month = (
            df_silver
            .filter(
                (col("year") == year)
                & (col("month") == month)
            )
            .select(
                "date",
                "mmsi",
                "mmsi_status",
                "cell_ll_lat",
                "cell_ll_lon"
            )
        )
        expected = (
            silver_month
            .groupBy("date", "mmsi")
            .agg(
                countDistinct("mmsi_status").alias(
                    "expected_mmsi_status_distinct_values"
                ),
                coalesce(
                    spark_sum(
                        when(
                            col("mmsi_status").isNull(),
                            1
                        ).otherwise(0)
                    ),
                    lit(0)
                ).alias(
                    "expected_mmsi_status_missing_observations"
                ),
                countDistinct(
                    when(
                        col("cell_ll_lat").isNotNull()
                        & col("cell_ll_lon").isNotNull(),
                        struct(
                            col("cell_ll_lat"),
                            col("cell_ll_lon")
                        )
                    )
                ).alias(
                    "expected_active_grid_cells"
                )
            )
            .withColumn(
                "expected_mmsi_status_conflict",
                col("expected_mmsi_status_distinct_values") > 1
            )
            .withColumn(
                "_silver_present",
                lit(1)
            )
        )
        gold_month = (
            df_gold
            .filter(
                (col("year") == year)
                & (col("month") == month)
            )
            .select(
                "date",
                "mmsi",
                "mmsi_status_distinct_values",
                "mmsi_status_conflict",
                "mmsi_status_missing_observations",
                "active_grid_cells"
            )
            .withColumn(
                "_gold_present",
                lit(1)
            )
        )
        joined = (
            expected.alias("s")
            .join(
                gold_month.alias("g"),
                (
                    (col("s.date") == col("g.date"))
                    & (col("s.mmsi") == col("g.mmsi"))
                ),
                "full_outer"
            )
        )
        matched = (
            col("s._silver_present").isNotNull()
            & col("g._gold_present").isNotNull()
        )
        stats = joined.agg(
            coalesce(
                spark_sum(
                    when(
                        col("g._gold_present").isNull(),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("missing_gold_keys"),
            coalesce(
                spark_sum(
                    when(
                        col("s._silver_present").isNull(),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("unexpected_gold_keys"),
            coalesce(
                spark_sum(
                    when(
                        matched
                        & ~(
                            col("g.mmsi_status_distinct_values")
                            .eqNullSafe(
                                col("s.expected_mmsi_status_distinct_values")
                            )
                        ),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("mmsi_status_distinct_mismatches"),
            coalesce(
                spark_sum(
                    when(
                        matched
                        & ~(
                            col("g.mmsi_status_conflict")
                            .eqNullSafe(
                                col("s.expected_mmsi_status_conflict")
                            )
                        ),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("mmsi_status_conflict_mismatches"),
            coalesce(
                spark_sum(
                    when(
                        matched
                        & ~(
                            col("g.mmsi_status_missing_observations")
                            .eqNullSafe(
                                col("s.expected_mmsi_status_missing_observations")
                            )
                        ),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("mmsi_status_missing_mismatches"),
            coalesce(
                spark_sum(
                    when(
                        matched
                        & ~(
                            col("g.active_grid_cells")
                            .eqNullSafe(
                                col("s.expected_active_grid_cells")
                            )
                        ),
                        1
                    ).otherwise(0)
                ),
                lit(0)
            ).alias("active_grid_cells_mismatches")
        ).first().asDict()
        for name in advanced_mismatches:
            advanced_mismatches[name] += stats[name]
    print("\n========== ADVANCED FIELD VALIDATION ==========")
    for name, value in advanced_mismatches.items():
        print(f"{name}: {value}")
    checks = {
        "Schema": schema_pass,
        "Null primary keys": gold_metrics["null_keys"] == 0,
        "Record reconciliation": observation_match,
        "Total hours": hours_match,
        "Total fishing hours": fishing_hours_match,
        "MMSI review observations": mmsi_review_match,
        "Hours threshold observations": hours_threshold_match,
        "Missing coordinates": missing_coordinates_match,
        "Duplicate primary keys": duplicate_pass,
        "Gold missing keys":advanced_mismatches["missing_gold_keys"] == 0,
        "Unexpected Gold keys":advanced_mismatches["unexpected_gold_keys"] == 0,
        "MMSI status distinct values":advanced_mismatches["mmsi_status_distinct_mismatches"] == 0,
        "MMSI status conflict":advanced_mismatches["mmsi_status_conflict_mismatches"] == 0,
        "MMSI status missing observations":advanced_mismatches["mmsi_status_missing_mismatches"] == 0,
        "Active grid cells":advanced_mismatches["active_grid_cells_mismatches"] == 0,}
    print("\n========== FINAL GOLD VALIDATION ==========")
    for name, result in checks.items():
        print(f"{name}: {'PASS' if result else 'FAIL'}")
    overall_pass = all(checks.values())
    print(
        "\nOVERALL STATUS:",
        "PASS" if overall_pass else "FAIL"
    )
    if not overall_pass:
        raise SystemExit(1)
finally:
    spark.stop()
