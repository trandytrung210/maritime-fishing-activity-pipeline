import json
import math
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    greatest,
    isnan,
    lit,
    when,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.storagelevel import StorageLevel

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
AREA_GOLD_ROOT = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
    / "area_fishing_activity"
)
REPORT_ROOT = PROJECT_ROOT / "reports" / "area_fishing_activity"
REPORT_PATH = REPORT_ROOT / "validation_report.json"
BUSINESS_KEY = ["cell_ll_lat", "cell_ll_lon"]
EXPECTED_GOLD_TYPES = {
    "cell_ll_lat": "double",
    "cell_ll_lon": "double",
    "unique_vessels": "long",
    "unique_fishing_vessels": "long",
    "total_hours": "double",
    "total_fishing_hours": "double",
    "active_days": "long",
    "observation_count": "long",
    "fishing_intensity": "double",
    "fishing_hours_per_vessel": "double",
}

INTEGER_METRICS = [
    "unique_vessels",
    "unique_fishing_vessels",
    "active_days",
    "observation_count",
]
DOUBLE_METRICS = [
    "total_hours",
    "total_fishing_hours",
    "fishing_intensity",
    "fishing_hours_per_vessel",
]

REL_TOL = 1e-9
ABS_TOL = 1e-6

def create_spark_session():
    return (
        SparkSession.builder
        .appName("ValidateAreaFishingActivity")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
def build_expected_area_from_silver(silver_eligible_df):
    """Independently aggregate Silver at the same grid-cell grain as Area Gold."""
    expected_df = (
        silver_eligible_df
        .groupBy(*BUSINESS_KEY)
        .agg(
            countDistinct("mmsi").alias("unique_vessels"),
            countDistinct(
                when(col("fishing_hours") > 0, col("mmsi"))
            ).alias("unique_fishing_vessels"),
            spark_sum("hours").alias("total_hours"),
            spark_sum("fishing_hours").alias("total_fishing_hours"),
            countDistinct("date").alias("active_days"),
            count(lit(1)).alias("observation_count"),
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
    return expected_df
def record_check(checks, name, passed, details=None):
    checks[name] = {
        "status": "PASS" if passed else "FAIL",
        "passed": bool(passed),
        "details": details or {},
    }
    print(f"{name}: {'PASS' if passed else 'FAIL'}")
    if details:
        for key, value in details.items():
            print(f"  {key}: {value}")


def python_values_match(expected, actual):
    if expected is None or actual is None:
        return expected is None and actual is None
    return math.isclose(
        float(expected),
        float(actual),
        rel_tol=REL_TOL,
        abs_tol=ABS_TOL,
    )
def main():
    print("=" * 60)
    print("AREA FISHING ACTIVITY VALIDATION")
    print("=" * 60)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Silver input: {SILVER_ROOT}")
    print(f"Area Gold input: {AREA_GOLD_ROOT}")

    from src.common.manifest import load_gold_changes

    changes = load_gold_changes()
    if not changes.get("has_silver_changes"):
        print(
            "No silver changes for this run. "
            "Area gold validation skipped (PASS)."
        )
        return

    if not SILVER_ROOT.exists():
        raise FileNotFoundError(f"Silver directory not found: {SILVER_ROOT}")
    if not AREA_GOLD_ROOT.exists():
        raise FileNotFoundError(
            f"Area Gold directory not found: {AREA_GOLD_ROOT}"
        )

    spark = create_spark_session()
    silver_eligible_df = None
    expected_area_df = None
    area_gold_df = None
    reconciliation_df = None
    overall_pass = False

    try:
        print(f"Spark version: {spark.version}")

        silver_df = spark.read.parquet(str(SILVER_ROOT))
        area_gold_df = spark.read.parquet(str(AREA_GOLD_ROOT)).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        required_silver_columns = [
            "date",
            "year",
            "month",
            "mmsi",
            "cell_ll_lat",
            "cell_ll_lon",
            "hours",
            "fishing_hours",
        ]
        missing_silver_columns = [
            name for name in required_silver_columns
            if name not in silver_df.columns
        ]
        if missing_silver_columns:
            raise ValueError(
                f"Silver is missing required columns: {missing_silver_columns}"
            )

        print("Silver required columns: PASS")

        checks = {}

        # The Build code filters only NULL coordinates. Keep this exact scope
        # for all downstream reconciliation checks.
        silver_eligible_df = (
            silver_df
            .filter(
                col("cell_ll_lat").isNotNull()
                & col("cell_ll_lon").isNotNull()
            )
            .persist(StorageLevel.DISK_ONLY)
        )
        silver_summary = (
            silver_eligible_df
            .agg(
                count(lit(1)).alias("observation_count"),
                spark_sum("hours").alias("total_hours"),
                spark_sum("fishing_hours").alias("total_fishing_hours"),
            )
            .first()
        )
        silver_observation_count = int(
            silver_summary["observation_count"] or 0
        )
        silver_total_hours = silver_summary["total_hours"]
        silver_total_fishing_hours = silver_summary["total_fishing_hours"]

        area_gold_count = area_gold_df.count()
        print(f"Silver observations eligible for Area Gold: {silver_observation_count:,}")
        print(f"Area Gold grid cells: {area_gold_count:,}")

        record_check(
            checks,
            "NON_EMPTY_DATASETS",
            silver_observation_count > 0 and area_gold_count > 0,
            {
                "silver_eligible_observations": silver_observation_count,
                "area_gold_grid_cells": area_gold_count,
            },
        )

        # 1. Exact Gold schema check.
        actual_gold_types = {
            field.name: field.dataType.typeName()
            for field in area_gold_df.schema.fields
        }
        missing_gold_columns = sorted(
            set(EXPECTED_GOLD_TYPES) - set(actual_gold_types)
        )
        unexpected_gold_columns = sorted(
            set(actual_gold_types) - set(EXPECTED_GOLD_TYPES)
        )
        datatype_mismatches = {
            name: {
                "expected": expected_type,
                "actual": actual_gold_types.get(name),
            }
            for name, expected_type in EXPECTED_GOLD_TYPES.items()
            if name in actual_gold_types
            and actual_gold_types[name] != expected_type
        }
        schema_pass = (
            not missing_gold_columns
            and not unexpected_gold_columns
            and not datatype_mismatches
        )
        record_check(
            checks,
            "GOLD_SCHEMA_AND_DATA_TYPES",
            schema_pass,
            {
                "missing_columns": missing_gold_columns,
                "unexpected_columns": unexpected_gold_columns,
                "datatype_mismatches": datatype_mismatches,
            },
        )

        # 2. Primary-key, duplicate and coordinate checks.
        null_key_rows = area_gold_df.filter(
            col("cell_ll_lat").isNull() | col("cell_ll_lon").isNull()
        ).count()
        record_check(
            checks,
            "NULL_PRIMARY_KEYS",
            null_key_rows == 0,
            {"null_key_rows": null_key_rows},
        )
        duplicate_groups = (
            area_gold_df
            .groupBy(*BUSINESS_KEY)
            .agg(count(lit(1)).alias("key_count"))
            .filter(col("key_count") > 1)
        )
        duplicate_group_count = duplicate_groups.count()
        duplicate_member_rows = (
            duplicate_groups
            .agg(spark_sum("key_count").alias("duplicate_member_rows"))
            .first()["duplicate_member_rows"]
            or 0
        )
        record_check(
            checks,
            "DUPLICATE_GRID_CELLS",
            duplicate_group_count == 0,
            {
                "duplicate_groups": int(duplicate_group_count),
                "rows_in_duplicate_groups": int(duplicate_member_rows),
            },
        )

        invalid_coordinate_condition = (
            isnan(col("cell_ll_lat"))
            | isnan(col("cell_ll_lon"))
            | (col("cell_ll_lat") < -90)
            | (col("cell_ll_lat") > 90)
            | (col("cell_ll_lon") < -180)
            | (col("cell_ll_lon") > 180)
        )
        invalid_gold_coordinates = area_gold_df.filter(
            invalid_coordinate_condition
        ).count()
        invalid_silver_coordinates = silver_eligible_df.filter(
            invalid_coordinate_condition
        ).count()
        record_check(
            checks,
            "COORDINATE_RANGE_AND_FORMAT",
            invalid_gold_coordinates == 0 and invalid_silver_coordinates == 0,
            {
                "invalid_silver_coordinate_rows": invalid_silver_coordinates,
                "invalid_gold_grid_cells": invalid_gold_coordinates,
                "latitude_range": "[-90, 90]",
                "longitude_range": "[-180, 180]",
            },
        )
          # Silver is expected to contain only non-INVALID rows. These fields
        # are required by the Area Gold aggregations and its business grain.
        missing_source_values = silver_eligible_df.filter(
            col("date").isNull()
            | col("mmsi").isNull()
            | col("hours").isNull()
            | col("fishing_hours").isNull()
        ).count()
        record_check(
            checks,
            "SILVER_REQUIRED_VALUE_COMPLETENESS",
            missing_source_values == 0,
            {"rows_with_null_date_mmsi_or_hours": missing_source_values},
        )

        # 3. Global observation and aggregate reconciliation.
        gold_summary = (
            area_gold_df
            .agg(
                spark_sum("observation_count").alias("observation_count"),
                spark_sum("total_hours").alias("total_hours"),
                spark_sum("total_fishing_hours").alias("total_fishing_hours"),
            )
            .first()
        )
        gold_observation_count = int(
            gold_summary["observation_count"] or 0
        )
        gold_total_hours = gold_summary["total_hours"]
        gold_total_fishing_hours = gold_summary["total_fishing_hours"]

        record_check(
            checks,
            "OBSERVATION_RECONCILIATION",
            silver_observation_count == gold_observation_count,
            {
                "silver_observations": silver_observation_count,
                "gold_sum_observation_count": gold_observation_count,
            },
        )
        record_check(
            checks,
            "TOTAL_HOURS_RECONCILIATION",
            python_values_match(silver_total_hours, gold_total_hours),
            {
                "silver_total_hours": silver_total_hours,
                "gold_total_hours": gold_total_hours,
                "relative_tolerance": REL_TOL,
                "absolute_tolerance": ABS_TOL,
            },
        )
        record_check(
            checks,
            "TOTAL_FISHING_HOURS_RECONCILIATION",
            python_values_match(
                silver_total_fishing_hours,
                gold_total_fishing_hours,
            ),
            {
                "silver_total_fishing_hours": silver_total_fishing_hours,
                "gold_total_fishing_hours": gold_total_fishing_hours,
                "relative_tolerance": REL_TOL,
                "absolute_tolerance": ABS_TOL,
            },
        )

        # 4. Business rules for the published Gold metrics.
        business_rule_violations = area_gold_df.filter(
            (col("unique_vessels") < 0)
            | (col("unique_fishing_vessels") < 0)
            | (col("unique_fishing_vessels") > col("unique_vessels"))
            | (col("unique_vessels") > col("observation_count"))
            | (col("active_days") <= 0)
            | (col("active_days") > col("observation_count"))
            | (col("observation_count") <= 0)
            | (col("total_hours") < 0)
            | (col("total_fishing_hours") < 0)
            | (col("fishing_intensity") < 0)
            | (col("fishing_hours_per_vessel") < 0)
        ).count()
        record_check(
            checks,
            "BUSINESS_RULES",
            business_rule_violations == 0,
            {"violating_grid_cells": business_rule_violations},
        )
        metric_null_or_nonfinite_condition = lit(False)
        for metric_name in INTEGER_METRICS + DOUBLE_METRICS:
            metric_col = col(metric_name)
            condition = metric_col.isNull()
            if metric_name in DOUBLE_METRICS:
                condition = (
                    condition
                    | isnan(metric_col)
                    | (spark_abs(metric_col) == lit(float("inf")))
                )
            metric_null_or_nonfinite_condition = (
                metric_null_or_nonfinite_condition | condition
            )
        incomplete_metric_rows = area_gold_df.filter(
            metric_null_or_nonfinite_condition
        ).count()
        record_check(
            checks,
            "GOLD_METRIC_COMPLETENESS",
            incomplete_metric_rows == 0,
            {"rows_with_null_nan_or_infinite_metrics": incomplete_metric_rows},
        )

        # 5. Per-grid-cell reconciliation. This catches offsetting errors
        # that can cancel out in global totals.
        expected_area_df = build_expected_area_from_silver(
            silver_eligible_df
        ).persist(StorageLevel.MEMORY_AND_DISK)
        expected_cell_count = expected_area_df.count()

        expected_alias = expected_area_df.alias("expected")
        actual_alias = area_gold_df.alias("actual")
        join_condition = (
            (expected_alias["cell_ll_lat"] == actual_alias["cell_ll_lat"])
            & (expected_alias["cell_ll_lon"] == actual_alias["cell_ll_lon"])
        )
        joined_df = expected_alias.join(
            actual_alias,
            join_condition,
            "full_outer",
        )

        selected_columns = [
            expected_alias["cell_ll_lat"].alias("expected_cell_ll_lat"),
            actual_alias["cell_ll_lat"].alias("actual_cell_ll_lat"),
        ]
        for metric_name in INTEGER_METRICS + DOUBLE_METRICS:
            selected_columns.extend(
                [
                    expected_alias[metric_name].alias(
                        f"expected_{metric_name}"
                    ),
                    actual_alias[metric_name].alias(
                        f"actual_{metric_name}"
                    ),
                ]
            )

        reconciliation_df = joined_df.select(*selected_columns).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        missing_gold_cells = reconciliation_df.filter(
            col("expected_cell_ll_lat").isNotNull()
            & col("actual_cell_ll_lat").isNull()
        ).count()
        unexpected_gold_cells = reconciliation_df.filter(
            col("expected_cell_ll_lat").isNull()
            & col("actual_cell_ll_lat").isNotNull()
        ).count()

        record_check(
            checks,
            "GRID_CELL_KEY_RECONCILIATION",
            missing_gold_cells == 0 and unexpected_gold_cells == 0,
            {
                "expected_silver_grid_cells": expected_cell_count,
                "actual_gold_grid_cells": area_gold_count,
                "missing_gold_cells": missing_gold_cells,
                "unexpected_gold_cells": unexpected_gold_cells,
            },
        )
        per_cell_mismatches = {}
        for metric_name in INTEGER_METRICS + DOUBLE_METRICS:
            expected_metric = col(f"expected_{metric_name}")
            actual_metric = col(f"actual_{metric_name}")

            if metric_name in INTEGER_METRICS:
                match_condition = expected_metric.eqNullSafe(actual_metric)
            else:
                both_null = (
                    expected_metric.isNull() & actual_metric.isNull()
                )
                both_present_and_close = (
                    expected_metric.isNotNull()
                    & actual_metric.isNotNull()
                    & (
                        spark_abs(expected_metric - actual_metric)
                        <= (
                            lit(ABS_TOL)
                            + greatest(
                                spark_abs(expected_metric),
                                spark_abs(actual_metric),
                            ) * lit(REL_TOL)
                        )
                    )
                )
                match_condition = both_null | both_present_and_close

            mismatch_count = reconciliation_df.filter(
                ~match_condition
            ).count()
            per_cell_mismatches[metric_name] = int(mismatch_count)

        for metric_name, mismatch_count in per_cell_mismatches.items():
            record_check(
                checks,
                f"PER_CELL_{metric_name.upper()}",
                mismatch_count == 0,
                {"mismatched_grid_cells": mismatch_count},
            )

        # fishing_intensity = total_fishing_hours / total_hours, or 0 if
        # total_hours <= 0.
        # fishing_hours_per_vessel = total_fishing_hours / unique_vessels,
        # not divided by unique_fishing_vessels.
        print("\nDerived metric definitions verified against Build logic:")
        print("  fishing_intensity = total_fishing_hours / total_hours (when > 0)")
        print("  fishing_hours_per_vessel = total_fishing_hours / unique_vessels (when > 0)")

        overall_pass = all(
            check["passed"] for check in checks.values()
        )
        report = {
            "validated_at": datetime.now(UTC).isoformat(),
            "silver_path": str(SILVER_ROOT),
            "area_gold_path": str(AREA_GOLD_ROOT),
            "silver_eligible_observations": silver_observation_count,
            "area_gold_grid_cells": area_gold_count,
            "expected_silver_grid_cells": expected_cell_count,
            "silver_total_hours": silver_total_hours,
            "gold_total_hours": gold_total_hours,
            "silver_total_fishing_hours": silver_total_fishing_hours,
            "gold_total_fishing_hours": gold_total_fishing_hours,
            "checks": checks,
            "overall_status": "PASS" if overall_pass else "FAIL",
        }

        REPORT_ROOT.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(
            json.dumps(report, ensure_ascii=False, indent=4, default=str),
            encoding="utf-8",
        )

        print("\n" + "=" * 60)
        print(f"AREA GOLD VALIDATION: {'PASS' if overall_pass else 'FAIL'}")
        print(f"Validation report: {REPORT_PATH}")
        print("=" * 60)

    finally:
        if reconciliation_df is not None:
            reconciliation_df.unpersist()
        if expected_area_df is not None:
            expected_area_df.unpersist()
        if area_gold_df is not None:
            area_gold_df.unpersist()
        if silver_eligible_df is not None:
            silver_eligible_df.unpersist()
        spark.stop()
        print("\nSpark Session stopped.")
    if not overall_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

