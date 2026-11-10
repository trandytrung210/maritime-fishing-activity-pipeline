
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.gold.build_gold.build_area_fishing_activity import (
    build_area_gold,
)


@pytest.fixture(scope="session")
def spark():

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-gold-area-schema")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_gold_area_schema(spark):

    # 1. Define input Silver schema
    silver_schema = StructType([
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("mmsi", StringType(), True),
        StructField("cell_ll_lat", DoubleType(), True),
        StructField("cell_ll_lon", DoubleType(), True),
        StructField("hours", DoubleType(), True),
        StructField("fishing_hours", DoubleType(), True),
    ])

    # 2. Create sample Silver data
    sample_data = [
        (
            date(2025, 1, 1),
            2025,
            "123456789",
            10.0,
            20.0,
            5.0,
            2.0,
        )
    ]

    silver_df = spark.createDataFrame(
        sample_data,
        schema=silver_schema,
    )

    # 3. Run Gold transformation
    gold_df = build_area_gold(silver_df)

    # 4. Define expected Gold schema
    expected_schema = {
        "cell_ll_lat": DoubleType(),
        "cell_ll_lon": DoubleType(),
        "unique_vessels": LongType(),
        "unique_fishing_vessels": LongType(),
        "total_hours": DoubleType(),
        "total_fishing_hours": DoubleType(),
        "active_days": LongType(),
        "observation_count": LongType(),
        "fishing_intensity": DoubleType(),
        "fishing_hours_per_vessel": DoubleType(),
    }

    # 5. Get actual Gold schema
    actual_schema = {
        field.name: field.dataType
        for field in gold_df.schema.fields
    }

    # 6. Check columns
    assert set(actual_schema) == set(expected_schema)

    # 7. Check data types
    for column_name, expected_type in expected_schema.items():

        actual_type = actual_schema[column_name]

        assert actual_type == expected_type, (
            f"Column {column_name}: "
            f"expected {expected_type}, "
            f"got {actual_type}"
        )

def test_gold_area_aggregation(spark):

    # 1. Create sample Silver data
    sample_data = [
        (date(2025, 1, 1), 2025, "123456789", 10.0, 20.0, 5.0, 2.0),
        (date(2025, 1, 1), 2025, "123456789", 10.0, 20.0, 3.0, 1.0),
        (date(2025, 1, 2), 2025, "987654321", 10.0, 20.0, 4.0, 0.0),
        (date(2025, 1, 2), 2025, "111111111", 10.0, 20.0, 2.0, 1.0),
        (date(2025, 1, 1), 2025, "222222222", 11.0, 21.0, 6.0, 0.0),
        (date(2025, 1, 3), 2025, "222222222", 11.0, 21.0, 2.0, 0.0),
        (date(2025, 1, 3), 2025, "333333333", None, 30.0, 7.0, 5.0),
    ]

    # 2. Define Silver schema
    silver_schema = StructType([
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("mmsi", StringType(), True),
        StructField("cell_ll_lat", DoubleType(), True),
        StructField("cell_ll_lon", DoubleType(), True),
        StructField("hours", DoubleType(), True),
        StructField("fishing_hours", DoubleType(), True),
    ])

    silver_df = spark.createDataFrame(
        sample_data,
        schema=silver_schema,
    )

    # 3. Execute Gold transformation
    gold_df = build_area_gold(silver_df)

    # 4. Collect results
    results = {
        (row["cell_ll_lat"], row["cell_ll_lon"]): row
        for row in gold_df.collect()
    }

    # 5. Check GROUP BY
    assert len(results) == 2

    # 6. Check area (10.0, 20.0)
    area_a = results[(10.0, 20.0)]

    assert area_a["unique_vessels"] == 3
    assert area_a["unique_fishing_vessels"] == 2
    assert area_a["total_hours"] == pytest.approx(14.0)
    assert area_a["total_fishing_hours"] == pytest.approx(4.0)
    assert area_a["active_days"] == 2
    assert area_a["observation_count"] == 4

    assert area_a["fishing_intensity"] == pytest.approx(4 / 14)
    assert area_a["fishing_hours_per_vessel"] == pytest.approx(4 / 3)

    # 7. Check area (11.0, 21.0)
    area_b = results[(11.0, 21.0)]

    assert area_b["unique_vessels"] == 1
    assert area_b["unique_fishing_vessels"] == 0
    assert area_b["total_hours"] == pytest.approx(8.0)
    assert area_b["total_fishing_hours"] == pytest.approx(0.0)
    assert area_b["active_days"] == 2
    assert area_b["observation_count"] == 2

    assert area_b["fishing_intensity"] == pytest.approx(0.0)
    assert area_b["fishing_hours_per_vessel"] == pytest.approx(0.0)

