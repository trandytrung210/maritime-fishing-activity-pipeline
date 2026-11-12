from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from dashboard.lib.paths import (
    AREA_GOLD,
    DAILY_GOLD,
    DQ_REVIEW,
    DQ_SUMMARY,
    MONTHLY_GOLD,
)

# Local sample partitions may include 2015; dashboards only use production years.
EXCLUDED_YEARS = frozenset({2015})


def _dataset(path: Path, *, hive: bool = True) -> ds.Dataset:
    if not path.exists():
        raise FileNotFoundError(f"Gold path not found: {path}")
    return ds.dataset(
        str(path),
        format="parquet",
        partitioning="hive" if hive else None,
    )


def parquet_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file in path.rglob("*.parquet"):
        total += int(pq.ParquetFile(file).metadata.num_rows)
    return total


def _drop_excluded_years(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "year" not in frame.columns:
        return frame
    years = frame["year"].astype(int)
    return frame.loc[~years.isin(EXCLUDED_YEARS)].copy()


@lru_cache(maxsize=1)
def load_monthly() -> pd.DataFrame:
    table = _dataset(MONTHLY_GOLD).to_table()
    frame = table.to_pandas()
    frame["year"] = frame["year"].astype(int)
    frame = _drop_excluded_years(frame)
    frame["month"] = frame["month"].astype(int).astype(str).str.zfill(2)
    frame["month_key"] = frame["year"].astype(str) + "-" + frame["month"]
    return frame


def available_months() -> list[str]:
    frame = load_monthly()
    return sorted(frame["month_key"].unique().tolist())


def monthly_trend() -> pd.DataFrame:
    frame = load_monthly()
    trend = (
        frame.groupby("month_key", as_index=False)
        .agg(
            vessels=("mmsi", "nunique"),
            total_hours=("total_hours", "sum"),
            total_fishing_hours=("total_fishing_hours", "sum"),
            active_days=("active_days", "sum"),
            observation_count=("observation_count", "sum"),
            review_flags=("mmsi_review_observations", "sum"),
            hours_review_flags=("hours_threshold_observations", "sum"),
        )
        .sort_values("month_key")
    )
    trend["fishing_share"] = trend["total_fishing_hours"] / trend["total_hours"].replace(
        0, float("nan")
    )
    return trend


def top_monthly_vessels(
    month_key: str | None = None,
    *,
    metric: str = "total_fishing_hours",
    top_n: int = 20,
) -> pd.DataFrame:
    frame = load_monthly()
    if month_key:
        frame = frame.loc[frame["month_key"] == month_key]
    ranked = (
        frame.sort_values(metric, ascending=False)
        .loc[
            :,
            [
                "month_key",
                "mmsi",
                "active_days",
                "total_hours",
                "total_fishing_hours",
                "active_grid_cells",
                "observation_count",
                "mmsi_review_observations",
                "hours_threshold_observations",
            ],
        ]
        .head(top_n)
        .reset_index(drop=True)
    )
    return ranked


@lru_cache(maxsize=8)
def load_daily_partition(year: int, month: str) -> pd.DataFrame:
    if int(year) in EXCLUDED_YEARS:
        return pd.DataFrame()
    month = str(month).zfill(2)
    # Prefer hive partition folders when present.
    partition = DAILY_GOLD / f"year={year}" / f"month={month}"
    if partition.exists():
        table = ds.dataset(str(partition), format="parquet").to_table()
    else:
        dataset = _dataset(DAILY_GOLD)
        table = dataset.to_table(
            filter=(ds.field("year") == year) & (ds.field("month") == int(month))
        )
    frame = table.to_pandas()
    if "month" in frame.columns:
        frame["month"] = frame["month"].astype(int).astype(str).str.zfill(2)
    return frame


def daily_overview(year: int, month: str) -> dict:
    frame = load_daily_partition(year, month)
    if frame.empty:
        return {
            "rows": 0,
            "vessels": 0,
            "days": 0,
            "total_hours": 0.0,
            "total_fishing_hours": 0.0,
            "status_conflict_rows": 0,
            "review_status_rows": 0,
        }
    return {
        "rows": int(len(frame)),
        "vessels": int(frame["mmsi"].nunique()),
        "days": int(frame["date"].nunique()),
        "total_hours": float(frame["total_hours"].fillna(0).sum()),
        "total_fishing_hours": float(frame["total_fishing_hours"].fillna(0).sum()),
        "status_conflict_rows": int(frame["mmsi_status_conflict"].fillna(False).sum()),
        "review_status_rows": int((frame["mmsi_status"] == "REVIEW").sum()),
    }


def daily_trend(year: int, month: str) -> pd.DataFrame:
    frame = load_daily_partition(year, month)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "vessels",
                "total_hours",
                "total_fishing_hours",
                "observation_count",
            ]
        )
    return (
        frame.groupby("date", as_index=False)
        .agg(
            vessels=("mmsi", "nunique"),
            total_hours=("total_hours", "sum"),
            total_fishing_hours=("total_fishing_hours", "sum"),
            observation_count=("observation_count", "sum"),
        )
        .sort_values("date")
    )


def top_daily_vessels(
    year: int,
    month: str,
    *,
    metric: str = "total_fishing_hours",
    top_n: int = 20,
) -> pd.DataFrame:
    frame = load_daily_partition(year, month)
    if frame.empty:
        return frame
    ranked = (
        frame.groupby("mmsi", as_index=False)
        .agg(
            active_days=("date", "nunique"),
            total_hours=("total_hours", "sum"),
            total_fishing_hours=("total_fishing_hours", "sum"),
            active_grid_cells=("active_grid_cells", "sum"),
            observation_count=("observation_count", "sum"),
            status_conflicts=("mmsi_status_conflict", "sum"),
        )
        .sort_values(metric, ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    return ranked


@lru_cache(maxsize=1)
def load_area(sample_rows: int | None = None) -> pd.DataFrame:
    dataset = _dataset(AREA_GOLD, hive=False)
    if sample_rows is None:
        table = dataset.to_table()
    else:
        # Head is enough for exploratory charts when full scan is heavy.
        table = dataset.head(sample_rows)
    return table.to_pandas()


def area_top(metric: str = "total_fishing_hours", top_n: int = 50) -> pd.DataFrame:
    # Full scan once (cached) — area gold is ~2M rows but columns are narrow.
    frame = load_area()
    cols = [
        "cell_ll_lat",
        "cell_ll_lon",
        "unique_vessels",
        "unique_fishing_vessels",
        "total_hours",
        "total_fishing_hours",
        "active_days",
        "observation_count",
        "fishing_intensity",
        "fishing_hours_per_vessel",
    ]
    return (
        frame.sort_values(metric, ascending=False)
        .loc[:, cols]
        .head(top_n)
        .reset_index(drop=True)
    )


def area_kpis() -> dict:
    frame = load_area()
    return {
        "cells": int(len(frame)),
        "total_hours": float(frame["total_hours"].fillna(0).sum()),
        "total_fishing_hours": float(frame["total_fishing_hours"].fillna(0).sum()),
        "max_vessels_in_cell": int(frame["unique_vessels"].max() or 0),
        "max_fishing_hours_in_cell": float(frame["total_fishing_hours"].max() or 0.0),
    }


def load_dq_summary() -> dict | None:
    if not DQ_SUMMARY.exists():
        return None
    return json.loads(DQ_SUMMARY.read_text(encoding="utf-8"))


def load_dq_review(limit: int = 200) -> pd.DataFrame:
    if not DQ_REVIEW.exists():
        return pd.DataFrame()
    table = ds.dataset(str(DQ_REVIEW), format="parquet").to_table()
    frame = table.to_pandas()
    if frame.empty:
        return frame
    return frame.sort_values("total_hours", ascending=False).head(limit)


def _parquet_row_count_excluding_years(path: Path) -> int:
    """Count parquet rows under hive year= partitions, skipping EXCLUDED_YEARS."""
    if not path.exists():
        return 0
    total = 0
    year_dirs = [p for p in path.iterdir() if p.is_dir() and p.name.startswith("year=")]
    if not year_dirs:
        return parquet_row_count(path)
    for year_dir in year_dirs:
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        if year in EXCLUDED_YEARS:
            continue
        total += parquet_row_count(year_dir)
    return total


def layer_inventory() -> dict:
    return {
        "daily_rows": _parquet_row_count_excluding_years(DAILY_GOLD),
        "monthly_rows": _parquet_row_count_excluding_years(MONTHLY_GOLD),
        "area_rows": parquet_row_count(AREA_GOLD),
        "daily_path": str(DAILY_GOLD),
        "monthly_path": str(MONTHLY_GOLD),
        "area_path": str(AREA_GOLD),
    }
