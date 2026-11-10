from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_ROOT = PROJECT_ROOT / "data" / "processed" / "silver"
AREA_GOLD_OUTPUT = PROJECT_ROOT / "data" / "processed" / "gold" / "area_fishing_activity"


def main():
    print("Loading silver...")
    dataset = ds.dataset(str(SILVER_ROOT), format="parquet")
    table = dataset.to_table(
        columns=[
            "date",
            "mmsi",
            "cell_ll_lat",
            "cell_ll_lon",
            "hours",
            "fishing_hours",
        ]
    )
    df = table.to_pandas()
    print(f"Rows loaded: {len(df):,}")

    df = df.dropna(subset=["cell_ll_lat", "cell_ll_lon"]).copy()
    print(f"Rows with coordinates: {len(df):,}")

    print("Aggregating by grid cell...")
    grouped = df.groupby(["cell_ll_lat", "cell_ll_lon"], sort=False)
    area = grouped.agg(
        unique_vessels=("mmsi", "nunique"),
        total_hours=("hours", "sum"),
        total_fishing_hours=("fishing_hours", "sum"),
        active_days=("date", "nunique"),
        observation_count=("mmsi", "size"),
    ).reset_index()

    fishing_vessels = (
        df.loc[df["fishing_hours"] > 0]
        .groupby(["cell_ll_lat", "cell_ll_lon"], sort=False)["mmsi"]
        .nunique()
        .rename("unique_fishing_vessels")
        .reset_index()
    )
    area = area.merge(
        fishing_vessels,
        on=["cell_ll_lat", "cell_ll_lon"],
        how="left",
    )
    area["unique_fishing_vessels"] = (
        area["unique_fishing_vessels"].fillna(0).astype(int)
    )

    area["fishing_intensity"] = np.where(
        area["total_hours"] > 0,
        area["total_fishing_hours"] / area["total_hours"],
        0.0,
    )
    area["fishing_hours_per_vessel"] = np.where(
        area["unique_vessels"] > 0,
        area["total_fishing_hours"] / area["unique_vessels"],
        0.0,
    )

    print(f"Area cells: {len(area):,}")

    AREA_GOLD_OUTPUT.mkdir(parents=True, exist_ok=True)
    for pattern in ("*.parquet", "*.crc", "_SUCCESS"):
        for path in AREA_GOLD_OUTPUT.glob(pattern):
            path.unlink()

    out_file = AREA_GOLD_OUTPUT / "part-00000.parquet"
    pq.write_table(
        pa.Table.from_pandas(area, preserve_index=False),
        out_file,
    )
    (AREA_GOLD_OUTPUT / "_SUCCESS").write_text("")
    print(f"Wrote: {out_file}")

    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.float_format", lambda x: f"{x:.4f}")

    cols_v = [
        "cell_ll_lat",
        "cell_ll_lon",
        "unique_vessels",
        "unique_fishing_vessels",
        "total_fishing_hours",
        "fishing_intensity",
    ]
    cols_f = [
        "cell_ll_lat",
        "cell_ll_lon",
        "total_fishing_hours",
        "unique_vessels",
        "unique_fishing_vessels",
        "total_hours",
        "fishing_intensity",
    ]
    cols_l = [
        "cell_ll_lat",
        "cell_ll_lon",
        "unique_vessels",
        "total_hours",
        "total_fishing_hours",
        "fishing_intensity",
        "fishing_hours_per_vessel",
    ]

    print("\n" + "=" * 70)
    print("1) TOP 10 khu vuc NHIEU tau xuat hien nhat")
    print("=" * 70)
    print(
        area.sort_values(
            ["unique_vessels", "total_fishing_hours"],
            ascending=[False, False],
        )[cols_v]
        .head(10)
        .to_string(index=False)
    )

    print("\n" + "=" * 70)
    print("2) TOP 10 khu vuc IT tau xuat hien nhat")
    print("=" * 70)
    print(
        area.sort_values(
            ["unique_vessels", "total_fishing_hours"],
            ascending=[True, True],
        )[cols_v]
        .head(10)
        .to_string(index=False)
    )

    print("\n" + "=" * 70)
    print("3) TOP 10 khu vuc TONG thoi gian danh ca nhieu nhat")
    print("=" * 70)
    print(
        area.sort_values(
            ["total_fishing_hours", "unique_vessels"],
            ascending=[False, False],
        )[cols_f]
        .head(10)
        .to_string(index=False)
    )

    v_q75 = area["unique_vessels"].quantile(0.75)
    i_q25 = area["fishing_intensity"].quantile(0.25)
    print(
        f"\nThresholds: unique_vessels >= {v_q75:.0f} (p75), "
        f"fishing_intensity <= {i_q25:.4f} (p25)"
    )

    hl = area[
        (area["unique_vessels"] >= max(v_q75, 5))
        & (area["fishing_intensity"] <= i_q25)
    ].sort_values(
        ["unique_vessels", "fishing_intensity"],
        ascending=[False, True],
    )

    print("\n" + "=" * 70)
    print("4) TOP 10 khu vuc NHIEU tau nhung danh ca THAP")
    print("=" * 70)
    print(hl[cols_l].head(10).to_string(index=False))
    print(f"\nMatched high-presence/low-fishing cells: {len(hl):,}")

    top_v = area.sort_values("unique_vessels", ascending=False).iloc[0]
    bot_v = area.sort_values(
        ["unique_vessels", "total_hours"],
        ascending=[True, True],
    ).iloc[0]
    top_f = area.sort_values(
        "total_fishing_hours",
        ascending=False,
    ).iloc[0]
    top_hl = hl.iloc[0] if len(hl) else None

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(
        "Nhieu tau nhat: "
        f"lat={top_v.cell_ll_lat}, lon={top_v.cell_ll_lon} | "
        f"vessels={int(top_v.unique_vessels)} | "
        f"fishing_h={top_v.total_fishing_hours:.2f}"
    )
    print(
        "It tau nhat: "
        f"lat={bot_v.cell_ll_lat}, lon={bot_v.cell_ll_lon} | "
        f"vessels={int(bot_v.unique_vessels)} | "
        f"fishing_h={bot_v.total_fishing_hours:.2f}"
    )
    print(
        "Fishing h nhieu nhat: "
        f"lat={top_f.cell_ll_lat}, lon={top_f.cell_ll_lon} | "
        f"fishing_h={top_f.total_fishing_hours:.2f} | "
        f"vessels={int(top_f.unique_vessels)}"
    )
    if top_hl is not None:
        print(
            "Nhieu tau/thap fishing: "
            f"lat={top_hl.cell_ll_lat}, lon={top_hl.cell_ll_lon} | "
            f"vessels={int(top_hl.unique_vessels)} | "
            f"intensity={top_hl.fishing_intensity:.4f} | "
            f"fishing_h={top_hl.total_fishing_hours:.2f}"
        )


if __name__ == "__main__":
    main()
