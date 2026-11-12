from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.lib import data as gold_data
from dashboard.lib.charts import area_scatter, bar_ranked
from dashboard.lib.style import brand_header, inject_theme, metric_strip

st.set_page_config(page_title="Area Fishing Gold", layout="wide")
inject_theme()
brand_header(
    "Area fishing activity",
    "Grid-cell Gold for where fishing concentrates: unique vessels, "
    "fishing hours, intensity, and high-presence / low-fishing cells.",
    kicker="build_area_fishing_activity",
)

with st.spinner("Loading area gold (PyArrow)…"):
    kpis = gold_data.area_kpis()
    top_fishing = gold_data.area_top("total_fishing_hours", top_n=40)
    top_vessels = gold_data.area_top("unique_vessels", top_n=40)
    low_intensity = (
        gold_data.load_area()
        .loc[lambda df: df["unique_vessels"] >= 5]
        .sort_values(
            ["fishing_intensity", "unique_vessels"],
            ascending=[True, False],
        )
        .head(25)
    )

metric_strip(
    [
        ("Grid cells", f"{kpis['cells']:,}", "accent"),
        ("Fishing hours", f"{kpis['total_fishing_hours']:,.0f}", "warn"),
        ("Peak vessels/cell", f"{kpis['max_vessels_in_cell']:,}", None),
        (
            "Peak fishing hrs/cell",
            f"{kpis['max_fishing_hours_in_cell']:,.0f}",
            "alert",
        ),
    ]
)

st.plotly_chart(
    area_scatter(
        top_fishing,
        "Top fishing-hour cells (size = fishing hours, color = intensity)",
    ),
    use_container_width=True,
)

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(
        bar_ranked(
            top_fishing.head(15)
            .assign(
                cell=lambda df: df["cell_ll_lat"].round(2).astype(str)
                + ", "
                + df["cell_ll_lon"].round(2).astype(str)
            )
            .iloc[::-1],
            x="total_fishing_hours",
            y="cell",
            title="Top cells by fishing hours",
        ),
        use_container_width=True,
    )
with c2:
    st.plotly_chart(
        bar_ranked(
            top_vessels.head(15)
            .assign(
                cell=lambda df: df["cell_ll_lat"].round(2).astype(str)
                + ", "
                + df["cell_ll_lon"].round(2).astype(str)
            )
            .iloc[::-1],
            x="unique_vessels",
            y="cell",
            title="Top cells by unique vessels",
        ),
        use_container_width=True,
    )

st.subheader("High vessel presence, low fishing intensity")
st.caption("Filter: unique_vessels ≥ 5, ranked by lowest fishing_intensity.")
st.dataframe(
    low_intensity[
        [
            "cell_ll_lat",
            "cell_ll_lon",
            "unique_vessels",
            "total_hours",
            "total_fishing_hours",
            "fishing_intensity",
            "fishing_hours_per_vessel",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
