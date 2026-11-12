from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.lib import data as gold_data
from dashboard.lib.charts import bar_ranked, line_dual_axis
from dashboard.lib.style import brand_header, inject_theme, metric_strip

st.set_page_config(page_title="Monthly Vessel Gold", layout="wide")
inject_theme()
brand_header(
    "Monthly vessel activity",
    "Answers: which vessels fish the most, which stay active the most days, "
    "and how effort shifts across months.",
    kicker="build_monthly_gold",
)

months = ["All months", *gold_data.available_months()]
month_key = st.selectbox("Scope", months, index=0)
metric = st.radio(
    "Rank by",
    ["total_fishing_hours", "active_days", "total_hours", "active_grid_cells"],
    horizontal=True,
)

selected = None if month_key == "All months" else month_key
trend = gold_data.monthly_trend()
top = gold_data.top_monthly_vessels(selected, metric=metric, top_n=30)

if selected is None:
    metric_strip(
        [
            ("Months", f"{len(trend):,}", None),
            ("Vessel-months", f"{gold_data.layer_inventory()['monthly_rows']:,}", "accent"),
            (
                "Fishing hours",
                f"{trend['total_fishing_hours'].sum():,.0f}",
                "warn",
            ),
            (
                "Review flags",
                f"{int(trend['review_flags'].sum() + trend['hours_review_flags'].sum()):,}",
                "alert",
            ),
        ]
    )
else:
    slice_df = trend.loc[trend["month_key"] == selected]
    row = slice_df.iloc[0] if not slice_df.empty else None
    if row is not None:
        metric_strip(
            [
                ("Vessels", f"{int(row['vessels']):,}", "accent"),
                ("Fishing hours", f"{row['total_fishing_hours']:,.0f}", "warn"),
                ("Active-day sum", f"{int(row['active_days']):,}", None),
                (
                    "Fishing share",
                    f"{(row['fishing_share'] or 0) * 100:.1f}%",
                    None,
                ),
            ]
        )

st.plotly_chart(
    line_dual_axis(
        trend,
        x="month_key",
        y_left="total_fishing_hours",
        y_right="vessels",
        title="Fishing hours and vessel count by month",
    ),
    use_container_width=True,
)

st.plotly_chart(
    bar_ranked(
        top.head(20).iloc[::-1],
        x=metric,
        y="mmsi",
        title=f"Top vessels by {metric}"
        + (f" — {selected}" if selected else " — all months"),
    ),
    use_container_width=True,
)

st.subheader("Ranked vessels")
st.dataframe(top, use_container_width=True, hide_index=True)
