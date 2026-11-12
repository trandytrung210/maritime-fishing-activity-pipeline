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

st.set_page_config(page_title="Daily Vessel Gold", layout="wide")
inject_theme()
brand_header(
    "Daily vessel activity",
    "Gold table built as one row per vessel per day: hours, fishing hours, "
    "active grid cells, and MMSI status consistency flags.",
    kicker="build_daily_gold",
)

months = gold_data.available_months()
if not months:
    st.warning("No monthly gold partitions found to drive the month filter.")
    st.stop()

default_idx = len(months) - 1
month_key = st.selectbox("Month partition", months, index=default_idx)
year_s, month_s = month_key.split("-")
year_i = int(year_s)

with st.spinner(f"Loading daily gold for {month_key}…"):
    overview = gold_data.daily_overview(year_i, month_s)
    trend = gold_data.daily_trend(year_i, month_s)
    top = gold_data.top_daily_vessels(year_i, month_s, top_n=25)

metric_strip(
    [
        ("Vessel-days", f"{overview['rows']:,}", "accent"),
        ("Unique vessels", f"{overview['vessels']:,}", None),
        ("Fishing hours", f"{overview['total_fishing_hours']:,.0f}", "warn"),
        ("Status conflicts", f"{overview['status_conflict_rows']:,}", "alert"),
    ]
)

c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(
        line_dual_axis(
            trend,
            x="date",
            y_left="total_fishing_hours",
            y_right="vessels",
            title=f"Daily fishing hours & vessels — {month_key}",
        ),
        use_container_width=True,
    )
with c2:
    st.plotly_chart(
        bar_ranked(
            top.head(15).iloc[::-1],
            x="total_fishing_hours",
            y="mmsi",
            title="Top vessels by fishing hours (month)",
        ),
        use_container_width=True,
    )

st.subheader("Top vessels detail")
st.dataframe(top, use_container_width=True, hide_index=True)
st.caption(
    "Source: `data/processed/gold/daily_vessel_activity` "
    "(partition-filtered via PyArrow)."
)
