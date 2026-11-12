from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.lib import data as gold_data
from dashboard.lib.charts import line_dual_axis
from dashboard.lib.style import brand_header, inject_theme, metric_strip

st.set_page_config(
    page_title="Maritime Gold Dashboards",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_theme()
brand_header(
    "Maritime Fishing Gold",
    "Interactive views over the Gold layer built by the medallion pipeline: "
    "daily vessel activity, monthly vessel rankings, area fishing intensity, "
    "and daily-hours data-quality outcomes.",
)

inventory = gold_data.layer_inventory()
metric_strip(
    [
        ("Daily Gold rows", f"{inventory['daily_rows']:,}", "accent"),
        ("Monthly Gold rows", f"{inventory['monthly_rows']:,}", None),
        ("Area cells", f"{inventory['area_rows']:,}", None),
        ("Source", "Parquet / PyArrow", "warn"),
    ]
)

st.markdown(
    '<p class="section-note">'
    "Use the sidebar pages for focused analysis. Overview below answers: "
    "how fishing activity moves across months, and which vessels dominate."
    "</p>",
    unsafe_allow_html=True,
)

trend = gold_data.monthly_trend()
left, right = st.columns([1.6, 1])
with left:
    st.plotly_chart(
        line_dual_axis(
            trend,
            x="month_key",
            y_left="total_fishing_hours",
            y_right="vessels",
            title="Monthly fishing hours vs active vessels",
        ),
        use_container_width=True,
    )
with right:
    st.subheader("What each Gold table answers")
    st.markdown(
        """
        - **Daily vessel activity** — one row per vessel per day  
        - **Monthly vessel activity** — who fished most / stayed active longest  
        - **Area fishing activity** — which grid cells concentrate effort  
        - **Daily hours DQ** — NORMAL / INFO / REVIEW for hours > 24h / 48h  
        """
    )
    top = gold_data.top_monthly_vessels(top_n=8)
    st.dataframe(
        top[
            [
                "month_key",
                "mmsi",
                "total_fishing_hours",
                "active_days",
                "active_grid_cells",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.caption(
    "Data roots: "
    f"`{inventory['daily_path']}` · "
    f"`{inventory['monthly_path']}` · "
    f"`{inventory['area_path']}`"
)
