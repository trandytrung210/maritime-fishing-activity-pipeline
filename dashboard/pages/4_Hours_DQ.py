from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.lib import data as gold_data
from dashboard.lib.charts import dq_donut
from dashboard.lib.style import brand_header, inject_theme, metric_strip

st.set_page_config(page_title="Daily Hours DQ", layout="wide")
inject_theme()
brand_header(
    "Daily vessel hours DQ",
    "Quality report from Gold daily totals: NORMAL (≤24h), INFO (≤48h), "
    "REVIEW (>48h), plus null/negative guards.",
    kicker="build_daily_vessel_hours_dq",
)

summary_doc = gold_data.load_dq_summary()
if not summary_doc:
    st.warning(
        "No DQ summary found at `reports/daily_vessel_hours_dq/summary.json`. "
        "Run the daily hours DQ Gold task first."
    )
    st.stop()

summary = summary_doc.get("summary", {})
metric_strip(
    [
        ("Total records", f"{summary.get('total_records', 0):,}", "accent"),
        ("NORMAL", f"{summary.get('normal_records', 0):,}", None),
        ("INFO", f"{summary.get('info_records', 0):,}", "warn"),
        ("REVIEW", f"{summary.get('review_records', 0):,}", "alert"),
    ]
)

c1, c2 = st.columns([1.1, 1])
with c1:
    st.plotly_chart(dq_donut(summary), use_container_width=True)
with c2:
    st.subheader("Rules")
    rules = summary_doc.get("rules", {})
    for name, rule in rules.items():
        st.markdown(f"- **{name}**: `{rule}`")
    st.caption(
        f"Generated at (UTC): {summary_doc.get('generated_at_utc', 'n/a')}"
    )

review = gold_data.load_dq_review(limit=200)
st.subheader("REVIEW records (hours > 48)")
if review.empty:
    st.success("No REVIEW parquet rows (or review output is empty).")
else:
    st.dataframe(review, use_container_width=True, hide_index=True)
