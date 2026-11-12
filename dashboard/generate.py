"""Generate interactive HTML dashboards from Gold Parquet (PyArrow + Plotly)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.lib import data as gold_data
from dashboard.lib.charts import (
    area_scatter,
    bar_ranked,
    dq_donut,
    line_dual_axis,
)
from dashboard.lib.paths import PROJECT_ROOT

OUT_DIR = PROJECT_ROOT / "reports" / "dashboards"


def _write(fig: go.Figure, path: Path, title: str) -> None:
    fig.update_layout(title=title)
    path.write_text(
        fig.to_html(full_html=True, include_plotlyjs="cdn"),
        encoding="utf-8",
    )
    print(f"Wrote {path}")


def _index(pages: list[tuple[str, str, str]]) -> str:
    cards = "\n".join(
        f"""
        <a class="card" href="{href}">
          <div class="kicker">{kicker}</div>
          <h2>{title}</h2>
        </a>
        """
        for href, kicker, title in pages
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Maritime Gold Dashboards</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;600&family=Sora:wght@600;700&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --ink:#07141c; --foam:#dff5f2; --teal:#2ec4b6; --amber:#e9b44c; --line:rgba(146,201,214,.18);
    }}
    body {{
      margin:0; font-family:"Instrument Sans",sans-serif; color:var(--foam);
      background:
        radial-gradient(1000px 500px at 10% -10%, rgba(46,196,182,.2), transparent 55%),
        linear-gradient(180deg,#07141c,#102a36);
      min-height:100vh;
    }}
    main {{ max-width:1100px; margin:0 auto; padding:3rem 1.25rem 4rem; }}
    .hero {{
      border:1px solid var(--line); border-radius:20px; padding:1.5rem 1.6rem 1.7rem;
      background:linear-gradient(135deg, rgba(46,196,182,.14), rgba(14,48,62,.4));
      margin-bottom:1.5rem;
    }}
    .kicker {{ color:var(--teal); text-transform:uppercase; letter-spacing:.14em; font-size:.75rem; font-weight:600; }}
    h1 {{ font-family:Sora,sans-serif; margin:.35rem 0 .5rem; font-size:2rem; }}
    p {{ color:rgba(223,245,242,.78); line-height:1.5; max-width:46rem; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:1rem; }}
    .card {{
      display:block; text-decoration:none; color:inherit; border:1px solid var(--line);
      border-radius:16px; padding:1.1rem 1.15rem; background:rgba(8,28,38,.72);
      transition:transform .18s ease, border-color .18s ease;
    }}
    .card:hover {{ transform:translateY(-3px); border-color:rgba(46,196,182,.45); }}
    .card h2 {{ font-family:Sora,sans-serif; font-size:1.15rem; margin:.35rem 0 0; }}
    .meta {{ margin-top:1.5rem; color:rgba(223,245,242,.55); font-size:.9rem; }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="kicker">Maritime Gold Analytics</div>
      <h1>Gold layer dashboards</h1>
      <p>
        Interactive views generated from Parquet produced by
        <code>build_daily_gold</code>, <code>build_monthly_gold</code>,
        <code>build_area_fishing_activity</code>, and
        <code>build_daily_vessel_hours_dq</code>.
      </p>
    </section>
    <div class="grid">{cards}</div>
    <p class="meta">Generated locally from <code>data/processed/gold</code> via PyArrow + Plotly.</p>
  </main>
</body>
</html>
"""


def build_overview() -> Path:
    trend = gold_data.monthly_trend()
    inv = gold_data.layer_inventory()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=trend["month_key"],
            y=trend["total_fishing_hours"],
            name="fishing_hours",
            line=dict(color="#2ec4b6", width=2.5),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=trend["month_key"],
            y=trend["vessels"],
            name="vessels",
            line=dict(color="#e9b44c", width=2.2),
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title=(
            f"Monthly fishing hours vs vessels · "
            f"daily={inv['daily_rows']:,} · monthly={inv['monthly_rows']:,} · "
            f"area={inv['area_rows']:,}"
        ),
        paper_bgcolor="#07141c",
        plot_bgcolor="rgba(7,20,28,0.35)",
        font=dict(color="#dff5f2"),
    )
    fig.update_yaxes(title_text="Fishing hours", secondary_y=False)
    fig.update_yaxes(title_text="Vessels", secondary_y=True)
    path = OUT_DIR / "overview.html"
    path.write_text(fig.to_html(full_html=True, include_plotlyjs="cdn"), encoding="utf-8")
    print(f"Wrote {path}")
    return path


def build_monthly() -> Path:
    trend = gold_data.monthly_trend()
    top = gold_data.top_monthly_vessels(top_n=20)
    fig1 = line_dual_axis(
        trend,
        "month_key",
        "total_fishing_hours",
        "vessels",
        "Monthly fishing hours vs vessels",
    )
    fig2 = bar_ranked(
        top.head(20).iloc[::-1],
        "total_fishing_hours",
        "mmsi",
        "Top vessels by fishing hours (all months)",
    )
    path = OUT_DIR / "monthly.html"
    html = f"""<!doctype html><html><head><meta charset="utf-8"/><title>Monthly Gold</title></head>
<body style="margin:0;background:#07141c;color:#dff5f2;font-family:sans-serif">
<h1 style="padding:1rem 1.25rem 0">Monthly vessel activity</h1>
{fig1.to_html(full_html=False, include_plotlyjs="cdn")}
{fig2.to_html(full_html=False, include_plotlyjs=False)}
</body></html>"""
    path.write_text(html, encoding="utf-8")
    print(f"Wrote {path}")
    return path


def build_daily_latest() -> Path:
    months = gold_data.available_months()
    if not months:
        raise RuntimeError("No monthly partitions available for daily dashboard")
    month_key = months[-1]
    year_s, month_s = month_key.split("-")
    year_i = int(year_s)
    trend = gold_data.daily_trend(year_i, month_s)
    top = gold_data.top_daily_vessels(year_i, month_s, top_n=20)
    overview = gold_data.daily_overview(year_i, month_s)
    fig1 = line_dual_axis(
        trend,
        "date",
        "total_fishing_hours",
        "vessels",
        f"Daily fishing hours & vessels — {month_key}",
    )
    fig2 = bar_ranked(
        top.head(20).iloc[::-1],
        "total_fishing_hours",
        "mmsi",
        f"Top vessels — {month_key}",
    )
    path = OUT_DIR / "daily.html"
    html = f"""<!doctype html><html><head><meta charset="utf-8"/><title>Daily Gold</title></head>
<body style="margin:0;background:#07141c;color:#dff5f2;font-family:sans-serif">
<h1 style="padding:1rem 1.25rem 0">Daily vessel activity ({month_key})</h1>
<p style="padding:0 1.25rem">Vessel-days={overview['rows']:,} · vessels={overview['vessels']:,} ·
fishing hours={overview['total_fishing_hours']:,.0f} · conflicts={overview['status_conflict_rows']:,}</p>
{fig1.to_html(full_html=False, include_plotlyjs="cdn")}
{fig2.to_html(full_html=False, include_plotlyjs=False)}
</body></html>"""
    path.write_text(html, encoding="utf-8")
    print(f"Wrote {path}")
    return path


def build_area() -> Path:
    top = gold_data.area_top("total_fishing_hours", top_n=80)
    kpis = gold_data.area_kpis()
    fig1 = area_scatter(
        top,
        "Top fishing-hour cells (size=fishing hours, color=intensity)",
    )
    labeled = top.head(15).assign(
        cell=lambda df: df["cell_ll_lat"].round(2).astype(str)
        + ", "
        + df["cell_ll_lon"].round(2).astype(str)
    )
    fig2 = bar_ranked(
        labeled.iloc[::-1],
        "total_fishing_hours",
        "cell",
        "Top cells by fishing hours",
    )
    path = OUT_DIR / "area.html"
    html = f"""<!doctype html><html><head><meta charset="utf-8"/><title>Area Gold</title></head>
<body style="margin:0;background:#07141c;color:#dff5f2;font-family:sans-serif">
<h1 style="padding:1rem 1.25rem 0">Area fishing activity</h1>
<p style="padding:0 1.25rem">Cells={kpis['cells']:,} · fishing hours={kpis['total_fishing_hours']:,.0f} ·
peak vessels/cell={kpis['max_vessels_in_cell']:,}</p>
{fig1.to_html(full_html=False, include_plotlyjs="cdn")}
{fig2.to_html(full_html=False, include_plotlyjs=False)}
</body></html>"""
    path.write_text(html, encoding="utf-8")
    print(f"Wrote {path}")
    return path


def build_dq() -> Path | None:
    doc = gold_data.load_dq_summary()
    if not doc:
        print("Skipping DQ dashboard: summary.json missing")
        return None
    summary = doc["summary"]
    fig = dq_donut(summary)
    path = OUT_DIR / "hours_dq.html"
    html = f"""<!doctype html><html><head><meta charset="utf-8"/><title>Hours DQ</title></head>
<body style="margin:0;background:#07141c;color:#dff5f2;font-family:sans-serif">
<h1 style="padding:1rem 1.25rem 0">Daily vessel hours DQ</h1>
<pre style="padding:0 1.25rem;color:#9fd0c8">{json.dumps(summary, indent=2)}</pre>
{fig.to_html(full_html=False, include_plotlyjs="cdn")}
</body></html>"""
    path.write_text(html, encoding="utf-8")
    print(f"Wrote {path}")
    return path


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    build_overview()
    build_monthly()
    build_daily_latest()
    build_area()
    build_dq()
    index = OUT_DIR / "index.html"
    index.write_text(
        _index(
            [
                ("overview.html", "overview", "Month trends & Gold scale"),
                ("daily.html", "build_daily_gold", "Daily vessel activity"),
                ("monthly.html", "build_monthly_gold", "Monthly vessel rankings"),
                ("area.html", "build_area_fishing_activity", "Area fishing intensity"),
                ("hours_dq.html", "build_daily_vessel_hours_dq", "Daily hours DQ"),
            ]
        ),
        encoding="utf-8",
    )
    print(f"Wrote {index}")
    print(f"Open: {index}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
