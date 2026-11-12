from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(7, 20, 28, 0.25)",
    font=dict(color="#dff5f2", family="Instrument Sans"),
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)


def _apply(fig: go.Figure) -> go.Figure:
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_xaxes(gridcolor="rgba(146, 201, 214, 0.12)", zeroline=False)
    fig.update_yaxes(gridcolor="rgba(146, 201, 214, 0.12)", zeroline=False)
    return fig


def line_dual_axis(
    frame: pd.DataFrame,
    x: str,
    y_left: str,
    y_right: str,
    title: str,
) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=frame[x],
            y=frame[y_left],
            name=y_left,
            mode="lines",
            line=dict(color="#2ec4b6", width=2.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=frame[x],
            y=frame[y_right],
            name=y_right,
            mode="lines",
            line=dict(color="#e9b44c", width=2.2),
            yaxis="y2",
        )
    )
    fig.update_layout(
        title=title,
        yaxis=dict(title=y_left),
        yaxis2=dict(title=y_right, overlaying="y", side="right"),
    )
    return _apply(fig)


def bar_ranked(frame: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    fig = px.bar(
        frame,
        x=x,
        y=y,
        title=title,
        color_discrete_sequence=["#2ec4b6"],
    )
    fig.update_layout(xaxis_title="", yaxis_title=y)
    return _apply(fig)


def area_scatter(frame: pd.DataFrame, title: str) -> go.Figure:
    fig = px.scatter(
        frame,
        x="cell_ll_lon",
        y="cell_ll_lat",
        size="total_fishing_hours",
        color="fishing_intensity",
        hover_data=[
            "unique_vessels",
            "unique_fishing_vessels",
            "total_hours",
            "active_days",
        ],
        color_continuous_scale=["#0c2430", "#2ec4b6", "#e9b44c"],
        title=title,
    )
    fig.update_traces(marker=dict(line=dict(width=0), opacity=0.85))
    fig.update_layout(
        xaxis_title="Longitude",
        yaxis_title="Latitude",
        coloraxis_colorbar_title="Intensity",
    )
    return _apply(fig)


def dq_donut(summary: dict) -> go.Figure:
    labels = ["NORMAL", "INFO", "REVIEW", "REVIEW_NULL", "REVIEW_NEGATIVE"]
    keys = [
        "normal_records",
        "info_records",
        "review_records",
        "null_hours_records",
        "negative_hours_records",
    ]
    values = [int(summary.get(key, 0)) for key in keys]
    colors = ["#2ec4b6", "#e9b44c", "#ef6f6c", "#8aa4b0", "#c084fc"]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.62,
                marker=dict(colors=colors),
                textinfo="label+percent",
            )
        ]
    )
    fig.update_layout(title="Daily hours DQ mix")
    return _apply(fig)
