"""Plotly figure factories for the Streamlit app."""

from __future__ import annotations
"""Plotly figure factories for the Streamlit app."""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

STATUS_COLORS = {
    "pending": "#636EFA",
    "running": "#FFA15A",
    "success": "#00CC96",
    "warning": "#FECB52",
    "error": "#EF553B",
}

CREDENTIAL_COLORS = {
    "success": "#00CC96",
    "warn": "#FECB52",
    "error": "#EF553B",
    "info": "#AB63FA",
}

CREDENTIAL_VALUES = {
    "success": 1.0,
    "warn": 0.6,
    "error": 0.15,
    "info": 0.4,
}


def indicator_card(title: str, value: float | int | str, suffix: str = "") -> go.Figure:
    """Return a Plotly indicator card."""

    fig = go.Figure(
        go.Indicator(
            mode="number",
            value=value if isinstance(value, (int, float)) else None,
            number={"suffix": suffix},
            title={"text": title},
        )
    )
    if isinstance(value, str):
        fig.data[0].mode = "number"
        fig.data[0].value = None
        fig.data[0].number = {"suffix": suffix, "valueformat": ""}
        fig.add_annotation(text=value, showarrow=False, font=dict(size=32))
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    return fig


def status_timeline(steps: list[dict]) -> go.Figure:
    """Render a timeline showing ingest progress steps."""

    if not steps:
        return go.Figure()

    names = [step["name"] for step in steps]
    starts = [step.get("t_start") for step in steps]
    finishes = [step.get("t_end", datetime.now()) for step in steps]
    durations = [max((finish - start).total_seconds(), 0.1) for start, finish in zip(starts, finishes)]
    colors = [STATUS_COLORS.get(step.get("status", "pending"), STATUS_COLORS["pending"]) for step in steps]

    fig = go.Figure(
        go.Bar(
            x=durations,
            y=names,
            orientation="h",
            marker_color=colors,
            text=[step.get("status", "pending").title() for step in steps],
            hovertext=[
                f"Start: {start}<br>End: {finish}<br>Status: {step.get('status')}"
                for start, finish, step in zip(starts, finishes, steps)
            ],
            hoverinfo="text",
        )
    )
    fig.update_layout(
        xaxis_title="Duration (s)",
        yaxis_title="Step",
        margin=dict(l=100, r=10, t=10, b=40),
        height=300,
    )
    return fig


def mini_timeseries(df: pd.DataFrame, x: str = "date", y: str = "value", title: str = "") -> go.Figure:
    """Render a compact timeseries line chart."""

    fig = go.Figure()
    if not df.empty and x in df.columns and y in df.columns:
        fig.add_trace(
            go.Scatter(x=df[x], y=df[y], mode="lines+markers", line=dict(width=2))
        )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=260, title=title)
    return fig


def build_credential_indicators(statuses: list[dict[str, str]]) -> go.Figure:
    """Render credential status indicators as a single Plotly figure."""

    if not statuses:
        return go.Figure()

    cols = len(statuses)
    fig = make_subplots(rows=1, cols=cols, specs=[[{"type": "indicator"} for _ in range(cols)]], horizontal_spacing=0.12)

    for idx, status in enumerate(statuses, start=1):
        severity = status.get("severity", "info")
        color = CREDENTIAL_COLORS.get(severity, CREDENTIAL_COLORS["info"])
        value = CREDENTIAL_VALUES.get(severity, CREDENTIAL_VALUES["info"])
        fig.add_trace(
            go.Indicator(
                mode="gauge",
                value=value,
                gauge={
                    "shape": "bullet",
                    "axis": {"range": [0, 1], "visible": False},
                    "bar": {"color": color},
                    "bgcolor": "rgba(255, 255, 255, 0.08)",
                },
                domain={"row": 0, "column": idx - 1},
            ),
            row=1,
            col=idx,
        )
        column_center = (idx - 0.5) / cols
        fig.add_annotation(
            x=column_center,
            y=0.85,
            text=f"<b>{status.get('name', '')}</b>",
            showarrow=False,
            font=dict(size=14),
        )
        fig.add_annotation(
            x=column_center,
            y=0.6,
            text=f"<span style='color:{color};font-size:13px'>{status.get('status', '').title()}</span>",
            showarrow=False,
        )
        fig.add_annotation(
            x=column_center,
            y=0.32,
            text=f"<span style='font-size:11px'>{status.get('message', '')}</span>",
            showarrow=False,
        )

    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=10),
        height=220,
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig
