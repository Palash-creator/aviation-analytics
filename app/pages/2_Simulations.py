"""Simulations placeholder page."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.utils.plotting import mini_timeseries


def _load_processed(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
    return pd.DataFrame()


def render(config: dict[str, Any], is_admin: bool) -> None:
    st.title("Simulations")
    st.info("Coming next: US map with trajectories, last 20 departures/arrivals, and interactive filters (dark, Plotly).")

    processed_root = Path(config["paths"]["processed"])
    otp_path = processed_root / "otp_daily.parquet"
    otp_df = _load_processed(otp_path)
    if otp_df.empty:
        st.warning("Run the ingest pipeline to see preview data for simulations.")
        return

    otp_df = otp_df.copy()
    otp_df["date"] = pd.to_datetime(otp_df["date"])
    totals = otp_df.groupby("date")["movements"].sum().reset_index()
    fig = mini_timeseries(totals, x="date", y="movements", title="Daily Movements – Preview")
    st.plotly_chart(fig, use_container_width=True)
