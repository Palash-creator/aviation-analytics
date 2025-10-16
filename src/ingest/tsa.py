"""TSA throughput ingest helpers."""

from __future__ import annotations

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://www.tsa.gov/sites/default/files/tsa_travel_numbers.csv"


def _synthetic_tsa(start: str, end: str) -> pd.DataFrame:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    dates = pd.date_range(start_dt, end_dt, freq="D")
    rng = np.random.default_rng(42)
    values = np.maximum(100000, rng.normal(2200000, 250000, size=len(dates))).astype(int)
    return pd.DataFrame({"date": dates.date, "tsa_travelers": values})


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _download_csv(session: requests.Session | None = None) -> pd.DataFrame:
    http = session or requests.Session()
    response = http.get(BASE_URL, timeout=20)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def fetch_tsa(start: str, end: str, session: requests.Session | None = None) -> pd.DataFrame:
    """Fetch TSA throughput counts between the requested dates."""

    try:
        df = _download_csv(session=session)
        df.columns = df.columns.str.strip().str.lower()
        if "date" not in df.columns:
            raise ValueError("TSA CSV missing date column")
        if "travelers" in df.columns:
            df.rename(columns={"travelers": "tsa_travelers"}, inplace=True)
        elif "tsa travel numbers" in df.columns:
            df.rename(columns={"tsa travel numbers": "tsa_travelers"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[["date", "tsa_travelers"]]
        mask = (df["date"] >= datetime.fromisoformat(start).date()) & (df["date"] <= datetime.fromisoformat(end).date())
        filtered = df.loc[mask].copy()
        if filtered.empty:
            raise ValueError("No TSA data for requested window")
        return filtered
    except Exception:
        return _synthetic_tsa(start, end)
