"""Synthetic BTS OTP ingest utilities.

The real BTS On-Time Performance dataset requires complex queries and
optional authentication. For this prototype, we provide resilient
helpers that attempt to fetch public sample data and gracefully fall
back to deterministic synthetic data when network resources are
unavailable. The downstream aggregation logic mirrors what a production
pipeline would execute.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.dates import date_range

SAMPLE_URL = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/flights-5k.json"


def _icao_to_iata(airport: str) -> str:
    """Convert a four-letter ICAO identifier to a three-letter IATA code."""

    if len(airport) == 4 and airport.startswith("K"):
        return airport[1:]
    return airport


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _download_sample() -> pd.DataFrame:
    """Download a small public flight sample."""

    df = pd.read_json(SAMPLE_URL)
    df["FlightDate"] = pd.to_datetime(df["date"]).dt.date
    df.rename(columns={"origin": "Origin", "destination": "Dest"}, inplace=True)
    df["Cancelled"] = 0
    df["Diverted"] = 0
    return df[["FlightDate", "Origin", "Dest", "Cancelled", "Diverted"]]


def _synthetic_rows(airports: Iterable[str], start: str, end: str) -> pd.DataFrame:
    """Generate deterministic synthetic flight rows for offline usage."""

    start_date = datetime.fromisoformat(str(start)).date()
    end_date = datetime.fromisoformat(str(end)).date()
    records: list[dict] = []
    for airport in airports:
        rng = np.random.default_rng(abs(hash(airport)) % (2**32))
        for day in date_range(start_date, end_date):
            departures = int(rng.normal(350, 40))
            arrivals = int(rng.normal(340, 40))
            departures = max(departures, 50)
            arrivals = max(arrivals, 50)
            cancel_rate = 0.02 + 0.01 * rng.random()
            divert_rate = 0.005 * rng.random()
            for _ in range(departures):
                records.append(
                    {
                        "FlightDate": day,
                        "Origin": _icao_to_iata(airport),
                        "Dest": "ZZZ",
                        "Cancelled": int(rng.random() < cancel_rate),
                        "Diverted": int(rng.random() < divert_rate),
                    }
                )
            for _ in range(arrivals):
                records.append(
                    {
                        "FlightDate": day,
                        "Origin": "ZZZ",
                        "Dest": _icao_to_iata(airport),
                        "Cancelled": int(rng.random() < cancel_rate),
                        "Diverted": int(rng.random() < divert_rate),
                    }
                )
    return pd.DataFrame.from_records(records)


def fetch_otp(airports: list[str], start: str, end: str) -> pd.DataFrame:
    """Fetch BTS OTP rows for selected airports.

    Parameters
    ----------
    airports:
        Airport ICAO identifiers.
    start, end:
        ISO date strings.
    """

    try:
        df = _download_sample()
        mask = df["Origin"].isin({_icao_to_iata(a) for a in airports}) | df["Dest"].isin(
            {_icao_to_iata(a) for a in airports}
        )
        filtered = df.loc[mask].copy()
        filtered = filtered[(filtered["FlightDate"] >= datetime.fromisoformat(start).date()) & (filtered["FlightDate"] <= datetime.fromisoformat(end).date())]
        if filtered.empty:
            raise ValueError("No sample data available for requested airports; using synthetic data")
        return filtered
    except Exception:
        return _synthetic_rows(airports, start, end)


def build_daily_movements(df: pd.DataFrame, airport: str, include_canceled: bool = False) -> pd.DataFrame:
    """Aggregate OTP rows into daily movement counts for an airport."""

    if df.empty:
        return pd.DataFrame(columns=["date", "airport", "dep_count", "arr_count", "movements"])

    df = df.copy()
    df["FlightDate"] = pd.to_datetime(df["FlightDate"]).dt.date
    iata = _icao_to_iata(airport)

    if not include_canceled:
        df = df[(df["Cancelled"] == 0) & (df["Diverted"] == 0)]

    dep_counts = (
        df[df["Origin"] == iata]
        .groupby("FlightDate")
        .size()
        .rename("dep_count")
    )
    arr_counts = (
        df[df["Dest"] == iata]
        .groupby("FlightDate")
        .size()
        .rename("arr_count")
    )

    daily = pd.concat([dep_counts, arr_counts], axis=1).fillna(0)
    daily["movements"] = daily["dep_count"] + daily["arr_count"]
    daily.reset_index(inplace=True)
    daily.rename(columns={"FlightDate": "date"}, inplace=True)
    daily["airport"] = airport
    return daily[["date", "airport", "dep_count", "arr_count", "movements"]]
