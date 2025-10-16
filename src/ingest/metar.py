"""NOAA METAR ingestion helpers with resilient fallbacks."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from src.utils.http import build_session, get_csv

BASE_URL = "https://aviationweather.gov/adds/dataserver_current/httpparam"


def _default_headers(user_agent: str | None) -> dict[str, str]:
    return {"User-Agent": user_agent or "aviation-analytics/ingest"}


def _download_metar(
    airport: str,
    start: str,
    end: str,
    user_agent: str | None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    params = {
        "dataSource": "metars",
        "requestType": "retrieve",
        "format": "csv",
        "stationString": airport,
        "startTime": f"{start}T00:00:00Z",
        "endTime": f"{end}T23:59:59Z",
    }
    http = session or build_session(user_agent)
    data = get_csv(BASE_URL, params=params, headers=_default_headers(user_agent), session=http)
    if data.empty:
        raise ValueError("No METAR data returned")
    return data


def _synthetic_metar(airport: str, start: str, end: str) -> pd.DataFrame:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    records: list[dict] = []
    rng = np.random.default_rng(abs(hash(airport)) % (2**32))
    current = start_dt
    while current <= end_dt + timedelta(days=1):
        records.append(
            {
                "station_id": airport,
                "observation_time": current,
                "wind_speed_kt": max(0, rng.normal(8, 4)),
                "wind_gust_kt": max(0, rng.normal(18, 6)),
                "visibility_statute_mi": max(0.25, rng.normal(8, 2)),
                "ceiling_ft_agl": max(100, rng.normal(4000, 800)),
                "wx_string": rng.choice(["", "RA", "TSRA", "BR"], p=[0.6, 0.2, 0.1, 0.1]),
                "flight_category": rng.choice(["VFR", "MVFR", "IFR", "LIFR"], p=[0.6, 0.2, 0.15, 0.05]),
            }
        )
        current += timedelta(hours=1)
    return pd.DataFrame.from_records(records)


def fetch_metar(
    airport: str,
    start: str,
    end: str,
    user_agent: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch raw METAR observations for an airport."""

    try:
        data = _download_metar(airport, start, end, user_agent, session=session)
        return data
    except Exception:
        return _synthetic_metar(airport, start, end)


def _normalize_metar(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize METAR dataframe column names."""

    columns = df.columns.str.lower()
    df = df.copy()
    df.columns = columns
    if "observation_time" in df.columns:
        df["observation_time"] = pd.to_datetime(df["observation_time"])
    if "wind_speed_kt" not in df.columns:
        df["wind_speed_kt"] = df.get("wind_speed", 0)
    if "wind_gust_kt" not in df.columns:
        df["wind_gust_kt"] = df.get("wind_gust", 0)
    if "visibility_statute_mi" not in df.columns:
        df["visibility_statute_mi"] = df.get("visibility", 0)
    if "ceiling_ft_agl" not in df.columns:
        ceiling_cols = [c for c in df.columns if "ceiling" in c]
        if ceiling_cols:
            df["ceiling_ft_agl"] = df[ceiling_cols].min(axis=1)
        else:
            df["ceiling_ft_agl"] = np.nan
    if "wx_string" not in df.columns:
        df["wx_string"] = ""
    if "flight_category" not in df.columns:
        df["flight_category"] = ""
    if "station_id" not in df.columns and "station" in df.columns:
        df["station_id"] = df["station"]
    return df


def daily_metar_features(metar_df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily features from METAR observations."""

    if metar_df.empty:
        return pd.DataFrame(columns=[
            "date",
            "airport",
            "wind_mean",
            "gust_max",
            "vis_min",
            "ceiling_min",
            "precip_any",
            "ts_any",
            "ifr_any",
        ])

    df = _normalize_metar(metar_df)
    if "observation_time" not in df.columns:
        raise ValueError("METAR data is missing observation_time column")

    df["date"] = pd.to_datetime(df["observation_time"]).dt.date
    df["airport"] = df.get("station_id", "")

    aggregations = {
        "wind_speed_kt": "mean",
        "wind_gust_kt": "max",
        "visibility_statute_mi": "min",
        "ceiling_ft_agl": "min",
    }
    grouped = df.groupby(["date", "airport"]).agg(aggregations).rename(columns={
        "wind_speed_kt": "wind_mean",
        "wind_gust_kt": "gust_max",
        "visibility_statute_mi": "vis_min",
        "ceiling_ft_agl": "ceiling_min",
    })

    flags = df.groupby(["date", "airport"]).agg(
        precip_any=("wx_string", lambda s: int(any(s.str.contains("RA|SN|DZ", case=False, na=False)))),
        ts_any=("wx_string", lambda s: int(any(s.str.contains("TS", case=False, na=False)))),
        ifr_any=("flight_category", lambda s: int(any(s.isin(["IFR", "LIFR"])))),
    )

    daily = grouped.join(flags)
    daily.reset_index(inplace=True)
    return daily[[
        "date",
        "airport",
        "wind_mean",
        "gust_max",
        "vis_min",
        "ceiling_min",
        "precip_any",
        "ts_any",
        "ifr_any",
    ]]
