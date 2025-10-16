"""HTTP helpers with retry and credential-aware configuration."""

from __future__ import annotations

import os
from io import StringIO
from typing import Any, Mapping

import pandas as pd
import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential

DEFAULT_TIMEOUT = (15, 30)
_SUCCESS_CODES = {200, 201, 202, 204}


def build_session(user_agent: str | None = None) -> requests.Session:
    """Create a requests session with default headers and optional User-Agent."""

    session = requests.Session()
    headers: dict[str, str] = {
        "Accept": "application/json, text/csv, */*",
    }
    if user_agent:
        headers["User-Agent"] = user_agent
    session.headers.update(headers)
    session.max_redirects = 5
    return session


def _merge_params(url: str, params: Mapping[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = dict(params or {})
    api_key = os.getenv("DATA_GOV_API_KEY", "").strip()
    if api_key and "api.data.gov" in url and "api_key" not in merged:
        merged["api_key"] = api_key
    return merged


def _merge_headers(url: str, headers: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = dict(headers or {})
    flightaware_key = os.getenv("FLIGHTAWARE_API_KEY", "").strip()
    if flightaware_key and "flightaware" in url.lower():
        merged.setdefault("x-apikey", flightaware_key)
    return merged


def _resolve_auth(url: str) -> tuple[str, str] | None:
    user = os.getenv("OPENSKY_USER", "").strip()
    password = os.getenv("OPENSKY_PASS", "").strip()
    if user and password and "opensky-network.org" in url:
        return user, password
    return None


def _raise_for_status(url: str, response: Response) -> None:
    if response.status_code not in _SUCCESS_CODES:
        reason = response.reason or "Unknown error"
        raise RuntimeError(
            f"Request to {url} failed with status {response.status_code}: {reason}."
        )


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=8))
def _get(
    url: str,
    params: Mapping[str, Any] | None,
    headers: Mapping[str, str] | None,
    session: requests.Session | None,
) -> Response:
    http = session or build_session()
    merged_params = _merge_params(url, params)
    merged_headers = _merge_headers(url, headers)
    if merged_headers:
        http.headers.update(merged_headers)
    auth = _resolve_auth(url)
    response = http.get(url, params=merged_params, timeout=DEFAULT_TIMEOUT, auth=auth)
    _raise_for_status(url, response)
    return response


def get_json(
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Fetch JSON content with retries."""

    response = _get(url, params, headers, session)
    return response.json()


def get_csv(
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch CSV content with retries and return a dataframe."""

    response = _get(url, params, headers, session)
    return pd.read_csv(StringIO(response.text))
