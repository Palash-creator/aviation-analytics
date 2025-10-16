"""Environment and credential helpers for the aviation analytics app."""

from __future__ import annotations

import os
import re
from typing import Iterable

from pathlib import Path

from dotenv import load_dotenv

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def load_env(dotenv_path: str | None = None) -> None:
    """Load environment variables from a .env file if present.

    Streamlit can execute pages from nested working directories, so we resolve the
    repository root relative to this module to locate the default ``.env`` file.
    """

    if dotenv_path is None:
        repo_root = Path(__file__).resolve().parents[2]
        candidate = repo_root / ".env"
        dotenv_path = str(candidate) if candidate.exists() else None

    load_dotenv(dotenv_path=dotenv_path, override=False)


def get_env_bool(name: str, default: bool = False) -> bool:
    """Return the boolean interpretation of an environment variable."""

    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in _TRUE_VALUES


def require_env(keys: Iterable[str]) -> dict[str, str]:
    """Return required environment variables or raise an informative error."""

    missing: list[str] = []
    values: dict[str, str] = {}
    for key in keys:
        value = os.getenv(key, "").strip()
        if not value:
            missing.append(key)
        else:
            values[key] = value
    if missing:
        formatted = ", ".join(missing)
        raise ValueError(
            "Missing required environment variables: "
            f"{formatted}. Update your .env file and restart the app."
        )
    return values


def optional_env(keys: Iterable[str]) -> dict[str, str]:
    """Return only the environment variables that are set and non-empty."""

    values: dict[str, str] = {}
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            values[key] = value
    return values


def validate_credentials() -> list[dict[str, str]]:
    """Inspect credential-related environment variables and summarize their status."""

    load_env()
    statuses: list[dict[str, str]] = []

    data_gov_key = os.getenv("DATA_GOV_API_KEY", "").strip()
    if data_gov_key:
        statuses.append(
            {
                "name": "Data.gov", "status": "Present", "severity": "success",
                "message": "API key configured for higher request limits."
            }
        )
    else:
        statuses.append(
            {
                "name": "Data.gov", "status": "Optional", "severity": "warn",
                "message": "Set DATA_GOV_API_KEY to avoid anonymous rate limits."
            }
        )

    noaa_user_agent = os.getenv("NOAA_USER_AGENT", "").strip()
    if _EMAIL_PATTERN.match(noaa_user_agent):
        statuses.append(
            {
                "name": "NOAA User-Agent", "status": "OK", "severity": "success",
                "message": "Using configured email for NOAA METAR requests."
            }
        )
    elif noaa_user_agent:
        statuses.append(
            {
                "name": "NOAA User-Agent", "status": "Invalid", "severity": "error",
                "message": "Provide a valid email (e.g. yourname@example.com)."
            }
        )
    else:
        statuses.append(
            {
                "name": "NOAA User-Agent", "status": "Missing", "severity": "error",
                "message": "Set NOAA_USER_AGENT to an email to avoid request rejections."
            }
        )

    statuses.append(
        {
            "name": "TSA Throughput", "status": "Public", "severity": "success",
            "message": "Dataset is public and requires no credentials."
        }
    )

    opensky_user = os.getenv("OPENSKY_USER", "").strip()
    opensky_pass = os.getenv("OPENSKY_PASS", "").strip()
    if opensky_user and opensky_pass:
        statuses.append(
            {
                "name": "OpenSky", "status": "Configured", "severity": "success",
                "message": "Credentials ready for Step-2 trajectory features."
            }
        )
    else:
        statuses.append(
            {
                "name": "OpenSky", "status": "Not configured", "severity": "info",
                "message": "Optional – set OPENSKY_USER and OPENSKY_PASS when needed."
            }
        )

    flightaware_key = os.getenv("FLIGHTAWARE_API_KEY", "").strip()
    if flightaware_key:
        statuses.append(
            {
                "name": "FlightAware", "status": "Configured", "severity": "success",
                "message": "API key present for future AeroAPI integrations."
            }
        )
    else:
        statuses.append(
            {
                "name": "FlightAware", "status": "Not configured", "severity": "info",
                "message": "Optional – add FLIGHTAWARE_API_KEY when ready."
            }
        )

    return statuses
