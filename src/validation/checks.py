"""Data validation utilities."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel

from src.utils.dates import coverage_ratio, date_range

Status = Literal["pass", "warn", "fail"]


class CheckResult(BaseModel):
    """Outcome for a validation check."""

    name: str
    status: Status
    message: str
    value: Any | None = None
    expected: Any | None = None


def _schema_check(name: str, df: pd.DataFrame, required: list[str]) -> CheckResult:
    missing = [col for col in required if col not in df.columns]
    status: Status = "pass" if not missing else "fail"
    message = "Schema OK" if not missing else f"Missing columns: {', '.join(missing)}"
    return CheckResult(name=name, status=status, message=message, value=list(df.columns), expected=required)


def _null_check(name: str, df: pd.DataFrame) -> CheckResult:
    total = df.size if not df.empty else 1
    nulls = int(df.isna().sum().sum())
    ratio = nulls / total
    if ratio == 0:
        status: Status = "pass"
    elif ratio < 0.05:
        status = "warn"
    else:
        status = "fail"
    message = f"Null ratio: {ratio:.2%}"
    return CheckResult(name=name, status=status, message=message, value=ratio, expected="<5%")


def _duplicate_check(name: str, df: pd.DataFrame, subset: list[str]) -> CheckResult:
    duplicates = int(df.duplicated(subset=subset).sum())
    status: Status = "pass" if duplicates == 0 else "fail"
    message = "No duplicates" if duplicates == 0 else f"{duplicates} duplicate rows"
    return CheckResult(name=name, status=status, message=message, value=duplicates, expected=0)


def _value_range_check(name: str, series: pd.Series, minimum: float, maximum: float) -> CheckResult:
    if series.empty:
        return CheckResult(name=name, status="warn", message="Series empty", value=None, expected=f"{minimum}-{maximum}")
    below = (series < minimum).sum()
    above = (series > maximum).sum()
    total = len(series)
    if below == 0 and above == 0:
        status: Status = "pass"
        message = "Within expected range"
    elif (below + above) / total < 0.05:
        status = "warn"
        message = f"{below + above} values out of range"
    else:
        status = "fail"
        message = f"{below + above} values out of range"
    return CheckResult(name=name, status=status, message=message, value={"below": int(below), "above": int(above)}, expected=f"{minimum}-{maximum}")


def _coverage_check(name: str, df: pd.DataFrame, start: datetime, end: datetime) -> CheckResult:
    if df.empty or "date" not in df.columns:
        return CheckResult(name=name, status="fail", message="Missing date coverage", value=0.0, expected=1.0)
    ratio = coverage_ratio(pd.to_datetime(df["date"]), start.date(), end.date())
    status: Status = "pass" if ratio >= 0.95 else ("warn" if ratio >= 0.75 else "fail")
    message = f"Coverage: {ratio:.1%}"
    return CheckResult(name=name, status=status, message=message, value=ratio, expected=">=95%")


def run_all_checks(datasets: dict[str, pd.DataFrame], params: dict) -> list[CheckResult]:
    """Run validation checks across ingest outputs."""

    results: list[CheckResult] = []
    start = datetime.fromisoformat(params["start"]) if isinstance(params["start"], str) else params["start"]
    end = datetime.fromisoformat(params["end"]) if isinstance(params["end"], str) else params["end"]

    otp_raw = datasets.get("otp", pd.DataFrame())
    otp_daily = datasets.get("otp_daily", pd.DataFrame())
    metar_daily = datasets.get("metar_daily", pd.DataFrame())
    tsa_daily = datasets.get("tsa_daily", pd.DataFrame())

    results.append(_schema_check("OTP schema", otp_raw, ["FlightDate", "Origin", "Dest", "Cancelled", "Diverted"]))
    results.append(_schema_check("OTP daily schema", otp_daily, ["date", "airport", "dep_count", "arr_count", "movements"]))
    results.append(_schema_check("METAR daily schema", metar_daily, [
        "date",
        "airport",
        "wind_mean",
        "gust_max",
        "vis_min",
        "ceiling_min",
        "precip_any",
        "ts_any",
        "ifr_any",
    ]))
    results.append(_schema_check("TSA schema", tsa_daily, ["date", "tsa_travelers"]))

    results.append(_null_check("OTP nulls", otp_raw))
    results.append(_null_check("METAR nulls", metar_daily))
    results.append(_null_check("TSA nulls", tsa_daily))

    if not otp_daily.empty:
        results.append(_duplicate_check("OTP daily duplicates", otp_daily, ["date", "airport"]))
    if not metar_daily.empty:
        results.append(_duplicate_check("METAR daily duplicates", metar_daily, ["date", "airport"]))
    if not tsa_daily.empty:
        results.append(_duplicate_check("TSA duplicates", tsa_daily, ["date"]))

    results.append(_coverage_check("OTP coverage", otp_daily, start, end))
    results.append(_coverage_check("METAR coverage", metar_daily, start, end))
    results.append(_coverage_check("TSA coverage", tsa_daily, start, end))

    if "movements" in otp_daily.columns:
        results.append(_value_range_check("Daily movements", otp_daily["movements"], 0, 3000))
    if "wind_mean" in metar_daily.columns:
        results.append(_value_range_check("Wind mean", metar_daily["wind_mean"], 0, 150))
    if "gust_max" in metar_daily.columns:
        results.append(_value_range_check("Wind gust", metar_daily["gust_max"], 0, 200))
    if "vis_min" in metar_daily.columns:
        results.append(_value_range_check("Visibility", metar_daily["vis_min"], 0, 15))
    if "ceiling_min" in metar_daily.columns:
        results.append(_value_range_check("Ceiling", metar_daily["ceiling_min"], 0, 20000))
    if "tsa_travelers" in tsa_daily.columns:
        results.append(_value_range_check("TSA travelers", tsa_daily["tsa_travelers"], 1000, 4000000))

    if not otp_daily.empty and not metar_daily.empty and not tsa_daily.empty:
        dates = set(pd.to_datetime(otp_daily["date"]).dt.date)
        dates &= set(pd.to_datetime(metar_daily["date"]).dt.date)
        dates &= set(pd.to_datetime(tsa_daily["date"]).dt.date)
        expected = set(date_range(start.date(), end.date()))
        ratio = len(dates) / len(expected) if expected else 0
        status: Status = "pass" if ratio >= 0.8 else ("warn" if ratio >= 0.5 else "fail")
        results.append(
            CheckResult(
                name="Cross-dataset date overlap",
                status=status,
                message=f"Shared coverage: {ratio:.1%}",
                value=ratio,
                expected=">=80%",
            )
        )

    return results
