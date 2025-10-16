"""Admin ingest workflow."""

from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.ingest import metar, otp, tsa
from src.utils.http import build_session, get_csv, get_json
from src.utils.dates import DateWindow, coverage_ratio, window_from_days_back
from src.utils.io import write_csv, write_manifest, write_parquet
from src.utils.logging import log_ingest
from src.utils.plotting import (
    build_credential_indicators,
    indicator_card,
    mini_timeseries,
    status_timeline,
)
from src.utils.secrets import validate_credentials
from src.validation.checks import CheckResult, run_all_checks


DATA_GOV_PING_URL = "https://api.data.gov/ed/collegescorecard/v1/schools.json"
NOAA_TEST_STATION = "KATL"
NOAA_LOOKBACK_HOURS = 6
TSA_PREVIEW_ROWS = 5


@st.cache_data(show_spinner=False)
def fetch_otp_cached(airports: tuple[str, ...], start: str, end: str) -> pd.DataFrame:
    return otp.fetch_otp(list(airports), start, end)


@st.cache_data(show_spinner=False)
def fetch_metar_cached(airport: str, start: str, end: str, user_agent: str) -> pd.DataFrame:
    session = _get_http_session(user_agent)
    return metar.fetch_metar(airport, start, end, user_agent, session=session)


@st.cache_data(show_spinner=False)
def fetch_tsa_cached(start: str, end: str) -> pd.DataFrame:
    session = _get_http_session(None)
    return tsa.fetch_tsa(start, end, session=session)


@st.cache_resource(show_spinner=False)
def _get_http_session(user_agent: str | None):
    return build_session(user_agent)


def _format_error(exc: Exception) -> str:
    message = str(exc)
    if len(message) > 120:
        return f"{message[:117]}..."
    return message


def _run_credential_tests() -> list[dict[str, str]]:
    results: list[dict[str, str]] = []

    data_gov_key = os.getenv("DATA_GOV_API_KEY", "").strip()
    if data_gov_key:
        try:
            get_json(DATA_GOV_PING_URL, params={"per_page": 1})
            results.append(
                {
                    "name": "Data.gov", "status": "OK", "severity": "success",
                    "message": "API key responded successfully.",
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "name": "Data.gov", "status": "Error", "severity": "error",
                    "message": _format_error(exc),
                }
            )
    else:
        results.append(
            {
                "name": "Data.gov", "status": "Skipped", "severity": "warn",
                "message": "Optional key not configured; using public endpoints.",
            }
        )

    noaa_user_agent = os.getenv("NOAA_USER_AGENT", "").strip()
    if not noaa_user_agent or "@" not in noaa_user_agent:
        results.append(
            {
                "name": "NOAA METAR", "status": "Invalid", "severity": "error",
                "message": "Set NOAA_USER_AGENT to a valid email for authenticated requests.",
            }
        )
    else:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=NOAA_LOOKBACK_HOURS)
        params = {
            "dataSource": "metars",
            "requestType": "retrieve",
            "format": "csv",
            "stationString": NOAA_TEST_STATION,
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        try:
            session = _get_http_session(noaa_user_agent)
            df = get_csv(metar.BASE_URL, params=params, headers={"User-Agent": noaa_user_agent}, session=session)
            if df.empty:
                results.append(
                    {
                        "name": "NOAA METAR", "status": "No data", "severity": "warn",
                        "message": "No recent METAR observations returned for test station.",
                    }
                )
            else:
                results.append(
                    {
                        "name": "NOAA METAR", "status": "OK", "severity": "success",
                        "message": f"Received {len(df)} observations for {NOAA_TEST_STATION}.",
                    }
                )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "name": "NOAA METAR", "status": "Error", "severity": "error",
                    "message": _format_error(exc),
                }
            )

    try:
        session = _get_http_session(None)
        tsa_df = get_csv(tsa.BASE_URL, session=session)
        preview = len(tsa_df.head(TSA_PREVIEW_ROWS))
        if preview:
            results.append(
                {
                    "name": "TSA Throughput", "status": "OK", "severity": "success",
                    "message": f"Fetched {preview} sample rows from TSA CSV.",
                }
            )
        else:
            results.append(
                {
                    "name": "TSA Throughput", "status": "Empty", "severity": "warn",
                    "message": "CSV returned no rows during test.",
                }
            )
    except Exception as exc:  # noqa: BLE001
        results.append(
            {
                "name": "TSA Throughput", "status": "Error", "severity": "error",
                "message": _format_error(exc),
            }
        )

    return results


def _hash_dataframe(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    digest = pd.util.hash_pandas_object(df, index=False).values.tobytes()
    return hashlib.sha256(digest).hexdigest()


def _write_raw_outputs(otp_df: pd.DataFrame, metar_dfs: dict[str, pd.DataFrame], tsa_df: pd.DataFrame, raw_root: Path) -> None:
    if not otp_df.empty:
        otp_copy = otp_df.copy()
        otp_copy["date"] = pd.to_datetime(otp_copy["FlightDate"]).dt.date
        for airport in sorted(set(otp_copy["Origin"]) | set(otp_copy["Dest"])):
            airport_dir = raw_root / "otp" / str(airport)
            subset = otp_copy[(otp_copy["Origin"] == airport) | (otp_copy["Dest"] == airport)]
            for day, day_df in subset.groupby("date"):
                write_csv(day_df.drop(columns=["date"]), airport_dir / f"{day}.csv")

    for airport, df in metar_dfs.items():
        if df.empty:
            continue
        airport_dir = raw_root / "metar" / airport
        df = df.copy()
        obs_time = df.get("observation_time")
        if obs_time is None:
            obs_time = df.get("time", datetime.utcnow())
        df["obs_date"] = pd.to_datetime(obs_time).dt.date
        for day, day_df in df.groupby("obs_date"):
            write_csv(day_df, airport_dir / f"{day}.csv")

    if not tsa_df.empty:
        tsa_dir = raw_root / "tsa" / "national"
        tsa_copy = tsa_df.copy()
        tsa_copy["date"] = pd.to_datetime(tsa_copy["date"]).dt.date
        for day, day_df in tsa_copy.groupby("date"):
            write_csv(day_df, tsa_dir / f"{day}.csv")


def _run_validations(datasets: dict[str, pd.DataFrame], params: dict[str, Any]) -> list[CheckResult]:
    return run_all_checks(datasets, params)


def _write_processed_outputs(datasets: dict[str, pd.DataFrame], params: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    processed_root = Path(config["paths"]["processed"])
    processed_root.mkdir(parents=True, exist_ok=True)

    otp_daily = datasets.get("otp_daily", pd.DataFrame())
    metar_daily = datasets.get("metar_daily", pd.DataFrame())
    tsa_daily = datasets.get("tsa_daily", pd.DataFrame())

    write_parquet(otp_daily, processed_root / "otp_daily.parquet")
    write_parquet(metar_daily, processed_root / "wx_daily.parquet")
    write_parquet(tsa_daily, processed_root / "tsa_daily.parquet")

    manifest = {
        "run_timestamp": datetime.utcnow().isoformat(),
        "params": params,
        "rows": {
            "otp_daily": int(len(otp_daily)),
            "metar_daily": int(len(metar_daily)),
            "tsa_daily": int(len(tsa_daily)),
        },
        "hashes": {
            "otp_daily": _hash_dataframe(otp_daily),
            "metar_daily": _hash_dataframe(metar_daily),
            "tsa_daily": _hash_dataframe(tsa_daily),
        },
    }
    write_manifest(manifest, processed_root / "manifest.json")
    return manifest


def _validation_summary(checks: list[CheckResult]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Check": c.name,
                "Status": c.status,
                "Message": c.message,
            }
            for c in checks
        ]
    )


def _render_kpis(otp_df: pd.DataFrame, otp_daily: pd.DataFrame, checks: list[CheckResult], window: DateWindow, airports: list[str]) -> None:
    total_rows = len(otp_df)
    first_airport = airports[0] if airports else None
    coverage = 0.0
    if first_airport and not otp_daily.empty:
        airport_df = otp_daily[otp_daily["airport"] == first_airport]
        coverage = coverage_ratio(pd.to_datetime(airport_df["date"]), window.start, window.end)
    passed = sum(1 for check in checks if check.status == "pass")
    total = len(checks)

    col1, col2, col3 = st.columns(3)
    col1.plotly_chart(indicator_card("OTP Rows", total_rows), use_container_width=True)
    col2.plotly_chart(indicator_card("Coverage", round(coverage * 100, 1), suffix="%"), use_container_width=True)
    col3.plotly_chart(indicator_card("Checks", passed, suffix=f"/{total}"), use_container_width=True)


def _render_dataset_section(title: str, df: pd.DataFrame, date_col: str, value_col: str, sample_cols: list[str]) -> None:
    with st.expander(title, expanded=True):
        if df.empty:
            st.warning("No data available yet.")
            return
        plot_df = df.copy()
        plot_df[date_col] = pd.to_datetime(plot_df[date_col])
        plot_df = plot_df.sort_values(date_col)
        fig = mini_timeseries(plot_df, x=date_col, y=value_col, title=f"{title} – {value_col}")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(f"**Last date:** {plot_df[date_col].max().date()}")
        available_cols = [col for col in sample_cols if col in df.columns]
        st.dataframe(df[available_cols].head(10), use_container_width=True)


def _display_results(ingest_state: dict[str, Any]) -> None:
    datasets = ingest_state.get("datasets", {})
    checks = ingest_state.get("checks", [])
    params = ingest_state.get("params", {})
    start = params.get("start")
    end = params.get("end")
    if start and end:
        window = DateWindow(start=datetime.fromisoformat(start).date(), end=datetime.fromisoformat(end).date())
    else:
        window = DateWindow(start=datetime.utcnow().date(), end=datetime.utcnow().date())
    airports = params.get("airports", [])
    _render_kpis(datasets.get("otp", pd.DataFrame()), datasets.get("otp_daily", pd.DataFrame()), checks, window, airports)
    summary_df = _validation_summary(checks)
    st.subheader("Validation summary")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    _render_dataset_section(
        "OTP Daily Movements",
        datasets.get("otp_daily", pd.DataFrame()),
        "date",
        "movements",
        ["date", "airport", "dep_count", "arr_count", "movements"],
    )
    _render_dataset_section(
        "METAR Daily Features",
        datasets.get("metar_daily", pd.DataFrame()),
        "date",
        "wind_mean",
        ["date", "airport", "wind_mean", "gust_max", "vis_min", "ceiling_min", "precip_any", "ts_any", "ifr_any"],
    )
    _render_dataset_section(
        "TSA Throughput",
        datasets.get("tsa_daily", pd.DataFrame()),
        "date",
        "tsa_travelers",
        ["date", "tsa_travelers"],
    )

    if any(check.status == "fail" for check in checks):
        st.error("One or more validation checks failed. Please review the dataset sections above.")


def render(config: dict[str, Any], is_admin: bool) -> None:
    if not is_admin:
        st.error("You do not have access to this page.")
        st.stop()

    st.title("Admin · Ingest")

    credential_statuses = validate_credentials()
    st.subheader("Credential status")
    st.plotly_chart(build_credential_indicators(credential_statuses), use_container_width=True)

    if any(status["name"] == "NOAA User-Agent" and status["severity"] == "error" for status in credential_statuses):
        st.error(
            "NOAA User-Agent is missing or invalid. Update NOAA_USER_AGENT in your .env "
            "to avoid NOAA API rejections."
        )

    if "credential_tests" not in st.session_state:
        st.session_state["credential_tests"] = None

    if st.button("Test Credentials"):
        with st.spinner("Running credential smoke tests..."):
            st.session_state["credential_tests"] = _run_credential_tests()

    if st.session_state.get("credential_tests"):
        st.plotly_chart(
            build_credential_indicators(st.session_state["credential_tests"]),
            use_container_width=True,
        )

    timezone = config["app"].get("timezone", "UTC")
    default_days = int(config["app"].get("ingest_days_back", 365))
    default_airports = config["app"].get("default_airports", [])
    window_default = window_from_days_back(default_days, timezone)

    with st.sidebar:
        st.header("Controls")
        airports = st.multiselect("Airports", options=default_airports, default=default_airports)
        date_selection = st.date_input(
            "Date range",
            value=(window_default.start, window_default.end),
            max_value=window_default.end,
        )
        if isinstance(date_selection, tuple):
            start_date, end_date = date_selection
        else:
            start_date = date_selection
            end_date = date_selection
        include_canceled = st.checkbox("Include canceled/diverted", value=False)
        run_button = st.button("Run Ingest", type="primary")
        rerun_checks = st.button("Re-run Validations")
        save_outputs = st.button("Save Outputs")

    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    window = DateWindow(start=start_date, end=end_date)

    if run_button and not airports:
        st.warning("Select at least one airport to run ingest.")
    if run_button and airports:
        st.session_state.pop("ingest_data", None)
        timeline_placeholder = st.empty()
        progress = st.progress(0)
        steps: list[dict[str, Any]] = []

        def _start_step(name: str) -> dict[str, Any]:
            step = {"name": name, "t_start": datetime.utcnow(), "status": "running"}
            steps.append(step)
            timeline_placeholder.plotly_chart(status_timeline(steps), use_container_width=True)
            return step

        def _end_step(step: dict[str, Any], status: str) -> None:
            step["status"] = status
            step["t_end"] = datetime.utcnow()
            timeline_placeholder.plotly_chart(status_timeline(steps), use_container_width=True)

        with st.status("Ingest running...", expanded=True) as status_widget:
            try:
                start_time = time.time()
                status_widget.write("Starting OTP pull")
                step = _start_step("OTP pull")
                otp_raw = fetch_otp_cached(tuple(sorted(airports)), start_iso, end_iso)
                _end_step(step, "success")
                progress.progress(20)

                status_widget.write("Aggregating daily movements")
                step = _start_step("Daily movements")
                daily_frames = [
                    otp.build_daily_movements(otp_raw, airport, include_canceled=include_canceled)
                    for airport in airports
                ]
                otp_daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
                _end_step(step, "success")
                progress.progress(35)

                status_widget.write("Fetching METAR data")
                step = _start_step("METAR pull")
                user_agent = os.getenv("NOAA_USER_AGENT", "")
                metar_data = {
                    airport: fetch_metar_cached(airport, start_iso, end_iso, user_agent)
                    for airport in airports
                }
                metar_daily_frames = []
                for airport, df in metar_data.items():
                    try:
                        metar_daily_frames.append(metar.daily_metar_features(df))
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"Failed to compute METAR features for {airport}: {exc}")
                metar_daily = pd.concat(metar_daily_frames, ignore_index=True) if metar_daily_frames else pd.DataFrame()
                _end_step(step, "success")
                progress.progress(55)

                status_widget.write("Fetching TSA throughput")
                step = _start_step("TSA pull")
                tsa_daily = fetch_tsa_cached(start_iso, end_iso)
                _end_step(step, "success")
                progress.progress(70)

                status_widget.write("Running validations")
                step = _start_step("Validations")
                datasets = {
                    "otp": otp_raw,
                    "otp_daily": otp_daily,
                    "metar_daily": metar_daily,
                    "tsa_daily": tsa_daily,
                }
                params = {
                    "start": start_iso,
                    "end": end_iso,
                    "airports": airports,
                    "include_canceled": include_canceled,
                }
                checks = _run_validations(datasets, params)
                _end_step(step, "success")
                progress.progress(85)

                status_widget.write("Writing outputs")
                step = _start_step("Write Parquet")
                raw_root = Path(config["paths"]["raw"])
                _write_raw_outputs(otp_raw, metar_data, tsa_daily, raw_root)
                manifest = _write_processed_outputs(datasets, params, config)
                _end_step(step, "success")
                progress.progress(100)

                duration = time.time() - start_time
                status_widget.update(label="Ingest complete", state="complete")
                st.success("Ingest complete ✅")

                log_ingest(
                    {
                        "airports": airports,
                        "start": start_iso,
                        "end": end_iso,
                        "rows": {k: len(v) for k, v in datasets.items()},
                        "duration_seconds": round(duration, 2),
                        "result": "OK",
                    }
                )

                st.session_state["ingest_data"] = {
                    "datasets": datasets,
                    "checks": checks,
                    "manifest": manifest,
                    "params": params,
                    "metar_raw": metar_data,
                }

            except Exception as exc:  # noqa: BLE001
                status_widget.update(label="Ingest failed", state="error")
                st.error(f"Ingest failed: {exc}")
                log_ingest(
                    {
                        "airports": airports,
                        "start": start_iso,
                        "end": end_iso,
                        "result": "FAIL",
                        "error": str(exc),
                    }
                )
                return

        if "ingest_data" in st.session_state:
            ingest_state = st.session_state["ingest_data"]
            _display_results(ingest_state)

    if rerun_checks and "ingest_data" in st.session_state:
        ingest_state = st.session_state["ingest_data"]
        datasets = ingest_state["datasets"]
        params = ingest_state["params"]
        checks = _run_validations(datasets, params)
        ingest_state["checks"] = checks
        st.success("Validation checks refreshed.")
        _display_results(ingest_state)

    if save_outputs and "ingest_data" in st.session_state:
        ingest_state = st.session_state["ingest_data"]
        manifest = _write_processed_outputs(ingest_state["datasets"], ingest_state["params"], config)
        ingest_state["manifest"] = manifest
        st.success("Outputs saved successfully.")

    if "ingest_data" in st.session_state and not run_button and not rerun_checks:
        _display_results(st.session_state["ingest_data"])
