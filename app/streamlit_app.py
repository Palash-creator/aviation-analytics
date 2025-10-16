"""Streamlit entry point for the aviation analytics platform."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any

import plotly.io as pio
import streamlit as st
import toml
from dotenv import load_dotenv


def _load_config(path: str = "config.toml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return toml.load(handle)


def _resolve_page_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import page at {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _available_pages(is_admin: bool) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    if is_admin:
        pages.append(
            {
                "title": "Admin → Ingest",
                "icon": "🛠️",
                "path": Path(__file__).parent / "pages" / "1_Admin_Ingest.py",
            }
        )
    pages.append(
        {
            "title": "Simulations",
            "icon": "🛫",
            "path": Path(__file__).parent / "pages" / "2_Simulations.py",
        }
    )
    return pages


def _render_with_navigation(pages: list[dict[str, Any]], config: dict[str, Any], is_admin: bool) -> None:
    def _fallback_render() -> None:
        choice = st.sidebar.radio("Navigation", [page["title"] for page in pages])
        selected = next(page for page in pages if page["title"] == choice)
        module = _resolve_page_module(selected["path"])
        if hasattr(module, "render"):
            module.render(config=config, is_admin=is_admin)
        else:
            raise AttributeError(f"Page {selected['path']} missing render() function")

    if hasattr(st, "Page") and hasattr(st, "navigation"):
        try:
            st_pages = [
                st.Page(path=str(page["path"]), title=page["title"], icon=page["icon"])
                for page in pages
            ]
            navigation = st.navigation(pages=st_pages)
            st.session_state["app_context"] = {"config": config, "is_admin": is_admin}
            navigation.run()
            return
        except TypeError:
            # Streamlit versions prior to 1.29 expose st.Page without the new signature.
            pass

    _fallback_render()


def main() -> None:
    load_dotenv()
    config = _load_config()
    is_admin = os.getenv("IS_ADMIN", "").lower() == "yes"

    st.set_page_config(
        page_title=config["app"].get("title", "US Aviation Data"),
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="✈️",
    )
    pio.templates.default = config["app"].get("plotly_template", "plotly_dark")
    st.session_state["app_context"] = {"config": config, "is_admin": is_admin}

    pages = _available_pages(is_admin)
    _render_with_navigation(pages, config, is_admin)


if __name__ == "__main__":
    main()
