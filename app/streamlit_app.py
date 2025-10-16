"""Streamlit entry point for the aviation analytics platform."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from typing import Any

import plotly.io as pio
import streamlit as st
import toml

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils.secrets import get_env_bool, load_env


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
        titles = [page["title"] for page in pages]
        choice = st.sidebar.radio("Navigate", titles)
        if "Admin" in choice and not is_admin:
            st.error("Admin page is restricted. Set IS_ADMIN=Yes in your .env.")
            st.stop()
        selected = next(page for page in pages if page["title"] == choice)
        st.session_state["_manual_page_render"] = True
        module = _resolve_page_module(selected["path"])
        try:
            if hasattr(module, "render"):
                module.render(config=config, is_admin=is_admin)
            else:
                raise AttributeError(f"Page {selected['path']} missing render() function")
        finally:
            st.session_state.pop("_manual_page_render", None)

    _fallback_render()


def main() -> None:
    load_env()
    config = _load_config()

    st.set_page_config(
        page_title=config["app"].get("title", "US Aviation Data"),
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="✈️",
    )

    is_admin = get_env_bool("IS_ADMIN")
    pio.templates.default = config["app"].get("plotly_template", "plotly_dark")
    st.session_state["app_context"] = {"config": config, "is_admin": is_admin}

    pages = _available_pages(is_admin)
    if hasattr(st, "Page") and hasattr(st, "navigation"):
        try:
            st_pages = [
                st.Page(path=str(page["path"]), title=page["title"], icon=page["icon"])
                for page in pages
            ]
            navigation = st.navigation(pages=st_pages)
            navigation.run()
            return
        except TypeError:
            pass

    _render_with_navigation(pages, config, is_admin)


if __name__ == "__main__":
    main()
