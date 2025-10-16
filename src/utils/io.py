"""Input/output utilities for ingest pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd


def _ensure_parent(path: Path) -> None:
    """Ensure the parent directory for *path* exists."""

    path.parent.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: str | os.PathLike[str]) -> None:
    """Write a dataframe to a parquet file, creating directories as needed."""

    target = Path(path)
    _ensure_parent(target)
    df.to_parquet(target, index=False)


def read_parquet(path: str | os.PathLike[str]) -> pd.DataFrame:
    """Read a parquet dataset from disk."""

    return pd.read_parquet(Path(path))


def write_manifest(payload: dict[str, Any], path: str | os.PathLike[str]) -> None:
    """Persist a JSON manifest describing an ingest run."""

    target = Path(path)
    _ensure_parent(target)
    with target.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True, default=str)


def write_csv(df: pd.DataFrame, path: str | os.PathLike[str], *, index: bool = False) -> None:
    """Write a dataframe to CSV, ensuring the directory exists."""

    target = Path(path)
    _ensure_parent(target)
    df.to_csv(target, index=index)


def list_files(path: str | os.PathLike[str]) -> list[Path]:
    """Return a sorted list of files located under *path*."""

    root = Path(path)
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_file())
