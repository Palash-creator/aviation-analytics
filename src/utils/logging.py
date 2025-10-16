"""Logging helpers for ingest runs."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def log_ingest(event: dict[str, Any], log_dir: str = "logs", filename: str = "ingest.log") -> None:
    """Append an ingest event to the structured log file."""

    path = Path(log_dir) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp": datetime.utcnow().isoformat(), **event}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
