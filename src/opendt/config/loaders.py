"""Topology and environment loading helpers."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def read_topology(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        logger.warning("⚠️ Topology not found, a default will be used at runtime")
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_topology(path: str, topology: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(topology, handle, indent=2)


def backup_topology(path: str) -> None:
    if not os.path.exists(path):
        return
    backup_path = f"{path}.backup"
    with open(path, "r", encoding="utf-8") as src:
        data = json.load(src)
    with open(backup_path, "w", encoding="utf-8") as dst:
        json.dump(data, dst, indent=2)


def topology_mtime(path: str) -> float:
    return Path(path).stat().st_mtime if os.path.exists(path) else 0.0
