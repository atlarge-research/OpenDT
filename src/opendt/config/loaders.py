"""Topology and environment loading helpers."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _read_json(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_topology(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        logger.warning("⚠️ Topology not found, a default will be used at runtime")
        return None
    return _read_json(path)


def _write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def write_topology(path: str, topology: dict[str, Any]) -> None:
    _write_json(path, topology)


def _backup_json(path: str) -> None:
    if not os.path.exists(path):
        return
    backup_path = f"{path}.backup"
    data = _read_json(path)
    if data is None:
        return
    _write_json(backup_path, data)


def backup_topology(path: str) -> None:
    _backup_json(path)


def topology_mtime(path: str) -> float:
    return Path(path).stat().st_mtime if os.path.exists(path) else 0.0


def read_slo(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        logger.info("⚠️ SLO configuration not found; defaults will be used until one is created")
        return None
    return _read_json(path)


def write_slo(path: str, slo: dict[str, Any]) -> None:
    _write_json(path, slo)


def backup_slo(path: str) -> None:
    _backup_json(path)


def slo_mtime(path: str) -> float:
    return Path(path).stat().st_mtime if os.path.exists(path) else 0.0


def read_experiment_config(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        logger.info("⚠️ Experiment configuration not found; defaults will be used")
        return None
    return _read_json(path)


def write_experiment_config(path: str, config: dict[str, Any]) -> None:
    _write_json(path, config)


def backup_experiment_config(path: str) -> None:
    _backup_json(path)


def experiment_config_mtime(path: str) -> float:
    return Path(path).stat().st_mtime if os.path.exists(path) else 0.0
