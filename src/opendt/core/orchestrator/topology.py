"""Topology hashing and file-watcher utilities."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, Callable

from ...config import loaders

logger = logging.getLogger(__name__)


def topology_hash(topo: dict[str, Any] | None) -> str:
    if topo is None:
        return ""
    try:
        canonical = json.dumps(topo, sort_keys=True, separators=(",", ":"))
    except Exception:
        canonical = str(topo)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def watch_topology_file(
    path: str,
    stop_event: threading.Event,
    on_change: Callable[[dict[str, Any]], None],
) -> None:
    last_mtime = 0.0
    while not stop_event.is_set():
        try:
            if os.path.exists(path):
                mtime = loaders.topology_mtime(path)
                if mtime != last_mtime:
                    topo = loaders.read_topology(path)
                    if topo is not None:
                        on_change(topo)
                        last_mtime = mtime
                        logger.info("🔁 Topology file changed; dashboard state updated")
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.warning("Topology watcher error: %s", exc)
        time.sleep(0.5)
