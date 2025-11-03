"""SLO hashing and watcher helpers mirroring topology utilities."""
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


def slo_hash(slo: dict[str, Any] | None) -> str:
    if not slo:
        return ""
    try:
        canonical = json.dumps(slo, sort_keys=True, separators=(",", ":"))
    except Exception:
        canonical = str(slo)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def watch_slo_file(
    path: str,
    stop_event: threading.Event,
    on_change: Callable[[dict[str, Any]], None],
) -> None:
    last_mtime = 0.0
    while not stop_event.is_set():
        try:
            if os.path.exists(path):
                mtime = loaders.slo_mtime(path)
                if mtime != last_mtime:
                    slo = loaders.read_slo(path)
                    if slo is not None:
                        on_change(slo)
                        last_mtime = mtime
                        logger.info("🔁 SLO file changed; dashboard state updated")
                    else:
                        last_mtime = mtime
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.warning("SLO watcher error: %s", exc)
        time.sleep(0.5)
