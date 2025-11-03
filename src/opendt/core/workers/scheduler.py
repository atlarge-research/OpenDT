"""Utilities for spawning background worker threads."""
from __future__ import annotations

import threading
from typing import Callable


def start_thread(target: Callable[[], None], *, daemon: bool = False) -> threading.Thread:
    thread = threading.Thread(target=target, daemon=daemon)
    thread.start()
    return thread
