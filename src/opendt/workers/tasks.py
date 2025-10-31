"""Task helpers for background orchestration jobs."""
from __future__ import annotations

from typing import Callable


def run_task(task: Callable[[], None]) -> None:
    task()
