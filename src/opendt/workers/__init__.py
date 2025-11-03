"""Compatibility layer for ``opendt.workers`` imports."""

from opendt.core.workers.scheduler import start_thread
from opendt.core.workers.tasks import run_task

__all__ = ["start_thread", "run_task"]
