"""Dependency wiring for API routes."""
from __future__ import annotations

from ..core.orchestrator.controller import OpenDTOrchestrator

_orchestrator = OpenDTOrchestrator()


def get_orchestrator() -> OpenDTOrchestrator:
    return _orchestrator
