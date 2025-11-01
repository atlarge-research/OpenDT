"""Compatibility layer for ``opendt.simulation`` imports."""

from opendt.core.simulation import adapters  # re-export module namespace
from opendt.core.simulation.runner import OpenDCRunner

__all__ = ["adapters", "OpenDCRunner"]
