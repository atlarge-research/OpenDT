"""Compatibility shims for the legacy ``opendt.orchestrator`` import path."""

from opendt.core.orchestrator import OpenDTOrchestrator  # re-export for callers

__all__ = ["OpenDTOrchestrator"]
