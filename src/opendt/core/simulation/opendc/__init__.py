"""Bundled OpenDC simulator binaries."""

from pathlib import Path

__all__ = ["SIMULATOR_ROOT"]

SIMULATOR_ROOT = Path(__file__).resolve().parent
"""Path to the packaged OpenDC simulator assets."""
