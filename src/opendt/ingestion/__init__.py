"""Compatibility shim for ``opendt.ingestion`` imports."""

from opendt.adapters.ingestion import models  # re-export module namespace

__all__ = ["models"]
