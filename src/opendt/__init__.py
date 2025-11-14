"""OpenDT package initialization."""
from __future__ import annotations

# Keep package import lightweight. Application entrypoints should call
# `load_dotenv()` as part of startup to ensure environment variables are
# populated for runtime-only concerns.

from .app import create_app

__all__ = ["create_app"]
