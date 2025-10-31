"""OpenDT package initialization."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# Load environment variables early so downstream modules can rely on them.
load_dotenv()  # fall back to default discovery (useful for tooling)
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from .app import create_app

__all__ = ["create_app"]
