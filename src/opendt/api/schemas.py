"""Request/response schemas for the API layer."""
from __future__ import annotations

from pydantic import BaseModel, Field


class SLORequest(BaseModel):
    energy_target: float = Field(gt=0)
    runtime_target: float = Field(gt=0)
