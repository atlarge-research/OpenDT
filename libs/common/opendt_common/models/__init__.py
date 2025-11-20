"""Shared Pydantic models for OpenDT."""

from opendt_common.models.fragment import Fragment
from opendt_common.models.consumption import Consumption
from opendt_common.models.task import Task

# Update forward references for Task.fragments
Task.model_rebuild()

__all__ = ["Task", "Fragment", "Consumption"]
