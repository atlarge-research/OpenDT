"""Canonical telemetry message schemas."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TaskMessage:
    id: int
    submission_time: str
    duration: int
    cpu_count: int
    cpu_capacity: float
    mem_capacity: int


@dataclass
class FragmentMessage:
    id: int
    duration: int
    cpu_usage: float
    submission_time: str
