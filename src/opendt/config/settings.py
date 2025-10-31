"""Runtime settings and orchestrator thresholds."""
from __future__ import annotations

import os
from dataclasses import dataclass


IMPROVEMENT_DELTA: float = 0.05
WINDOW_TRY_BUDGET_SEC: float = 30.0
MAX_TRIES_PER_WINDOW: int = 1
NO_IMPROVEMENT_STOP_AFTER: int = 3

TIME_SCALE: float = 1 / 10
REAL_WINDOW_SIZE_SEC: int = 5 * 60
VIRTUAL_WINDOW_SIZE: float = REAL_WINDOW_SIZE_SEC * TIME_SCALE


@dataclass(frozen=True)
class SLOTargets:
    energy_target: float = 10.0
    runtime_target: float = 2.0

    def to_dict(self) -> dict[str, float]:
        return {
            "energy_target": self.energy_target,
            "runtime_target": self.runtime_target,
        }


def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def openai_api_key() -> str | None:
    return os.environ.get("OPENAI_API_KEY")
