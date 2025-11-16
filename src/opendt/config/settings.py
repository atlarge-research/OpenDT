"""Runtime settings and orchestrator thresholds."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict


IMPROVEMENT_DELTA: float = 0.05
WINDOW_TRY_BUDGET_SEC: float = 30.0
MAX_TRIES_PER_WINDOW: int = 1
NO_IMPROVEMENT_STOP_AFTER: int = 3

TIME_SCALE: float = 1 / 10
REAL_WINDOW_SIZE_SEC: int = 5 * 60
VIRTUAL_WINDOW_SIZE: float = REAL_WINDOW_SIZE_SEC * TIME_SCALE

# Fast mode speedup factor: how much faster than TIME_SCALE to run experiments
# E.g., 30x means: virtual_time * TIME_SCALE / 30 = real_time
FAST_MODE_SPEEDUP_FACTOR: int = 30


@dataclass(frozen=True)
class SLOTargets:
    energy_target: float = 10.0
    runtime_target: float = 2.0

    def to_dict(self) -> dict[str, float]:
        return {
            "energy_target": float(self.energy_target),
            "runtime_target": float(self.runtime_target),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "SLOTargets":
        """Coerce arbitrary payloads into a validated ``SLOTargets`` instance."""

        data = data or {}
        defaults = cls()

        def _coerce(key: str, fallback: float) -> float:
            try:
                value = data.get(key, fallback)
                if value is None:
                    return fallback
                return float(value)
            except (TypeError, ValueError):
                return fallback

        return cls(
            energy_target=_coerce("energy_target", defaults.energy_target),
            runtime_target=_coerce("runtime_target", defaults.runtime_target),
        )


def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def openai_api_key() -> str | None:
    return os.environ.get("OPENAI_API_KEY")


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_mode: bool = False
    enable_optimization: bool = True
    fast_mode: bool = False
    experiment_name: str = "default"
    output_path: str = "output/experiments"

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_mode": self.experiment_mode,
            "enable_optimization": self.enable_optimization,
            "fast_mode": self.fast_mode,
            "experiment_name": self.experiment_name,
            "output_path": self.output_path,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "ExperimentConfig":
        """Coerce arbitrary payloads into a validated ExperimentConfig instance."""
        data = data or {}
        defaults = cls()

        return cls(
            experiment_mode=bool(data.get("experiment_mode", defaults.experiment_mode)),
            enable_optimization=bool(data.get("enable_optimization", defaults.enable_optimization)),
            fast_mode=bool(data.get("fast_mode", defaults.fast_mode)),
            experiment_name=str(data.get("experiment_name", defaults.experiment_name)),
            output_path=str(data.get("output_path", defaults.output_path)),
        )


def experiment_config() -> ExperimentConfig:
    """Load experiment configuration from config/experiment.json."""
    from . import loaders
    experiment_path = os.environ.get("OPENDT_EXPERIMENT_PATH", "/app/config/experiment.json")
    data = loaders.read_experiment_config(experiment_path)
    return ExperimentConfig.from_dict(data)
