"""State helpers for the OpenDT orchestrator."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Any, Dict, List


@dataclass
class OrchestratorState:
    status: str = "stopped"
    cycle_count: int = 0
    cycle_count_opt: int = 0
    last_simulation: Dict[str, Any] | None = None
    last_optimization: Dict[str, Any] | None = None
    total_tasks: int = 0
    total_fragments: int = 0
    current_window: str | None = None
    current_topology: Dict[str, Any] | None = None
    best_config: Dict[str, Any] | None = None
    topology_updates: int = 0
    slo_targets: Dict[str, Any] = field(default_factory=dict)
    window_baseline_score: float | None = None
    window_best_score: float | None = None
    window_trials: int = 0
    window_accepted: bool = False
    window_time_used_sec: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__


@dataclass
class SimulationResultsBuffer:
    results: List[Dict[str, Any]] = field(default_factory=list)
    timestamps: List[str] = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)


@dataclass
class OrchestratorRuntime:
    stop_event: Event = field(default_factory=Event)


def default_state_dict() -> Dict[str, Any]:
    return OrchestratorState().as_dict()
