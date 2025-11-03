"""Base protocol for optimization strategies."""
from __future__ import annotations

from typing import Protocol


class OptimizationStrategy(Protocol):
    def optimize(self, simulation_results, batch_data, slo_targets, current_topology=None):
        ...
