"""Scoring helpers shared across optimization strategies."""
from __future__ import annotations


def performance_score(sim_results: dict[str, float]) -> float:
    energy = sim_results.get('energy_kwh', 5.0)
    performance = sim_results.get('runtime_hours', 1.0)
    return (energy * 2.0) + (performance * 1.0)
