"""Unit tests for the LLM optimization strategies and topology translation."""

import copy

import pytest

from llm import LLM


def sample_topology():
    """Return a minimal topology fixture for optimizer exercises."""
    return {
        "clusters": [
            {
                "name": "C01",
                "hosts": [
                    {
                        "name": "H01",
                        "count": 2,
                        "cpu": {"coreCount": 16, "coreSpeed": 2400},
                        "memory": {"memorySize": 34359738368},
                    }
                ],
            }
        ]
    }


def test_rule_based_optimization_downscales_energy(monkeypatch):
    """The rule-based optimizer should downscale when consumption exceeds the SLO."""
    optimizer = LLM(openai_key=None)
    topology = sample_topology()
    sim_results = {"energy_kwh": 15.0, "runtime_hours": 1.5, "cpu_utilization": 0.7}
    batch = {"task_count": 5}
    slo = {"energy_target": 10.0, "runtime_target": 2.0}

    result = optimizer.rule_based_optimization(sim_results, batch, slo, current_topology=topology)

    assert result["type"] == "rule_based"
    assert result["action_taken"] in {"downscale", "massive downscale"}
    assert result["new_topology"]["clusters"][0]["hosts"][0]["cpu"]["coreSpeed"] <= 2400


def test_rule_based_tracks_best_configuration():
    """Persist the best configuration score after an optimization run."""
    optimizer = LLM(openai_key=None)
    topology = sample_topology()
    sim_results = {"energy_kwh": 5.0, "runtime_hours": 1.0}

    optimizer.rule_based_optimization(sim_results, {"task_count": 2}, {"energy_target": 10.0, "runtime_target": 2.0}, current_topology=topology)
    assert optimizer.best_config is not None
    assert optimizer.best_score < float("inf")


def test_convert_llm_to_topology_adds_hosts():
    """Ensure LLM recommendations are merged as new hosts into the topology."""
    optimizer = LLM(openai_key="dummy")
    topology = sample_topology()
    rec = type("Obj", (), {
        "cluster_name": ["C01", "C02"],
        "host_name": ["H02", "H99"],
        "count": [1, 2],
        "coreCount": [12, 20],
        "coreSpeed": [2200, 2600],
    })()

    new_topology = optimizer.convert_llm_to_topology(rec, topology)

    cluster_names = {c["name"] for c in new_topology["clusters"]}
    assert "C02" in cluster_names
    host_counts = [h["count"] for c in new_topology["clusters"] for h in c["hosts"] if h["name"] in {"H02", "H99"}]
    assert host_counts == [1, 2]
