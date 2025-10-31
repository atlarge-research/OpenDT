"""Unit tests for the LLM optimization strategies and topology translation."""

import json
import sys

import pytest

from opendt.optimization.llm import LLM


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


def test_extract_text_content_handles_structured_payloads():
    """The helper should flatten LangChain message payloads into a JSON string."""
    optimizer = LLM(openai_key="dummy")

    message = type(
        "Message",
        (),
        {
            "content": [
                {"type": "text", "text": "{"},
                "\"foo\"",
                type("Chunk", (), {"text": ": \"bar\"}"})(),
            ]
        },
    )()

    flattened = optimizer._extract_text_content(message)
    assert flattened == '{"foo": "bar"}'


def test_llm_optimization_parses_ai_message_list(monkeypatch):
    """LLM.optimize should parse structured AIMessage content returned by LangChain."""

    optimizer = LLM(openai_key="dummy")
    topology = sample_topology()
    sim_results = {"energy_kwh": 9.0, "runtime_hours": 1.2, "cpu_utilization": 0.5}
    batch = {"task_count": 4, "fragment_count": 12, "avg_cpu_usage": 0.4}
    slo = {"energy_target": 10.0, "runtime_target": 2.0}

    payload = {
        "cluster_name": ["C01"],
        "host_name": ["H02"],
        "count": [1],
        "coreCount": [24],
        "coreSpeed": [2500],
    }

    class DummyChatModule:
        class ChatOpenAI:  # noqa: D401 - simple stub for tests
            def __init__(self, *args, **kwargs):  # pragma: no cover - trivial
                pass

            def invoke(self, prompt):  # pragma: no cover - simple deterministic stub
                return type(
                    "AIMessage",
                    (),
                    {
                        "content": [
                            {"type": "text", "text": json.dumps(payload)},
                        ]
                    },
                )()

    monkeypatch.setitem(sys.modules, "langchain_openai", DummyChatModule)

    result = optimizer.llm_optimization(sim_results, batch, slo, current_topology=topology)

    assert result["type"] == "llm"
    assert result["recommendations"]["host_name"] == payload["host_name"]
    assert any(host["name"] == "H02" for c in result["new_topology"]["clusters"] for host in c["hosts"])
