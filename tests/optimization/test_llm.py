"""Unit tests for the LLM optimization strategies and topology translation."""

import json
import os
import sys
import types

import pytest

from opendt.core.optimization.llm import LLM


@pytest.fixture(scope="module")
def openai_key():
    """Return the configured OpenAI API key so tests exercise the real credential."""

    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY must be set for LLM integration tests")
    return key


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
    assert result["new_topology"]["clusters"][0]["hosts"][0]["count"] < topology["clusters"][0]["hosts"][0]["count"]


def test_rule_based_tracks_best_configuration():
    """Persist the best configuration score after an optimization run."""
    optimizer = LLM(openai_key=None)
    topology = sample_topology()
    sim_results = {"energy_kwh": 5.0, "runtime_hours": 1.0}

    optimizer.rule_based_optimization(sim_results, {"task_count": 2}, {"energy_target": 10.0, "runtime_target": 2.0}, current_topology=topology)
    assert optimizer.best_config is not None
    assert optimizer.best_score < float("inf")


def test_rule_based_returns_best_config_snapshot():
    """Rule-based results should expose the cached best configuration snapshot."""

    optimizer = LLM(openai_key=None)
    topology = sample_topology()

    result = optimizer.rule_based_optimization(
        {"energy_kwh": 6.0, "runtime_hours": 1.1},
        {"task_count": 3},
        {"energy_target": 10.0, "runtime_target": 2.0},
        current_topology=topology,
    )

    assert result["best_config"] is not None
    assert result["best_config"] is not topology
    assert result["best_config"]["clusters"][0]["hosts"][0]["cpu"]["coreCount"] == 16
    assert result["best_score"] == optimizer.best_score
    assert result["best_score"] == pytest.approx(optimizer.best_score)


def test_convert_llm_to_topology_adds_hosts(openai_key):
    """Ensure LLM recommendations are merged as new hosts into the topology."""
    optimizer = LLM(openai_key=openai_key)
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


def test_extract_text_content_handles_structured_payloads(openai_key):
    """The helper should flatten LangChain message payloads into a JSON string."""
    optimizer = LLM(openai_key=openai_key)

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


def test_llm_optimization_parses_ai_message_list(monkeypatch, openai_key):
    """LLM.optimize should parse structured AIMessage content returned by LangChain."""

    optimizer = LLM(openai_key=openai_key)
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
            def __init__(self, *_, api_key=None, **__):  # pragma: no cover - trivial
                assert api_key == openai_key

            def invoke(self, prompt):  # pragma: no cover - simple deterministic stub
                assert "SIMULATION RESULTS" in prompt
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
    monkeypatch.setitem(sys.modules, "langchain_core", types.ModuleType("langchain_core"))
    parser_module = types.ModuleType("langchain_core.output_parsers")

    class DummyParser:
        def __init__(self, pydantic_object):  # pragma: no cover - simple stub
            self._model = pydantic_object

        def get_format_instructions(self):  # pragma: no cover - simple stub
            return "Return a JSON object with topology recommendations."

        def parse(self, content):  # pragma: no cover - simple stub
            data = json.loads(content)
            return self._model(**data)

    parser_module.JsonOutputParser = DummyParser
    monkeypatch.setitem(sys.modules, "langchain_core.output_parsers", parser_module)

    result = optimizer.llm_optimization(sim_results, batch, slo, current_topology=topology)

    assert result["type"] == "llm"
    assert result["recommendations"]["host_name"] == payload["host_name"]
    assert any(host["name"] == "H02" for c in result["new_topology"]["clusters"] for host in c["hosts"])


def test_optimize_without_key_returns_rule_based():
    """When no API key is configured the optimizer must fall back to the rule-based engine."""

    optimizer = LLM(openai_key=None)
    topology = sample_topology()

    outcome = optimizer.optimize(
        simulation_results={"energy_kwh": 12.0, "runtime_hours": 1.8},
        batch_data={"task_count": 6},
        slo_targets={"energy_target": 10.0, "runtime_target": 2.0},
        current_topology=topology,
    )

    assert outcome["type"] == "rule_based"
    assert "No OpenAI API key" in outcome["reason"]
    assert optimizer.best_config is None


def test_optimize_falls_back_when_llm_errors(monkeypatch, openai_key):
    """If the LLM call fails the optimizer should gracefully fall back to the rule-based plan."""

    optimizer = LLM(openai_key=openai_key)

    def explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(optimizer, "llm_optimization", explode)

    fallback = optimizer.optimize(
        simulation_results={"energy_kwh": 8.0, "runtime_hours": 1.1},
        batch_data={"task_count": 3},
        slo_targets={"energy_target": 10.0, "runtime_target": 2.0},
        current_topology=sample_topology(),
    )

    assert fallback["type"] == "rule_based"
    assert fallback["reason"].startswith("LLM Error: boom")


def test_convert_llm_to_topology_updates_existing_host(openai_key):
    """Existing topology entries should be updated rather than duplicated."""

    optimizer = LLM(openai_key=openai_key)
    topology = sample_topology()

    recommendations = {
        "cluster_name": ["C01"],
        "host_name": ["H01"],
        "count": [4],
        "coreCount": [28],
        "coreSpeed": [2600],
    }

    updated = optimizer.convert_llm_to_topology(recommendations, topology)
    host = updated["clusters"][0]["hosts"][0]

    assert host["count"] == 4
    assert host["cpu"]["coreCount"] == 28
    assert host["cpu"]["coreSpeed"] == 2600


def test_convert_llm_to_topology_uses_defaults_for_missing_fields(openai_key):
    """Missing optional recommendation fields should fall back to sensible defaults."""

    optimizer = LLM(openai_key=openai_key)
    original = {"clusters": []}
    rec = {
        "cluster_name": ["C07"],
        "host_name": ["H11"],
        "coreCount": [20],
        # intentionally omit count and coreSpeed to trigger defaults
    }

    updated = optimizer.convert_llm_to_topology(rec, original)

    assert original == {"clusters": []}  # ensure we didn't mutate the input
    host = updated["clusters"][0]["hosts"][0]
    assert host["count"] == 1
    assert host["cpu"]["coreCount"] == 20
    assert host["cpu"]["coreSpeed"] == 2400


def test_update_best_configuration_only_on_improvement(openai_key):
    """Only better performance scores should replace the stored best configuration."""

    optimizer = LLM(openai_key=openai_key)
    initial = sample_topology()
    worse = {"clusters": [{"name": "C99", "hosts": []}]}

    optimizer.update_best_configuration({"energy_kwh": 12.0, "runtime_hours": 2.5}, worse)
    best_before = optimizer.best_config

    optimizer.update_best_configuration({"energy_kwh": 6.0, "runtime_hours": 1.0}, initial)

    assert optimizer.best_config is not best_before
    assert optimizer.best_config["clusters"][0]["name"] == "C01"
    assert optimizer.best_config["clusters"][0]["hosts"][0]["cpu"]["coreCount"] == 16
    assert optimizer.best_score == pytest.approx(13.0)


def test_extract_text_content_handles_non_list_payloads(openai_key):
    """Extractor should gracefully process raw strings and mapping-based payloads."""

    optimizer = LLM(openai_key=openai_key)

    assert optimizer._extract_text_content("  hello \n") == "hello"

    message = type("Message", (), {"content": {"text": "ignored", "other": 1}})()
    assert optimizer._extract_text_content(message) == "{'text': 'ignored', 'other': 1}"


def test_extract_text_content_handles_empty_message(openai_key):
    """Empty or None messages should result in an empty string."""

    optimizer = LLM(openai_key=openai_key)

    assert optimizer._extract_text_content(None) == ""
    blank_message = type("Message", (), {"content": []})()
    assert optimizer._extract_text_content(blank_message) == ""
