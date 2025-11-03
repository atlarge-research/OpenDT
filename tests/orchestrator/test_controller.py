"""Integration tests for the main orchestrator module."""

import json
from pathlib import Path

import pytest

from opendt.core.orchestrator.controller import OpenDTOrchestrator


@pytest.fixture
def orchestrator(monkeypatch):
    """Instantiate an orchestrator without background watchers for isolated tests."""
    monkeypatch.setattr(OpenDTOrchestrator, "start_topology_watcher", lambda self: None)
    monkeypatch.setattr(OpenDTOrchestrator, "start_slo_watcher", lambda self: None)
    monkeypatch.setattr(OpenDTOrchestrator, "_ensure_slo_file", lambda self: None)
    orch = OpenDTOrchestrator()
    orch.stop_event.set()
    return orch


def test_score_prefers_lower_values(orchestrator):
    """Lower energy/runtime metrics should produce a higher score."""
    orchestrator.slo_targets = {"energy_target": 10.0, "runtime_target": 2.0}
    better = orchestrator._score({"energy_kwh": 9.0, "runtime_hours": 1.8})
    worse = orchestrator._score({"energy_kwh": 15.0, "runtime_hours": 2.5})
    assert better < worse
    assert better < 0  # below target should produce a negative (better-than-SLO) delta


def test_topology_update_creates_backup(orchestrator, tmp_path):
    """Updating the topology should persist a backup and increment counters."""
    topology_path = tmp_path / "topology.json"
    initial = {"clusters": [{"name": "A", "hosts": []}]}
    topology_path.write_text(json.dumps(initial))

    orch = orchestrator
    orch.topology_path = str(topology_path)
    orch.state["current_topology"] = initial
    orch.state["topology_updates"] = 0
    orch.last_topology_hash = orch._topo_hash(initial)

    new_topology = {"clusters": [{"name": "A", "hosts": [{"name": "H1", "count": 1, "cpu": {"coreCount": 8, "coreSpeed": 2200}, "memory": {"memorySize": 1024}}]}]}
    updated = orch.update_topology_file(new_topology)

    assert updated is True
    written = json.loads(topology_path.read_text())
    assert written == new_topology
    backup = json.loads(Path(str(topology_path) + ".backup").read_text())
    assert backup == initial
    assert orch.state["topology_updates"] == 1


def test_update_slo_file_persists_and_backs_up(orchestrator, tmp_path):
    """SLO updates should be written to disk and previous values backed up."""
    slo_path = tmp_path / "slo.json"

    orch = orchestrator
    orch.slo_path = str(slo_path)
    orch.last_slo_hash = None

    first = orch.update_slo_file({"energy_target": 8.0, "runtime_target": 1.5})
    assert first == "applied"
    written = json.loads(slo_path.read_text())
    assert written["energy_target"] == pytest.approx(8.0)
    assert orch.slo_targets["runtime_target"] == pytest.approx(1.5)

    second = orch.update_slo_file({"energy_target": 7.5, "runtime_target": 1.25})
    assert second == "applied"
    backup = json.loads((tmp_path / "slo.json.backup").read_text())
    assert backup["energy_target"] == pytest.approx(8.0)

    noop = orch.update_slo_file({"energy_target": 7.5, "runtime_target": 1.25})
    assert noop == "noop"


def test_run_simulation_passes_window_data(orchestrator):
    """Simulations must receive the sampled window data and topology snapshot."""
    captured = {}

    def fake_run_simulation(*, tasks_data, fragments_data, topology_data, expName="simple"):
        captured["tasks"] = tasks_data
        captured["fragments"] = fragments_data
        captured["topology"] = topology_data
        captured["expName"] = expName
        return {"energy_kwh": 1.0}

    orch = orchestrator
    orch.opendc_runner = type("R", (), {"run_simulation": staticmethod(fake_run_simulation)})
    orch.state["current_topology"] = {"clusters": []}

    batch = {
        "tasks_sample": [{"id": 1}],
        "fragments_sample": [{"id": 1}],
    }
    result = orch.run_simulation(batch, expName="window-1")

    assert result == {"energy_kwh": 1.0}
    assert captured["tasks"] == batch["tasks_sample"]
    assert captured["fragments"] == batch["fragments_sample"]
    assert captured["topology"] == orch.state["current_topology"]
    assert captured["expName"] == "window-1"


def test_topo_hash_stable(orchestrator):
    """Equivalent topologies should hash identically regardless of key order."""
    topo_a = {"clusters": [{"name": "A", "hosts": [{"name": "h", "count": 1}]}]}
    topo_b = {"clusters": [{"hosts": [{"count": 1, "name": "h"}], "name": "A"}]}
    assert orchestrator._topo_hash(topo_a) == orchestrator._topo_hash(topo_b)
