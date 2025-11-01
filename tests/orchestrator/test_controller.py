"""Integration tests for the main orchestrator module."""

import json
from pathlib import Path

import pytest

from opendt.core.orchestrator.controller import OpenDTOrchestrator


@pytest.fixture
def orchestrator(monkeypatch):
    """Instantiate an orchestrator without background watchers for isolated tests."""
    monkeypatch.setattr(OpenDTOrchestrator, "start_topology_watcher", lambda self: None)
    orch = OpenDTOrchestrator()
    orch.stop_event.set()
    return orch


def test_score_prefers_lower_values(orchestrator):
    """Lower energy/runtime metrics should produce a higher score."""
    orchestrator.slo_targets = {"energy_target": 10.0, "runtime_target": 2.0}
    better = orchestrator._score({"energy_kwh": 9.0, "runtime_hours": 1.8})
    worse = orchestrator._score({"energy_kwh": 15.0, "runtime_hours": 2.5})
    assert better > worse


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


def test_record_datalake_wraps_paths(orchestrator, tmp_path):
    captured = {}

    class StubLake:
        def append_run(self, **kwargs):
            captured.update(kwargs)

    artifact = tmp_path / "power.parquet"
    artifact.write_text("artifact")

    orchestrator.datalake = StubLake()
    orchestrator._record_datalake(
        run_type="baseline",
        result={
            "energy_kwh": 1.0,
            "cpu_utilization": 0.5,
            "runtime_hours": 1.0,
            "max_power_draw": 100.0,
            "status": "success",
            "raw_output_dir": "/tmp/out",
        },
        topology={"clusters": []},
        metadata={"window_end": "t"},
        timeseries={"power_draw": []},
        artifacts={"power": str(artifact)},
        window_id="t",
        cycle=1,
        attempt=0,
        exp_name="exp",
        score=88.0,
        timestamp=None,
    )

    assert "artifacts" in captured
    assert "power" in captured["artifacts"]
    assert captured["artifacts"]["power"].name == artifact.name
