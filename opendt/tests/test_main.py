import json
from pathlib import Path

import pytest

import main


@pytest.fixture
def orchestrator(monkeypatch):
    monkeypatch.setattr(main.OpenDTOrchestrator, "start_topology_watcher", lambda self: None)
    orch = main.OpenDTOrchestrator()
    orch.stop_event.set()
    return orch


def test_score_prefers_lower_values(orchestrator):
    orchestrator.slo_targets = {"energy_target": 10.0, "runtime_target": 2.0}
    better = orchestrator._score({"energy_kwh": 9.0, "runtime_hours": 1.8})
    worse = orchestrator._score({"energy_kwh": 15.0, "runtime_hours": 2.5})
    assert better > worse


def test_topology_update_creates_backup(orchestrator, tmp_path):
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


def test_flask_set_slo_endpoint():
    client = main.app.test_client()
    response = client.post("/api/set_slo", json={"energy_target": 5, "runtime_target": 1})
    assert response.status_code == 200
    data = response.get_json()
    assert data["energy_target"] == 5
    assert data["runtime_target"] == 1
    assert main.orchestrator.slo_targets["energy_target"] == 5
    assert main.orchestrator.slo_targets["runtime_target"] == 1


def test_topo_hash_stable(orchestrator):
    topo_a = {"clusters": [{"name": "A", "hosts": [{"name": "h", "count": 1}]}]}
    topo_b = {"clusters": [{"hosts": [{"count": 1, "name": "h"}], "name": "A"}]}
    assert orchestrator._topo_hash(topo_a) == orchestrator._topo_hash(topo_b)
