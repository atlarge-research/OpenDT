"""API route integration tests."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from opendt.app import create_app
from opendt.api.dependencies import get_orchestrator


@pytest.fixture
def client():
    app = create_app()
    return app.test_client()


def test_set_slo_endpoint_updates_targets(client):
    orchestrator = get_orchestrator()
    slo_path = Path(orchestrator.slo_path)
    slo_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = Path(str(slo_path) + ".backup")

    original = slo_path.read_text() if slo_path.exists() else None
    backup_original = backup_path.read_text() if backup_path.exists() else None

    try:
        response = client.post("/api/set_slo", json={"energy_target": 5, "runtime_target": 1})
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "success"
        assert data["energy_target"] == 5
        assert data["runtime_target"] == 1
        assert orchestrator.slo_targets["energy_target"] == 5
        assert orchestrator.slo_targets["runtime_target"] == 1

        persisted = json.loads(slo_path.read_text())
        assert persisted["energy_target"] == 5
        assert persisted["runtime_target"] == 1
    finally:
        if original is not None:
            slo_path.write_text(original)
        elif slo_path.exists():
            slo_path.unlink()

        if backup_original is not None:
            backup_path.write_text(backup_original)
        elif backup_path.exists():
            backup_path.unlink()


def test_accept_recommendation_requires_existing_topology(client):
    orchestrator = get_orchestrator()
    previous = orchestrator.state.get("best_config")
    try:
        orchestrator.state["best_config"] = None
        response = client.post("/api/accept_recommendation")
        assert response.status_code == 400
        assert response.get_json()["error"] == "No recommendation available"
    finally:
        orchestrator.state["best_config"] = previous


def test_accept_recommendation_applies_best_config(client, monkeypatch):
    orchestrator = get_orchestrator()
    previous = orchestrator.state.get("best_config")
    previous_updates = orchestrator.state.get("topology_updates", 0)

    staged = {"clusters": [{"name": "A", "hosts": []}]}
    applied = {}

    def fake_update_topology(new_topology):
        applied["topology"] = deepcopy(new_topology)
        orchestrator.state["current_topology"] = new_topology
        orchestrator.state["topology_updates"] = orchestrator.state.get("topology_updates", 0) + 1
        return True

    monkeypatch.setattr(orchestrator, "update_topology_file", fake_update_topology)

    try:
        orchestrator.state["best_config"] = {"config": staged, "score": 1.0}
        response = client.post("/api/accept_recommendation")
        assert response.status_code == 200
        data = response.get_json()
        assert data["applied_config"] == staged
        assert applied["topology"] == staged
        assert orchestrator.state["best_config"]["config"] == staged
    finally:
        orchestrator.state["best_config"] = previous
        orchestrator.state["topology_updates"] = previous_updates


def test_accept_recommendation_allows_custom_payload(client, monkeypatch):
    orchestrator = get_orchestrator()
    previous = orchestrator.state.get("best_config")
    previous_updates = orchestrator.state.get("topology_updates", 0)

    target = {
        "clusters": [
            {
                "name": "B",
                "hosts": [
                    {
                        "name": "H1",
                        "count": 2,
                        "cpu": {"coreCount": 16, "coreSpeed": 2400},
                        "memory": {"memorySize": 34359738368},
                    }
                ],
            }
        ]
    }

    applied = {}

    def fake_update_topology(new_topology):
        applied["topology"] = deepcopy(new_topology)
        orchestrator.state["topology_updates"] = orchestrator.state.get("topology_updates", 0) + 1
        return True

    monkeypatch.setattr(orchestrator, "update_topology_file", fake_update_topology)

    try:
        orchestrator.state["best_config"] = {"config": {"clusters": []}, "score": 3.2}
        response = client.post("/api/accept_recommendation", json={"topology": target})
        assert response.status_code == 200
        data = response.get_json()
        assert data["applied_config"] == target
        assert applied["topology"] == target
        assert orchestrator.state["best_config"]["config"] == target
    finally:
        orchestrator.state["best_config"] = previous
        orchestrator.state["topology_updates"] = previous_updates


def test_accept_recommendation_reports_failure(client, monkeypatch):
    orchestrator = get_orchestrator()
    previous = orchestrator.state.get("best_config")

    monkeypatch.setattr(orchestrator, "update_topology_file", lambda *_: False)

    try:
        orchestrator.state["best_config"] = {"config": {"clusters": []}}
        response = client.post("/api/accept_recommendation")
        assert response.status_code == 500
        assert response.get_json()["error"] == "Failed to update topology file"
    finally:
        orchestrator.state["best_config"] = previous
