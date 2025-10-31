"""API route integration tests."""
from __future__ import annotations

import pytest

from opendt.app import create_app
from opendt.api.dependencies import get_orchestrator


@pytest.fixture
def client():
    app = create_app()
    return app.test_client()


def test_set_slo_endpoint_updates_targets(client):
    orchestrator = get_orchestrator()
    response = client.post("/api/set_slo", json={"energy_target": 5, "runtime_target": 1})
    assert response.status_code == 200
    data = response.get_json()
    assert data["energy_target"] == 5
    assert data["runtime_target"] == 1
    assert orchestrator.slo_targets["energy_target"] == 5
    assert orchestrator.slo_targets["runtime_target"] == 1
