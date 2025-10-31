import pytest


@pytest.mark.usefixtures("client")
def test_flask_set_slo_endpoint(client):
    response = client.post("/api/set_slo", json={"energy_target": 5, "runtime_target": 1})
    assert response.status_code == 200
    data = response.get_json()
    assert data["energy_target"] == 5
    assert data["runtime_target"] == 1
    orchestrator = client.application.orchestrator
    assert orchestrator.slo_targets["energy_target"] == 5
    assert orchestrator.slo_targets["runtime_target"] == 1
