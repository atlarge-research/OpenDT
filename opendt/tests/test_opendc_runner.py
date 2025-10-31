from pathlib import Path

import pandas as pd

from opendc_runner import OpenDCRunner


def test_create_workload_writes_parquet(tmp_path, monkeypatch):
    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    tasks = [{"id": 1, "submission_time": "2024-01-01T00:00:00Z", "duration": 1000, "cpu_count": 2, "cpu_capacity": 2.4, "mem_capacity": 1024}]
    fragments = [{"id": 1, "duration": 500, "cpu_usage": 0.5}]

    workload_dir = runner.create_workload(tasks, fragments)

    tasks_file = Path(workload_dir) / "tasks.parquet"
    frags_file = Path(workload_dir) / "fragments.parquet"

    assert tasks_file.exists()
    assert frags_file.exists()

    tasks_df = pd.read_parquet(tasks_file)
    frags_df = pd.read_parquet(frags_file)

    assert len(tasks_df) == 1
    assert len(frags_df) == 1


def test_create_enhanced_mock_results_appends(tmp_path, monkeypatch):
    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    result = runner.create_enhanced_mock_results(tasks_data=[{"id": 1}], fragments_data=[{"id": 1}])

    assert result["status"] == "mock"
    power_file = Path(tmp_path) / "powerSource.parquet"
    host_file = Path(tmp_path) / "host.parquet"
    assert power_file.exists()
    assert host_file.exists()

    updated = runner.create_enhanced_mock_results(tasks_data=[{"id": 1}], fragments_data=[{"id": 1}])
    assert updated["energy_kwh"] >= result["energy_kwh"]
