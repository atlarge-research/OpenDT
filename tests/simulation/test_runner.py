"""Regression tests for the OpenDC runner workload generation utilities."""

from pathlib import Path

import pandas as pd
import pytest

from opendt.simulation.runner import OpenDCRunner


def test_create_workload_writes_parquet(tmp_path, monkeypatch):
    """Workload creation should persist tasks/fragments parquet datasets."""
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


def test_run_simulation_without_runner_raises():
    """Running a simulation without the binary should raise a helpful exception."""
    runner = OpenDCRunner()
    runner.opendc_path = None

    with pytest.raises(FileNotFoundError):
        runner.run_simulation(tasks_data=None, fragments_data=None, topology_data={})
