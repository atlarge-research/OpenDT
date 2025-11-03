"""Regression tests for the OpenDC runner workload generation utilities."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from opendt.core.simulation.runner import OpenDCRunner


def test_create_workload_writes_parquet(tmp_path, monkeypatch):
    """Workload creation should persist tasks/fragments parquet datasets."""
    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    tasks = [
        {
            "id": 1,
            "submission_time": "2024-01-01T00:00:00Z",
            "duration": 1000,
            "cpu_count": 2,
            "cpu_capacity": 2.4,
            "mem_capacity": 1024,
        }
    ]
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


def test_create_workload_handles_missing_payload(tmp_path, monkeypatch):
    """The helper should create the workload directory even without payload data."""
    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    target_dir = tmp_path / "empty"

    def _ensure_dir():
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir

    monkeypatch.setattr("opendt.core.simulation.runner.ensure_workload_dir", _ensure_dir)

    workload_dir = Path(runner.create_workload(None, None))

    assert workload_dir.exists()
    assert list(workload_dir.iterdir()) == []


def test_run_simulation_without_runner_raises():
    """Running a simulation without the binary should raise a helpful exception."""
    runner = OpenDCRunner()
    runner.opendc_path = None

    with pytest.raises(FileNotFoundError):
        runner.run_simulation(tasks_data=None, fragments_data=None, topology_data={})


def test_run_simulation_falls_back_to_shell_for_non_executable(tmp_path, monkeypatch):
    """When the runner lacks execute permissions, we should invoke it through /bin/sh."""

    runner = OpenDCRunner()

    non_exec_path = tmp_path / "OpenDCExperimentRunner"
    non_exec_path.write_text("echo noop")
    non_exec_path.chmod(0o644)

    runner.opendc_path = str(non_exec_path)

    commands: list[list[str]] = []

    def fake_run(args, capture_output, text, timeout, env):  # type: ignore[override]
        commands.append(list(args))

        class Result:
            returncode = 0
            stdout = "noop"
            stderr = ""

        return Result()

    monkeypatch.setattr("opendt.core.simulation.runner.subprocess.run", fake_run)
    monkeypatch.setattr(
        runner,
        "parse_opendc_results",
        lambda: {"status": "ok"},
    )

    result = runner.run_simulation(tasks_data=None, fragments_data=None, topology_data={})

    assert result == {"status": "ok"}
    assert commands, "Expected subprocess.run to be invoked"
    assert commands[0][0] == "/bin/sh"
    assert Path(commands[0][1]) == non_exec_path


def test_run_simulation_surfaces_process_failures(tmp_path, monkeypatch):
    """Non-zero subprocess exits should bubble up with stdout/stderr context."""

    runner = OpenDCRunner()

    binary_path = tmp_path / "OpenDCExperimentRunner"
    binary_path.write_text("#!/bin/sh\nexit 2")
    binary_path.chmod(0o755)
    runner.opendc_path = str(binary_path)

    def fake_run(*_args, **_kwargs):  # type: ignore[override]
        class Result:
            returncode = 2
            stdout = "boom"
            stderr = "stacktrace"

        return Result()

    monkeypatch.setattr("opendt.core.simulation.runner.subprocess.run", fake_run)

    with pytest.raises(RuntimeError) as exc:
        runner.run_simulation(tasks_data=None, fragments_data=None, topology_data={})

    assert "exit code 2" in str(exc.value)
    assert "boom" in str(exc.value)
    assert "stacktrace" in str(exc.value)


def test_run_simulation_invokes_runner_with_expected_arguments(tmp_path, monkeypatch):
    """Successful subprocess launches should forward the topology and experiment path."""

    runner = OpenDCRunner()

    binary_path = tmp_path / "OpenDCExperimentRunner"
    binary_path.write_text("#!/bin/sh\nexit 0")
    binary_path.chmod(0o755)
    runner.opendc_path = str(binary_path)

    workload_dir = tmp_path / "workload"
    workload_dir.mkdir()

    monkeypatch.setattr("opendt.core.simulation.runner.ensure_workload_dir", lambda: workload_dir)

    recorded: dict[str, Any] = {}

    def fake_run(args, capture_output, text, timeout, env):  # type: ignore[override]
        recorded["args"] = args
        recorded["capture_output"] = capture_output
        recorded["text"] = text
        recorded["timeout"] = timeout
        recorded["env"] = env

        class Result:
            returncode = 0
            stdout = "all good"
            stderr = ""

        return Result()

    monkeypatch.setattr("opendt.core.simulation.runner.subprocess.run", fake_run)
    monkeypatch.setattr(
        runner,
        "parse_opendc_results",
        lambda: {"status": "success", "energy_kwh": 0.0},
    )

    outcome = runner.run_simulation(
        tasks_data=[{"id": 1}],
        fragments_data=[{"id": 1}],
        topology_data={"nodes": []},
        expName="unit-test",
    )

    assert outcome == {"status": "success", "energy_kwh": 0.0}
    assert recorded["args"][0] == runner.opendc_path
    assert "--experiment-path" in recorded["args"]
    assert recorded["capture_output"] is True
    assert recorded["text"] is True
    assert recorded["timeout"] == 120
    expected_java = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")
    assert recorded["env"].get("JAVA_HOME") == expected_java


def test_parse_results_aggregates_metrics(tmp_path, monkeypatch):
    """Parsing should summarise energy, utilisation and runtime from parquet output."""

    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    power = pd.DataFrame(
        {
            "energy_usage": [3_600_000, 1_800_000],
            "power_draw": [100.0, 150.5],
        }
    )
    host = pd.DataFrame({"cpu_utilization": [0.4, 0.6]})
    service = pd.DataFrame({"timestamp": [1_000, 9_001_000]})

    power.to_parquet(tmp_path / "powerSource.parquet")
    host.to_parquet(tmp_path / "host.parquet")
    service.to_parquet(tmp_path / "service.parquet")

    result = runner.parse_opendc_results()

    assert result == {
        "energy_kwh": pytest.approx(1.5, rel=1e-3),
        "cpu_utilization": pytest.approx(0.5, rel=1e-3),
        "max_power_draw": pytest.approx(150.5, rel=1e-3),
        "runtime_hours": pytest.approx(2.5, rel=1e-3),
        "status": "success",
    }


def test_parse_results_handles_missing_files(tmp_path, monkeypatch):
    """If no parquet outputs exist we still return a success payload with zeroes."""

    runner = OpenDCRunner()
    monkeypatch.setenv("OPENDT_SIM_DIR", str(tmp_path))

    result = runner.parse_opendc_results()

    assert result == {
        "energy_kwh": 0.0,
        "cpu_utilization": 0.0,
        "max_power_draw": 0.0,
        "runtime_hours": 0.0,
        "status": "success",
    }
