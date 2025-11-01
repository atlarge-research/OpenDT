from datetime import datetime, timezone
from pathlib import Path

from opendt.core.datalake import DataLake


def test_append_run_creates_files(tmp_path):
    root = tmp_path / "lake"
    lake = DataLake(root)

    artifact = tmp_path / "sample.parquet"
    artifact.write_text("data")

    record = lake.append_run(
        run_type="baseline",
        metrics={"energy_kwh": 1.2, "status": "success", "cpu_utilization": 0.4, "runtime_hours": 1.5, "max_power_draw": 120.0},
        topology={"clusters": []},
        metadata={"window_end": "2024-01-01T00:00:00Z"},
        timeseries={"power_draw": [{"timestamp": "2024-01-01T00:00:00Z", "value": 1.2}]},
        artifacts={"power": artifact},
        window_id="2024-01-01T00:00:00Z",
        cycle=1,
        attempt=0,
        exp_name="window_1_baseline",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        score=75.0,
    )

    index = Path(root) / "index.parquet"
    assert index.exists()
    run_dir = Path(root) / "runs" / record.run_id
    assert run_dir.exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "metrics.json").exists()
    assert (run_dir / "topology.json").exists()
    assert (run_dir / "metadata.json").exists()
    assert (run_dir / "timeseries.json").exists()
    copied = run_dir / "artifacts" / artifact.name
    assert copied.exists()
    assert copied.read_text() == "data"


def test_list_runs_returns_sorted(tmp_path):
    lake = DataLake(tmp_path)
    first = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    second = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    lake.append_run(run_type="baseline", metrics={}, topology=None, timestamp=first, metadata=None, timeseries=None, artifacts=None)
    lake.append_run(run_type="optimization", metrics={}, topology=None, timestamp=second, metadata=None, timeseries=None, artifacts=None)

    runs = lake.list_runs()
    assert len(runs) == 2
    assert runs[0]["timestamp"].startswith("2024-01-02")


def test_load_run_returns_payload(tmp_path):
    lake = DataLake(tmp_path)
    record = lake.append_run(run_type="baseline", metrics={}, topology=None, metadata=None, timeseries=None, artifacts=None)

    payload = lake.load_run(record.run_id)
    assert payload["summary"]["run_id"] == record.run_id
    assert isinstance(payload["metrics"], dict)
    assert isinstance(payload["timeseries"], dict)
