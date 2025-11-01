"""Unit coverage for the simulation data adapter utilities."""

from __future__ import annotations

import math

import pandas as pd

from opendt.core.simulation import adapters


def test_ensure_workload_dir_creates_directory(tmp_path):
    target = tmp_path / "nested" / "workload"

    path = adapters.ensure_workload_dir(str(target))

    assert path == target
    assert path.exists()
    assert path.is_dir()


def test_tasks_to_table_applies_defaults():
    table = adapters.tasks_to_table(
        [
            {
                "id": 7,
                "duration": 42,
                "cpu_count": 4,
                "cpu_capacity": 3.2,
                # purposely omit submission_time and mem_capacity
            }
        ]
    )

    df = table.to_pandas()

    assert list(df.columns) == [
        "id",
        "submission_time",
        "duration",
        "cpu_count",
        "cpu_capacity",
        "mem_capacity",
    ]
    assert df.loc[0, "id"] == 7
    assert df.loc[0, "duration"] == 42
    assert df.loc[0, "cpu_count"] == 4
    assert math.isclose(df.loc[0, "cpu_capacity"], 3.2)
    # defaults to start of 2024 converted to milliseconds epoch
    assert df.loc[0, "submission_time"] == pd.Timestamp("2024-01-01").value // 1_000_000
    assert df.loc[0, "mem_capacity"] == 1024 ** 3


def test_fragments_to_table_populates_usage():
    table = adapters.fragments_to_table(
        [
            {
                "id": 3,
                "duration": 10_000,
                "cpu_usage": 0.75,
            },
            {
                "id": 4,
            },
        ]
    )

    df = table.to_pandas()

    assert list(df.columns) == ["id", "duration", "cpu_count", "cpu_usage"]
    assert df.loc[0, "id"] == 3
    assert df.loc[0, "duration"] == 10_000
    assert df.loc[0, "cpu_count"] == 1
    assert math.isclose(df.loc[0, "cpu_usage"], 0.75)
    # Missing values should fall back to defaults
    assert df.loc[1, "duration"] == 10_000
    assert math.isclose(df.loc[1, "cpu_usage"], 0.5)
