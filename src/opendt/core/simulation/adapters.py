"""Helpers translating streaming data into OpenDC artifacts."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd
import pyarrow as pa


def ensure_workload_dir(base: str = "/tmp/opendt_workload") -> Path:
    path = Path(base)
    path.mkdir(parents=True, exist_ok=True)
    return path


def tasks_to_table(tasks_data: Iterable[Mapping[str, object]]) -> pa.Table:
    tasks_df = pd.DataFrame([
        {
            "id": task.get("id", 0),
            "submission_time": int(pd.to_datetime(task.get("submission_time", "2024-01-01")).value) // 1_000_000,
            "duration": task.get("duration", 30000),
            "cpu_count": task.get("cpu_count", 1),
            "cpu_capacity": task.get("cpu_capacity", 2400.0),
            "mem_capacity": task.get("mem_capacity", 1024 ** 3),
        }
        for task in tasks_data
    ])

    schema = pa.schema(
        [
            pa.field("id", pa.int32(), False),
            pa.field("submission_time", pa.int64(), False),
            pa.field("duration", pa.int64(), False),
            pa.field("cpu_count", pa.int32(), False),
            pa.field("cpu_capacity", pa.float64(), False),
            pa.field("mem_capacity", pa.int64(), False),
        ]
    )
    return pa.Table.from_pandas(tasks_df, schema=schema, preserve_index=False)


def fragments_to_table(fragments_data: Iterable[Mapping[str, object]]) -> pa.Table:
    frags_df = pd.DataFrame([
        {
            "id": frag.get("id", 0),
            "duration": frag.get("duration", 10000),
            "cpu_count": 1,
            "cpu_usage": frag.get("cpu_usage", 0.5),
        }
        for frag in fragments_data
    ])

    schema = pa.schema(
        [
            pa.field("id", pa.int32(), False),
            pa.field("duration", pa.int64(), False),
            pa.field("cpu_count", pa.int32(), False),
            pa.field("cpu_usage", pa.float64(), False),
        ]
    )
    return pa.Table.from_pandas(frags_df, schema=schema, preserve_index=False)
