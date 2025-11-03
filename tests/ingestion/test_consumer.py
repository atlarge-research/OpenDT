"""End-to-end tests for assembling DigitalTwinConsumer telemetry windows."""

from datetime import datetime, timedelta

import pandas as pd

from opendt.adapters.ingestion.kafka.consumer import DigitalTwinConsumer


def _iso(ts: datetime) -> str:
    """Return the ISO-8601 string representation of ``ts``."""
    return ts.isoformat()


def test_create_batch_compiles_window_data():
    """Create a window with tasks/fragments and ensure ``create_batch`` aggregates it."""
    consumer = DigitalTwinConsumer(bootstrap_servers="localhost:9092", kafka_group_id="test")
    now = datetime.utcnow()
    task = {"id": 1, "submission_time": _iso(now), "duration": 100, "cpu_count": 2, "cpu_capacity": 2.4, "mem_capacity": 1024}
    fragment = {"id": 1, "submission_time": _iso(now + timedelta(seconds=10)), "duration": 50, "cpu_usage": 0.5}

    with consumer.windows_lock:
        window = consumer._DigitalTwinConsumer__add_to_window(task, "tasks")
        consumer._DigitalTwinConsumer__add_to_window(fragment, "fragments")
        window["ready"] = True

    batch = consumer.create_batch(window_number=1)

    assert batch["task_count"] == 1
    assert batch["fragment_count"] == 1
    assert isinstance(batch["avg_cpu_usage"], float)
    assert batch["tasks_sample"][0]["id"] == 1
    assert batch["fragments_sample"][0]["id"] == 1
    assert "window_info" in batch
