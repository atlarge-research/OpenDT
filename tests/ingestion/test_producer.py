"""Behavioral tests for the TimedKafkaProducer streaming helpers."""

from datetime import datetime

import pandas as pd

from opendt.adapters.ingestion.kafka.producer import TimedKafkaProducer


class DummyProducer:
    """Kafka producer test double that records outbound messages."""
    def __init__(self):
        self.messages = []

    def send(self, topic, key=None, value=None):
        self.messages.append((topic, key, value))

    def flush(self):
        pass


def test_tasks_streaming_thread_sends_in_order(monkeypatch):
    """Ensure tasks are emitted sequentially with the correct key and payload."""
    producer = TimedKafkaProducer(bootstrap_servers="localhost:9092")
    producer.start_streaming_barrier = type("B", (), {"wait": staticmethod(lambda: None)})
    producer.producer = DummyProducer()

    now = datetime.utcnow()
    tasks = pd.DataFrame([
        {"id": 1, "submission_time": now, "duration": 100, "cpu_count": 2, "cpu_capacity": 2.4, "mem_capacity": 1024},
        {"id": 2, "submission_time": now, "duration": 200, "cpu_count": 4, "cpu_capacity": 3.0, "mem_capacity": 2048},
    ])

    monkeypatch.setattr("opendt.adapters.ingestion.kafka.producer.sleep", lambda _: None)
    producer.tasks_streaming_thread(tasks, now)

    assert len(producer.producer.messages) == 2
    first = producer.producer.messages[0]
    assert first[0] == "tasks"
    assert first[1]["id"] == 1
    assert first[2]["duration"] == 100


def test_fragments_streaming_thread_sends(monkeypatch):
    """Verify fragment payloads are flushed to the expected Kafka topic."""
    producer = TimedKafkaProducer(bootstrap_servers="localhost:9092")
    producer.start_streaming_barrier = type("B", (), {"wait": staticmethod(lambda: None)})
    producer.producer = DummyProducer()

    now = datetime.utcnow()
    frags = pd.DataFrame([
        {"id": 1, "submission_time": now, "duration": 50, "cpu_usage": 0.5},
        {"id": 1, "submission_time": now, "duration": 60, "cpu_usage": 0.6},
    ])

    monkeypatch.setattr("opendt.adapters.ingestion.kafka.producer.sleep", lambda _: None)
    producer.fragments_streaming_thread(frags, now)

    assert len(producer.producer.messages) == 2
    topics = {topic for topic, *_ in producer.producer.messages}
    assert topics == {"fragments"}
