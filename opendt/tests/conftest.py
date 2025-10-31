"""Shared pytest fixtures and Kafka stubs used across the test suite."""

import sys
import types

import pytest


fake_kafka = types.ModuleType("kafka")


class _KafkaProducer:
    """Minimal KafkaProducer stub used to satisfy orchestrator imports."""
    def __init__(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass

    def flush(self):
        pass


class _KafkaConsumer:
    """Minimal KafkaConsumer stub returning an empty iterator."""
    def __init__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return iter(())


fake_kafka.KafkaProducer = _KafkaProducer
fake_kafka.KafkaConsumer = _KafkaConsumer

sys.modules.setdefault("kafka", fake_kafka)

import main


@pytest.fixture(scope="session", autouse=True)
def stop_background_threads():
    """Ensure the global orchestrator threads do not interfere with tests."""
    try:
        main.orchestrator.stop_event.set()
    except AttributeError:
        pass
    yield
    try:
        main.orchestrator.stop_event.set()
    except AttributeError:
        pass
