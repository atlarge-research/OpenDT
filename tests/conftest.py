"""Shared pytest fixtures and Kafka stubs used across the test suite."""

import sys
from pathlib import Path
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


fake_kafka = types.ModuleType("kafka")

fake_dotenv = types.ModuleType("dotenv")
fake_dotenv.load_dotenv = lambda *args, **kwargs: None


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
sys.modules.setdefault("dotenv", fake_dotenv)

from opendt.api.dependencies import get_orchestrator


@pytest.fixture(scope="session", autouse=True)
def stop_background_threads():
    """Ensure the global orchestrator threads do not interfere with tests."""
    orchestrator = get_orchestrator()
    orchestrator.stop_event.set()
    yield
    orchestrator.stop_event.set()
