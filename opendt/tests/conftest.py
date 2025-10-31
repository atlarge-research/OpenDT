import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import types

import pytest


fake_kafka = types.ModuleType("kafka")


class _KafkaProducer:
    def __init__(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass

    def flush(self):
        pass


class _KafkaConsumer:
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
