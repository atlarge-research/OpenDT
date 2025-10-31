import sys
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

from opendt.app import create_app
from opendt.orchestrator.controller import OpenDTOrchestrator


@pytest.fixture
def orchestrator(monkeypatch):
    monkeypatch.setattr(OpenDTOrchestrator, "start_topology_watcher", lambda self: None)
    orch = OpenDTOrchestrator()
    orch.stop_event.set()
    return orch


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(OpenDTOrchestrator, "start_topology_watcher", lambda self: None)
    flask_app = create_app()
    flask_app.orchestrator.stop_event.set()
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()
