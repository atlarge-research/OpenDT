"""Shared utilities for OpenDT services."""

from opendt_common.utils.kafka import get_kafka_consumer, get_kafka_producer
from opendt_common.utils.topics import ensure_topics_exist, wait_for_topics

__all__ = [
    "get_kafka_producer",
    "get_kafka_consumer",
    "ensure_topics_exist",
    "wait_for_topics",
]
