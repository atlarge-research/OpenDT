"""Compatibility proxy for Kafka ingestion helpers."""

from opendt.adapters.ingestion.kafka.consumer import DigitalTwinConsumer
from opendt.adapters.ingestion.kafka.producer import TimedKafkaProducer

__all__ = ["DigitalTwinConsumer", "TimedKafkaProducer"]
