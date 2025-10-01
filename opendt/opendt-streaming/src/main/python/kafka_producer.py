#!/usr/bin/env python3
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any
from datetime import datetime


class OpenDTKafkaConsumer:
    """OpenDT Kafka Consumer for telemetry data"""

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None

    def setup_consumer(self, topics: list) -> KafkaConsumer:
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='opendt-consumer-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
            )
            print(f"✅ Connected to Kafka: {topics}")
            return self.consumer

        except KafkaError as e:
            print(f"❌ Kafka connection failed: {e}")
            raise

    def process_telemetry_stream(self):
        """Process incoming telemetry data"""
        topics = ['tasks', 'fragments', 'topology_updates']
        consumer = self.setup_consumer(topics)

        print("🔍 Listening to OpenDT telemetry streams...")
        print("=" * 60)

        try:
            for message in consumer:
                self._handle_message(message)

        except KeyboardInterrupt:
            print("\n🛑 Stopping consumer...")
        finally:
            consumer.close()

    def _handle_message(self, message):
        """Handle individual Kafka message"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        topic = message.topic
        key = message.key
        value = message.value

        # Format output based on topic
        if topic == 'tasks':
            self._print_task_data(timestamp, key, value)
        elif topic == 'fragments':
            self._print_fragment_data(timestamp, key, value)
        elif topic == 'topology_updates':
            self._print_topology_update(timestamp, value)

    def _print_task_data(self, timestamp: str, key: Dict, value: Dict):
        """Format task telemetry"""
        task_id = key.get('id', 'unknown') if key else 'unknown'
        duration = value.get('duration', 0)
        cpu_count = value.get('cpu_count', 0)

        print(f"📋 [{timestamp}] TASK    | ID: {task_id:>6} | Duration: {duration:>8}ms | CPUs: {cpu_count}")

    def _print_fragment_data(self, timestamp: str, key: Dict, value: Dict):
        """Format fragment telemetry"""
        frag_id = key.get('id', 'unknown') if key else 'unknown'
        duration = value.get('duration', 0)
        cpu_usage = value.get('cpu_usage', 0.0)

        print(f"🔥 [{timestamp}] FRAGMENT| ID: {frag_id:>6} | Duration: {duration:>8}ms | CPU: {cpu_usage:.2f}%")

    def _print_topology_update(self, timestamp: str, value: Dict):
        """Format topology update"""
        update_type = value.get('type', 'unknown')
        print(f"🏗️  [{timestamp}] TOPOLOGY| Type: {update_type}")


def main():
    """Main consumer entry point"""
    consumer = OpenDTKafkaConsumer()
    consumer.process_telemetry_stream()


if __name__ == "__main__":
    main()
