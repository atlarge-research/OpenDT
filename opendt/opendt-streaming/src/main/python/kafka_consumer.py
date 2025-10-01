#!/usr/bin/env python3
"""OpenDT Kafka consumer for telemetry data"""
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any
from datetime import datetime
import time
import sys


class OpenDTKafkaConsumer:
    """Real-time Kafka consumer for OpenDT telemetry"""

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.message_counts = {'tasks': 0, 'fragments': 0, 'topology_updates': 0}

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
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=60000  # 60 second timeout
            )
            print(f"✅ Connected to Kafka: {topics}")
            return self.consumer

        except KafkaError as e:
            print(f"❌ Kafka connection failed: {e}")
            raise

    def process_telemetry_stream(self):
        """Process incoming telemetry data"""
        topics = ['tasks', 'fragments', 'topology_updates', 'simulation_results']
        consumer = self.setup_consumer(topics)

        print("🔍 OpenDT Digital Twin - Telemetry Monitor")
        print("=" * 60)
        print("Listening for real-time datacenter telemetry...")
        print("Topics: tasks, fragments, topology_updates, simulation_results")
        print("Press Ctrl+C to stop")
        print("-" * 60)

        try:
            start_time = time.time()

            for message in consumer:
                self._handle_message(message)

                # Print summary every 50 messages
                total_messages = sum(self.message_counts.values())
                if total_messages % 50 == 0 and total_messages > 0:
                    self._print_summary(start_time)

        except KeyboardInterrupt:
            print("\n🛑 Stopping telemetry monitor...")
        finally:
            consumer.close()
            self._print_final_summary(start_time if 'start_time' in locals() else time.time())

    def _handle_message(self, message):
        """Handle individual Kafka message"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        topic = message.topic
        key = message.key
        value = message.value

        self.message_counts[topic] = self.message_counts.get(topic, 0) + 1

        # Format output based on topic
        if topic == 'tasks':
            self._print_task_data(timestamp, key, value)
        elif topic == 'fragments':
            self._print_fragment_data(timestamp, key, value)
        elif topic == 'topology_updates':
            self._print_topology_update(timestamp, value)
        elif topic == 'simulation_results':
            self._print_simulation_results(timestamp, value)

    def _print_task_data(self, timestamp: str, key: Dict, value: Dict):
        """Format task telemetry"""
        task_id = key.get('id', 'unknown') if key else 'unknown'
        duration = value.get('duration', 0)
        cpu_count = value.get('cpu_count', 0)
        cpu_capacity = value.get('cpu_capacity', 0.0)

        print(
            f"📋 [{timestamp}] TASK    | ID:{task_id:>6} | Duration:{duration:>8}ms | CPUs:{cpu_count} | Cap:{cpu_capacity:.1f}MHz")

    def _print_fragment_data(self, timestamp: str, key: Dict, value: Dict):
        """Format fragment telemetry"""
        frag_id = key.get('id', 'unknown') if key else 'unknown'
        duration = value.get('duration', 0)
        cpu_usage = value.get('cpu_usage', 0.0)

        print(f"🔥 [{timestamp}] FRAGMENT| ID:{frag_id:>6} | Duration:{duration:>8}ms | Usage:{cpu_usage:>6.2f}MHz")

    def _print_topology_update(self, timestamp: str, value: Dict):
        """Format topology update"""
        update_type = value.get('type', 'unknown')
        action = value.get('action', 'unknown')
        reason = value.get('reason', 'No reason provided')

        print(f"🏗️  [{timestamp}] TOPOLOGY| Action:{action:>10} | Reason: {reason}")

    def _print_simulation_results(self, timestamp: str, value: Dict):
        """Format simulation results"""
        energy = value.get('energy_kwh', 0.0)
        runtime = value.get('runtime_hours', 0.0)
        cpu_util = value.get('cpu_utilization', 0.0)

        print(f"📊 [{timestamp}] SIM_DONE| Energy:{energy:>6.2f}kWh | Runtime:{runtime:>6.1f}h | CPU:{cpu_util:.1%}")

    def _print_summary(self, start_time: float):
        """Print periodic summary"""
        runtime = time.time() - start_time
        total_msgs = sum(self.message_counts.values())
        rate = total_msgs / runtime if runtime > 0 else 0

        print(f"\n📈 SUMMARY | Runtime: {runtime:.1f}s | Messages: {total_msgs} | Rate: {rate:.1f}/s")
        for topic, count in self.message_counts.items():
            if count > 0:
                print(f"   {topic}: {count}")
        print("-" * 60)

    def _print_final_summary(self, start_time: float):
        """Print final summary"""
        runtime = time.time() - start_time
        total_msgs = sum(self.message_counts.values())

        print(f"\n✅ Session Complete")
        print(f"   Total Runtime: {runtime:.1f} seconds")
        print(f"   Total Messages: {total_msgs}")
        print(f"   Average Rate: {total_msgs / runtime:.2f} messages/second" if runtime > 0 else "")

        for topic, count in self.message_counts.items():
            if count > 0:
                print(f"   {topic.capitalize()}: {count} messages")


def main():
    """Main consumer entry point"""
    consumer = OpenDTKafkaConsumer()
    consumer.process_telemetry_stream()


if __name__ == "__main__":
    main()
