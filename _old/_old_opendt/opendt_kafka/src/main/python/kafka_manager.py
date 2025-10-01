#!/usr/bin/env python3
import subprocess
import json
import time
from typing import Dict, List, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


class OpenDTKafkaManager:
    """Manage Kafka operations for OpenDT"""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.producer = None

    def setup_kafka_cluster(self):
        """Complete Kafka cluster setup"""
        print("🚀 Setting up OpenDT Kafka cluster...")

        # 1. Verify Kafka is running
        self._verify_kafka_running()

        # 2. Create admin client
        self._setup_admin_client()

        # 3. Create topics
        self._create_opendt_topics()

        # 4. Setup producer
        self._setup_producer()

        print("✅ Kafka cluster setup complete!")

    def _verify_kafka_running(self):
        """Check if Kafka is running"""
        try:
            # Test connection with a quick topic list
            result = subprocess.run([
                f"{self._get_kafka_home()}/bin/kafka-topics.sh",
                "--bootstrap-server", self.bootstrap_servers,
                "--list"
            ], capture_output=True, text=True, timeout=5)

            if result.returncode == 0:
                print("✅ Kafka cluster is running")
                return True
            else:
                raise Exception("Kafka not responding")

        except Exception as e:
            print(f"❌ Kafka cluster not available: {e}")
            print("💡 Start Kafka first with: $KAFKA_HOME/bin/kafka-server-start.sh config/server.properties")
            raise

    def _get_kafka_home(self) -> str:
        """Get Kafka installation path"""
        import os
        kafka_home = os.environ.get('KAFKA_HOME', '/opt/kafka')
        return kafka_home

    def _setup_admin_client(self):
        """Initialize Kafka admin client"""
        from kafka.admin import KafkaAdminClient
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='opendt-admin'
        )

    def _create_opendt_topics(self):
        """Create all required OpenDT topics"""
        topics = [
            NewTopic(name="tasks", num_partitions=4, replication_factor=1),
            NewTopic(name="fragments", num_partitions=4, replication_factor=1),
            NewTopic(name="topology_updates", num_partitions=2, replication_factor=1),
            NewTopic(name="simulation_results", num_partitions=2, replication_factor=1),
            NewTopic(name="llm_recommendations", num_partitions=1, replication_factor=1)
        ]

        try:
            fs = self.admin_client.create_topics(new_topics=topics, validate_only=False)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print(f"✅ Created topic: {topic}")
                except TopicAlreadyExistsError:
                    print(f"ℹ️  Topic already exists: {topic}")
                except Exception as e:
                    print(f"❌ Failed to create topic {topic}: {e}")
        except Exception as e:
            print(f"❌ Topic creation failed: {e}")

    def _setup_producer(self):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print("✅ Producer initialized")

    def send_telemetry_batch(self, topic: str, records: List[Dict]):
        """Send batch of telemetry records"""
        if not self.producer:
            raise Exception("Producer not initialized")

        for record in records:
            key = {"id": record.get("id")}
            value = {k: v for k, v in record.items() if k != "id"}

            self.producer.send(topic, key=key, value=value)

        self.producer.flush()
        print(f"📤 Sent {len(records)} records to {topic}")

    def get_topic_info(self) -> Dict:
        """Get information about all topics"""
        metadata = self.admin_client.describe_topics()
        return {topic: info for topic, info in metadata.items()}

    def cleanup_topics(self):
        """Delete all OpenDT topics"""
        topics_to_delete = ["tasks", "fragments", "topology_updates", "simulation_results", "llm_recommendations"]

        try:
            fs = self.admin_client.delete_topics(topics=topics_to_delete, timeout_ms=10000)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"🗑️  Deleted topic: {topic}")
                except Exception as e:
                    print(f"❌ Failed to delete topic {topic}: {e}")
        except Exception as e:
            print(f"❌ Topic deletion failed: {e}")


def main():
    """CLI interface for Kafka management"""
    import sys

    manager = OpenDTKafkaManager()

    if len(sys.argv) < 2:
        print("Usage: python kafka_manager.py [setup|cleanup|info]")
        return

    command = sys.argv[1]

    if command == "setup":
        manager.setup_kafka_cluster()
    elif command == "cleanup":
        manager.cleanup_topics()
    elif command == "info":
        info = manager.get_topic_info()
        print(json.dumps(info, indent=2))
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
