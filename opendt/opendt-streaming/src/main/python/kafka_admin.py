#!/usr/bin/env python3
"""Kafka administration utilities for OpenDT"""
import subprocess
import json
import time
from typing import Dict, List
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
import sys


class OpenDTKafkaAdmin:
    """Kafka administration for OpenDT digital twin system"""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.producer = None

    def setup_complete_kafka_system(self):
        """Complete Kafka system setup for OpenDT"""
        print("🚀 Setting up OpenDT Kafka system...")

        # 1. Verify Kafka is running
        self._verify_kafka_running()

        # 2. Create admin client
        self._setup_admin_client()

        # 3. Create all topics
        self._create_opendt_topics()

        # 4. Configure topics for optimal performance
        self._configure_topics()

        # 5. Setup producer
        self._setup_producer()

        # 6. Verify setup
        self._verify_setup()

        print("✅ OpenDT Kafka system ready!")

    def _verify_kafka_running(self):
        """Check if Kafka is running"""
        try:
            result = subprocess.run([
                'kafka-topics', '--list',
                '--bootstrap-server', self.bootstrap_servers
            ], capture_output=True, text=True, timeout=10)

            if result.returncode == 0:
                print("✅ Kafka cluster is running")
                return True
            else:
                raise Exception(f"Kafka not responding: {result.stderr}")

        except subprocess.TimeoutExpired:
            raise Exception("Kafka connection timeout")
        except Exception as e:
            print(f"❌ Kafka cluster not available: {e}")
            print("💡 Start Kafka with: brew services start kafka")
            raise

    def _setup_admin_client(self):
        """Initialize Kafka admin client"""
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='opendt-admin'
        )
        print("✅ Admin client connected")

    def _create_opendt_topics(self):
        """Create all required OpenDT topics"""
        topics = [
            NewTopic(name="tasks", num_partitions=4, replication_factor=1),
            NewTopic(name="fragments", num_partitions=4, replication_factor=1),
            NewTopic(name="topology_updates", num_partitions=2, replication_factor=1),
            NewTopic(name="simulation_results", num_partitions=2, replication_factor=1),
            NewTopic(name="llm_recommendations", num_partitions=1, replication_factor=1),
            NewTopic(name="system_metrics", num_partitions=2, replication_factor=1)
        ]

        created_topics = []

        try:
            fs = self.admin_client.create_topics(new_topics=topics, validate_only=False)
            for topic, f in fs.items():
                try:
                    f.result()
                    created_topics.append(topic)
                    print(f"✅ Created topic: {topic}")
                except TopicAlreadyExistsError:
                    print(f"ℹ️  Topic already exists: {topic}")
                    created_topics.append(topic)
                except Exception as e:
                    print(f"❌ Failed to create topic {topic}: {e}")
        except Exception as e:
            print(f"❌ Topic creation failed: {e}")

        return created_topics

    def _configure_topics(self):
        """Configure topics for optimal performance"""
        topic_configs = {
            "tasks": {
                "retention.ms": "86400000",  # 24 hours
                "compression.type": "lz4",
                "cleanup.policy": "delete"
            },
            "fragments": {
                "retention.ms": "43200000",  # 12 hours
                "compression.type": "lz4",
                "cleanup.policy": "delete"
            },
            "topology_updates": {
                "retention.ms": "604800000",  # 7 days
                "cleanup.policy": "compact"
            },
            "simulation_results": {
                "retention.ms": "2592000000",  # 30 days
                "cleanup.policy": "delete"
            }
        }

        try:
            # Configure each topic
            for topic_name, config in topic_configs.items():
                resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                configs = {resource: config}

                self.admin_client.alter_configs(configs)
                print(f"✅ Configured topic: {topic_name}")

        except Exception as e:
            print(f"⚠️ Topic configuration warning: {e}")

    def _setup_producer(self):
        """Initialize high-performance Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            compression_type='lz4',
            max_in_flight_requests_per_connection=5
        )
        print("✅ High-performance producer initialized")

    def _verify_setup(self):
        """Verify complete setup"""
        try:
            # List topics
            metadata = self.admin_client.list_topics(timeout=5)
            topic_names = list(metadata.topics.keys())

            required_topics = ['tasks', 'fragments', 'topology_updates', 'simulation_results']
            missing_topics = [t for t in required_topics if t not in topic_names]

            if missing_topics:
                print(f"⚠️ Missing topics: {missing_topics}")
                return False
            else:
                print(f"✅ All required topics available: {required_topics}")
                return True

        except Exception as e:
            print(f"❌ Verification failed: {e}")
            return False

    def send_system_message(self, topic: str, message_type: str, data: dict):
        """Send system message to Kafka"""
        if not self.producer:
            self._setup_producer()

        key = {"type": message_type, "timestamp": time.time()}
        value = {
            "message_type": message_type,
            "timestamp": time.time(),
            "data": data
        }

        try:
            self.producer.send(topic, key=key, value=value)
            self.producer.flush()
            print(f"📤 Sent {message_type} message to {topic}")
        except Exception as e:
            print(f"❌ Failed to send message: {e}")

    def get_topic_info(self) -> Dict:
        """Get comprehensive topic information"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)

            topic_info = {}
            for topic_name, topic_metadata in metadata.topics.items():
                topic_info[topic_name] = {
                    "partitions": len(topic_metadata.partitions),
                    "error": topic_metadata.error.description if topic_metadata.error else None,
                    "is_internal": topic_metadata.is_internal
                }

            return topic_info

        except Exception as e:
            print(f"❌ Failed to get topic info: {e}")
            return {}

    def cleanup_topics(self):
        """Clean up all OpenDT topics"""
        opendt_topics = [
            "tasks", "fragments", "topology_updates",
            "simulation_results", "llm_recommendations", "system_metrics"
        ]

        try:
            fs = self.admin_client.delete_topics(topics=opendt_topics, timeout_ms=10000)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"🗑️ Deleted topic: {topic}")
                except Exception as e:
                    print(f"❌ Failed to delete topic {topic}: {e}")
        except Exception as e:
            print(f"❌ Topic cleanup failed: {e}")


def main():
    """CLI interface for Kafka administration"""
    if len(sys.argv) < 2:
        print("Usage: python kafka_admin.py [setup|info|cleanup|test]")
        return

    admin = OpenDTKafkaAdmin()
    command = sys.argv[1]

    if command == "setup":
        admin.setup_complete_kafka_system()
    elif command == "info":
        info = admin.get_topic_info()
        print("\n📊 Topic Information:")
        print(json.dumps(info, indent=2))
    elif command == "cleanup":
        admin.cleanup_topics()
    elif command == "test":
        admin.send_system_message("system_metrics", "health_check", {"status": "testing"})
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
