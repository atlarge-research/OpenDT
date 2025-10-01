#!/usr/bin/env python3
import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta
import sys
import os

# Add the kafka manager to path
sys.path.append('opendt_kafka/src/main/python')
from opendt.opendt_kafka.src.main.python.kafka_manager import OpenDTKafkaManager


class PythonTelemetrySim:
    """Pure Python telemetry simulator"""

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    def setup_producer(self):
        """Setup Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )

    def load_parquet_data(self, tasks_path: str, fragments_path: str):
        """Load parquet data using pandas"""
        try:
            tasks_df = pd.read_parquet(tasks_path)
            fragments_df = pd.read_parquet(fragments_path)

            print(f"✅ Loaded {len(tasks_df)} tasks, {len(fragments_df)} fragments")
            return tasks_df, fragments_df

        except Exception as e:
            print(f"❌ Failed to load data: {e}")
            # Create mock data for demo
            return self.create_mock_data()

    def create_mock_data(self):
        """Create mock telemetry data"""
        print("📝 Creating mock telemetry data...")

        now = datetime.now()

        # Mock tasks
        tasks_data = []
        for i in range(100):
            tasks_data.append({
                'id': i,
                'submission_time': now + timedelta(seconds=i * 2),
                'duration': 5000 + (i * 100),
                'cpu_count': 4,
                'memory': 8192.0
            })

        # Mock fragments
        fragments_data = []
        for i in range(200):
            fragments_data.append({
                'id': i // 2,  # Multiple fragments per task
                'duration': 1000 + (i * 50),
                'cpu_usage': 50.0 + (i % 40),
                'memory_usage': 4096.0
            })

        tasks_df = pd.DataFrame(tasks_data)
        fragments_df = pd.DataFrame(fragments_data)

        return tasks_df, fragments_df

    def stream_to_kafka(self, tasks_df, fragments_df):
        """Stream data to Kafka"""
        if not self.producer:
            self.setup_producer()

        print("🚀 Starting telemetry streaming...")

        # Stream tasks
        for _, row in tasks_df.iterrows():
            key = {"id": int(row['id'])}
            value = {
                "submission_time": row['submission_time'].isoformat() if hasattr(row['submission_time'],
                                                                                 'isoformat') else str(
                    row['submission_time']),
                "duration": int(row['duration']),
                "cpu_count": int(row['cpu_count']),
                "memory": float(row['memory'])
            }

            self.producer.send('tasks', key=key, value=value)

            if row['id'] % 10 == 0:
                print(f"📋 Sent task {row['id']}")
                time.sleep(0.1)  # Small delay

        # Stream fragments
        for _, row in fragments_df.iterrows():
            key = {"id": int(row['id'])}
            value = {
                "duration": int(row['duration']),
                "cpu_usage": float(row['cpu_usage']),
                "memory_usage": float(row['memory_usage'])
            }

            self.producer.send('fragments', key=key, value=value)

            if row['id'] % 20 == 0:
                print(f"🔥 Sent fragment {row['id']}")
                time.sleep(0.05)

        self.producer.flush()
        print("✅ Telemetry streaming complete!")


def main():
    if len(sys.argv) < 3:
        print("Usage: python python_telemetry_sim.py <tasks_path> <fragments_path>")
        tasks_path = "mock"
        fragments_path = "mock"
    else:
        tasks_path = sys.argv[1]
        fragments_path = sys.argv[2]

    # Setup Kafka first
    manager = OpenDTKafkaManager()
    manager.setup_kafka_cluster()

    # Start simulator
    sim = PythonTelemetrySim()

    if tasks_path == "mock":
        tasks_df, fragments_df = sim.create_mock_data()
    else:
        tasks_df, fragments_df = sim.load_parquet_data(tasks_path, fragments_path)

    sim.stream_to_kafka(tasks_df, fragments_df)


if __name__ == "__main__":
    main()
