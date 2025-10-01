#!/usr/bin/env python3
"""OpenDT Kafka telemetry producer"""
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
import sys
import os
from pathlib import Path


class OpenDTTelemetryProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
            acks='all',
            retries=3
        )
        print(f"✅ Producer connected to {bootstrap_servers}")

    def load_workload_data(self, tasks_path: str, fragments_path: str):
        """Load parquet workload data"""
        try:
            tasks_df = pd.read_parquet(tasks_path)
            fragments_df = pd.read_parquet(fragments_path)
            print(f"✅ Loaded {len(tasks_df)} tasks, {len(fragments_df)} fragments")
            return tasks_df, fragments_df
        except Exception as e:
            print(f"❌ Failed to load workload data: {e}")
            return self.create_mock_workload()

    def create_mock_workload(self):
        """Create mock workload data for testing"""
        print("📝 Creating mock workload data...")

        # Mock tasks
        tasks_data = []
        for i in range(100):
            tasks_data.append({
                'id': i,
                'submission_time': datetime.now(),
                'duration': 300000 + (i * 10000),  # 5-15 minutes
                'cpu_count': min(4, max(1, i % 8)),
                'cpu_capacity': 2400.0 + (i % 1200),
                'mem_capacity': 8192 + (i * 1024)
            })

        # Mock fragments
        fragments_data = []
        for i in range(500):
            fragments_data.append({
                'id': i // 5,  # Multiple fragments per task
                'duration': 60000 + (i * 1000),  # 1-10 minutes
                'cpu_count': 1,
                'cpu_usage': 1200.0 + (i % 1000)
            })

        tasks_df = pd.DataFrame(tasks_data)
        fragments_df = pd.DataFrame(fragments_data)

        return tasks_df, fragments_df

    def stream_telemetry(self, tasks_df, fragments_df, real_time_factor=0.1):
        """Stream telemetry data to Kafka"""
        print(f"🚀 Starting telemetry streaming (factor: {real_time_factor}x)")

        task_count = 0
        fragment_count = 0

        try:
            # Stream tasks
            for _, task in tasks_df.iterrows():
                key = {"id": int(task['id'])}

                # Handle timestamp conversion
                submission_time = task['submission_time']
                if hasattr(submission_time, 'isoformat'):
                    submission_time_str = submission_time.isoformat()
                else:
                    submission_time_str = str(submission_time)

                value = {
                    "submission_time": submission_time_str,
                    "duration": int(task['duration']),
                    "cpu_count": int(task['cpu_count']),
                    "cpu_capacity": float(task.get('cpu_capacity', 2400.0)),
                    "mem_capacity": int(task.get('mem_capacity', 8192))
                }

                self.producer.send('tasks', key=key, value=value)
                task_count += 1

                if task_count % 10 == 0:
                    print(f"📋 Sent {task_count} tasks...")
                    time.sleep(0.1 * real_time_factor)

            # Stream fragments
            for _, fragment in fragments_df.iterrows():
                key = {"id": int(fragment['id'])}
                value = {
                    "duration": int(fragment['duration']),
                    "cpu_count": int(fragment.get('cpu_count', 1)),
                    "cpu_usage": float(fragment['cpu_usage'])
                }

                self.producer.send('fragments', key=key, value=value)
                fragment_count += 1

                if fragment_count % 50 == 0:
                    print(f"🔥 Sent {fragment_count} fragments...")
                    time.sleep(0.05 * real_time_factor)

            # Ensure all messages are sent
            self.producer.flush()
            print(f"✅ Streaming complete: {task_count} tasks, {fragment_count} fragments")

        except Exception as e:
            print(f"❌ Streaming error: {e}")
        finally:
            self.producer.close()


def main():
    producer = OpenDTTelemetryProducer()

    # Try to load real workload data, fallback to mock data
    workload_base = Path("resources/workload_traces")

    # Look for parquet files
    tasks_path = None
    fragments_path = None

    # Check various possible locations
    search_paths = [
        workload_base / "kafka_stream",
        workload_base / "06_10_2022_21_57_30-06_10_2022_22_02_30",
        Path("_old/data/workload_traces/surf_new"),
        Path("data/workload_traces/surf_new")
    ]

    for search_path in search_paths:
        if search_path.exists():
            task_files = list(search_path.glob("**/tasks.parquet"))
            frag_files = list(search_path.glob("**/fragments.parquet"))

            if task_files and frag_files:
                tasks_path = str(task_files[0])
                fragments_path = str(frag_files[0])
                print(f"📂 Found workload data in: {search_path}")
                break

    if tasks_path and fragments_path:
        tasks_df, fragments_df = producer.load_workload_data(tasks_path, fragments_path)
    else:
        print("⚠️ No parquet files found, using mock data")
        tasks_df, fragments_df = producer.create_mock_workload()

    # Stream the data
    producer.stream_telemetry(tasks_df, fragments_df)


if __name__ == "__main__":
    main()
