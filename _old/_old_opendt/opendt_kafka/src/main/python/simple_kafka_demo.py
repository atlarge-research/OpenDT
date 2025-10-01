#!/usr/bin/env python3
"""
Ultra-simple Kafka demo - no dependencies on pandas, no Scala
"""
import json
import time
import sys
from datetime import datetime


def create_simple_telemetry_producer():
    """Simple telemetry producer without external dependencies"""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("❌ kafka-python not found. Install with:")
        print("   conda install -c conda-forge kafka-python")
        return None

    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        print("✅ Kafka producer connected!")
        return producer
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print("💡 Make sure Kafka is running on localhost:9092")
        return None


def send_mock_data(producer):
    """Send mock telemetry data"""
    print("🚀 Sending mock telemetry data...")

    # Send 50 tasks
    for i in range(50):
        key = {"id": i}
        value = {
            "timestamp": datetime.now().isoformat(),
            "duration": 1000 + (i * 100),
            "cpu_count": 4,
            "memory": 8192.0
        }

        producer.send('tasks', key=key, value=value)
        print(f"📋 Sent task {i}")
        time.sleep(0.2)  # Small delay

    # Send 100 fragments
    for i in range(100):
        key = {"id": i // 2}  # Multiple fragments per task
        value = {
            "timestamp": datetime.now().isoformat(),
            "duration": 500 + (i * 50),
            "cpu_usage": 50.0 + (i % 40),
            "memory_usage": 4096.0
        }

        producer.send('fragments', key=key, value=value)
        if i % 10 == 0:
            print(f"🔥 Sent fragment {i}")
        time.sleep(0.1)

    producer.flush()
    print("✅ All telemetry data sent!")


def main():
    producer = create_simple_telemetry_producer()
    if producer:
        send_mock_data(producer)
        producer.close()


if __name__ == "__main__":
    main()
