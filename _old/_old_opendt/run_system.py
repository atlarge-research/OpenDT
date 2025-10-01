#!/usr/bin/env python3
"""
Complete OpenDT system runner - orchestrates everything
"""
import subprocess
import time
import sys
import os


def check_kafka_running():
    """Check if Kafka is running"""
    result = subprocess.run([
        'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'
    ], capture_output=True, text=True)
    return result.returncode == 0


def ensure_kafka_topics():
    """Create required topics"""
    topics = ['tasks', 'fragments', 'topology_updates', 'simulation_results']

    for topic in topics:
        subprocess.run([
            'kafka-topics', '--create', '--topic', topic,
            '--bootstrap-server', 'localhost:9092',
            '--partitions', '3', '--replication-factor', '1'
        ], capture_output=True)
        print(f"✅ Topic '{topic}' ready")


def start_consumer():
    """Start Kafka consumer in background"""
    return subprocess.Popen([
        sys.executable, 'opendt_kafka/src/main/python/simple_kafka_consumer.py'
    ])


def start_producer():
    """Start telemetry producer"""
    return subprocess.Popen([
        sys.executable, 'opendt_kafka/src/main/python/simple_kafka_demo.py'
    ])


def main():
    print("🚀 Starting OpenDT Digital Twin System")

    # 1. Check Kafka
    if not check_kafka_running():
        print("❌ Kafka not running. Start with: brew services start kafka")
        return
    print("✅ Kafka is running")

    # 2. Setup topics
    ensure_kafka_topics()

    # 3. Start consumer
    consumer_proc = start_consumer()
    print("✅ Consumer started")
    time.sleep(2)

    # 4. Start producer
    producer_proc = start_producer()
    print("✅ Producer started")

    try:
        print("🔥 System running! Press Ctrl+C to stop")
        producer_proc.wait()  # Wait for producer to finish
        print("✅ Producer completed")

        print("Press Ctrl+C to stop consumer...")
        consumer_proc.wait()

    except KeyboardInterrupt:
        print("\n🛑 Stopping system...")
        consumer_proc.terminate()
        producer_proc.terminate()

        # Wait for cleanup
        try:
            consumer_proc.wait(timeout=5)
            producer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            consumer_proc.kill()
            producer_proc.kill()

        print("✅ System stopped")


if __name__ == "__main__":
    main()
