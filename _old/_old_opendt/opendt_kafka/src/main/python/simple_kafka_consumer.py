#!/usr/bin/env python3
"""
Ultra-simple Kafka consumer - no external dependencies except kafka-python
"""
import json
from datetime import datetime


def start_simple_consumer():
    """Simple Kafka consumer"""
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("❌ kafka-python not found. Install with:")
        print("   conda install -c conda-forge kafka-python")
        return

    try:
        consumer = KafkaConsumer(
            'tasks', 'fragments',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            group_id='simple-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )

        print("🔍 Listening to Kafka streams...")
        print("=" * 60)

        for message in consumer:
            timestamp = datetime.now().strftime("%H:%M:%S")
            topic = message.topic
            key = message.key
            value = message.value

            # Simple formatting
            if topic == 'tasks':
                task_id = key.get('id', 'unknown') if key else 'unknown'
                duration = value.get('duration', 0)
                print(f"📋 [{timestamp}] TASK {task_id:>3} | Duration: {duration}ms")

            elif topic == 'fragments':
                frag_id = key.get('id', 'unknown') if key else 'unknown'
                duration = value.get('duration', 0)
                cpu = value.get('cpu_usage', 0)
                print(f"🔥 [{timestamp}] FRAG {frag_id:>3} | Duration: {duration}ms | CPU: {cpu:.1f}%")

    except Exception as e:
        print(f"❌ Consumer failed: {e}")
        print("💡 Make sure Kafka is running and topics exist")


if __name__ == "__main__":
    start_simple_consumer()
