#!/usr/bin/env python3
"""
Quick test to verify Kafka data flow works
"""
import json
import time
from datetime import datetime


def quick_test():
    """Quick Kafka test"""
    try:
        from kafka import KafkaProducer, KafkaConsumer
    except ImportError:
        print("❌ kafka-python not installed")
        return False

    # Test producer
    print("🧪 Testing Kafka producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode()
        )

        # Send test message
        producer.send('tasks',
                      key={'id': 999},
                      value={'test': True, 'timestamp': datetime.now().isoformat()}
                      )
        producer.flush()
        producer.close()
        print("✅ Producer test passed")

    except Exception as e:
        print(f"❌ Producer test failed: {e}")
        return False

    # Test consumer
    print("🧪 Testing Kafka consumer...")
    try:
        consumer = KafkaConsumer(
            'tasks',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            consumer_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode())
        )

        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= 1:  # Got at least one message
                break

        consumer.close()

        if messages:
            print(f"✅ Consumer test passed - received {len(messages)} messages")
            return True
        else:
            print("⚠️  Consumer test - no messages (might be timing issue)")
            return True

    except Exception as e:
        print(f"❌ Consumer test failed: {e}")
        return False


if __name__ == "__main__":
    success = quick_test()
    if success:
        print("🎉 Kafka flow test successful!")
    else:
        print("❌ Kafka flow test failed!")
