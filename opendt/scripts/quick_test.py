#!/usr/bin/env python3
"""Quick system health check"""
import subprocess
import json


def test_kafka():
    """Test Kafka connectivity"""
    try:
        result = subprocess.run(['kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print("✅ Kafka: Running")
            return True
        else:
            print("❌ Kafka: Not responding")
            return False
    except Exception as e:
        print(f"❌ Kafka: Error - {e}")
        return False


def test_opendc():
    """Test OpenDC runner"""
    import os
    runner_path = "opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner"
    if os.path.exists(runner_path):
        print("✅ OpenDC: Runner found")
        return True
    else:
        print("❌ OpenDC: Runner not found")
        return False


def main():
    print("🔍 OpenDT System Health Check")
    print("=" * 40)

    kafka_ok = test_kafka()
    opendc_ok = test_opendc()

    if kafka_ok and opendc_ok:
        print("\n🎉 System ready!")
        return True
    else:
        print("\n⚠️  System has issues")
        return False


if __name__ == "__main__":
    main()
