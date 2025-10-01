# OpenDT - Digital Twin for Datacenters

Real-time datacenter simulation with Kafka streaming and LLM-powered optimization.

## Quick Start

1. Start Kafka:
`brew services start kafka`

2. Setup topics:
`./scripts/setup_kafka.sh`

3. Run system:
`python opendt-orchestrator/digital_twin_runner.py`

4. Monitor:
`python opendt-streaming/src/main/python/kafka_consumer.py`

## Components

- **opendt-streaming**: Kafka-based telemetry streaming
- **opendt-simulator**: OpenDC integration for datacenter simulation  
- **opendt-intelligence**: LLM-powered topology optimization
- **opendt-orchestrator**: System coordination and control

## Prerequisites

- Python 3.10+, Scala 2.12+, Java 11+
- Apache Kafka 4.x (KRaft mode)
- OpenDC ExperimentRunner (included)

See individual component README files for detailed setup.
