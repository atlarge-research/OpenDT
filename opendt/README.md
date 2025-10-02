# OpenDT - Digital Twin for Datacenters

Real-time datacenter simulation with Kafka streaming and LLM-powered optimization.

## Quick Start - Brew + Docker

Firstly start Kafka on the system: "

```bash
cd opendt
export OPENAI_API_KEY="your-key-here"
docker-compose up
```

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
