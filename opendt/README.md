# OpenDT - Digital Twin for Datacenters

Real-time datacenter simulation with Kafka streaming and LLM-powered optimization.

## Quick Start - Docker

Docker does all the magic:

```bash
cd opendt
export OPENAI_API_KEY="your-key-here"
docker-compose down
docker-compose up --build
```

Then, open http://localhost:8080 for the orchestrator UI.

## Running the Test Suite

The test harness mirrors production by running inside the Docker Compose stack. From the project root run:

```bash
./scripts/run_tests.sh
```

Pass additional pytest arguments as needed, for example `./scripts/run_tests.sh -k kafka` to focus on Kafka-related specs.

