# Sim-Worker Service

Simulation Worker service that consumes workload events from Kafka and performs simulations.

## Features

- Consumes tasks, fragments, and consumption events from Kafka
- Runs digital twin simulations based on workload data
- Produces simulation results back to Kafka
- Scalable worker architecture (can run multiple instances)

## Development

### Local Setup

```bash
pip install -e ../../libs/common
pip install -e .
```

Set environment variables:
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export WORKER_ID=worker-1
```

Run:
```bash
python -m sim_worker.main
```

### Docker

The service runs automatically via docker-compose. Multiple worker instances can be scaled:

```bash
docker compose up --scale sim-worker=3
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `WORKER_ID` | Unique worker identifier | `worker-1` |
| `CONSUMER_GROUP` | Kafka consumer group | `sim-workers` |

## Kafka Topics

### Consumes:
- `tasks` - Task events to simulate
- `fragments` - Fragment events for fine-grained simulation
- `consumption` - Power consumption data

### Produces:
- `simulation-results` - Simulation output data
- `simulation-metrics` - Performance metrics
