# Development Guide

This document explains all steps needed to get OpenDT running locally for development.

## Prerequisites

### Docker
OpenDT uses Docker for running the app in release mode, but also to easily run the Kafka broker service (and in the future, the database service). Make sure Docker is installed by following the instructions [here](https://docs.docker.com/desktop).

You can verify that it is installed by running
```sh
docker --version
```

## Quick Start

### Start Development

From the root directory, run:

```bash
docker compose up -d
```

This command will:

- Start the Kafka broker service
- Start the OpenDT Flask Python service

## Logging
To see the logs of the Kafka service, run:
```sh
docker compose logs -f --tail=1 kafka
```

To see _all_ the logs of the OpenDT app, run:
```sh
docker compose logs -f --tail=1 opendt
```

To see _just_ the logs of the Python modules, run:
```sh
docker compose logs --tail=1 -f --no-log-prefix opendt | jq 'select(.logger | startswith("werkzeug") | not)'
```

To see _just_ the logs of the Flask web app, run:
```sh
docker compose logs --tail=1 -f --no-log-prefix opendt | jq 'select(.logger | startswith("werkzeug"))'
```

## Repository

High-level overview of the repository:
```
OpenDT/
│
├── src/opendt/
│   ├── app.py ──────────────────── Flask application factory
│   ├── cli.py ──────────────────── Main entry point
│   │
│   ├── api/
│   │   ├── routes.py ───────────── REST endpoints + UI routes
│   │   ├── schemas.py ──────────── Pydantic validation models
│   │   └── dependencies.py ─────── Orchestrator singleton
│   │
│   ├── core/
│   │   │
│   │   ├── orchestrator/
│   │   │   ├── controller.py ──── Main orchestrator (coordination)
│   │   │   ├── state.py ───────── System state & buffers
│   │   │   ├── topology.py ────── Topology file watcher
│   │   │   └── slo.py ─────────── SLO file watcher
│   │   │
│   │   ├── simulation/
│   │   │   ├── runner.py ──────── OpenDC simulator wrapper
│   │   │   └── adapters.py ────── Parquet data conversion
│   │   │
│   │   ├── optimization/
│   │   │   ├── base.py ────────── Strategy protocol
│   │   │   ├── llm.py ─────────── OpenAI-based optimizer
│   │   │   ├── rule_based.py ──── Heuristic fallback
│   │   │   └── scoring.py ─────── SLO scoring functions
│   │   │
│   │   └── workers/
│   │       ├── scheduler.py ───── Thread utilities
│   │       └── tasks.py ────────── Background task definitions
│   │
│   ├── adapters/
│   │   └── ingestion/
│   │       └── kafka/
│   │           ├── producer.py ─── Streams workload to Kafka
│   │           └── consumer.py ─── Consumes & creates windows
│   │
│   └── config/
│       ├── settings.py ────────── Environment variables
│       └── loaders.py ─────────── JSON file I/O
│
├── src/templates/
│   └── index.html ─────────────── Dashboard UI
│
├── src/static/
│   ├── style.css
│   └── js/
│       ├── boot.js ────────────── App initialization
│       ├── polling.js ─────────── Status polling
│       ├── charts.js ──────────── Plotly visualization
│       ├── recommendations.js ── LLM recommendation UI
│       └── ui.js ──────────────── Event handlers
│
├── config/
│   ├── topology.json ──────────── Cluster/host configuration
│   └── slo.json ───────────────── Energy/runtime targets
│
├── surf-workload/
│   ├── tasks.parquet ──────────── Sample workload tasks
│   └── fragments.parquet ──────── Sample workload fragments
│
├── tests/
│   ├── api/
│   ├── config/
│   ├── ingestion/
│   ├── optimization/
│   ├── orchestrator/
│   └── simulation/
│
├── docker-compose.yml ─────────── Kafka + OpenDT services
├── Dockerfile ─────────────────── Container build
└── requirements.txt ───────────── Python dependencies
```

System flow overview:
```
Docker Compose
│
├── Kafka Service
│   └── Topics: tasks, fragments
│
└── OpenDT Service
    │
    └── Flask App (cli.py → app.py)
        │
        ├── API Routes (/api/*)
        │   └── → OpenDTOrchestrator
        │
        └── OpenDTOrchestrator (core/orchestrator/controller.py)
            │
            ├── Producer Thread
            │   └── TimedKafkaProducer
            │       └── Streams surf-workload/*.parquet → Kafka
            │
            └── Consumer Thread
                └── DigitalTwinConsumer
                    └── Creates windows from Kafka
                        │
                        └── For each window:
                            │
                            ├── 1. Baseline
                            │   └── OpenDCRunner → simulation results
                            │
                            ├── 2. Optimize (loop)
                            │   ├── LLM.optimize() → new topology
                            │   ├── OpenDCRunner → test results
                            │   └── Score vs SLO targets
                            │
                            └── 3. Update State
                                └── Store best config
                                    │
                                    └── UI polls /api/status
```
