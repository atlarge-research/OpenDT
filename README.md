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

## Project Layout

The Python package has been consolidated into two high-level namespaces:

- `opendt.core` holds orchestrator logic, simulation utilities, optimization
  strategies, and worker helpers that make up the core runtime.
- `opendt.adapters` contains infrastructure integrations such as Kafka
  ingestion clients.

The legacy import paths (for example `opendt.orchestrator` and
`opendt.simulation`) remain available as thin compatibility shims so existing
code continues to run without modification.

### Simulation Data Lake

Simulation runs are now persisted under `data/datalake` using a compact index
(`index.parquet`) plus per-run folders with metrics, topology snapshots, and raw
artifacts copied from the OpenDC outputs. The dashboard exposes an in-browser
explorer ("Data Lake") that lets operators browse the historical runs,
visualise the recorded power/CPU timeseries, and inspect the topology that was
applied for each cycle. The same records are also served via the API at
`/api/datalake/index` and `/api/datalake/run/<run_id>` for automation and offline
analysis.

## Running the Test Suite

The test harness mirrors production by running inside the Docker Compose stack. From the project root run:

```bash
./scripts/run_tests.sh
```

Pass additional pytest arguments as needed, for example `./scripts/run_tests.sh -k kafka` to focus on Kafka-related specs.

