# OpenDT - Open Digital Twin for Datacenters

Real-time datacenter simulation with Kafka streaming and distributed system optimization.

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** - For running services
- **Make** - For convenience commands
- **Python 3.11+** - For local development
- **uv** - Python package manager (install: `curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Development Setup

```bash
# 1. Setup virtual environment (isolates from global Python)
make setup

# 2. Activate virtual environment
source .venv/bin/activate

# 3. Verify setup
make verify

# 4. Run tests
make test
```

See [DEV_SETUP.md](DEV_SETUP.md) for detailed development instructions.

### Start the System

```bash
# Default configuration
make up

# Or with custom configuration
make up config=config/stress_test.yaml

# View logs
make logs

# Access services
# - API: http://localhost:8000
# - API Docs: http://localhost:8000/docs
# - Frontend: http://localhost:3000
```

## 📋 Configuration

OpenDT uses a **hybrid configuration strategy**:

- **Static YAML** for startup configuration
- **Kafka (`sys.config` topic)** for runtime updates

### Default Configuration

The default configuration is located at `config/default.yaml`:

```yaml
workload: "SURF"

simulation:
  speed_factor: 10.0  # 1.0 = Realtime, -1 = Max Speed
  window_size_minutes: 5

features:
  calibration_enabled: false
```

### Workload Convention

The `workload` field is a name (e.g., "SURF") that maps to a directory structure:

```
data/
└── SURF/
    ├── tasks.parquet
    ├── fragments.parquet
    └── consumption.parquet
```

### Custom Configurations

Create additional configuration files for different scenarios:

```bash
# Create a stress test configuration
cat > config/stress_test.yaml <<EOF
workload: "STRESS"
simulation:
  speed_factor: -1  # Max speed
  window_size_minutes: 1
features:
  calibration_enabled: true
EOF

# Run with custom config
make run config=config/stress_test.yaml
```

### Runtime Configuration Updates

Services listen to the `sys.config` Kafka topic for dynamic updates:

```python
# Example: Update simulation speed at runtime
from opendt_common import DynamicConfigEvent

event = DynamicConfigEvent(
    setting_key="simulation.speed_factor",
    new_value=20.0,
    source="api"
)
# Publish to Kafka topic 'sys.config'
```

## 🛠️ Available Commands

| Command | Description |
|---------|-------------|
| `make up` | Start with clean slate (deletes volumes) |
| `make run config=...` | Start with custom configuration |
| `make down` | Stop all services |
| `make logs` | View all logs |
| `make logs-api` | View API logs only |
| `make ps` | Show running containers |
| `make shell-api` | Open shell in API container |
| `make kafka-topics` | List Kafka topics |
| `make help` | Show all commands |

## 📚 Documentation

- **[START_HERE.md](START_HERE.md)** - Complete setup guide
- **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Detailed configuration and troubleshooting
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Command reference card
- **[MONOREPO_STRUCTURE.md](MONOREPO_STRUCTURE.md)** - Project structure

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  dc-mock    │─────>│    Kafka     │<─────│ sim-worker  │
│ (Producer)  │      │   (Broker)   │      │ (Consumer)  │
└─────────────┘      └──────┬───────┘      └──────┬──────┘
                            │                     │
                            v                     v
                     ┌──────────────┐      ┌─────────────┐
                     │  opendt-api  │      │ PostgreSQL  │
                     │  (FastAPI)   │─────>│  Database   │
                     └──────┬───────┘      └─────────────┘
                            │
                            v
                     ┌──────────────┐
                     │  Frontend    │
                     │  (Next.js)   │
                     └──────────────┘
```

### Services

- **dc-mock** - Workload producer (reads Parquet → Kafka)
- **sim-worker** - Simulation engine (Kafka → simulations)
- **opendt-api** - FastAPI backend (REST API + DB)
- **frontend** - Next.js UI

### Infrastructure

- **Kafka** - KRaft mode (no Zookeeper)
- **PostgreSQL** - Persistent storage
- All services support hot reload in development

## 🎯 Next Steps

1. **Read** [START_HERE.md](START_HERE.md) for detailed setup
2. **Prepare** your workload data in `data/WORKLOAD_NAME/`
3. **Customize** configuration in `config/`
4. **Run** `make up` to start the system
5. **Develop** your simulation logic and API endpoints

## 📖 Learn More

- Configuration system: See `libs/common/opendt_common/config.py`
- Service documentation: Check each service's `README.md`
- API documentation: http://localhost:8000/docs (when running)
