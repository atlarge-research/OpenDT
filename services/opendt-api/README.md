# OpenDT API Service

FastAPI backend service that interfaces with PostgreSQL database and Kafka message broker.

## Features

- RESTful API for managing simulations
- Database persistence with SQLAlchemy
- Kafka integration for event streaming
- API documentation via Swagger/OpenAPI

## Development

### Local Setup

1. Install dependencies (from repo root):
```bash
pip install -e libs/common
pip install -e services/opendt-api
```

2. Set environment variables:
```bash
export DATABASE_URL="postgresql://opendt:opendt_dev_password@localhost:5432/opendt"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

3. Run the service:
```bash
cd services/opendt-api
uvicorn opendt_api.main:app --reload
```

### Docker Development

From the repo root:
```bash
make up
```

The API will be available at:
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Health Check
- `GET /health` - Service health status

### Simulations
- `GET /simulations` - List all simulations
- `POST /simulations` - Create a new simulation
- `GET /simulations/{id}` - Get simulation details
- `DELETE /simulations/{id}` - Delete a simulation

### Tasks
- `GET /tasks` - List all tasks
- `POST /tasks` - Submit a new task

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `LOG_LEVEL` | Logging level | `INFO` |

## Testing

Run tests:
```bash
pytest
```

With coverage:
```bash
pytest --cov=opendt_api --cov-report=html
```
