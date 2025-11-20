"""OpenDT API - Main FastAPI Application."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import shared models from common library
from opendt_common import Task, Fragment, Consumption
from opendt_common.utils import get_kafka_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info("Starting OpenDT API service...")
    
    # Initialize Kafka producer (stored in app state for reuse)
    try:
        app.state.kafka_producer = get_kafka_producer()
        logger.info("Kafka producer initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        app.state.kafka_producer = None
    
    # TODO: Initialize database connection
    # TODO: Run database migrations
    
    yield
    
    # Shutdown
    logger.info("Shutting down OpenDT API service...")
    if app.state.kafka_producer:
        app.state.kafka_producer.close()
        logger.info("Kafka producer closed")


# Create FastAPI application
app = FastAPI(
    title="OpenDT API",
    description="Open Digital Twin - Backend API for distributed system simulation",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "OpenDT API",
        "version": "0.1.0",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    kafka_status = "connected" if app.state.kafka_producer else "disconnected"
    
    return {
        "status": "healthy",
        "kafka": kafka_status,
        # TODO: Add database health check
        # "database": db_status,
    }


# ============================================================================
# EXAMPLE ENDPOINTS (Using Shared Models)
# ============================================================================

@app.get("/api/tasks", response_model=list[Task])
async def list_tasks():
    """List all tasks (example endpoint)."""
    # TODO: Fetch from database
    return []


@app.post("/api/tasks", response_model=Task)
async def create_task(task: Task):
    """Create a new task (example endpoint)."""
    logger.info(f"Creating task: {task.id}")
    
    # TODO: Save to database
    
    # Send to Kafka (example)
    if app.state.kafka_producer:
        from opendt_common.utils.kafka import send_message
        send_message(
            app.state.kafka_producer,
            topic="tasks",
            message=task.model_dump(mode="json"),
            key=task.id
        )
    
    return task


@app.get("/api/fragments", response_model=list[Fragment])
async def list_fragments():
    """List all fragments (example endpoint)."""
    # TODO: Fetch from database
    return []


@app.get("/api/consumption", response_model=list[Consumption])
async def list_consumption():
    """List consumption data (example endpoint)."""
    # TODO: Fetch from database
    return []


# ============================================================================
# TODO: Add more endpoints for:
# - Simulations management
# - Workload submission
# - Results querying
# - Monitoring & metrics
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
