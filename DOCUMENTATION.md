# OpenDT - Digital Twin for Datacenters Documentation

OpenDT is a sophisticated digital twin system designed for real-time datacenter simulation with Kafka streaming and LLM-powered optimization. This documentation provides a comprehensive overview of the project structure and its key components.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Configuration](#configuration)
5. [API](#api)
6. [Running the Project](#running-the-project)

## Project Overview

OpenDT is a digital twin system that combines real-time datacenter simulation with advanced optimization techniques. It uses:
- Kafka for real-time data streaming
- OpenAI's LLM for optimization
- OpenDC for datacenter simulation
- FastAPI/Flask for the web interface

## Architecture

The project follows a modular architecture with these main components:

```
opendt/
├── src/opendt/           # Main source code
│   ├── api/             # API endpoints and dependencies
│   ├── core/            # Core business logic
│   ├── adapters/        # External system adapters
│   |── config/          # Configuration management
|   |---cli.py           # Entry point
└── tests/               # Test suite
```

## Core Components

### 1. Orchestrator (core/orchestrator/)

The `OpenDTOrchestrator` is the central component that coordinates all system operations:

- **Controller**: Manages the main orchestration logic
- **State Management**: Handles system state and simulation results
- **Key Features**:
  - Real-time simulation management
  - Topology management
  - SLO (Service Level Objective) tracking
  - Results buffering
  - Kafka integration

### 2. Simulation (core/simulation/)

The simulation module handles datacenter simulation using OpenDC:

- **OpenDCRunner**: Main class for running simulations
  - Handles workload creation
  - Manages experiment configuration
  - Processes simulation results
  - Supports multiple simulation modes

### 3. Optimization (core/optimization/)

Handles datacenter optimization strategies:

- **LLM-based Optimization**: Uses OpenAI for intelligent optimization
- **Rule-based Fallback**: Alternative optimization when LLM is unavailable
- **Scoring System**: Evaluates simulation results based on energy and performance metrics

### 4. Adapters (adapters/)

Interface layers for external systems:

#### Ingestion
- **Kafka Integration**:
  - Consumer: Processes incoming data streams
  - Producer: Outputs simulation results

## Configuration

The system uses various configuration files:

1. **Topology Configuration** (`config/topology.json`):
   - Defines datacenter structure
   - Cluster configurations
   - Host specifications

2. **SLO Configuration** (`config/slo.json`):
   - Performance targets
   - Energy consumption limits
   - Runtime constraints

3. **Experiment Configuration**:
   - Simulation parameters
   - Export configurations
   - Monitoring settings

## API

The system provides a RESTful API for interaction:

### Key Endpoints:
- `/reset_topology`: Reset topology to initial configuration
- `/sim/timeseries`: Get simulation time series data
- Additional endpoints for system control and monitoring

## Running the Project

### Prerequisites:
- Docker and Docker Compose
- OpenAI API key
- Kafka setup (handled by Docker Compose)

### Quick Start:
1. Clone the repository
2. Set up environment:
   ```bash
   cd opendt
   export OPENAI_API_KEY="your-key-here"
   ```
3. Run with Docker:
   ```bash
   docker-compose down
   docker-compose up --build
   ```
4. Access the orchestrator UI at `http://localhost:8080`

## Testing

The project includes a comprehensive test suite:
- Unit tests for core components
- Integration tests for the orchestrator
- Simulation testing
- Optimization strategy validation

Run tests using:
```bash
pytest tests/
```

## Key Features

1. **Real-time Simulation**:
   - Dynamic workload processing
   - Live performance monitoring
   - Energy consumption tracking

2. **Intelligent Optimization**:
   - LLM-powered decision making
   - Rule-based fallback mechanisms
   - Performance scoring system

3. **Monitoring and Analysis**:
   - Real-time metrics
   - Historical data tracking
   - Performance visualization

4. **Scalability**:
   - Modular architecture
   - Containerized deployment
   - Kafka-based streaming

This documentation provides an overview of the OpenDT system. For detailed implementation specifics, refer to the inline documentation in the source code files.