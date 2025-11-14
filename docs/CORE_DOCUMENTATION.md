# OpenDT Core Components Documentation

This document provides detailed technical documentation for the core components of the OpenDT system, focusing on the orchestrator and optimization modules.

## Core Package Structure

```
core/
├── optimization/
│   ├── base.py
│   ├── llm.py
│   ├── rule_based.py
│   └── scoring.py
└── orchestrator/
    ├── controller.py
    ├── events.py
    ├── slo.py
    ├── state.py
    └── topology.py
```

## 1. Orchestrator Module

The orchestrator is the central control system of OpenDT, managing the digital twin's lifecycle and optimization processes.

### 1.1 OpenDTOrchestrator (controller.py)

Main orchestration class that coordinates all system operations.

#### Key Components:

1. **State Management**
   ```python
   self.state = default_state_dict()
   self.open_dc_buffer = SimulationResultsBuffer()
   ```
   - Maintains system state
   - Buffers simulation results
   - Tracks topology updates
   - Monitors SLO compliance

2. **External Integrations**
   ```python
   self.kafka_servers = kafka_bootstrap_servers()
   self.openai_key = openai_api_key()
   self.optimizer = LLM(self.openai_key)
   ```
   - Kafka integration for data streaming
   - OpenAI integration for LLM-based optimization
   - Simulation runner management

3. **Configuration Management**
   - Topology management
   - SLO configuration
   - File watching and updates

#### Key Methods:

1. **Initialization and Setup**
   - `load_initial_topology()`: Loads initial datacenter configuration
   - `load_initial_slo()`: Initializes Service Level Objectives
   - `start_topology_watcher()`: Monitors topology file changes
   - `start_slo_watcher()`: Watches for SLO updates

2. **Runtime Operations**
   - `run_consumer()`: Processes incoming data streams
   - `run_simulation()`: Executes simulation cycles
   - `simulation_timeseries()`: Retrieves time-series data

### 1.2 State Management (state.py)

Defines data structures for maintaining system state:

```python
@dataclass
class OrchestratorState:
    status: str = "stopped"
    cycle_count: int = 0
    current_topology: Dict[str, Any] | None = None
    slo_targets: Dict[str, Any] = field(default_factory=dict)
    window_baseline_score: float | None = None
    window_best_score: float | None = None
```

## 2. Optimization Module

The optimization module provides intelligent decision-making capabilities for datacenter configuration.

### 2.1 LLM-based Optimization (llm.py)

Implements AI-powered optimization using OpenAI's language models.

#### Key Features:

1. **Optimization Strategy**
   ```python
   class LLM:
       def optimize(
           self,
           simulation_results: Dict[str, Any],
           batch_data: Dict[str, Any],
           slo_targets: Dict[str, Any],
           current_topology: Dict[str, Any] | None = None
       ) -> Dict[str, Any]
   ```
   - Processes simulation results
   - Considers batch workload data
   - Respects SLO targets
   - Maintains best configuration history

2. **Performance Tracking**
   - Calculates performance scores
   - Updates best configurations
   - Maintains optimization history

3. **Fallback Mechanism**
   - Graceful degradation to rule-based optimization
   - Error handling and recovery
   - Best configuration persistence

### 2.2 Rule-based Optimization (rule_based.py)

Provides deterministic optimization rules as a fallback mechanism.

#### Key Features:

1. **Decision Logic**
   ```python
   def rule_based_optimization(
       sim_results: Dict[str, Any],
       batch_data: Dict[str, Any],
       slo_targets: Dict[str, Any],
       current_topology: Dict[str, Any] | None = None,
   ) -> Dict[str, Any]
   ```

2. **Optimization Rules**:
   - **Energy Management**:
     - Critical threshold (30% above target): Massive downscale
     - High threshold (15% above target): CPU frequency reduction
   
   - **Performance Management**:
     - Runtime exceeds SLO by 25%: Scale up CPU cores
     - Runtime exceeds SLO by 10%: Light scale-up
   
   - **Resource Consolidation**:
     - Under-utilized resources: Capacity reduction
     - Optimal state: Maintain current configuration

## 3. Integration Points

### 3.1 Kafka Integration
- Consumes workload data
- Produces optimization results
- Manages data streaming lifecycle

### 3.2 OpenAI Integration
- LLM-based decision making
- Topology optimization
- Performance prediction

## 4. Configuration

### 4.1 SLO Configuration
- Energy usage targets
- Runtime performance goals
- Resource utilization thresholds

### 4.2 Topology Configuration
- Cluster definitions
- Host specifications
- Resource allocation

## 5. Best Practices

1. **Optimization Strategy**
   - Start with rule-based optimization for baseline
   - Enable LLM optimization with valid OpenAI key
   - Monitor and adjust SLO targets based on results

2. **State Management**
   - Regularly persist state changes
   - Monitor topology updates
   - Track optimization history

3. **Performance Monitoring**
   - Track energy usage metrics
   - Monitor runtime performance
   - Evaluate optimization effectiveness

## 6. Error Handling

1. **LLM Optimization**
   - Graceful fallback to rule-based optimization
   - Error logging and reporting
   - State recovery mechanisms

2. **Configuration Management**
   - File watching error handling
   - Configuration validation
   - Backup management

This documentation covers the main components of the OpenDT core functionality, focusing on the orchestrator and optimization modules. For implementation details, refer to the respective source files in the core package.