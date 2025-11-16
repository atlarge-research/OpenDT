# Optimization

OpenDT uses optimization strategies to propose topology changes that improve energy efficiency and runtime performance against SLO targets.

## Overview

After simulating each window's workload on the current topology, the optimizer **proposes modified topologies** to better meet SLO goals. These proposals are tested by re-simulating the same window with different hardware configurations, and the best result is kept.

## SLO Targets

Service Level Objectives (SLOs) are defined in `config/slo.json`:

```json
{
  "energy_target": 2.0,   // kWh per window
  "runtime_target": 2.0   // simulated hours per window
}
```

These targets are **per-window** (5 minutes of workload trace time). The optimizer compares simulation results against these targets to determine if changes are needed.

### Scoring

The scoring function evaluates how well a topology performs:

```python
score = (energy_kwh * 2.0) + (runtime_hours * 1.0)
```

- **Lower is better**
- Energy is weighted 2× runtime (energy optimization prioritized)
- Used to compare baseline vs proposed topologies

## Optimizer Inputs

The optimizer receives comprehensive information about the current state:

**1. Simulation Results** (from baseline OpenDC run):
```python
{
    'energy_kwh': 0.84,
    'runtime_hours': 2.62,
    'cpu_utilization': 0.573,
    'max_power_draw': 400.0,
    'status': 'success'
}
```

**2. Batch Data** (from consumer window):
```python
{
    'task_count': 113,
    'fragment_count': 1243,
    'avg_cpu_usage': 2156.3,      // MHz
    'tasks_sample': [...],         // Full task objects
    'fragments_sample': [...],     // Full fragment objects
    'window_start': Timestamp(...),
    'window_end': Timestamp(...)
}
```

**3. SLO Targets**:
```python
{
    'energy_target': 2.0,
    'runtime_target': 2.0
}
```

**4. Current Topology**:
```python
{
    'clusters': [{
        'name': 'C01',
        'hosts': [{
            'name': 'H01',
            'count': 12,                    // Physical host count
            'cpu': {
                'coreCount': 64,            // Cores per CPU
                'coreSpeed': 4000           // MHz
            },
            'memory': {
                'memorySize': 214748364800  // Bytes
            }
        }]
    }]
}
```

## Optimization Strategies

### 1. LLM Optimizer

Uses OpenAI GPT-3.5 to make intelligent topology recommendations.

**Input to LLM:**
The system sends a structured prompt including:
```json
{
  "simulation_results": {
    "energy_kwh": 0.84,
    "runtime_hours": 2.62,
    "cpu_utilization": 0.573
  },
  "workload_characteristics": {
    "task_count": 113,
    "fragment_count": 1243,
    "avg_cpu_usage": 2156.3
  },
  "slo_targets": {
    "energy_target": 2.0,
    "runtime_target": 2.0
  },
  "current_topology": {
    "clusters": [{
      "name": "C01",
      "hosts": [{
        "name": "H01",
        "count": 12,
        "cpu": {"coreCount": 64, "coreSpeed": 4000}
      }]
    }]
  }
}
```

**Output from LLM:**
The LLM responds with a suggested topology modification (host counts, CPU cores, CPU speeds) that is converted to OpenDC format and tested via simulation.

**Fallback:** If the LLM call fails (timeout, API error, no API key), the system automatically falls back to rule-based optimization.

### 2. Rule-Based Optimizer

A deterministic, heuristic-based strategy used as fallback when LLM is unavailable.

**Decision Logic:**

| Condition | Action | Modification |
|-----------|--------|--------------|
| Energy ≥30% over target | Massive downscale | Reduce host count by 1 |
| Energy ≥15% over target | Downscale | Reduce CPU frequency by 10% (min 1800 MHz) |
| Runtime ≥25% over target | Scale up | Add 4 CPU cores (max 48) |
| Runtime ≥10% over target | Light scale up | Add 2 CPU cores (max 32) |
| Energy ≤-20% AND Runtime ≤-10% under target | Consolidate | Remove 2 CPU cores (min 8) |
| Otherwise | Maintain | No changes |

**Example:**
If energy is 2.8 kWh against a target of 2.0 kWh (40% over), the rule-based optimizer reduces host count from 12 to 11.

## Optimization Loop (Per Window)

For each window, the orchestrator follows this process:

1. **Baseline Simulation**
   - Run OpenDC simulation on current window's workload using current topology
   - Calculate baseline score from results

2. **Optimization Attempt(s)**
   - Call optimizer with baseline results, workload data, SLO targets, and current topology
   - Optimizer returns a proposed topology modification
   - Re-simulate the same window workload using the proposed topology
   - Calculate score for the proposed configuration

3. **Comparison & Selection**
   - If proposed score is better than baseline by at least `IMPROVEMENT_DELTA` (0.05), keep the proposed topology
   - Otherwise, keep the current topology

4. **Store Best Configuration**
   - Save the best topology and score in system state
   - User can view and accept recommendations via the UI

**Configuration:**
- `MAX_TRIES_PER_WINDOW = 1`: Number of optimization attempts per window (currently 1)
- `WINDOW_TRY_BUDGET_SEC = 30`: Maximum time allowed for optimization per window
- `IMPROVEMENT_DELTA = 0.05`: Minimum score improvement required to accept a change

## Window-Scoped Optimization

Optimization is performed **per-window**: only the current window's workload is used for testing topologies. Past windows are not re-simulated.

### Example

For Window 5 with 113 tasks and 1,243 fragments:

**Baseline:**
- Topology: 12 hosts × 64 cores @ 4000 MHz
- Simulate Window 5 workload → energy: 0.84 kWh, runtime: 2.62h → Score: 3.70

**Optimization:**
- LLM proposes: 10 hosts × 48 cores @ 3500 MHz
- Re-simulate Window 5 workload → energy: 0.72 kWh, runtime: 2.85h → Score: 4.29

**Decision:** Keep baseline topology (score 3.70 is better than 4.29)

The assumption is that a topology performing well on this window will likely work well for future similar workloads.

## User Interaction

The optimization loop **proposes** topologies but doesn't automatically apply them. The user can:

1. **View recommendations** in the web UI dashboard
2. **Accept a recommendation** via the `/api/accept_recommendation` endpoint

When a topology is accepted:
- It's written to `config/topology.json`
- The file watcher detects the change
- Future windows use the new topology as baseline

## Configuration

Key settings in `src/opendt/config/settings.py`:

```python
IMPROVEMENT_DELTA = 0.05          # Minimum score improvement to accept change
WINDOW_TRY_BUDGET_SEC = 30.0      # Max time for optimization per window
MAX_TRIES_PER_WINDOW = 1          # Number of optimization attempts (currently 1)
```
