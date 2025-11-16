# Experimentation Mode

OpenDT supports an **experiment mode** designed for systematic data collection and analysis of datacenter simulation runs. This mode enables you to collect comprehensive window-by-window data into Parquet files for offline analysis.

## Overview

In experiment mode, OpenDT:
- Collects detailed metrics for each 5-minute simulation window
- Stores results in a timestamped Parquet file
- Periodically flushes data to disk (every 10 windows) to prevent data loss
- Allows you to enable/disable optimization to compare baseline vs. optimized performance
- Uses an unbounded window queue to prevent data loss during fast replay

## Configuration File

Experiment mode is configured via `config/experiment.json`:

```json
{
  "experiment_mode": true,
  "enable_optimization": false,
  "experiment_name": "baseline_experiment",
  "output_path": "output/experiments"
}
```

### Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `experiment_mode` | boolean | Enable/disable experiment data collection. When `true`, data is collected into Parquet files. |
| `enable_optimization` | boolean | Enable/disable LLM-based topology optimization. When `false`, only baseline simulations run. |
| `fast_mode` | boolean | Enable/disable fast replay mode. When `true`, workload replay and window processing are significantly accelerated. |
| `experiment_name` | string | Name prefix for the output Parquet file. Timestamp is automatically appended. |
| `output_path` | string | Directory where Parquet files are saved (relative to workspace root). |

### Optimization Control

The `enable_optimization` flag controls whether OpenDT attempts to optimize the topology:

- **`false` (Baseline Mode)**: Only the baseline topology is simulated. No optimization attempts are made. This is useful for establishing baseline performance metrics or when you want faster experiment runs without the overhead of LLM calls and additional simulations.

- **`true` (Optimization Mode)**: After each baseline simulation, the LLM optimizer attempts to suggest an improved topology based on SLO targets. Both baseline and optimized results are collected.

### Fast Mode

The `fast_mode` flag controls the replay speed of the workload trace:

- **`false` (Normal Mode)**: Workload is replayed with 10x acceleration (TIME_SCALE = 0.1). A 7-day trace takes ~17 hours to complete. Windows wait 30 seconds between processing cycles.

- **`true` (Fast Mode)**: Workload is replayed with 1000x acceleration (100× the normal TIME_SCALE). The consumer uses tight polling (no wait between windows). This makes experiments complete much faster, with OpenDC simulation time (~1 second per window) becoming the primary bottleneck. A 7-day trace can complete in minutes instead of hours.

**When to use fast mode:**
- Running baseline-only experiments for quick results
- Testing configuration changes or new topologies
- Collecting large datasets for offline analysis
- When real-time pacing is not required

**Note:** Fast mode maintains system stability by keeping Kafka message pacing intact while eliminating artificial delays in window processing.

## Output Data

The Parquet file contains one row per window with the following columns:

| Column | Description |
|--------|-------------|
| `window_number` | Sequential window number (1, 2, 3, ...) |
| `window_start` | Window start timestamp (from workload trace) |
| `window_end` | Window end timestamp (from workload trace) |
| `task_count` | Number of tasks in this window |
| `fragment_count` | Number of fragments in this window |
| `avg_cpu_usage` | Average CPU usage (MHz) across all fragments |
| `baseline_energy_kwh` | Energy consumption (kWh) for baseline topology |
| `baseline_runtime_hours` | Runtime (simulated hours) for baseline topology |
| `baseline_cpu_utilization` | CPU utilization (%) for baseline topology |
| `baseline_max_power_draw` | Maximum power draw (W) for baseline topology |
| `baseline_score` | Performance score for baseline (lower = better) |
| `baseline_topology_json` | JSON string of the baseline topology configuration |
| `optimization_enabled` | Boolean indicating if optimization was enabled |
| `slo_targets_json` | JSON string of the SLO targets used for this window |

**When optimization is enabled**, additional columns are populated:

| Column | Description |
|--------|-------------|
| `optimization_score` | Performance score for optimized topology (lower = better) |
| `optimization_energy_kwh` | Energy consumption (kWh) for optimized topology |
| `optimization_runtime_hours` | Runtime (simulated hours) for optimized topology |
| `optimization_topology_json` | JSON string of the optimized topology configuration |

## Running an Experiment

### 1. Configure the Experiment

Edit `config/experiment.json` with your desired settings:

```json
{
  "experiment_mode": true,
  "enable_optimization": false,
  "fast_mode": false,
  "experiment_name": "my_experiment",
  "output_path": "output/experiments"
}
```

### 2. Restart OpenDT

The experiment configuration is loaded when the orchestrator initializes, so you need to restart the container:

```bash
docker compose restart opendt
```

### 3. Start the Experiment

Open the OpenDT UI at `http://localhost:8080` and click **Start**. The system will:
- Begin streaming the workload trace to Kafka
- Process windows as they arrive
- Simulate each window with OpenDC
- Collect metrics into the Parquet file
- Flush data to disk every 10 windows

### 4. Monitor Progress

Watch the logs to see data collection in action:

```bash
docker compose logs -f opendt
```

You'll see messages like:
```
📊 Experiment data collector initialized: my_experiment
📊 Data will be flushed every 10 windows to: output/experiments/my_experiment_20251116_170917.parquet
💾 Flushed 10 window records to output/experiments/my_experiment_20251116_170917.parquet
✅ Experiment complete: 42 total window records in output/experiments/my_experiment_20251116_170917.parquet
```

### 5. Stop the Experiment

Click **Stop** in the UI or stop the container:

```bash
docker compose stop opendt
```

Any remaining buffered data will be automatically flushed to the Parquet file.

## Analyzing Results

The output Parquet file is accessible in `output/experiments/` (mounted from the Docker container to your host).

Example Python code to load and analyze results:

```python
import pandas as pd
from pathlib import Path

# Load the most recent experiment
experiments_dir = Path("output/experiments")
parquet_files = list(experiments_dir.glob("*.parquet"))
most_recent = max(parquet_files, key=lambda p: p.stat().st_mtime)

df = pd.read_parquet(most_recent)

# Basic statistics
print(f"Total windows: {len(df)}")
print(f"Total energy (baseline): {df['baseline_energy_kwh'].sum():.2f} kWh")
print(f"Total runtime (baseline): {df['baseline_runtime_hours'].sum():.2f} hours")
print(f"Avg CPU utilization: {df['baseline_cpu_utilization'].mean():.2f}%")

# If optimization was enabled
if df['optimization_enabled'].any():
    print(f"\nOptimization savings:")
    print(f"  Energy saved: {(df['baseline_energy_kwh'] - df['optimization_energy_kwh']).sum():.2f} kWh")
    print(f"  Runtime saved: {(df['baseline_runtime_hours'] - df['optimization_runtime_hours']).sum():.2f} hours")
```

## Use Cases

### Baseline Performance Measurement

Disable optimization to establish baseline performance:

```json
{
  "experiment_mode": true,
  "enable_optimization": false,
  "fast_mode": false,
  "experiment_name": "baseline_only",
  "output_path": "output/experiments"
}
```

This gives you pure baseline metrics without the overhead of LLM optimization.

### Fast Baseline Experiments

Enable fast mode for rapid baseline data collection:

```json
{
  "experiment_mode": true,
  "enable_optimization": false,
  "fast_mode": true,
  "experiment_name": "fast_baseline",
  "output_path": "output/experiments"
}
```

This accelerates the experiment significantly, completing in minutes instead of hours.

### Optimization Comparison

Enable optimization to compare baseline vs. optimized performance:

```json
{
  "experiment_mode": true,
  "enable_optimization": true,
  "fast_mode": false,
  "experiment_name": "optimization_test",
  "output_path": "output/experiments"
}
```

Each window will contain both baseline and optimized metrics, allowing you to quantify the impact of the optimizer.

### Parameter Sweeps

Run multiple experiments with different topologies or SLO targets:

1. Update `config/topology.json` or `config/slo.json`
2. Update `experiment_name` in `config/experiment.json`
3. Restart and run the experiment
4. Repeat for each configuration

This allows systematic exploration of the design space.

## Notes

- **Performance**: With optimization disabled, experiments run significantly faster since each window only requires one simulation instead of multiple optimization attempts. Fast mode further accelerates experiments by eliminating artificial delays.

- **Fast Mode Trade-offs**: While fast mode dramatically reduces experiment time, it removes the realistic temporal pacing of workload replay. Use fast mode for rapid data collection, but use normal mode when temporal dynamics matter.

- **Data Integrity**: Experiment mode uses an unbounded window queue to prevent dropping windows, even if processing can't keep up with data production.

- **File Naming**: Parquet files are automatically timestamped (`{experiment_name}_{YYYYMMDD_HHMMSS}.parquet`) to prevent overwriting previous experiments.

- **Volume Mount**: The `output/` directory is mounted from the container to your host, so Parquet files persist even if the container is removed.
