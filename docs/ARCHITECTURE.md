# High-level architecture

OpenDT runs two concurrent threads that communicate via Kafka:

## 1. Producer Loop (Workload Replay)

The producer streams workload traces to Kafka, simulating real-time datacenter activity.

### Data Sources
- **tasks.parquet**
  - Columns: `id`, `submission_time`, `duration`, `cpu_count`, `cpu_capacity`, `mem_capacity`
  - Tasks have timestamps indicating when they were submitted
  
- **fragments.parquet**
  - Columns: `id` (task_id), `duration`, `cpu_count`, `cpu_usage`
  - Fragments have NO submission times in raw data
  - Each task is composed of ≥1 fragments for fine-grained resource modeling

More info about these fields can be found in the [OpenDC documentation](https://atlarge-research.github.io/opendc/docs/documentation/Input/Workload/#tasks).

### Key Transformations

**Fragment Timestamp Synthesis:**
Fragments inherit their parent task's submission time, offset by cumulative fragment durations. The synthesized timestamp is stored in the `submission_time` key of each fragment message:

```
Task starts at T=0
Fragment 1.submission_time = T=0 + duration[0]
Fragment 2.submission_time = T=0 + duration[0] + duration[1]
Fragment 3.submission_time = T=0 + duration[0] + duration[1] + duration[2]
...
```

This creates a sequential execution timeline where fragments "chain" together.

**Time-Scaled Replay:**
- Original trace: ~7 days of SURF workload data
- Replay speed: 10x accelerated (TIME_SCALE = 0.1)
- Real-time gaps between events are preserved but compressed
- Example: 1-hour workload gap → 6-minute real wait

### Workload Semantics

**Task-Fragment Relationship:**
In the AtLarge traces, tasks are _always_ equally split into fragments. For example, we can have a task with `duration=10000ms` split into 5 fragments of `duration=2000ms`.

**Computing Clock Cycles:**
Fragments specify the total number of clock cycles needed, calculated as:
```
total_cycles = duration (ms) × cpu_usage (MHz) × 1,000
```
Since MHz = million cycles/second and duration is in milliseconds.

**Simulation Behavior:**
The `duration` field represents the actual measured runtime of the original workload. However, when simulating on different topologies, the execution time varies based on CPU specifications.

**Example:**
A fragment with `cpu_count=1`, `cpu_usage=1000 MHz`, `duration=5000 ms`:
- Total cycles needed: `5000 × 1000 × 1,000 = 5,000,000,000 cycles` (5 billion)

When simulated on different hardware:
- **2 CPUs @ 1000 MHz each:** Total throughput = 2,000 MHz → `duration = 5,000,000,000 / (2000 × 1,000,000) = 2500 ms`
- **1 CPU @ 5000 MHz:** Total throughput = 5,000 MHz → `duration = 5,000,000,000 / (5000 × 1,000,000) = 1000 ms`

This is a simplification of reality but makes reasoning about the simulation behavior clearer.

### Output to Kafka

**Topic: "tasks"**
```json
{
  "key": {"id": "task_id"},
  "value": {
    "submission_time": "2022-10-06T22:00:00Z",
    "duration": 27935000,
    "cpu_count": 16,
    "cpu_capacity": 33600.0,
    "mem_capacity": 100000
  }
}
```

**Topic: "fragments"**
```json
{
  "key": {"id": "task_id"},
  "value": {
    "submission_time": "2022-10-06T22:00:30Z",
    "duration": 30000,
    "cpu_usage": 1953.0
  }
}
```

### Implementation
- Two parallel threads: one for tasks, one for fragments
- Both synchronized via barrier to start simultaneously
- Pacing via `sleep()` between messages to maintain temporal structure

---

## 2. Consumer Loop (Digital Twin)

The consumer organizes incoming Kafka messages into time-based windows and feeds them to the OpenDC simulator.

### Windowing Strategy

**Window Size:** 5 minutes of virtual trace time (`REAL_WINDOW_SIZE_SEC = 300s`)

**Processing:** Windows are created based on `submission_time` timestamps:
- As tasks/fragments arrive, they're assigned to windows based on their timestamp
- A window becomes "ready" when data from the next time window starts arriving
- Windows are processed sequentially (FIFO)

**Example:**
```
Window 5: 2022-10-06 22:22:00 → 22:27:00
  ├─ 113 tasks
  └─ 1,243 fragments (~11 per task)
```

### Output to Orchestrator

Each completed window yields a `batch_data` dictionary containing:
- `tasks_sample`: List of task objects for this window
- `fragments_sample`: List of fragment objects for this window  
- `task_count`, `fragment_count`: Counts
- `avg_cpu_usage`: Average CPU usage across fragments
- `window_start`, `window_end`: Time boundaries

### Simulation Flow

1. Orchestrator receives `batch_data`
2. OpenDC simulates workload on current topology
3. Returns: `energy_kwh`, `runtime_hours`, `cpu_utilization`, `max_power_draw`

**Real example from logs:**
- Input: 113 tasks, 1,243 fragments
- Output: `energy_kwh=0.84`, `runtime_hours=2.62`, `cpu_utilization=57.3%`

### Implementation
- Three threads: tasks consumer, fragments consumer, window processor
- Threads share window state via locks/conditions
- Task-fragment matching: Fragments are joined with their parent tasks by `id`
- Windows wait `VIRTUAL_WINDOW_SIZE = 30s` between processing (real time)
