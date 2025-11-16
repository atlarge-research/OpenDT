# High-level architecture

OpenDT runs two concurrent threads that communicate via Kafka:

## 1. Producer Loop (Workload Replay)

The producer streams workload traces to Kafka, simulating real-time datacenter activity.

### Data Sources
- **tasks.parquet** (7,850 tasks)
  - Columns: `id`, `submission_time`, `duration`, `cpu_count`, `cpu_capacity`, `mem_capacity`
  - Tasks have timestamps indicating when they were submitted
  
- **fragments.parquet** (2.3M fragments)
  - Columns: `id` (task_id), `duration`, `cpu_count`, `cpu_usage`
  - Fragments have NO submission times in raw data
  - Each task is composed of ≥1 fragments for fine-grained resource modeling

The SURF workload trace captures approximately **7 days of wall clock time**.

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
- Original trace: ~7 days of workload data
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

_[To be documented]_
