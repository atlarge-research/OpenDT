import pandas as pd
from libs.common.opendt_common import Task, Fragment, Consumption

print("Loading data...")
tasks_df = pd.read_parquet("data/SURF/tasks.parquet")
fragments_df = pd.read_parquet("data/SURF/fragments.parquet")
consumption_df = pd.read_parquet("data/SURF/consumption.parquet")

print(f"Tasks: {len(tasks_df)} rows")
print(f"Fragments: {len(fragments_df)} rows")
print(f"Consumption: {len(consumption_df)} rows")

print("\n--- Testing Task Model ---")
first_task = tasks_df.iloc[0].to_dict()
print(f"Raw task data: {first_task}")
try:
    task = Task(**first_task)
    print(f"✅ Task model works: ID={task.id}, time={task.submission_time}")
except Exception as e:
    print(f"❌ Task model failed: {e}")

print("\n--- Testing Fragment Model ---")
first_fragment = fragments_df.iloc[0].to_dict()
print(f"Raw fragment data: {first_fragment}")
try:
    fragment = Fragment(**first_fragment)
    print(f"✅ Fragment model works: task_id={fragment.task_id}, duration={fragment.duration}ms")
except Exception as e:
    print(f"❌ Fragment model failed: {e}")

print("\n--- Testing Consumption Model ---")
first_consumption = consumption_df.iloc[0].to_dict()
print(f"Raw consumption data: {first_consumption}")
try:
    consumption = Consumption(**first_consumption)
    print(f"✅ Consumption model works: power={consumption.power_draw}W, time={consumption.timestamp}")
except Exception as e:
    print(f"❌ Consumption model failed: {e}")

print("\n--- Testing Task Aggregation ---")
# Group fragments by task_id
fragments_by_task = fragments_df.groupby('id')
first_task_id = tasks_df.iloc[0]['id']
print(f"First task ID: {first_task_id}")

if first_task_id in fragments_by_task.groups:
    task_fragments = fragments_by_task.get_group(first_task_id)
    print(f"Found {len(task_fragments)} fragments for task {first_task_id}")
    
    # Create task with fragments
    task_dict = tasks_df.iloc[0].to_dict()
    task_dict['fragments'] = [Fragment(**row.to_dict()) for _, row in task_fragments.iterrows()]
    
    try:
        task_with_frags = Task(**task_dict)
        print(f"✅ Aggregation works: Task {task_with_frags.id} has {task_with_frags.fragment_count} fragments")
    except Exception as e:
        print(f"❌ Aggregation failed: {e}")
else:
    print(f"No fragments found for task {first_task_id}")

print("\n✅ All model tests complete!")
