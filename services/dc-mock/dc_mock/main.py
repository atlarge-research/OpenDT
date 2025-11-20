"""DC-Mock Service - Main Entry Point.

This service:
1. Loads tasks, fragments, and consumption data from Parquet files
2. Pre-aggregates fragments into their parent tasks (Task as Aggregate Root)
3. Streams events in time order (priority queue) to Kafka
4. Respects simulation speed_factor for timing
"""

import os
import time
import logging
from pathlib import Path
from typing import List, Tuple, Union
from datetime import datetime
import pandas as pd

from opendt_common.utils import get_kafka_producer
from opendt_common.utils.kafka import send_message
from opendt_common import load_config_from_env, AppConfig, WorkloadContext, Task, Fragment, Consumption

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WorkloadProducer:
    """Pre-aggregates and streams workload events to Kafka in time order."""
    
    def __init__(
        self,
        workload_context: WorkloadContext,
        kafka_bootstrap_servers: str,
        speed_factor: float
    ):
        """Initialize the workload producer.
        
        Args:
            workload_context: Workload context with resolved file paths
            kafka_bootstrap_servers: Kafka broker addresses
            speed_factor: Simulation speed multiplier (1.0 = realtime, -1 = max speed)
        """
        self.workload_context = workload_context
        self.producer = get_kafka_producer(kafka_bootstrap_servers)
        self.speed_factor = speed_factor
        
        logger.info(f"Initialized WorkloadProducer for workload: {workload_context.name}")
        logger.info(f"Workload directory: {workload_context.workload_dir}")
        logger.info(f"Simulation speed: {speed_factor}x")
        
        # Check file status
        file_status = workload_context.get_file_status()
        for file_type, exists in file_status.items():
            status = "✓" if exists else "✗"
            logger.info(f"  {status} {file_type}.parquet")
    
    def load_and_aggregate_data(self) -> Tuple[List[Task], List[Consumption]]:
        """Load Parquet files and pre-aggregate fragments into tasks.
        
        Returns:
            Tuple of (tasks, consumption_records)
        """
        logger.info("Loading data files...")
        
        # Step 1: Load tasks
        tasks_df = pd.read_parquet(self.workload_context.tasks_file)
        logger.info(f"Loaded {len(tasks_df)} tasks")
        
        # Step 2: Load fragments
        fragments_df = pd.read_parquet(self.workload_context.fragments_file)
        logger.info(f"Loaded {len(fragments_df)} fragments")
        
        # Step 3: Load consumption (optional)
        consumption_records = []
        if self.workload_context.consumption_file.exists():
            consumption_df = pd.read_parquet(self.workload_context.consumption_file)
            logger.info(f"Loaded {len(consumption_df)} consumption records")
            
            # Convert consumption to Pydantic models
            for _, row in consumption_df.iterrows():
                try:
                    consumption_records.append(Consumption(**row.to_dict()))
                except Exception as e:
                    logger.warning(f"Failed to parse consumption record: {e}")
        else:
            logger.warning("No consumption file found, continuing without power data")
        
        # Step 4: Aggregate fragments by task_id
        logger.info("Aggregating fragments into tasks...")
        fragments_by_task = fragments_df.groupby('id')
        
        # Step 5: Create Task objects with nested fragments
        tasks = []
        for _, task_row in tasks_df.iterrows():
            task_dict = task_row.to_dict()
            task_id = task_dict['id']
            
            # Get fragments for this task
            if task_id in fragments_by_task.groups:
                task_fragments_df = fragments_by_task.get_group(task_id)
                fragments = [Fragment(**frag_row.to_dict()) for _, frag_row in task_fragments_df.iterrows()]
                task_dict['fragments'] = fragments
            else:
                task_dict['fragments'] = []
            
            try:
                tasks.append(Task(**task_dict))
            except Exception as e:
                logger.warning(f"Failed to parse task {task_id}: {e}")
        
        logger.info(f"Created {len(tasks)} task aggregates")
        total_fragments = sum(len(t.fragments) for t in tasks)
        logger.info(f"Total fragments aggregated: {total_fragments}")
        
        return tasks, consumption_records
    
    def create_event_stream(
        self,
        tasks: List[Task],
        consumption: List[Consumption]
    ) -> List[Tuple[datetime, str, Union[Task, Consumption]]]:
        """Create a time-ordered event stream from tasks and consumption.
        
        Args:
            tasks: List of Task aggregates
            consumption: List of Consumption records
            
        Returns:
            Sorted list of (timestamp, event_type, event_object) tuples
        """
        logger.info("Creating time-ordered event stream...")
        
        events = []
        
        # Add task events
        for task in tasks:
            events.append((task.submission_time, "task", task))
        
        # Add consumption events
        for cons in consumption:
            events.append((cons.timestamp, "consumption", cons))
        
        # Sort by timestamp
        events.sort(key=lambda x: x[0])
        
        logger.info(f"Created event stream with {len(events)} events")
        logger.info(f"  - Tasks: {len(tasks)}")
        logger.info(f"  - Consumption: {len(consumption)}")
        
        if events:
            logger.info(f"Time range: {events[0][0]} to {events[-1][0]}")
        
        return events
    
    def stream_events(self, events: List[Tuple[datetime, str, Union[Task, Consumption]]]):
        """Stream events to Kafka in time order with proper timing.
        
        Args:
            events: Sorted list of (timestamp, event_type, event_object) tuples
        """
        if not events:
            logger.warning("No events to stream")
            return
        
        logger.info("Starting event streaming...")
        logger.info(f"Speed factor: {self.speed_factor}x")
        
        # Track simulation time
        sim_start_time = events[0][0]
        real_start_time = time.time()
        
        for i, (event_time, event_type, event_obj) in enumerate(events):
            # Calculate elapsed simulation time
            sim_elapsed = (event_time - sim_start_time).total_seconds()
            
            # Calculate required sleep based on speed_factor
            if self.speed_factor > 0:
                # Normal speed factor
                required_real_elapsed = sim_elapsed / self.speed_factor
                real_elapsed = time.time() - real_start_time
                sleep_time = required_real_elapsed - real_elapsed
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            # If speed_factor == -1, don't sleep (max speed)
            
            # Emit event to Kafka
            if event_type == "task":
                self._emit_task(event_obj)
            elif event_type == "consumption":
                self._emit_consumption(event_obj)
            
            # Progress logging
            if (i + 1) % 100 == 0:
                progress = (i + 1) / len(events) * 100
                logger.info(f"Progress: {i + 1}/{len(events)} events ({progress:.1f}%)")
        
        logger.info("Event streaming complete")
    
    def _emit_task(self, task: Task):
        """Emit a task aggregate to Kafka."""
        try:
            send_message(
                self.producer,
                topic="dc.workload",
                message=task.model_dump(mode='json'),
                key=str(task.id)
            )
        except Exception as e:
            logger.error(f"Failed to emit task {task.id}: {e}")
    
    def _emit_consumption(self, consumption: Consumption):
        """Emit a consumption record to Kafka."""
        try:
            send_message(
                self.producer,
                topic="dc.power",
                message=consumption.model_dump(mode='json'),
                key=None  # No key for consumption
            )
        except Exception as e:
            logger.error(f"Failed to emit consumption: {e}")
    
    def run(self):
        """Run the workload producer (main orchestration)."""
        logger.info("Starting DC-Mock Workload Producer")
        
        try:
            # Step 1: Load and aggregate data
            tasks, consumption = self.load_and_aggregate_data()
            
            # Step 2: Create time-ordered event stream
            events = self.create_event_stream(tasks, consumption)
            
            # Step 3: Stream events to Kafka
            self.stream_events(events)
            
            logger.info("✅ Workload production complete")
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Error in workload producer: {e}", exc_info=True)
            raise
        finally:
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main entry point."""
    # Load configuration from YAML
    try:
        config = load_config_from_env()
        logger.info(f"Loaded configuration for workload: {config.workload}")
        logger.info(f"Simulation speed: {config.simulation.speed_factor}x")
        logger.info(f"Window size: {config.simulation.window_size_minutes} minutes")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise
    
    # Get workload context with resolved paths
    data_path = Path(os.getenv("DATA_PATH", "/app/data"))
    workload_context = config.get_workload_context(base_path=data_path)
    
    # Check if workload directory exists
    if not workload_context.exists():
        logger.error(f"Workload directory does not exist: {workload_context.workload_dir}")
        logger.info("Available workloads in data directory:")
        for item in data_path.iterdir():
            if item.is_dir():
                logger.info(f"  - {item.name}")
        raise FileNotFoundError(f"Workload not found: {config.workload}")
    
    # Get Kafka configuration
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    # Wait for Kafka to be ready (simple retry logic)
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
            producer = WorkloadProducer(
                workload_context,
                kafka_bootstrap_servers,
                config.simulation.speed_factor
            )
            producer.run()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after maximum retries")
                raise


if __name__ == "__main__":
    main()
