"""Kafka producer streaming telemetry traces with real-time pacing."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from time import sleep
from typing import Optional
from collections import defaultdict

import pandas as pd
from kafka import KafkaProducer

from ....config.settings import FAST_MODE_SPEEDUP_FACTOR, TIME_SCALE

logger = logging.getLogger(__name__)


class TimedKafkaProducer:
    """Streams parquet data to Kafka with paced windows."""

    def __init__(self, bootstrap_servers: str, fast_mode: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.fast_mode = fast_mode
        self.producer: Optional[KafkaProducer] = None
        self.stop_streaming = threading.Event()
        self.start_time: Optional[datetime] = None

    def connect(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        logger.info("📡 Connected to Kafka: %s", self.bootstrap_servers)

    def workload_streaming_thread(self, tasks: pd.DataFrame, fragments_by_task: dict, workload_start_time: pd.Timestamp) -> None:
        """Stream consolidated task+fragments messages to Kafka."""
        # Record the actual wall-clock time when we start streaming
        streaming_start_time = time.time()
        total_tasks = len(tasks)
        
        logger.info("📤 Started streaming %d tasks with fragments", total_tasks)

        for index, row in tasks.iterrows():
            if self.stop_streaming.is_set():
                return

            task_id = int(row['id'])
            key = {'id': task_id}
            
            # Build task data
            task_data = {
                'submission_time': row['submission_time'].isoformat(),
                'duration': int(row['duration']),
                'cpu_count': int(row['cpu_count']),
                'cpu_capacity': float(row['cpu_capacity']),
                'mem_capacity': int(row['mem_capacity']),
            }
            
            # Get fragments for this task (if any)
            task_fragments = fragments_by_task.get(task_id, [])
            
            # Build consolidated message value
            value = {
                'task': task_data,
                'fragments': task_fragments
            }

            # Calculate when this message SHOULD be sent (absolute time since workload start)
            submission_time = row["submission_time"]
            time_since_workload_start = (submission_time - workload_start_time).total_seconds()
            
            # In normal mode, apply pacing with sleep
            if not self.fast_mode:
                # Apply speed scaling
                scaled_time = time_since_workload_start * TIME_SCALE
                
                # Calculate target wall-clock time
                target_time = streaming_start_time + scaled_time
                
                # Sleep until target time (if we're ahead)
                current_time = time.time()
                sleep_duration = target_time - current_time
                if sleep_duration > 0:
                    sleep(sleep_duration)

            # Send consolidated message to workload topic
            self.producer.send("workload", key=key, value=value)

            if index % 20 == 0:
                self.producer.flush()
            
            # Log progress every 100 tasks
            if index > 0 and index % 100 == 0:
                elapsed_real = time.time() - streaming_start_time
                progress_pct = (index / total_tasks) * 100
                if not self.fast_mode:
                    expected_elapsed = time_since_workload_start * TIME_SCALE
                    drift = elapsed_real - expected_elapsed
                    logger.info("📤 Workload: %d/%d (%.1f%%) | Elapsed: %.1fs | Expected: %.1fs | Drift: %+.2fs", 
                           index, total_tasks, progress_pct, elapsed_real, expected_elapsed, drift)
                else:
                    logger.info("📤 Workload: %d/%d (%.1f%%) | Elapsed: %.1fs", 
                           index, total_tasks, progress_pct, elapsed_real)

    def stream_parquet_data_timed(self, tasks_file: str, fragments_file: str):
        if not self.producer:
            self.connect()

        logger.info("📂 Loading parquet files...")
        tasks_df = pd.read_parquet(tasks_file)
        fragments_df = pd.read_parquet(fragments_file)

        tasks_df['submission_time'] = pd.to_datetime(tasks_df['submission_time'])

        logger.info("📊 Loaded %s tasks, %s fragments", len(tasks_df), len(fragments_df))

        # Sort tasks by submission time
        tasks_df = tasks_df.sort_values('submission_time').reset_index(drop=True)

        # Group fragments by task ID into a dictionary
        # Each fragment contains only raw fields: duration, cpu_count, cpu_usage
        fragments_by_task = defaultdict(list)
        for _, row in fragments_df.iterrows():
            task_id = int(row['id'])
            fragment = {
                'duration': int(row['duration']),
                'cpu_count': int(row['cpu_count']),
                'cpu_usage': float(row['cpu_usage']),
            }
            fragments_by_task[task_id].append(fragment)
        
        logger.info("📦 Grouped fragments by task: %d unique tasks", len(fragments_by_task))

        start_time = tasks_df['submission_time'].min()
        end_time = tasks_df['submission_time'].max()
        total_duration = (end_time - start_time).total_seconds()

        logger.info("⏰ Trace time span: %s to %s (%s hours)", start_time, end_time, total_duration / 3600)

        # Single thread for streaming workload
        workload_thread = threading.Thread(
            target=self.workload_streaming_thread, 
            args=(tasks_df, fragments_by_task, start_time)
        )

        workload_thread.start()
        logger.info("Started workload streaming thread")

        workload_thread.join()

        logger.info("✅ All data streamed successfully")
        return {
            'total_tasks': len(tasks_df),
            'total_fragments': len(fragments_df),
        }

    def stop(self) -> None:
        self.stop_streaming.set()
        logger.info("🛑 Producer stop requested")
