"""Kafka consumer that assembles workload windows."""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Dict, Optional

import pandas as pd
from kafka import KafkaConsumer

from ....config.settings import REAL_WINDOW_SIZE_SEC, TIME_SCALE

logger = logging.getLogger(__name__)


def kafka_serializer(message: bytes) -> Dict:
    return json.loads(message.decode())


class DigitalTwinConsumer:
    """Consumes tasks and fragments from Kafka and creates processing windows."""

    def __init__(self, bootstrap_servers: str, kafka_group_id: str, fast_mode: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.stop_consuming = threading.Event()
        self.fast_mode = fast_mode

        # Simple in-memory storage for all workload messages (consolidated format)
        # Each entry has: {'id': task_id, 'task': {}, 'fragments': [], 'submission_time': pd.Timestamp}
        self.all_workload_messages = []
        
        # Current window tracking
        self.current_window_start: Optional[pd.Timestamp] = None
        self.current_window_end: Optional[pd.Timestamp] = None
        
        # Lock for window data
        self.window_lock = threading.Lock()
        
        # Signal when a window is ready for processing
        self.window_ready = threading.Event()

    def process_windows(self):
        logger.info("📥 Starting Kafka consumer...")
        logger.info(f"🔧 Consumer mode: fast_mode={self.fast_mode}")

        # Start consumer thread
        workload_thread = threading.Thread(target=self.consume_workload, daemon=True)
        workload_thread.start()

        window_count = 0
        
        while not self.stop_consuming.is_set():
            # Wait for a window to be ready
            if self.fast_mode:
                # In fast mode, check frequently for ready windows
                window_became_ready = self.window_ready.wait(timeout=0.1)
            else:
                # In normal mode, wait based on TIME_SCALE pacing
                # Windows are REAL_WINDOW_SIZE_SEC (300s) of virtual time
                # Real wait time = virtual_time * TIME_SCALE
                window_wait_time = REAL_WINDOW_SIZE_SEC * TIME_SCALE
                window_became_ready = self.window_ready.wait(timeout=window_wait_time)
            
            if self.stop_consuming.is_set():
                break
            
            # Check if we have a window ready
            with self.window_lock:
                if self.current_window_start is not None and len(self.all_workload_messages) > 0:
                    # We have data, but window might not be "closed" yet
                    # Only yield if window_ready was signaled or we're stopping
                    if not window_became_ready and not self.stop_consuming.is_set():
                        continue
                    
                    window_count += 1
                    
                    # Create batch data from current window
                    batch_data = self._create_batch_from_current_window(window_count)
                    
                    # Clear window_ready flag for next window
                    self.window_ready.clear()
                    
                    if batch_data:
                        yield batch_data
        
        logger.info(f"🛑 Consumer stopped: processed {window_count} windows")

    def consume_workload(self) -> None:
        """Consume consolidated workload messages containing both task and fragments."""
        try:
            consumer = KafkaConsumer(
                'workload',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=kafka_serializer,
                key_deserializer=kafka_serializer,
            )
            logger.info("✅ Workload consumer connected to Kafka")

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                # Extract task and fragments from consolidated message
                task_id = message.key['id']
                task_data = message.value['task']
                fragments_list = message.value['fragments']
                
                task_submission_time = pd.to_datetime(task_data['submission_time'])
                
                # Store the workload message in consolidated format
                workload_message = {
                    'id': task_id,
                    'task': task_data,
                    'fragments': fragments_list,
                    'submission_time': task_submission_time
                }
                
                window_is_complete = False
                with self.window_lock:
                    # Add to global workload list
                    self.all_workload_messages.append(workload_message)
                    
                    # Initialize first window if needed
                    if self.current_window_start is None:
                        self.current_window_start = task_submission_time
                        self.current_window_end = task_submission_time + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC)
                        logger.info(f"📦 Started first window: {self.current_window_start} → {self.current_window_end}")
                    
                    # Check if this task is beyond the current window
                    if task_submission_time >= self.current_window_end:
                        # Current window is complete - signal it's ready for processing
                        # Count messages in the current window (cumulative up to but not including window_end)
                        messages_in_window = sum(1 for msg in self.all_workload_messages 
                                                if msg['submission_time'] < self.current_window_end)
                        logger.info(f"🔔 Window complete: {messages_in_window} workload messages up to {self.current_window_end}")
                        self.window_ready.set()
                        window_is_complete = True
                
                # Release lock before waiting for window to be processed
                # This prevents deadlock with process_windows trying to acquire the lock
                if window_is_complete:
                    # Wait for the window to be processed before starting new one
                    # This prevents accumulating too many ready windows
                    while self.window_ready.is_set() and not self.stop_consuming.is_set():
                        time.sleep(0.01)
                    
                    if self.stop_consuming.is_set():
                        break
                    
                    # Start new window (contiguous - starts where previous ended)
                    with self.window_lock:
                        self.current_window_start = self.current_window_end
                        self.current_window_end = self.current_window_start + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC)
                        logger.info(f"📦 Started new window: {self.current_window_start} → {self.current_window_end}")

        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Workload consumer error: %s", exc)

    def _create_batch_from_current_window(self, window_number: int):
        """Create batch data from the current window (should be called with window_lock held)."""
        # Dynamically create parquet data from all workload messages up to current window end
        # This includes ALL tasks and fragments seen so far with submission_time < window_end
        # Cumulative aggregation: each window contains all historical tasks up to its boundary
        
        tasks_list = []
        fragments_list = []
        
        for workload_msg in self.all_workload_messages:
            if workload_msg['submission_time'] < self.current_window_end:
                # Add task
                task_data = workload_msg['task'].copy()
                task_data['id'] = workload_msg['id']
                tasks_list.append(task_data)
                
                # Add all fragments for this task
                for fragment in workload_msg['fragments']:
                    fragment_data = fragment.copy()
                    fragment_data['id'] = workload_msg['id']
                    # Use task's submission_time for fragments
                    fragment_data['submission_time'] = workload_msg['task']['submission_time']
                    fragments_list.append(fragment_data)
        
        # Convert to DataFrames
        tasks_df = pd.DataFrame(tasks_list)
        frags_df = pd.DataFrame(fragments_list)
        
        # Calculate average CPU usage
        avg_cpu_usage = 0.0
        if not frags_df.empty and 'cpu_usage' in frags_df.columns:
            avg_cpu_usage = frags_df['cpu_usage'].mean()
        
        task_count = len(tasks_df)
        fragment_count = len(frags_df)
        
        # Debug: Show task time range
        if not tasks_df.empty:
            tasks_df['submission_time_dt'] = pd.to_datetime(tasks_df['submission_time'])
            task_time_min = tasks_df['submission_time_dt'].min()
            task_time_max = tasks_df['submission_time_dt'].max()
            logger.debug(f"🔍 Window {window_number} task time range: {task_time_min} to {task_time_max}")
            logger.debug(f"🔍 Window {window_number} boundary check: all tasks < {self.current_window_end}? {(tasks_df['submission_time_dt'] < self.current_window_end).all()}")
        
        logger.info(f"🔨 Window {window_number}: {self.current_window_start} → {self.current_window_end}")
        logger.info(f"✅ Window {window_number}: {task_count} tasks, {fragment_count} fragments (cumulative: all with submission_time < {self.current_window_end})")
        
        batch_data = {
            'task_count': task_count,
            'fragment_count': fragment_count,
            'avg_cpu_usage': avg_cpu_usage,
            'timestamp': time.time(),
            'window_number': window_number,
            'window_info': f"Window {window_number}: {task_count} tasks, {fragment_count} fragments",
            'window_start': self.current_window_start,
            'window_end': self.current_window_end,
            'tasks_sample': tasks_df.to_dict(orient='records'),
            'fragments_sample': frags_df.to_dict(orient='records'),
        }
        
        return batch_data

    def stop(self) -> None:
        self.stop_consuming.set()
