"""Kafka consumer that assembles workload windows."""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Deque, Dict, Optional

import pandas as pd
from kafka import KafkaConsumer

from ....config.settings import FAST_MODE_SPEEDUP_FACTOR, REAL_WINDOW_SIZE_SEC, TIME_SCALE, VIRTUAL_WINDOW_SIZE

logger = logging.getLogger(__name__)


def kafka_serializer(message: bytes) -> Dict:
    return json.loads(message.decode())


class DigitalTwinConsumer:
    """Consumes tasks and fragments from Kafka and creates processing windows."""

    def __init__(self, bootstrap_servers: str, kafka_group_id: str, experiment_mode: bool = False, fast_mode: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.tasks_buffer: Deque = deque(maxlen=2000)
        self.tasks_df = pd.DataFrame()
        self.fragments_buffer: Deque = deque(maxlen=10000)
        self.stop_consuming = threading.Event()
        self.kafka_group_id = kafka_group_id
        self.tasks_lock = threading.Lock()
        self.fragments_lock = threading.Lock()
        self.experiment_mode = experiment_mode
        self.fast_mode = fast_mode

        self.windows_lock = threading.Condition()
        # Use unbounded deque to prevent window drops when consumer lags behind
        self.windows: Deque = deque()
        
        # Track timing for drift calculation
        self.first_window_start_time: Optional[pd.Timestamp] = None
        self.first_window_wall_clock: Optional[float] = None
        
        # Track queue growth to detect consumer lag
        self.last_queue_size: int = 0
        self.last_queue_check_time: Optional[float] = None

    def process_windows(self):
        logger.info("📥 Starting Kafka consumers...")
        logger.info(f"🔧 Consumer mode: experiment_mode={self.experiment_mode}, fast_mode={self.fast_mode}")

        tasks_thread = threading.Thread(target=self.consume_tasks, daemon=True)
        fragments_thread = threading.Thread(target=self.consume_fragments, daemon=True)

        tasks_thread.start()
        fragments_thread.start()

        time.sleep(5)

        window_count = 0
        first_wait_for_win = True
        while not self.stop_consuming.is_set():
            batch_data = self.create_batch(window_count + 1)
            if batch_data:
                window_count += 1
                first_wait_for_win = True

                if batch_data['task_count'] > 0 or batch_data['fragment_count'] > 0:
                    yield batch_data
                
                # After processing a window, check immediately if another is ready
                # (minimal wait to avoid thread starvation)
                wait_time = 0.001 if self.fast_mode else 0.5
            else:
                # No window ready - wait longer to avoid spinning
                if self.fast_mode:
                    wait_time = 0.05
                else:
                    wait_time = VIRTUAL_WINDOW_SIZE if first_wait_for_win else 0.5
            
            if not self.stop_consuming.wait(wait_time):
                first_wait_for_win = False
                continue
            break
        
        logger.info(f"🛑 Consumer stopped: processed {window_count} windows")

    def __add_to_window(self, data: Dict, list_name: str):
        sub_time = pd.to_datetime(data["submission_time"])

        window = None
        
        # Find matching window or mark old windows as ready
        for curr_window in self.windows:
            # Check if this message belongs to this window
            if sub_time >= curr_window["start"] and sub_time <= curr_window["end"]:
                window = curr_window
                break
            
            # Mark older windows as ready when we see newer messages
            elif sub_time > curr_window["end"]:
                if list_name == "tasks":
                    curr_window["has_newer_task"] = True
                else:
                    curr_window["has_newer_fragment"] = True
                
                # Window is ready when BOTH newer task AND fragment have arrived
                if curr_window["has_newer_task"] and curr_window["has_newer_fragment"]:
                    curr_window["ready"] = True

        # Create new window if no match found
        if not window:
            window = {
                "start": sub_time,
                "end": sub_time + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC),
                "tasks": [],
                "fragments": [],
                "ready": False,
                "has_newer_task": False,
                "has_newer_fragment": False,
            }
            self.windows.append(window)
            
            # Log queue size periodically
            if len(self.windows) % 10 == 1:
                logger.info(f"📦 Windows in queue: {len(self.windows)} (latest: {window['start']})")

        window[list_name].append(data)
        return window

    def consume_tasks(self) -> None:
        try:
            consumer = KafkaConsumer(
                'tasks',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=kafka_serializer,
                key_deserializer=kafka_serializer,
            )
            logger.info("✅ Tasks consumer connected to Kafka")

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                task_data = message.key | message.value
                with self.windows_lock:
                    self.__add_to_window(task_data, "tasks")

        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Task consumer error: %s", exc)

    def consume_fragments(self) -> None:
        try:
            consumer = KafkaConsumer(
                'fragments',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=kafka_serializer,
                key_deserializer=kafka_serializer,
            )
            logger.info("✅ Fragments consumer connected to Kafka")

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                fragment_data = message.key | message.value
                with self.windows_lock:
                    self.__add_to_window(fragment_data, "fragments")

        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Fragment consumer error: %s", exc)

    def create_batch(self, window_number: int):
        with self.windows_lock:
            if len(self.windows) == 0:
                return None
            
            # Find the earliest ready window (chronologically, not by creation order)
            earliest_ready_idx = None
            earliest_start_time = None
            ready_windows = 0
            
            for idx, win in enumerate(self.windows):
                if win["ready"]:
                    ready_windows += 1
                    if earliest_start_time is None or win["start"] < earliest_start_time:
                        earliest_start_time = win["start"]
                        earliest_ready_idx = idx
            
            if earliest_ready_idx is None:
                return None
            
            # Remove the earliest ready window from the deque
            window = self.windows[earliest_ready_idx]
            del self.windows[earliest_ready_idx]
            
            # Calculate consumer drift (how far behind we are relative to expected processing speed)
            current_wall_clock = time.time()
            speedup_factor = FAST_MODE_SPEEDUP_FACTOR if self.fast_mode else 1
            
            if self.first_window_start_time is None:
                # First window - establish baseline
                self.first_window_start_time = window['start']
                self.first_window_wall_clock = current_wall_clock
                self.last_queue_check_time = current_wall_clock
                drift_seconds = 0.0
                queue_status = "✅"
            else:
                # Calculate how much virtual time has passed in the workload
                virtual_elapsed = (window['start'] - self.first_window_start_time).total_seconds()
                # Calculate how much real wall-clock time has passed
                real_elapsed = current_wall_clock - self.first_window_wall_clock
                # Expected real time based on TIME_SCALE and speedup factor
                # Formula: real_time = virtual_time * TIME_SCALE / speedup_factor
                expected_real_elapsed = virtual_elapsed * TIME_SCALE / speedup_factor
                # Drift in real seconds (positive = consumer is lagging)
                drift_seconds = real_elapsed - expected_real_elapsed
                
                # Check queue growth every few seconds
                queue_growth = len(self.windows) - self.last_queue_size
                time_since_check = current_wall_clock - self.last_queue_check_time
                
                if time_since_check >= 5.0:  # Check every 5 seconds
                    if queue_growth > 10:
                        queue_status = "🚨 QUEUE GROWING - REDUCE SPEEDUP!"
                    elif queue_growth > 0:
                        queue_status = "⚠️ Queue growing"
                    elif queue_growth < -5:
                        queue_status = "✅ Queue shrinking"
                    else:
                        queue_status = "✅ Queue stable"
                    
                    self.last_queue_size = len(self.windows)
                    self.last_queue_check_time = current_wall_clock
                else:
                    # Use previous status
                    if queue_growth > 10:
                        queue_status = "🚨 QUEUE GROWING"
                    elif queue_growth > 0:
                        queue_status = "⚠️ Growing"
                    else:
                        queue_status = "✅"
            
            logger.info(f"🔨 Window {window_number}: {window['start']} → {window['end']} | Idx {earliest_ready_idx}/{ready_windows} ready | Queue: {len(self.windows)} {queue_status} | Drift: %+.2fs", drift_seconds)

        # Accumulate tasks for this and all previous windows
        self.tasks_df = pd.concat([self.tasks_df, pd.DataFrame(window["tasks"])], ignore_index=True)
        frags_df = pd.DataFrame(window["fragments"])

        curr_tasks_df = pd.DataFrame()
        avg_cpu_usage = 0.0
        
        if not frags_df.empty:
            frags_df["submission_time"] = pd.to_datetime(frags_df["submission_time"])
            
            # Log window info
            logger.info(f"Window {window_number}: {window['start']} → {window['end']}")
            
            # Get tasks for the fragments in this window
            if not self.tasks_df.empty:
                unique_frag_task_ids = frags_df['id'].unique()
                curr_tasks_df = self.tasks_df[self.tasks_df["id"].isin(unique_frag_task_ids)]
                
                # Handle missing tasks gracefully
                if len(curr_tasks_df) != len(unique_frag_task_ids):
                    missing_count = len(unique_frag_task_ids) - len(curr_tasks_df)
                    logger.warning(f"⚠️ Window {window_number}: {missing_count} fragments have missing parent tasks - dropping them")
                    # Filter fragments to only those with available tasks
                    available_task_ids = set(curr_tasks_df["id"])
                    frags_df = frags_df[frags_df["id"].isin(available_task_ids)]
                    logger.info(f"Kept {len(frags_df)} fragments with available tasks")
            
            if not frags_df.empty:
                avg_cpu_usage = frags_df['cpu_usage'].mean()

        task_count = len(curr_tasks_df)
        fragment_count = len(frags_df)

        batch_data = {
            'task_count': task_count,
            'fragment_count': fragment_count,
            'avg_cpu_usage': avg_cpu_usage,
            'timestamp': time.time(),
            'window_number': window_number,
            'window_info': f"Window {window_number}: {task_count} tasks, {fragment_count} fragments",
            "window_start": window["start"],
            "window_end": window["end"],
            'tasks_sample': curr_tasks_df.to_dict(orient='records'),
            'fragments_sample': frags_df.to_dict(orient='records'),
        }

        logger.info(f"✅ Window {window_number}: {task_count} tasks, {fragment_count} fragments ready for simulation")
        return batch_data

    def stop(self) -> None:
        self.stop_consuming.set()
