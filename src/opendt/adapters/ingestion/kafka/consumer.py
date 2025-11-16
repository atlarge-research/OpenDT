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
        ready_count = 0
        
        # First pass: try to find matching window and mark ready windows
        for i in range(0, len(self.windows)):
            curr_window = self.windows[i]
            
            # Check if this message belongs to this window
            if sub_time >= curr_window["start"] and sub_time <= curr_window["end"]:
                window = curr_window
                # Don't break yet - continue to mark other windows as ready
            
            # Mark windows as ready if this message is chronologically after them
            elif sub_time >= curr_window["end"]:
                # Track that we've seen a newer message of this type
                if list_name == "tasks":
                    curr_window["has_newer_task"] = True
                else:  # fragments
                    curr_window["has_newer_fragment"] = True
                
                # Window is ready only if BOTH newer tasks and newer fragments have arrived
                if curr_window["has_newer_task"] and curr_window["has_newer_fragment"]:
                    if not curr_window["ready"]:
                        curr_window["ready"] = True
                        ready_count += 1

        # If no matching window found, create a new one
        if not window:
            window = {
                "start": sub_time,
                "end": sub_time + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC),
                "tasks": [],
                "fragments": [],
                "ready": False,
                "has_newer_task": False,      # True when a task from a newer window arrives
                "has_newer_fragment": False,  # True when a fragment from a newer window arrives
            }
            self.windows.append(window)
            
            # Log window queue size periodically to monitor buffering
            if len(self.windows) % 10 == 1:
                logger.info(f"📦 Windows in queue: {len(self.windows)} (latest: {window['start']}) - buffer growing, consumer may be lagging")

        window[list_name].append(data)
        
        # Log when windows become ready (but not too often)
        if ready_count > 0 and ready_count % 5 == 0:
            logger.info(f"✓ Marked {ready_count} windows as ready (total windows: {len(self.windows)})")
        
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

            message_count = 0
            for message in consumer:
                if self.stop_consuming.is_set():
                    logger.info(f"🛑 Tasks consumer stopped after {message_count} messages")
                    break

                message_count += 1
                task_data = message.key | message.value

                with self.windows_lock:
                    self.__add_to_window(task_data, "tasks")
                
                # Log progress every 1000 tasks
                if message_count % 1000 == 0:
                    logger.info(f"📨 Received {message_count} tasks")

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

            message_count = 0
            for message in consumer:
                if self.stop_consuming.is_set():
                    logger.info(f"🛑 Fragments consumer stopped after {message_count} messages")
                    break

                message_count += 1
                fragment_data = message.key | message.value

                with self.windows_lock:
                    self.__add_to_window(fragment_data, "fragments")
                
                # Log progress every 50000 fragments
                if message_count % 50000 == 0:
                    logger.info(f"📨 Received {message_count} fragments")

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

        self.tasks_df = pd.concat([self.tasks_df, pd.DataFrame(window["tasks"])], ignore_index=True)
        frags_df = pd.DataFrame(window["fragments"])

        curr_tasks_df = pd.DataFrame()
        avg_cpu_usage = 0.0
        if not self.tasks_df.empty and not frags_df.empty:
            unique_frag_task_ids = frags_df['id'].unique()
            logger.info(f"{len(unique_frag_task_ids)} unique task IDs in fragments")

            frags_df["submission_time"] = pd.to_datetime(frags_df["submission_time"])

            window_start = window["start"]
            window_end = window["end"]
            logger.info(f"wstart: {window_start}, wend: {window_end}")
            if (window_end - window_start).total_seconds() > REAL_WINDOW_SIZE_SEC:
                logger.error("Window is larger than expected, wsize in seconds = %s", (window_end - window_start).total_seconds())

            # Get tasks that are available in tasks_df
            curr_tasks_df = self.tasks_df[self.tasks_df["id"].isin(unique_frag_task_ids)]
            
            # Verify all tasks are available (should always be true with new ready logic)
            if len(curr_tasks_df) != len(unique_frag_task_ids):
                missing_count = len(unique_frag_task_ids) - len(curr_tasks_df)
                logger.error(f"❌ Window {window_number}: {missing_count} tasks still missing! This should not happen with new ready logic.")
                logger.error(f"Has newer task: {window['has_newer_task']}, Has newer fragment: {window['has_newer_fragment']}")
            
            assert len(curr_tasks_df) == len(unique_frag_task_ids), f"Task/fragment mismatch: {len(curr_tasks_df)} tasks vs {len(unique_frag_task_ids)} unique fragment task IDs"

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

        logger.info("📊 Window %s: %s tasks, %s fragments", window_number, task_count, fragment_count)
        return batch_data

    def stop(self) -> None:
        self.stop_consuming.set()
