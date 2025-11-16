"""Kafka consumer that assembles workload windows."""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Deque, Dict

import pandas as pd
from kafka import KafkaConsumer

from ....config.settings import REAL_WINDOW_SIZE_SEC, VIRTUAL_WINDOW_SIZE

logger = logging.getLogger(__name__)


def kafka_serializer(message: bytes) -> Dict:
    return json.loads(message.decode())


class DigitalTwinConsumer:
    """Consumes tasks and fragments from Kafka and creates processing windows."""

    def __init__(self, bootstrap_servers: str, kafka_group_id: str, experiment_mode: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.tasks_buffer: Deque = deque(maxlen=2000)
        self.tasks_df = pd.DataFrame()
        self.fragments_buffer: Deque = deque(maxlen=10000)
        self.stop_consuming = threading.Event()
        self.kafka_group_id = kafka_group_id
        self.tasks_lock = threading.Lock()
        self.fragments_lock = threading.Lock()
        self.experiment_mode = experiment_mode

        self.windows_lock = threading.Condition()
        # Use unbounded deque in experiment mode to prevent window drops
        if experiment_mode:
            self.windows: Deque = deque()
        else:
            self.windows: Deque = deque(maxlen=50)

    def process_windows(self):
        logger.info("📥 Starting Kafka consumers...")

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

            wait_time = VIRTUAL_WINDOW_SIZE if first_wait_for_win else 0.5
            if not self.stop_consuming.wait(wait_time):
                first_wait_for_win = False
                continue
            break

    def __add_to_window(self, data: Dict, list_name: str):
        sub_time = pd.to_datetime(data["submission_time"])

        window = None
        for i in range(0, len(self.windows)):
            curr_window = self.windows[i]
            if sub_time >= curr_window["start"] and sub_time <= curr_window["end"]:
                window = curr_window
                break
            if sub_time >= curr_window["end"]:
                curr_window["ready"] = True
            else:
                logger.error(f"Anomaly found for {list_name}!")

        if not window:
            window = {
                "start": sub_time,
                "end": sub_time + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC),
                "tasks": [],
                "fragments": [],
                "ready": False,
            }

            self.windows.append(window)

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
            if len(self.windows) == 0 or not self.windows[0]["ready"]:
                return None

            window = self.windows.popleft()

        self.tasks_df = pd.concat([self.tasks_df, pd.DataFrame(window["tasks"])], ignore_index=True)
        frags_df = pd.DataFrame(window["fragments"])

        curr_tasks_df = pd.DataFrame()
        avg_cpu_usage = 0.0
        if not self.tasks_df.empty and not frags_df.empty:
            frags = len(frags_df['id'].unique())
            logger.info(f"{frags} tasks should be!")

            frags_df["submission_time"] = pd.to_datetime(frags_df["submission_time"])

            window_start = window["start"]
            window_end = window["end"]
            logger.info(f"wstart: {window_start}, wend: {window_end}")
            if (window_end - window_start).total_seconds() > REAL_WINDOW_SIZE_SEC:
                logger.error("Window is larger than expected, wsize in seconds = %s", (window_end - window_start).total_seconds())

            curr_tasks_df = self.tasks_df[self.tasks_df["id"].isin(frags_df["id"])]

            assert len(curr_tasks_df) == len(frags_df["id"].unique())

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
