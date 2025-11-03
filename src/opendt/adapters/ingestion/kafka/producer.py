"""Kafka producer streaming telemetry traces with real-time pacing."""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from time import sleep
from typing import Optional

import pandas as pd
from kafka import KafkaProducer

from ....config.settings import TIME_SCALE

logger = logging.getLogger(__name__)


class TimedKafkaProducer:
    """Streams parquet data to Kafka with paced windows."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[KafkaProducer] = None
        self.stop_streaming = threading.Event()
        self.start_time: Optional[datetime] = None
        self.start_streaming_barrier = threading.Barrier(parties=2)

    def connect(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        logger.info("📡 Connected to Kafka: %s", self.bootstrap_servers)

    def tasks_streaming_thread(self, tasks: pd.DataFrame, start_time: pd.Timestamp) -> None:
        self.start_streaming_barrier.wait()
        logger.info("Started streaming tasks")

        last_submission_time = start_time
        for index, row in tasks.iterrows():
            if self.stop_streaming.is_set():
                return

            key = {'id': int(row['id'])}
            value = {
                'submission_time': row['submission_time'].isoformat(),
                'duration': int(row['duration']),
                'cpu_count': int(row['cpu_count']),
                'cpu_capacity': float(row['cpu_capacity']),
                'mem_capacity': int(row['mem_capacity']),
            }

            submission_time = row["submission_time"]
            if submission_time > last_submission_time:
                sleep_time = (submission_time - last_submission_time).total_seconds()
                sleep_time_virt = sleep_time * TIME_SCALE
                sleep(sleep_time_virt)

            self.producer.send("tasks", key=key, value=value)

            if index % 20 == 0:
                self.producer.flush()

            last_submission_time = submission_time

    def fragments_streaming_thread(self, frags: pd.DataFrame, start_time: pd.Timestamp) -> None:
        self.start_streaming_barrier.wait()
        logger.info("Started streaming fragments")

        last_submission_time = start_time

        for index, row in frags.iterrows():
            if self.stop_streaming.is_set():
                return

            key = {'id': int(row['id'])}
            value = {
                'duration': int(row['duration']),
                'cpu_usage': float(row['cpu_usage']),
                'submission_time': row['submission_time'].isoformat(),
            }

            submission_time = row["submission_time"]
            if submission_time > last_submission_time:
                sleep_time = (submission_time - last_submission_time).total_seconds()
                sleep_time_virt = sleep_time * TIME_SCALE
                sleep(sleep_time_virt)

            self.producer.send("fragments", key=key, value=value)

            if index % 100 == 0:
                self.producer.flush()

            last_submission_time = submission_time

    def stream_parquet_data_timed(self, tasks_file: str, fragments_file: str):
        if not self.producer:
            self.connect()

        logger.info("📂 Loading parquet files...")
        tasks_df = pd.read_parquet(tasks_file)
        fragments_df = pd.read_parquet(fragments_file)

        tasks_df['submission_time'] = pd.to_datetime(tasks_df['submission_time'])

        fragments_df["frag_nr"] = fragments_df.groupby("id").cumcount() + 1

        fragments_df = fragments_df.join(
            tasks_df.set_index("id")["submission_time"],
            on="id",
            how="left",
        )

        cum_dur = fragments_df.groupby("id")["duration"].cumsum()

        fragments_df["submission_time"] = fragments_df["submission_time"] + pd.to_timedelta(cum_dur, unit="ms")
        fragments_df = fragments_df.drop(columns=["frag_nr"])

        logger.info("📊 Loaded %s tasks, %s fragments", len(tasks_df), len(fragments_df))

        tasks_df = tasks_df.sort_values('submission_time')
        fragments_df = fragments_df.sort_values('submission_time')

        start_time = tasks_df['submission_time'].min()
        end_time = fragments_df['submission_time'].max()
        total_duration = (end_time - start_time).total_seconds()

        logger.info("⏰ Trace time span: %s to %s (%s hours)", start_time, end_time, total_duration / 3600)

        tasks_thread = threading.Thread(target=self.tasks_streaming_thread, args=(tasks_df, start_time,))
        frags_thread = threading.Thread(target=self.fragments_streaming_thread, args=(fragments_df, start_time,))

        tasks_thread.start()
        frags_thread.start()

        logger.info("Started producer threads")

        tasks_thread.join()
        frags_thread.join()

        logger.info("✅ All data streamed")
        return {
            'total_tasks': len(tasks_df),
            'total_fragments': len(fragments_df),
        }

    def stop(self) -> None:
        self.stop_streaming.set()
        logger.info("🛑 Producer stop requested")
