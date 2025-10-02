#!/usr/bin/env python3
import pandas as pd
import json
import time
from kafka import KafkaProducer
import logging
from pathlib import Path
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)


class TimedKafkaProducer:
    """Streams parquet data to Kafka with REAL 5-minute time windows (not all at once!)"""

    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.stop_streaming = threading.Event()

    def connect(self):
        """Connect to Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode()
        )
        logger.info(f"📡 Connected to Kafka: {self.bootstrap_servers}")

    def stream_parquet_data_timed(self, tasks_file, fragments_file):
        """Stream data in 5-minute windows with PROPER time spacing"""
        if not self.producer:
            self.connect()

        # Load parquet files
        logger.info("📂 Loading parquet files...")
        tasks_df = pd.read_parquet(tasks_file)
        fragments_df = pd.read_parquet(fragments_file)

        # Convert submission_time to datetime
        tasks_df['submission_time'] = pd.to_datetime(tasks_df['submission_time'])

        # Merge fragments with task submission times
        fragments_df = fragments_df.merge(
            tasks_df[['id', 'submission_time']],
            on='id',
            how='left'
        ).dropna(subset=['submission_time'])  # Remove fragments without matching tasks

        logger.info(f"📊 Loaded {len(tasks_df)} tasks, {len(fragments_df)} fragments")

        # Sort by submission time
        tasks_df = tasks_df.sort_values('submission_time')
        fragments_df = fragments_df.sort_values('submission_time')

        # Get time range and create windows
        start_time = tasks_df['submission_time'].min()
        end_time = tasks_df['submission_time'].max()
        total_duration = (end_time - start_time).total_seconds()

        logger.info(f"⏰ Trace time span: {start_time} to {end_time} ({total_duration / 3600:.1f} hours)")

        # Create 5-minute windows
        window_size = timedelta(minutes=5)
        current_window_start = start_time
        window_count = 0

        while current_window_start < end_time and not self.stop_streaming.is_set():
            current_window_end = current_window_start + window_size
            window_count += 1

            # Get data for this specific time window
            window_tasks = tasks_df[
                (tasks_df['submission_time'] >= current_window_start) &
                (tasks_df['submission_time'] < current_window_end)
                ]

            window_fragments = fragments_df[
                (fragments_df['submission_time'] >= current_window_start) &
                (fragments_df['submission_time'] < current_window_end)
                ]

            logger.info(
                f"📤 Window {window_count} ({current_window_start.strftime('%H:%M:%S')}-{current_window_end.strftime('%H:%M:%S')}): "
                f"{len(window_tasks)} tasks, {len(window_fragments)} fragments"
            )

            if len(window_tasks) > 0 or len(window_fragments) > 0:
                # Stream tasks for this window
                for _, row in window_tasks.iterrows():
                    if self.stop_streaming.is_set():
                        break

                    key = {'id': int(row['id'])}
                    value = {
                        'submission_time': row['submission_time'].isoformat(),
                        'duration': int(row['duration']),
                        'cpu_count': int(row['cpu_count']),
                        'cpu_capacity': float(row['cpu_capacity']),
                        'mem_capacity': int(row['mem_capacity'])
                    }
                    self.producer.send('tasks', key=key, value=value)

                # Stream fragments for this window - BATCH SEND WITH SMALL DELAYS
                fragment_batch_size = 50  # Send in small batches
                for i in range(0, len(window_fragments), fragment_batch_size):
                    if self.stop_streaming.is_set():
                        break

                    batch = window_fragments.iloc[i:i + fragment_batch_size]
                    for _, row in batch.iterrows():
                        key = {'id': int(row['id'])}
                        value = {
                            'duration': int(row['duration']),
                            'cpu_usage': float(row['cpu_usage']),
                            'submission_time': row['submission_time'].isoformat()
                        }
                        self.producer.send('fragments', key=key, value=value)

                    # Small delay between batches within the window
                    time.sleep(0.5)

                self.producer.flush()

            # *** CRITICAL: Wait full 60 seconds before next window ***
            logger.info(f"⏱️  Waiting 60 seconds before next window...")
            for i in range(60):
                if self.stop_streaming.is_set():
                    logger.info("🛑 Streaming stopped by user")
                    return {'total_tasks': len(tasks_df), 'total_fragments': len(fragments_df)}
                time.sleep(1)

                # Log countdown every 15 seconds
                if i % 15 == 0 and i > 0:
                    logger.info(f"⏱️  {60 - i} seconds remaining...")

            current_window_start = current_window_end

        logger.info("✅ All windows streamed")
        return {
            'total_tasks': len(tasks_df),
            'total_fragments': len(fragments_df)
        }

    def stop(self):
        """Stop streaming"""
        self.stop_streaming.set()
        logger.info("🛑 Producer stop requested")
