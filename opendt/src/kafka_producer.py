#!/usr/bin/env python3
import pandas as pd
import json
import time
from kafka import KafkaProducer
import logging
from pathlib import Path
from datetime import datetime, timedelta
import threading
from time import sleep
from consts import TIME_SCALE

logger = logging.getLogger(__name__)


class TimedKafkaProducer:
    """Streams parquet data to Kafka with REAL 5-minute time windows (not all at once!)"""

    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.stop_streaming = threading.Event()
        self.start_time: datetime = None
        self.start_streaming_barrier = threading.Barrier(parties=2)

    def connect(self):
        """Connect to Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode()
        )
        logger.info(f"📡 Connected to Kafka: {self.bootstrap_servers}")

    def tasks_streaming_thread(self, tasks: pd.DataFrame, start_time):
        #TODO  wait for both threads to start
        #have a barrier here
        
        self.start_streaming_barrier.wait()
        logger.info("Started streaming tasks")

        last_submission_time = start_time
        i = 0
        for _, row in tasks.iterrows():
            if self.stop_streaming.is_set():
                return
            
            key = {'id': int(row['id'])}
            value = {
                'submission_time': row['submission_time'].isoformat(),
                'duration': int(row['duration']),
                'cpu_count': int(row['cpu_count']),
                'cpu_capacity': float(row['cpu_capacity']),
                'mem_capacity': int(row['mem_capacity'])
            }
            
            submission_time = row["submission_time"]
            if submission_time > last_submission_time:
                sleep_time = (submission_time - last_submission_time).total_seconds()
                sleep_time_virt = sleep_time * TIME_SCALE
                
                sleep(sleep_time_virt)

            self.producer.send("tasks", key=key, value=value)

            if(i % 20 == 0):
                self.producer.flush()

            last_submission_time = submission_time
            i+=1

    def fragments_streaming_thread(self, frags: pd.DataFrame, start_time):
        #TODO  wait for both threads to start
        #have a barrier here

        self.start_streaming_barrier.wait()
        logger.info("Started streaming fragments")

        i = 0
        last_submission_time = start_time

        for _, row in frags.iterrows():
            if self.stop_streaming.is_set():
                return
            
            key = {'id': int(row['id'])}
            value = {
                'duration': int(row['duration']),
                'cpu_usage': float(row['cpu_usage']),
                'submission_time': row['submission_time'].isoformat()
            }
            
            submission_time = row["submission_time"]
            if submission_time > last_submission_time:
                sleep_time = (submission_time - last_submission_time).total_seconds()
                sleep_time_virt = sleep_time * TIME_SCALE

                sleep(sleep_time_virt)


            self.producer.send("fragments", key=key, value=value)

            if(i % 100 == 0):
                self.producer.flush()

            last_submission_time = submission_time
            i+= 1

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

        fragments_df["frag_nr"] = fragments_df.groupby("id").cumcount() + 1

        fragments_df = fragments_df.join(
            tasks_df.set_index("id")[["submission_time"]],
            on="id",
            how="left"
        )

        cum_dur = fragments_df.groupby("id")["duration"].cumsum()

        fragments_df["submission_time"] = fragments_df["submission_time"] + pd.to_timedelta(cum_dur, unit="ms")
        fragments_df = fragments_df.drop(columns=["frag_nr"])

        logger.info(f"📊 Loaded {len(tasks_df)} tasks, {len(fragments_df)} fragments")

        # Sort by submission time
        tasks_df = tasks_df.sort_values('submission_time')
        fragments_df = fragments_df.sort_values('submission_time')

        # Get time range and create windows
        start_time = tasks_df['submission_time'].min()
        end_time = fragments_df['submission_time'].max()
        total_duration = (end_time - start_time).total_seconds()

        logger.info(f"⏰ Trace time span: {start_time} to {end_time} ({total_duration / 3600:.1f} hours)")
        
        #now we should make 2 threads for streaming tasks and fragments
        #in both threads we do the follwoing:
        tasks_th = threading.Thread(target=self.tasks_streaming_thread, args=(tasks_df, start_time, ))
        frags_th = threading.Thread(target=self.fragments_streaming_thread, args=(fragments_df, start_time, ))

        tasks_th.start()
        frags_th.start()

        logger.info("Started producer threads")

        tasks_th.join()
        frags_th.join()

        logger.info("✅ All data streamed")
        return {
            'total_tasks': len(tasks_df),
            'total_fragments': len(fragments_df)
        }

    def stop(self):
        """Stop streaming"""
        self.stop_streaming.set()
        logger.info("🛑 Producer stop requested")
