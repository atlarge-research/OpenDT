#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer
import logging
from collections import deque
import threading
import pandas as pd
from consts import *

logger = logging.getLogger(__name__)

def kafka_serializer(m):
    return json.loads(m.decode())

class DigitalTwinConsumer:
    """Consumes tasks and fragments from Kafka and creates processing windows"""

    def __init__(self, bootstrap_servers, kafka_group_id):
        self.bootstrap_servers = bootstrap_servers
        self.tasks_buffer = deque(maxlen=2000)
        self.tasks_df = pd.DataFrame()
        self.fragments_buffer = deque(maxlen=10000)
        self.stop_consuming = threading.Event()
        self.kafka_group_id = kafka_group_id
        self.tasks_lock = threading.Lock()
        self.fragments_lock = threading.Lock()

        self.windows_lock = threading.Condition()
        self.windows = deque(maxlen=50)

    def process_windows(self):
        """Process streaming data in time windows"""
        logger.info("📥 Starting Kafka consumers...")

        # Start consumers in background threads
        tasks_thread = threading.Thread(target=self.consume_tasks, daemon=True)
        fragments_thread = threading.Thread(target=self.consume_fragments, daemon=True)

        tasks_thread.start()
        fragments_thread.start()

        # Wait for initial data
        time.sleep(5)

        # Process windows
        window_count = 0
        first_wait_for_win = True
        while not self.stop_consuming.is_set():
            
            batch_data = self.create_batch(window_count + 1)
            if batch_data:
                window_count += 1
                first_wait_for_win = True

                if batch_data['task_count'] > 0 or batch_data['fragment_count'] > 0:
                    yield batch_data

            # Wait for next window
            wait_time = VIRTUAL_WINDOW_SIZE if first_wait_for_win else 0.5
            if not self.stop_consuming.wait(wait_time):
                first_wait_for_win = False
                continue
            else:
                break

    def __add_to_window(self, data, list_name): 
        sub_time = pd.to_datetime(data["submission_time"])
                
        w = None
        for i in range(0, len(self.windows)):
            curr_w = self.windows[i]
            if sub_time >= curr_w["start"] and sub_time <= curr_w["end"]:
                w = curr_w
                break 
            elif sub_time >= curr_w["end"]:
                curr_w["ready"] = True
            else:
                logger.error(f"Anomaly found for {list_name}!")
            
        if not w:
            w = {
                "start": sub_time,
                "end": sub_time + pd.Timedelta(seconds=REAL_WINDOW_SIZE_SEC),
                "tasks": [],
                "fragments": [],
                "ready": False
            }

            self.windows.append(w)

        w[list_name].append(data)
        return w
            

    def consume_tasks(self):
        """Consume tasks from Kafka"""
        """"""
        try:
            consumer = KafkaConsumer(
                'tasks',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=kafka_serializer,
                key_deserializer=kafka_serializer
                #group_id = self.kafka_group_id,
            )

            #consumer.seek_to_beginning()

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                task_data = message.key | message.value

                with self.windows_lock:
                    self.__add_to_window(task_data, "tasks")

        except Exception as e:
            logger.error(f"Task consumer error: {e}")

    def consume_fragments(self):
        """Consume fragments from Kafka"""
        try:
            consumer = KafkaConsumer(
                'fragments',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=kafka_serializer,
                key_deserializer=kafka_serializer
                #group_id = self.kafka_group_id,
            )

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                fragment_data = message.key | message.value

                with self.windows_lock:
                    self.__add_to_window(fragment_data, "fragments")
                

        except Exception as e:
            logger.error(f"Fragment consumer error: {e}")

    def create_batch(self, window_number):
        """Create a processing batch from current buffer data"""

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

            wstart = window["start"]
            wend = window["end"]
            logger.info(f"wstart: {wstart}, wend: {wend}")
            if (wend - wstart).total_seconds() > REAL_WINDOW_SIZE_SEC:
                logger.error(f"Window is larger than expected, wsize in seconds = {(wend - wstart).total_seconds()}")

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
            'fragments_sample': frags_df.to_dict(orient='records')
        }

        logger.info(f"📊 Window {window_number}: {task_count} tasks, {fragment_count} fragments")
        return batch_data

    def stop(self):
        """Stop consuming"""
        self.stop_consuming.set()
        logger.info("🛑 Consumer stop requested")
