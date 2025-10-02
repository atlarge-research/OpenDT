#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer
import logging
from collections import deque
import threading
from datetime import datetime

logger = logging.getLogger(__name__)


class DigitalTwinConsumer:
    """Consumes tasks and fragments from Kafka and creates processing windows"""

    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.tasks_buffer = deque(maxlen=2000)
        self.fragments_buffer = deque(maxlen=10000)
        self.window_size_seconds = 30
        self.stop_consuming = threading.Event()

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
        while not self.stop_consuming.is_set():
            window_count += 1
            batch_data = self.create_batch(window_count)

            if batch_data['task_count'] > 0 or batch_data['fragment_count'] > 0:
                yield batch_data

            # Wait for next window
            if not self.stop_consuming.wait(self.window_size_seconds):
                continue
            else:
                break

    def consume_tasks(self):
        """Consume tasks from Kafka"""
        try:
            consumer = KafkaConsumer(
                'tasks',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=5000
            )

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                task_data = message.value
                self.tasks_buffer.append(task_data)

                if len(self.tasks_buffer) % 50 == 0:
                    logger.info(f"📥 Tasks buffer: {len(self.tasks_buffer)}")

        except Exception as e:
            logger.error(f"Task consumer error: {e}")

    def consume_fragments(self):
        """Consume fragments from Kafka"""
        try:
            consumer = KafkaConsumer(
                'fragments',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=5000
            )

            for message in consumer:
                if self.stop_consuming.is_set():
                    break

                fragment_data = message.value
                self.fragments_buffer.append(fragment_data)

                if len(self.fragments_buffer) % 100 == 0:
                    logger.info(f"📥 Fragments buffer: {len(self.fragments_buffer)}")

        except Exception as e:
            logger.error(f"Fragment consumer error: {e}")

    def create_batch(self, window_number):
        """Create a processing batch from current buffer data"""
        tasks_snapshot = list(self.tasks_buffer)
        fragments_snapshot = list(self.fragments_buffer)

        # Clear buffers after capturing data
        self.tasks_buffer.clear()
        self.fragments_buffer.clear()

        task_count = len(tasks_snapshot)
        fragment_count = len(fragments_snapshot)

        # Calculate average CPU usage
        avg_cpu_usage = 0.0
        if fragments_snapshot:
            cpu_usages = [f.get('cpu_usage', 0) for f in fragments_snapshot]
            avg_cpu_usage = sum(cpu_usages) / len(cpu_usages)

        batch_data = {
            'task_count': task_count,
            'fragment_count': fragment_count,
            'avg_cpu_usage': avg_cpu_usage,
            'timestamp': time.time(),
            'window_number': window_number,
            'window_info': f"Window {window_number}: {task_count} tasks, {fragment_count} fragments",
            'tasks_sample': tasks_snapshot,
            'fragments_sample': fragments_snapshot
        }

        logger.info(f"📊 Window {window_number}: {task_count} tasks, {fragment_count} fragments")
        return batch_data

    def stop(self):
        """Stop consuming"""
        self.stop_consuming.set()
        logger.info("🛑 Consumer stop requested")
