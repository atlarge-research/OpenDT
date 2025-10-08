#!/usr/bin/env python3
import time
import json
import os
import threading
import logging
from flask import Flask, render_template, jsonify
from kafka_producer import TimedKafkaProducer
from kafka_consumer import DigitalTwinConsumer
from opendc_runner import OpenDCRunner
from llm_optimizer import SimpleOptimizer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)


class OpenDTOrchestrator:
    def __init__(self):
        self.kafka_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.openai_key = os.environ.get('OPENAI_API_KEY')

        self.state = {
            'status': 'stopped',
            'cycle_count': 0,
            'last_simulation': None,
            'last_optimization': None,
            'total_tasks': 0,
            'total_fragments': 0,
            'current_window': None
        }

        # Components
        self.producer = None
        self.consumer = None
        self.opendc_runner = OpenDCRunner()
        self.optimizer = SimpleOptimizer(self.openai_key)

        # Threading control
        self.stop_event = threading.Event()
        self.producer_thread = None
        self.consumer_thread = None

    def start_system(self):
        """Start the complete digital twin system with proper controls"""
        logger.info("🚀 Starting OpenDT Digital Twin System")
        self.state['status'] = 'starting'
        self.stop_event.clear()

        try:
            # Initialize components
            self.producer = TimedKafkaProducer(self.kafka_servers)
            self.consumer = DigitalTwinConsumer(self.kafka_servers, f"OpenDT_telemetry")

            # Start consumer thread
            self.consumer_thread = threading.Thread(target=self.run_consumer, daemon=False)
            self.consumer_thread.start()

            time.sleep(5)

            # Start producer thread
            self.producer_thread = threading.Thread(target=self.run_producer, daemon=False)
            self.producer_thread.start()

            self.state['status'] = 'running'
            logger.info("✅ System started successfully")

        except Exception as e:
            logger.error(f"Failed to start system: {e}")
            self.state['status'] = 'error'

    def stop_system(self):
        """Stop the system properly"""
        logger.info("🛑 Stopping OpenDT Digital Twin System")
        self.state['status'] = 'stopping'

        # Signal all threads to stop
        self.stop_event.set()

        if self.producer:
            self.producer.stop()

        if self.consumer:
            self.consumer.stop()

        # Wait for threads to finish
        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=5)

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

        self.state['status'] = 'stopped'
        logger.info("✅ System stopped")

    def run_producer(self):
        """Stream parquet data to Kafka in time windows"""
        try:
            logger.info("📡 Starting timed telemetry producer...")
            stats = self.producer.stream_parquet_data_timed(
                tasks_file='/app/data/tasks.parquet',
                fragments_file='/app/data/fragments.parquet'
            )
            self.state['total_tasks'] = stats['total_tasks']
            self.state['total_fragments'] = stats['total_fragments']
            logger.info(f"✅ Producer finished: {stats}")
        except Exception as e:
            logger.error(f"Producer error: {e}")

    def run_consumer(self):
        """Process streaming data and run optimization cycles"""
        try:
            logger.info("📥 Starting digital twin consumer...")

            for cycle, batch_data in enumerate(self.consumer.process_windows()):
                if self.stop_event.is_set():
                    break

                cycle += 1
                self.state['cycle_count'] = cycle
                self.state['current_window'] = batch_data.get('window_info', 'Processing...')

                logger.info(f"🔄 Processing cycle {cycle}")

                # Run OpenDC simulation
                sim_results = self.run_simulation(batch_data)
                self.state['last_simulation'] = sim_results

                # Run LLM optimization
                if self.openai_key:
                    opt_results = self.optimizer.optimize(sim_results, batch_data)
                    self.state['last_optimization'] = opt_results
                    logger.info(f"🤖 Optimization: {opt_results.get('action_taken', 'none')}")

                # Short cycle delay
                if not self.stop_event.wait(15):  # 15 second cycles
                    continue
                else:
                    break

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Consumer error: {e}")

    def run_simulation(self, batch_data):
        """Run OpenDC simulation with windowed data"""
        logger.info("🔄 Running OpenDC simulation...")

        # Get tasks and fragments from batch
        tasks_data = batch_data.get('tasks_sample', [])
        fragments_data = batch_data.get('fragments_sample', [])

        # Load current topology
        topology_path = '/app/config/topology_template.json'
        if os.path.exists(topology_path):
            with open(topology_path, 'r') as f:
               topology_data = json.load(f)
        else: 
            topology_data = {
                "clusters": [{
                    "name": "C01",
                    "hosts": [{
                        "name": "H01",
                        "count": 2,
                        "cpu": {"coreCount": 16, "coreSpeed": 2400},
                        "memory": {"memorySize": 34359738368}
                    }]
                }]
            }

        # Run simulation
        #
        results = self.opendc_runner.run_simulation(
            tasks_data=tasks_data,
            fragments_data=fragments_data,
            topology_data=topology_data
        )

        logger.info(f"📊 Simulation Results: {results}")
        return results


# Global orchestrator
orchestrator = OpenDTOrchestrator()


# Web Dashboard
@app.route('/')
def dashboard():
    return render_template("index.html", state=orchestrator.state)


@app.route('/api/status')
def api_status():
    return jsonify(orchestrator.state)


@app.route('/api/start', methods=['POST'])
def api_start():
    if orchestrator.state['status'] in ['stopped', 'error']:
        threading.Thread(target=orchestrator.start_system, daemon=True).start()
        return jsonify({'message': 'System is starting...'})
    return jsonify({'message': f'System is {orchestrator.state["status"]}'})


@app.route('/api/stop', methods=['POST'])
def api_stop():
    if orchestrator.state['status'] in ['running', 'starting']:
        orchestrator.stop_system()
        return jsonify({'message': 'System stopped'})
    return jsonify({'message': 'System already stopped'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)