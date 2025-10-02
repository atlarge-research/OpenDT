#!/usr/bin/env python3
import time
import json
import os
import threading
import logging
from flask import Flask, render_template_string, jsonify
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
            self.consumer = DigitalTwinConsumer(self.kafka_servers)

            # Start producer thread
            self.producer_thread = threading.Thread(target=self.run_producer, daemon=False)
            self.producer_thread.start()

            # Wait for initial data
            time.sleep(8)

            # Start consumer thread
            self.consumer_thread = threading.Thread(target=self.run_consumer, daemon=False)
            self.consumer_thread.start()

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
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>OpenDT Digital Twin</title>
        <style>
            body { font-family: Arial; padding: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px; }
            .metric { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .metric h3 { margin: 0 0 10px 0; color: #2c3e50; }
            .metric .value { font-size: 24px; font-weight: bold; color: #27ae60; }
            .controls { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .btn { padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
            .btn-primary { background: #3498db; color: white; }
            .btn-danger { background: #e74c3c; color: white; }
            .status-running { color: #27ae60; font-weight: bold; }
            .status-stopped { color: #e74c3c; font-weight: bold; }
            .status-stopping { color: #f39c12; font-weight: bold; }
            .data { background: white; padding: 20px; border-radius: 8px; font-family: monospace; margin-bottom: 20px; }
            .window-info { background: #ecf0f1; padding: 10px; border-radius: 4px; margin-bottom: 10px; }
        </style>
        <script>
            function startSystem() {
                fetch('/api/start', {method: 'POST'})
                    .then(r => r.json())
                    .then(data => { alert(data.message); setTimeout(() => location.reload(), 2000); });
            }
            function stopSystem() {
                fetch('/api/stop', {method: 'POST'})
                    .then(r => r.json())
                    .then(data => { alert(data.message); setTimeout(() => location.reload(), 1000); });
            }
            setInterval(() => location.reload(), 10000); // Auto-refresh every 10s
        </script>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏢 OpenDT Digital Twin Dashboard</h1>
                <p>Real-time datacenter simulation with 5-minute time windows</p>
            </div>

            <div class="controls">
                <h3>System Controls</h3>
                <button class="btn btn-primary" onclick="startSystem()">▶️ Start System</button>
                <button class="btn btn-danger" onclick="stopSystem()">⏹️ Stop System</button>
                <span class="status-{{ state.status }}">
                    ● Status: {{ state.status.title() }}
                </span>

                {% if state.current_window %}
                <div class="window-info">
                    <strong>Current Window:</strong> {{ state.current_window }}
                </div>
                {% endif %}
            </div>

            <div class="metrics">
                <div class="metric">
                    <h3>🔄 Processing</h3>
                    <div class="value">{{ state.cycle_count }}</div>
                    <small>Optimization Cycles</small>
                </div>

                <div class="metric">
                    <h3>📊 Workload</h3>
                    <div class="value">{{ state.total_tasks or 0 }}</div>
                    <small>Total Tasks</small>
                </div>

                {% if state.last_simulation %}
                <div class="metric">
                    <h3>⚡ Energy</h3>
                    <div class="value">{{ "%.2f"|format(state.last_simulation.energy_kwh) }}</div>
                    <small>kWh Usage</small>
                </div>

                <div class="metric">
                    <h3>💻 CPU</h3>
                    <div class="value">{{ "%.1f"|format(state.last_simulation.cpu_utilization * 100) }}%</div>
                    <small>Utilization</small>
                </div>

                <div class="metric">
                    <h3>⏱️ Runtime</h3>
                    <div class="value">{{ "%.1f"|format(state.last_simulation.runtime_hours or 0) }}h</div>
                    <small>Simulation Time</small>
                </div>
                {% endif %}
            </div>

            {% if state.last_simulation %}
            <div class="data">
                <h3>📈 Latest OpenDC Results</h3>
                <pre>{{ state.last_simulation | tojson(indent=2) }}</pre>
            </div>
            {% endif %}

            {% if state.last_optimization %}
            <div class="data">
                <h3>🤖 LLM Optimization</h3>
                <pre>{{ state.last_optimization | tojson(indent=2) }}</pre>
            </div>
            {% endif %}
        </div>
    </body>
    </html>
    """, state=orchestrator.state)


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
