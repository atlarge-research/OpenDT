#!/usr/bin/env python3
import time
import json
import os
import threading
import logging
import hashlib
from flask import Flask, render_template, jsonify
from kafka_producer import TimedKafkaProducer
from kafka_consumer import DigitalTwinConsumer
from opendc_runner import OpenDCRunner
from llm_optimizer import SimpleOptimizer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- tuning knobs for the improvement loop ---
IMPROVEMENT_DELTA = 0.05  # minimum score improvement to accept a topology
WINDOW_TRY_BUDGET_SEC = 30.0  # hard time budget per data window
MAX_TRIES_PER_WINDOW = 8  # cap to avoid runaway trials even if simulations are super fast
NO_IMPROVEMENT_STOP_AFTER = 3  # stop early if we see this many consecutive non-better proposals


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
            'current_window': None,
            'current_topology': None,
            'best_config': None,
            'topology_updates': 0,
            # live optimization diagnostics (per window)
            'window_baseline_score': None,
            'window_best_score': None,
            'window_trials': 0,
            'window_accepted': False,
            'window_time_used_sec': 0.0,
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

        # Topology file plumbing
        self.topology_path = '/app/config/topology_template.json'
        self.last_topology_hash = None

        # Load initial topology and start file watcher (keeps dashboard in sync)
        self.load_initial_topology()
        self.start_topology_watcher()

    # ---------------- Topology I/O ----------------

    def load_initial_topology(self):
        if os.path.exists(self.topology_path):
            with open(self.topology_path, 'r') as f:
                topo = json.load(f)
            self.state['current_topology'] = topo
            self.last_topology_hash = self._topo_hash(topo)
            logger.info("📄 Loaded initial topology configuration")
        else:
            logger.warning("⚠️ Topology not found, a default will be used at runtime")

    def start_topology_watcher(self):
        self._topology_last_mtime = 0.0
        self._watch_thread = threading.Thread(target=self._watch_topology_file, daemon=True)
        self._watch_thread.start()

    def _watch_topology_file(self):
        while not self.stop_event.is_set():
            try:
                if os.path.exists(self.topology_path):
                    mtime = os.path.getmtime(self.topology_path)
                    if mtime != self._topology_last_mtime:
                        with open(self.topology_path, 'r') as f:
                            new_topology = json.load(f)
                        self.state['current_topology'] = new_topology
                        self.state['topology_updates'] = (self.state.get('topology_updates') or 0) + 1
                        self._topology_last_mtime = mtime
                        logger.info("🔁 Topology file changed; dashboard state updated")
            except Exception as e:
                logger.warning(f"Topology watcher error: {e}")
            time.sleep(0.5)

    def _topo_hash(self, topo: dict) -> str:
        try:
            canonical = json.dumps(topo, sort_keys=True, separators=(',', ':'))
        except Exception:
            canonical = str(topo)
        return hashlib.sha256(canonical.encode('utf-8')).hexdigest()

    def update_topology_file(self, new_topology: dict) -> bool:
        if not new_topology:
            return False
        new_hash = self._topo_hash(new_topology)
        if self.last_topology_hash == new_hash:
            logger.info("↩️ Skipping apply: topology identical to current (no-op)")
            return False
        try:
            backup_path = self.topology_path + '.backup'
            if os.path.exists(self.topology_path):
                with open(self.topology_path, 'r') as f:
                    backup_data = json.load(f)
                with open(backup_path, 'w') as f:
                    json.dump(backup_data, f, indent=2)

            with open(self.topology_path, 'w') as f:
                json.dump(new_topology, f, indent=2)

            self.state['current_topology'] = new_topology
            self.state['topology_updates'] += 1
            self.last_topology_hash = new_hash
            logger.info(f"✅ Applied new topology (update #{self.state['topology_updates']})")
            return True
        except Exception as e:
            logger.error(f"Failed to update topology file: {e}")
            return False

    # ---------------- Lifecycle ----------------

    def start_system(self):
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

    def _score(self, sim_results: dict) -> float:
        energy = sim_results.get('energy_kwh', 5.0) or 0.0
        runtime = sim_results.get('runtime_hours', 1.0) or 0.0
        return (2.0 * float(energy)) + (1.0 * float(runtime))

    def run_consumer(self):
        """Per window: run baseline → time-bounded proposal loop → commit only if better."""
        try:
            logger.info("📥 Starting digital twin consumer...")
            for cycle, batch_data in enumerate(self.consumer.process_windows(), start=1):
                if self.stop_event.is_set():
                    break

                # window meta
                self.state['cycle_count'] = cycle
                self.state['current_window'] = batch_data.get('window_info', 'Processing...')
                logger.info(f"🔄 Processing cycle {cycle}")

                # 1) baseline on current topology
                baseline = self.run_simulation(batch_data)
                self.state['last_simulation'] = baseline
                baseline_score = self._score(baseline)

                # track a few lightweight stats for the dashboard
                self.state.update({
                    'window_baseline_score': round(baseline_score, 3),
                    'window_best_score': round(baseline_score, 3),
                    'window_trials': 0,
                    'window_accepted': False,
                })

                best_topology = self.state.get('current_topology')
                best_score = baseline_score
                seen = {self._topo_hash(best_topology) if best_topology else ''}
                tries = 0
                deadline = time.monotonic() + WINDOW_TRY_BUDGET_SEC

                # 2) improvement loop (strict time budget)
                while (
                        time.monotonic() < deadline
                        and tries < MAX_TRIES_PER_WINDOW
                        and not self.stop_event.is_set()
                ):
                    tries += 1
                    opt = self.optimizer.optimize(baseline, batch_data, current_topology=best_topology)
                    self.state['last_optimization'] = opt

                    proposed = opt.get('new_topology')
                    if not proposed:
                        continue
                    h = self._topo_hash(proposed)
                    if h in seen:
                        continue
                    seen.add(h)

                    probe = self.opendc_runner.run_simulation(
                        tasks_data=batch_data.get('tasks_sample', []),
                        fragments_data=batch_data.get('fragments_sample', []),
                        topology_data=proposed
                    )
                    score = self._score(probe)
                    if score < best_score - IMPROVEMENT_DELTA:
                        best_topology, best_score = proposed, score
                        self.state['window_best_score'] = round(best_score, 3)

                    self.state['window_trials'] = tries

                # 3) apply only if better than baseline
                if best_score < baseline_score - IMPROVEMENT_DELTA and best_topology:
                    if self.update_topology_file(best_topology):
                        self.state['window_accepted'] = True
                        self.state['best_config'] = {'config': best_topology, 'score': round(best_score, 3)}
                        logger.info(f"✅ Applied improved topology: {best_score:.3f} < {baseline_score:.3f}")
                else:
                    logger.info(f"📎 No commit (best {best_score:.3f} vs baseline {baseline_score:.3f})")

                # don’t block the next window tick
                if self.stop_event.wait(0.1):
                    break

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Consumer error: {e}")

    # ---------------- Simulation wrapper ----------------

    def run_simulation(self, batch_data):
        """Run OpenDC simulation with windowed data"""
        logger.info("🔄 Running OpenDC simulation...")

        # Get tasks and fragments from batch
        tasks_data = batch_data.get('tasks_sample', [])
        fragments_data = batch_data.get('fragments_sample', [])
        topology_data = self.state.get('current_topology')
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


@app.route('/api/topology')
def api_topology():
    return jsonify({
        'current_topology': orchestrator.state.get('current_topology'),
        'best_config': orchestrator.state.get('best_config'),
        'topology_updates': orchestrator.state.get('topology_updates', 0)
    })


@app.route('/api/reset_topology', methods=['POST'])
def api_reset_topology():
    try:
        orchestrator.load_initial_topology()
        return jsonify({'message': 'Topology reset to initial configuration'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # NOTE: ensure index.html is under ./templates/index.html
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
