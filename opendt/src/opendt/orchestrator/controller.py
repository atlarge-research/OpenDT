#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import threading
import time
from typing import Dict, Optional, Set

from ..ingestion.kafka.producer import TimedKafkaProducer
from ..ingestion.kafka.consumer import DigitalTwinConsumer
from ..optimization.llm import LLM
from ..simulation.runner import OpenDCRunner

IMPROVEMENT_DELTA = 0.05
WINDOW_TRY_BUDGET_SEC = 30.0
MAX_TRIES_PER_WINDOW = 1
NO_IMPROVEMENT_STOP_AFTER = 3

logger = logging.getLogger(__name__)


class OpenDTOrchestrator:
    def __init__(self) -> None:
        self.kafka_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.openai_key = os.environ.get('OPENAI_API_KEY')
        self.slo_targets = {
            'energy_target': 10.0,
            'runtime_target': 2,
        }

        self.state: Dict[str, Optional[object]] = {
            'status': 'stopped',
            'cycle_count': 0,
            'cycle_count_opt': 0,
            'last_simulation': None,
            'last_optimization': None,
            'total_tasks': 0,
            'total_fragments': 0,
            'current_window': None,
            'current_topology': None,
            'best_config': None,
            'topology_updates': 0,
            'window_baseline_score': None,
            'window_best_score': None,
            'window_trials': 0,
            'window_accepted': False,
            'window_time_used_sec': 0.0,
        }

        self.open_dc_results = []
        self.open_dc_results_timestamps = []
        self.open_dc_results_lock = threading.Lock()

        self.producer: Optional[TimedKafkaProducer] = None
        self.consumer: Optional[DigitalTwinConsumer] = None
        self.opendc_runner = OpenDCRunner()
        self.optimizer = LLM(self.openai_key)

        self.stop_event = threading.Event()
        self.producer_thread: Optional[threading.Thread] = None
        self.consumer_thread: Optional[threading.Thread] = None

        self.topology_path = '/app/config/topology.json'
        self.last_topology_hash: Optional[str] = None

        self.load_initial_topology()
        self.start_topology_watcher()

    def load_initial_topology(self) -> None:
        if os.path.exists(self.topology_path):
            with open(self.topology_path, 'r') as f:
                topo = json.load(f)
            self.state['current_topology'] = topo
            self.last_topology_hash = self._topo_hash(topo)
            logger.info("📄 Loaded initial topology configuration")
        else:
            logger.warning("⚠️ Topology not found, a default will be used at runtime")

    def start_topology_watcher(self) -> None:
        self._topology_last_mtime = 0.0
        self._watch_thread = threading.Thread(target=self._watch_topology_file, daemon=True)
        self._watch_thread.start()

    def _watch_topology_file(self) -> None:
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

    def _topo_hash(self, topo: Optional[dict]) -> str:
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

    def start_system(self) -> None:
        logger.info("🚀 Starting OpenDT Digital Twin System")
        self.state['status'] = 'starting'
        self.stop_event.clear()

        try:
            self.producer = TimedKafkaProducer(self.kafka_servers)
            self.consumer = DigitalTwinConsumer(self.kafka_servers, f"OpenDT_telemetry")

            self.consumer_thread = threading.Thread(target=self.run_consumer, daemon=False)
            self.consumer_thread.start()

            time.sleep(5)

            self.producer_thread = threading.Thread(target=self.run_producer, daemon=False)
            self.producer_thread.start()

            self.state['status'] = 'running'
            logger.info("✅ System started successfully")

        except Exception as e:
            logger.error(f"Failed to start system: {e}")
            self.state['status'] = 'error'

    def stop_system(self) -> None:
        logger.info("🛑 Stopping OpenDT Digital Twin System")
        self.state['status'] = 'stopping'

        self.stop_event.set()

        if self.producer:
            self.producer.stop()

        if self.consumer:
            self.consumer.stop()

        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=5)

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

        self.state['status'] = 'stopped'
        logger.info("✅ System stopped")

    def run_producer(self) -> None:
        try:
            logger.info("📡 Starting timed telemetry producer...")
            stats = self.producer.stream_parquet_data_timed(
                tasks_file='/../app/data/tasks.parquet',
                fragments_file='/../app/data/fragments.parquet'
            )
            self.state['total_tasks'] = stats['total_tasks']
            self.state['total_fragments'] = stats['total_fragments']
            logger.info(f"✅ Producer finished: {stats}")
        except Exception as e:
            logger.error(f"Producer error: {e}")

    def _score(self, sim_results: dict) -> float:
        energy = sim_results.get('energy_kwh', 0.0) or 0.0
        runtime = sim_results.get('runtime_hours', 0.0) or 0.0

        energy_target = self.slo_targets.get('energy_target', 10.0)
        runtime_target = self.slo_targets.get('runtime_target', 2.0)

        energy_score = min(100, max(0, (energy - energy_target) / energy_target * 100))
        runtime_score = min(100, max(0, (runtime - runtime_target) / runtime_target * 100))

        ENERGY_WEIGHT = 0.6
        RUNTIME_WEIGHT = 0.4

        final_score = (energy_score * ENERGY_WEIGHT) + (runtime_score * RUNTIME_WEIGHT)
        return round(100 - final_score, 2)

    def run_consumer(self) -> None:
        try:
            logger.info("📥 Starting digital twin consumer...")
            for cycle, batch_data in enumerate(self.consumer.process_windows(), start=1):
                if self.stop_event.is_set():
                    break

                self.state['cycle_count'] = cycle
                self.state['current_window'] = batch_data.get('window_info', 'Processing...')
                logger.info(f"🔄 Processing cycle {cycle}")

                logger.info(f"Running a simulation with baseline: {self.state.get('current_topology')}")
                baseline = self.run_simulation(batch_data)

                with self.open_dc_results_lock:
                    self.open_dc_results.append(baseline)
                    self.open_dc_results_timestamps.append(batch_data["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ"))

                self.state['last_simulation'] = baseline
                baseline_score = self._score(baseline)

                self.state.update({
                    'window_baseline_score': round(baseline_score, 3),
                    'window_best_score': round(baseline_score, 3),
                    'window_trials': 0,
                    'window_accepted': False,
                })

                best_topology = self.state.get('current_topology')
                best_score = baseline_score
                seen: Set[str] = {self._topo_hash(best_topology) if best_topology else ''}
                tries = 0
                deadline = time.monotonic() + WINDOW_TRY_BUDGET_SEC

                while (
                    time.monotonic() < deadline
                    and tries < MAX_TRIES_PER_WINDOW
                    and not self.stop_event.is_set()
                ):
                    tries += 1
                    opt = self.optimizer.optimize(
                        baseline,
                        batch_data,
                        self.slo_targets,
                        current_topology=best_topology,
                    )

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
                        topology_data=proposed,
                    )

                    self.state['last_optimization'] = opt
                    self.state['last_optimization']['energy_kwh'] = probe.get('energy_kwh', None)
                    self.state['last_optimization']['runtime_hours'] = probe.get('runtime_hours', None)
                    self.state['last_optimization']['cpu_utilization'] = probe.get('cpu_utilization', None)
                    self.state['last_optimization']['max_power_draw'] = probe.get('max_power_draw', None)

                    self.state['cycle_count_opt'] += 1
                    score = self._score(probe)
                    if score < best_score - IMPROVEMENT_DELTA:
                        best_topology, best_score = proposed, score
                        self.state['window_best_score'] = round(best_score, 3)

                    self.state['window_trials'] = tries

                self.state['best_config'] = {'config': best_topology, 'score': round(best_score, 3)}

                if self.stop_event.wait(0.1):
                    break

        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(f"Consumer error: {e}")

    def run_simulation(self, batch_data, expName="simple"):
        logger.info("🔄 Running OpenDC simulation...")

        tasks_data = batch_data.get('tasks_sample', [])
        fragments_data = batch_data.get('fragments_sample', [])
        topology_data = self.state.get('current_topology')
        results = self.opendc_runner.run_simulation(
            tasks_data=tasks_data,
            fragments_data=fragments_data,
            topology_data=topology_data,
            expName=expName,
        )
        logger.info(f"📊 Simulation Results: {results}")
        return results
