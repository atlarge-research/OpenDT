"""Core OpenDT orchestrator implementation."""
from __future__ import annotations

import logging
import threading
import time
from copy import deepcopy
from typing import Any

from ..config import loaders
from ..config.settings import (
    IMPROVEMENT_DELTA,
    MAX_TRIES_PER_WINDOW,
    WINDOW_TRY_BUDGET_SEC,
    SLOTargets,
    kafka_bootstrap_servers,
    openai_api_key,
)
from ..ingestion.kafka.consumer import DigitalTwinConsumer
from ..ingestion.kafka.producer import TimedKafkaProducer
from ..optimization.llm import LLM
from ..orchestrator.state import SimulationResultsBuffer, default_state_dict
from ..simulation.runner import OpenDCRunner
from .topology import topology_hash, watch_topology_file

logger = logging.getLogger(__name__)


class OpenDTOrchestrator:
    def __init__(self) -> None:
        self.kafka_servers = kafka_bootstrap_servers()
        self.openai_key = openai_api_key()
        self.slo_targets: dict[str, float] = SLOTargets().to_dict()

        self.state = default_state_dict()

        self.open_dc_buffer = SimulationResultsBuffer()

        self.producer: TimedKafkaProducer | None = None
        self.consumer: DigitalTwinConsumer | None = None
        self.opendc_runner = OpenDCRunner()
        self.optimizer = LLM(self.openai_key)

        self.stop_event = threading.Event()
        self.producer_thread: threading.Thread | None = None
        self.consumer_thread: threading.Thread | None = None

        self.topology_path = "/app/config/topology.json"
        self.last_topology_hash: str | None = None

        self.load_initial_topology()
        self.start_topology_watcher()

    def load_initial_topology(self) -> None:
        topo = loaders.read_topology(self.topology_path)
        if topo is not None:
            self.state["current_topology"] = topo
            self.last_topology_hash = topology_hash(topo)
            logger.info("📄 Loaded initial topology configuration")

    def start_topology_watcher(self) -> None:
        def _on_change(new_topology: dict[str, Any]) -> None:
            self.state["current_topology"] = new_topology
            self.state["topology_updates"] = (self.state.get("topology_updates") or 0) + 1

        self._watch_thread = threading.Thread(
            target=watch_topology_file,
            args=(self.topology_path, self.stop_event, _on_change),
            daemon=True,
        )
        self._watch_thread.start()

    def _topo_hash(self, topo: dict[str, Any] | None) -> str:
        return topology_hash(topo)

    def update_topology_file(self, new_topology: dict[str, Any]) -> bool:
        if not new_topology:
            return False
        new_hash = self._topo_hash(new_topology)
        if self.last_topology_hash == new_hash:
            logger.info("↩️ Skipping apply: topology identical to current (no-op)")
            return False
        try:
            loaders.backup_topology(self.topology_path)
            loaders.write_topology(self.topology_path, new_topology)
            self.state["current_topology"] = new_topology
            self.state["topology_updates"] += 1
            self.last_topology_hash = new_hash
            logger.info(
                "✅ Applied new topology (update #%s)",
                self.state["topology_updates"],
            )
            return True
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Failed to update topology file: %s", exc)
            return False

    def start_system(self) -> None:
        logger.info("🚀 Starting OpenDT Digital Twin System")
        self.state["status"] = "starting"
        self.stop_event.clear()

        try:
            self.producer = TimedKafkaProducer(self.kafka_servers)
            self.consumer = DigitalTwinConsumer(self.kafka_servers, "OpenDT_telemetry")

            self.consumer_thread = threading.Thread(target=self.run_consumer, daemon=False)
            self.consumer_thread.start()

            time.sleep(5)

            self.producer_thread = threading.Thread(target=self.run_producer, daemon=False)
            self.producer_thread.start()

            self.state["status"] = "running"
            logger.info("✅ System started successfully")
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Failed to start system: %s", exc)
            self.state["status"] = "error"

    def stop_system(self) -> None:
        logger.info("🛑 Stopping OpenDT Digital Twin System")
        self.state["status"] = "stopping"
        self.stop_event.set()

        if self.producer:
            self.producer.stop()

        if self.consumer:
            self.consumer.stop()

        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=5)

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

        self.state["status"] = "stopped"
        logger.info("✅ System stopped")

    def run_producer(self) -> None:
        try:
            logger.info("📡 Starting timed telemetry producer...")
            stats = self.producer.stream_parquet_data_timed(
                tasks_file="/../app/data/tasks.parquet",
                fragments_file="/../app/data/fragments.parquet",
            )
            self.state["total_tasks"] = stats["total_tasks"]
            self.state["total_fragments"] = stats["total_fragments"]
            logger.info("✅ Producer finished: %s", stats)
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Producer error: %s", exc)

    def _score(self, sim_results: dict[str, Any]) -> float:
        energy = sim_results.get("energy_kwh", 0.0) or 0.0
        runtime = sim_results.get("runtime_hours", 0.0) or 0.0

        energy_target = self.slo_targets.get("energy_target", 10.0)
        runtime_target = self.slo_targets.get("runtime_target", 2.0)

        energy_score = min(100, max(0, (energy - energy_target) / energy_target * 100))
        runtime_score = min(100, max(0, (runtime - runtime_target) / runtime_target * 100))

        energy_weight = 0.6
        runtime_weight = 0.4

        final_score = (energy_score * energy_weight) + (runtime_score * runtime_weight)
        return round(100 - final_score, 2)

    def _append_simulation_result(self, result: dict[str, Any], timestamp: str) -> None:
        with self.open_dc_buffer.lock:
            self.open_dc_buffer.results.append(result)
            self.open_dc_buffer.timestamps.append(timestamp)

    def _simulation_timestamps(self) -> list[str]:
        with self.open_dc_buffer.lock:
            return list(self.open_dc_buffer.timestamps)

    def _simulation_results(self) -> list[dict[str, Any]]:
        with self.open_dc_buffer.lock:
            return list(self.open_dc_buffer.results)

    def run_consumer(self) -> None:
        try:
            logger.info("📥 Starting digital twin consumer...")
            for cycle, batch_data in enumerate(self.consumer.process_windows(), start=1):
                if self.stop_event.is_set():
                    break

                self.state["cycle_count"] = cycle
                self.state["current_window"] = batch_data.get("window_info", "Processing...")
                logger.info("🔄 Processing cycle %s", cycle)

                baseline = self.run_simulation(batch_data)

                timestamp = batch_data["window_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
                self._append_simulation_result(baseline, timestamp)

                self.state["last_simulation"] = baseline
                baseline_score = self._score(baseline)

                self.state.update(
                    {
                        "window_baseline_score": round(baseline_score, 3),
                        "window_best_score": round(baseline_score, 3),
                        "window_trials": 0,
                        "window_accepted": False,
                    }
                )

                best_topology = self.state.get("current_topology")
                best_score = baseline_score
                seen = {self._topo_hash(best_topology) if best_topology else ""}
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

                    proposed = opt.get("new_topology")
                    if not proposed:
                        continue
                    topo_hash = self._topo_hash(proposed)
                    if topo_hash in seen:
                        continue
                    seen.add(topo_hash)

                    probe = self.opendc_runner.run_simulation(
                        tasks_data=batch_data.get("tasks_sample", []),
                        fragments_data=batch_data.get("fragments_sample", []),
                        topology_data=proposed,
                        expName=f"window_{cycle}_try_{tries}",
                    )

                    self.state["last_optimization"] = opt
                    self.state["last_optimization"]["energy_kwh"] = probe.get("energy_kwh", None)
                    self.state["last_optimization"]["runtime_hours"] = probe.get("runtime_hours", None)
                    self.state["last_optimization"]["cpu_utilization"] = probe.get("cpu_utilization", None)
                    self.state["last_optimization"]["max_power_draw"] = probe.get("max_power_draw", None)

                    self.state["cycle_count_opt"] += 1
                    score = self._score(probe)
                    if score < best_score - IMPROVEMENT_DELTA:
                        best_topology, best_score = proposed, score
                        self.state["window_best_score"] = round(best_score, 3)

                    self.state["window_trials"] = tries

                self.state["best_config"] = {
                    "config": best_topology,
                    "score": round(best_score, 3),
                }

                if self.stop_event.wait(0.1):
                    break
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.exception("Consumer error: %s", exc)

    def run_simulation(self, batch_data: dict[str, Any], expName: str = "simple") -> dict[str, Any]:
        logger.info("🔄 Running OpenDC simulation...")
        tasks_data = batch_data.get("tasks_sample", [])
        fragments_data = batch_data.get("fragments_sample", [])
        topology_data = self.state.get("current_topology")
        results = self.opendc_runner.run_simulation(
            tasks_data=tasks_data,
            fragments_data=fragments_data,
            topology_data=topology_data,
            expName=expName,
        )
        logger.info("📊 Simulation Results: %s", results)
        return results

    def simulation_timeseries(self) -> dict[str, Any]:
        results = self._simulation_results()
        timestamps = self._simulation_timestamps()
        cpu_usages = [res.get("cpu_utilization") for res in results]
        power_usages = [res.get("energy_kwh") for res in results]
        return {
            "cpu_usages": cpu_usages,
            "power_usages": power_usages,
            "timestamps": deepcopy(timestamps),
        }
