#!/usr/bin/env python3
import asyncio
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import os

# Add the kafka manager to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'opendt_kafka', 'src', 'main', 'python'))

from kafka_manager import OpenDTKafkaManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TelemetryData:
    """Enhanced telemetry data structure"""
    timestamp: float
    tasks: List[Dict]
    fragments: List[Dict]
    metrics: Dict[str, float]


class EnhancedKafkaHandler:
    """Enhanced Kafka operations for OpenDT"""

    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.manager = OpenDTKafkaManager(bootstrap_servers)

    def setup_topics(self):
        """Setup all required Kafka topics"""
        logger.info("🔧 Setting up Kafka topics...")
        self.manager.setup_kafka_cluster()

    def start_telemetry_streaming(self, tasks_path: str, fragments_path: str):
        """Start Spark-based telemetry streaming"""
        logger.info("📡 Starting telemetry streaming...")

        # Build Scala jar first
        build_result = subprocess.run(
            ["sbt", "assembly"],
            cwd="opendt_kafka/",
            capture_output=True,
            text=True
        )

        if build_result.returncode != 0:
            logger.error(f"Scala build failed: {build_result.stderr}")
            raise Exception("Failed to build Kafka streaming jar")

        # Start Spark streaming job
        cmd = [
            f"{self._get_spark_home()}/bin/spark-submit",
            "--class", "opendt.TelemetrySim",
            "--master", "local[4]",
            "--conf", "spark.sql.adaptive.enabled=true",
            "opendt_kafka/target/scala-2.12/opendt-kafka-assembly-0.1.0.jar",
            tasks_path,
            fragments_path
        ]

        logger.info(f"🚀 Executing: {' '.join(cmd)}")
        return subprocess.Popen(cmd)

    def _get_spark_home(self) -> str:
        """Get Spark installation path"""
        import os
        return os.environ.get('SPARK_HOME', '/opt/spark')


class DigitalTwinOrchestrator:
    """Main orchestration system for OpenDT"""

    def __init__(self, config: Dict):
        self.config = config
        self.kafka = EnhancedKafkaHandler(config["kafka_servers"])
        self.running = False
        self.telemetry_process = None

    async def start(self):
        """Start the complete digital twin system"""
        logger.info("🚀 Starting OpenDT Digital Twin Orchestrator")

        try:
            # 1. Setup Kafka infrastructure
            self.kafka.setup_topics()

            # 2. Start telemetry streaming
            self.telemetry_process = self.kafka.start_telemetry_streaming(
                self.config["tasks_path"],
                self.config["fragments_path"]
            )

            # 3. Start orchestration loop
            self.running = True
            await self._orchestration_loop()

        except Exception as e:
            logger.error(f"❌ Orchestrator startup failed: {e}")
            raise
        finally:
            await self._cleanup()

    async def _orchestration_loop(self):
        """Main digital twin orchestration loop"""
        cycle_count = 0

        while self.running:
            cycle_start = time.time()
            logger.info(f"🔄 Starting orchestration cycle {cycle_count + 1}")

            try:
                # 1. Collect and analyze telemetry
                telemetry_data = await self._collect_telemetry_window()

                # 2. Run OpenDC simulation if we have data
                if telemetry_data.tasks or telemetry_data.fragments:
                    sim_results = await self._run_simulation(telemetry_data)

                    # 3. Get LLM topology recommendations
                    recommendations = await self._get_topology_recommendations(sim_results)

                    # 4. Apply recommendations
                    await self._apply_recommendations(recommendations)
                else:
                    logger.info("ℹ️  No telemetry data available yet")

                # 5. Wait for next cycle
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, self.config["cycle_interval_seconds"] - cycle_duration)

                logger.info(
                    f"⏱️  Cycle {cycle_count + 1} completed in {cycle_duration:.2f}s, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)

                cycle_count += 1

            except Exception as e:
                logger.error(f"❌ Orchestration cycle failed: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def _collect_telemetry_window(self) -> TelemetryData:
        """Collect telemetry data from the current window"""
        return TelemetryData(
            timestamp=time.time(),
            tasks=[],
            fragments=[],
            metrics={"cpu_utilization": 0.75, "memory_usage": 0.60}
        )

    async def _run_simulation(self, telemetry: TelemetryData) -> Dict:
        """Run OpenDC simulation with current telemetry"""
        logger.info("🔬 Running OpenDC simulation...")

        # Mock simulation results for now
        return {
            "energy_kwh": 1.25,
            "max_power_draw": 450.0,
            "avg_cpu_utilization": 0.78,
            "simulation_time": telemetry.timestamp
        }

    async def _get_topology_recommendations(self, sim_results: Dict) -> Dict:
        """Get LLM-based topology recommendations"""
        logger.info("🤖 Getting LLM topology recommendations...")

        # Mock recommendations for now
        return {
            "action": "scale_up",
            "target_hosts": 4,
            "reason": "High CPU utilization detected"
        }

    async def _apply_recommendations(self, recommendations: Dict):
        """Apply topology recommendations"""
        if not recommendations:
            logger.info("ℹ️  No recommendations to apply")
            return

        logger.info(f"💡 Applying recommendations: {recommendations}")

        # Send recommendations to Kafka
        if hasattr(self.kafka.manager, 'producer') and self.kafka.manager.producer:
            try:
                self.kafka.manager.producer.send(
                    'topology_updates',
                    key={'timestamp': int(time.time())},
                    value=recommendations
                )
                logger.info("📤 Sent topology update to Kafka")
            except Exception as e:
                logger.error(f"Failed to send topology update: {e}")

    async def _cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up orchestrator...")
        self.running = False

        if self.telemetry_process:
            self.telemetry_process.terminate()
            try:
                self.telemetry_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.telemetry_process.kill()

        logger.info("✅ Cleanup complete")


async def main():
    """Main entry point"""
    config = {
        "kafka_servers": "localhost:9092",
        "opendc_path": "./opendt_llm/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
        "tasks_path": "../_old/data/workload_traces/surf_new/tasks.parquet",
        "fragments_path": "../_old/data/workload_traces/surf_new/fragments.parquet",
        "topology_path": "../_old/data/topologies/datacenter.json",
        "workload_path": "../_old/data/workload_traces/surf_new",
        "cycle_interval_seconds": 300  # 5 minutes
    }

    orchestrator = DigitalTwinOrchestrator(config)
    await orchestrator.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down gracefully...")
