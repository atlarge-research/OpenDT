#!/usr/bin/env python3
"""Complete digital twin system runner"""
import asyncio
import subprocess
import json
import time
import sys
import os
from pathlib import Path


class DigitalTwinRunner:
    def __init__(self, config_path="config.json"):
        self.processes = {}

        # Find config file
        cfg = Path(config_path)
        if not cfg.is_file():
            cfg = Path(__file__).parent / "config.json"

        with open(cfg) as f:
            self.config = json.load(f)

        # Set OpenAI API key if available
        if "OPENAI_API_KEY" in os.environ:
            self.openai_api_key = os.environ["OPENAI_API_KEY"]
        else:
            print("⚠️ OPENAI_API_KEY not set, LLM optimization will be limited")
            self.openai_api_key = None

    async def start_system(self):
        """Start complete digital twin system"""
        print("🚀 Starting OpenDT Digital Twin System")
        print("=" * 50)

        try:
            # 1. System health check
            self._system_health_check()

            # 2. Initialize Kafka
            self._initialize_kafka_system()

            # 3. Start telemetry producer (background)
            self._start_telemetry_producer()

            # 4. Wait for initial data
            print("⏱️ Waiting for telemetry data (10s)...")
            await asyncio.sleep(10)

            # 5. Start orchestration loop
            await self._orchestration_loop()

        except KeyboardInterrupt:
            print("\n🛑 Shutting down system...")
            await self._cleanup()
        except Exception as e:
            print(f"❌ System startup failed: {e}")
            await self._cleanup()

    def _system_health_check(self):
        """Comprehensive system health check"""
        print("🔍 System health check...")

        # Check Kafka
        try:
            result = subprocess.run(['kafka-topics', '--list', '--bootstrap-server',
                                     self.config['kafka_bootstrap_servers']],
                                    capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                raise Exception("Kafka not responding")
            print("✅ Kafka: Running")
        except Exception as e:
            print(f"❌ Kafka: {e}")
            print("💡 Start Kafka with: brew services start kafka")
            raise

        # Check Python dependencies
        try:
            import pandas, kafka
            print("✅ Dependencies: Available")
        except ImportError as e:
            print(f"❌ Missing dependency: {e}")
            raise

        # Check OpenDC runner
        opendc_path = Path(self.config.get('opendc_runner_path', ''))
        if opendc_path.exists():
            print("✅ OpenDC: Runner found")
        else:
            print("⚠️ OpenDC: Runner not found, using mock simulation")

    def _initialize_kafka_system(self):
        """Initialize complete Kafka system"""
        print("🔧 Initializing Kafka system...")

        # Use the kafka admin to setup everything
        try:
            admin_script = Path("opendt-streaming/src/main/python/kafka_admin.py")
            if admin_script.exists():
                result = subprocess.run([sys.executable, str(admin_script), "setup"],
                                        capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    print("✅ Kafka system initialized")
                else:
                    print(f"⚠️ Kafka setup warning: {result.stderr}")
            else:
                print("⚠️ Kafka admin script not found, using basic setup")
                self._basic_kafka_setup()
        except Exception as e:
            print(f"⚠️ Kafka initialization error: {e}")
            self._basic_kafka_setup()

    def _basic_kafka_setup(self):
        """Basic Kafka topic setup"""
        topics = ['tasks', 'fragments', 'topology_updates', 'simulation_results']
        for topic in topics:
            subprocess.run([
                'kafka-topics', '--create', '--topic', topic,
                '--bootstrap-server', self.config['kafka_bootstrap_servers'],
                '--partitions', '3', '--replication-factor', '1'
            ], capture_output=True)
        print("✅ Basic Kafka topics created")

    def _start_telemetry_producer(self):
        """Start telemetry data producer in background"""
        producer_script = Path("opendt-streaming/src/main/python/kafka_producer.py")

        if producer_script.exists():
            cmd = [sys.executable, str(producer_script)]
            self.processes['producer'] = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            print("✅ Telemetry producer started")
        else:
            print("⚠️ Producer script not found")

    async def _orchestration_loop(self):
        """Main orchestration loop with enhanced error handling"""
        cycle = 0
        experiment_history = []
        current_topology_path = Path(self.config.get('default_topology',
                                                     'resources/topologies/small_datacenter.json'))

        # Ensure topology file exists
        if not current_topology_path.exists():
            self._create_default_topology(current_topology_path)

        print("🔄 Starting orchestration cycles...")
        print(f"   Cycle interval: {self.config['cycle_interval_seconds']}s")
        print(f"   Topology file: {current_topology_path}")

        while True:
            cycle += 1
            cycle_start = time.time()

            print(f"\n{'=' * 20} CYCLE {cycle} {'=' * 20}")

            try:
                # 1. Run OpenDC simulation
                sim_results = self._run_opendc_simulation(current_topology_path)

                # 2. Get LLM recommendations
                recommendations = await self._get_llm_recommendations(
                    sim_results, current_topology_path, experiment_history
                )

                # 3. Apply recommendations
                if recommendations.get('success'):
                    await self._apply_topology_updates(recommendations, current_topology_path)

                # 4. Record results
                experiment_record = {
                    'cycle': cycle,
                    'timestamp': time.time(),
                    'simulation_results': sim_results,
                    'recommendations': recommendations
                }
                experiment_history.append(experiment_record)

                # Keep only last 10 cycles
                if len(experiment_history) > 10:
                    experiment_history = experiment_history[-10:]

                # 5. Cycle timing
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, self.config['cycle_interval_seconds'] - cycle_duration)

                print(f"⏱️ Cycle {cycle} completed in {cycle_duration:.1f}s")
                if sleep_time > 0:
                    print(f"😴 Sleeping for {sleep_time:.1f}s...")
                    await asyncio.sleep(sleep_time)

            except Exception as e:
                print(f"❌ Cycle {cycle} error: {e}")
                print("⏳ Waiting 60s before retry...")
                await asyncio.sleep(60)

    def _run_opendc_simulation(self, topology_path: Path) -> dict:
        """Run OpenDC simulation with current topology"""
        print("🔬 Running OpenDC simulation...")

        # Create experiment configuration
        experiment_config = {
            "name": f"digital_twin_{int(time.time())}",
            "topologies": [{"pathToFile": str(topology_path)}],
            "workloads": [{"pathToFile": self.config["workload_traces"], "type": "ComputeWorkload"}],
            "exportModels": [{"exportInterval": 150, "filesToExport": ["host", "powerSource"]}]
        }

        # Write experiment file
        exp_file = Path("opendt-simulator/experiments/current_experiment.json")
        exp_file.parent.mkdir(parents=True, exist_ok=True)

        with open(exp_file, 'w') as f:
            json.dump(experiment_config, f, indent=2)

        # Try to run real OpenDC simulation
        opendc_runner = Path(self.config.get('opendc_runner_path', ''))

        if opendc_runner.exists():
            try:
                result = subprocess.run([
                    str(opendc_runner),
                    "--experiment-path", str(exp_file)
                ], capture_output=True, text=True, timeout=self.config.get('simulation_timeout', 300))

                if result.returncode == 0:
                    print("✅ OpenDC simulation completed successfully")
                    # TODO: Parse actual results from output files
                    return self._parse_simulation_results()
                else:
                    print(f"⚠️ OpenDC simulation warning: {result.stderr}")
                    return self._mock_simulation_results()

            except subprocess.TimeoutExpired:
                print("⏰ OpenDC simulation timeout, using mock results")
                return self._mock_simulation_results()
            except Exception as e:
                print(f"❌ OpenDC simulation error: {e}")
                return self._mock_simulation_results()
        else:
            print("📋 Using mock simulation results")
            return self._mock_simulation_results()

    def _mock_simulation_results(self) -> dict:
        """Generate mock simulation results"""
        import random
        return {
            "energy_kwh": round(random.uniform(0.8, 2.5), 2),
            "runtime_hours": round(random.uniform(12, 48), 1),
            "cpu_utilization": round(random.uniform(0.3, 0.9), 2),
            "max_power_draw": round(random.uniform(200, 500), 1),
            "timestamp": time.time(),
            "mock": True
        }

    def _parse_simulation_results(self) -> dict:
        """Parse actual OpenDC simulation results"""
        # TODO: Implement parsing of OpenDC output files
        return self._mock_simulation_results()

    async def _get_llm_recommendations(self, sim_results: dict, topology_path: Path,
                                       history: list) -> dict:
        """Get LLM-based topology recommendations"""
        print("🤖 Getting LLM recommendations...")

        if not self.openai_api_key:
            print("⚠️ No OpenAI API key, using fallback recommendations")
            return self._get_fallback_recommendations(sim_results)

        try:
            llm_script = Path("opendt-intelligence/llm_optimizer.py")

            if llm_script.exists():
                # Prepare arguments
                cmd = [
                    sys.executable, str(llm_script),
                    '--simulation-results', json.dumps(sim_results),
                    '--topology', str(topology_path),
                    '--api-key', self.openai_api_key
                ]

                # Run LLM optimizer
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

                if result.returncode == 0:
                    recommendations = json.loads(result.stdout)
                    print("✅ LLM recommendations received")
                    return recommendations
                else:
                    print(f"⚠️ LLM optimization failed: {result.stderr}")
                    return self._get_fallback_recommendations(sim_results)

            else:
                print("⚠️ LLM optimizer script not found")
                return self._get_fallback_recommendations(sim_results)

        except Exception as e:
            print(f"❌ LLM recommendation error: {e}")
            return self._get_fallback_recommendations(sim_results)

    def _get_fallback_recommendations(self, sim_results: dict) -> dict:
        """Provide fallback recommendations when LLM is not available"""
        energy = sim_results.get('energy_kwh', 1.0)
        cpu_util = sim_results.get('cpu_utilization', 0.5)

        if energy > 2.0:
            action = "reduce_power"
            reason = "High energy consumption detected"
        elif cpu_util > 0.85:
            action = "scale_up"
            reason = "High CPU utilization"
        elif cpu_util < 0.3:
            action = "consolidate"
            reason = "Low resource utilization"
        else:
            action = "maintain"
            reason = "Configuration appears optimal"

        return {
            "success": True,
            "analysis": {"energy_efficiency": "needs_attention" if energy > 2.0 else "good"},
            "recommendations": [{
                "action": action,
                "reason": reason,
                "priority": "medium",
                "fallback": True
            }]
        }

    async def _apply_topology_updates(self, recommendations: dict, topology_path: Path):
        """Apply topology recommendations"""
        print("💡 Applying recommendations...")

        if recommendations.get('optimized_topology'):
            # Save optimized topology
            with open(topology_path, 'w') as f:
                json.dump(recommendations['optimized_topology'], f, indent=2)
            print("✅ Topology updated with LLM recommendations")

        # Send update notification to Kafka
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=self.config['kafka_bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode()
            )

            producer.send('topology_updates', value={
                "timestamp": time.time(),
                "type": "llm_optimization",
                "recommendations": recommendations.get('llm_recommendations', {}),
                "analysis": recommendations.get('analysis', {})
            })
            producer.flush()
            producer.close()
            print("📤 Sent topology update to Kafka")

        except Exception as e:
            print(f"⚠️ Failed to send Kafka update: {e}")

    def _create_default_topology(self, topology_path: Path):
        """Create default topology file"""
        default_topology = {
            "clusters": [
                {
                    "name": "C01",
                    "hosts": [
                        {
                            "name": "H01",
                            "count": 2,
                            "cpu": {
                                "coreCount": 16,
                                "coreSpeed": 2400
                            },
                            "memory": {
                                "memorySize": 34359738368
                            }
                        }
                    ]
                }
            ]
        }

        topology_path.parent.mkdir(parents=True, exist_ok=True)
        with open(topology_path, 'w') as f:
            json.dump(default_topology, f, indent=2)
        print(f"✅ Created default topology: {topology_path}")

    async def _cleanup(self):
        """Cleanup all processes"""
        print("🧹 Cleaning up system...")

        for name, process in self.processes.items():
            if process and process.poll() is None:
                print(f"🛑 Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=5)
                    print(f"✅ Stopped {name}")
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"🔪 Killed {name}")

        print("✅ Cleanup complete")


async def main():
    """Main entry point"""
    runner = DigitalTwinRunner()
    await runner.start_system()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
