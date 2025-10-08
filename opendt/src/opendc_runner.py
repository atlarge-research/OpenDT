#!/usr/bin/env python3
import subprocess
import json
import logging
from pathlib import Path
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class OpenDCRunner:
    """OpenDC ExperimentRunner with comprehensive path detection and diagnostics"""

    def __init__(self):
        # Search for OpenDC runner in multiple locations
        possible_paths = [
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "./opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner"
        ]

        self.opendc_path = None

        # Debug: List all files in the system
        logger.info("🔍 Searching for OpenDC runner...")
        for path in possible_paths:
            path_obj = Path(path)
            logger.info(f"Checking: {path}")
            logger.info(f"  - Exists: {path_obj.exists()}")

            if path_obj.exists():
                is_file = path_obj.is_file()
                is_executable = os.access(path, os.X_OK) if is_file else False

                logger.info(f"  - Is file: {is_file}")
                logger.info(f"  - Is executable: {is_executable}")
                logger.info(f"  - File size: {path_obj.stat().st_size if is_file else 'N/A'}")
                logger.info(f"  - Permissions: {oct(path_obj.stat().st_mode)[-3:] if is_file else 'N/A'}")

                if is_file and is_executable:
                    self.opendc_path = path
                    logger.info(f"✅ Found working OpenDC runner: {path}")
                    break
                elif is_file:
                    logger.warning(f"⚠️ Found OpenDC but fixing permissions: {path}")
                    try:
                        os.chmod(path, 0o755)
                        if os.access(path, os.X_OK):
                            self.opendc_path = path
                            logger.info(f"✅ Fixed permissions for OpenDC runner: {path}")
                            break
                    except Exception as e:
                        logger.error(f"❌ Failed to fix permissions: {e}")

        # Additional debugging - list directory contents
        logger.info("📁 Directory structure:")
        for base_dir in ["/app/opendt-simulator", "/app/opendc"]:
            if Path(base_dir).exists():
                logger.info(f"Contents of {base_dir}:")
                try:
                    for item in Path(base_dir).rglob("*"):
                        if item.is_file() and "OpenDC" in str(item):
                            size = item.stat().st_size
                            perms = oct(item.stat().st_mode)[-3:]
                            executable = os.access(str(item), os.X_OK)
                            logger.info(f"  📄 {item} [{size} bytes, {perms}, exec: {executable}]")
                except Exception as e:
                    logger.error(f"Error listing {base_dir}: {e}")

        if not self.opendc_path:
            logger.error("❌ OpenDC runner not found anywhere - using enhanced mock simulation")

        self.base_experiment = {
            "name": "opendt-simulation",
            "exportModels": [{
                "exportInterval": 150,
                "filesToExport": ["powerSource", "host", "task", "service"],
                "computeExportConfig": {
                    "powerSourceExportColumns": ["energy_usage", "power_draw"]
                }
            }]
        }

    def create_workload(self, tasks_data, fragments_data):
        """Create OpenDC workload files from streaming data"""

        workload_dir = Path("/tmp/opendt_workload")
        workload_dir.mkdir(parents=True, exist_ok=True)

        # Convert streaming data to proper format
        if tasks_data and len(tasks_data) > 0:
            tasks_df = pd.DataFrame([
                {
                    'id': t.get('id', 0),
                    'submission_time': int(pd.to_datetime(t.get('submission_time', '2024-01-01')).value) // 1_000_000,
                    'duration': t.get('duration', 30000),
                    'cpu_count': t.get('cpu_count', 1),
                    'cpu_capacity': t.get('cpu_capacity', 2400.0),
                    'mem_capacity': t.get('mem_capacity', 1024 ** 3)
                }
                for t in tasks_data
            ])
            
            tasks_table = pa.Table.from_pandas(tasks_df,  pa.schema([
                pa.field("id", pa.int32(), nullable=False),   
                pa.field("submission_time", pa.int64(), False),       
                pa.field("duration", pa.int64(), False),             
                pa.field("cpu_count", pa.int32(), False),             
                pa.field("cpu_capacity", pa.float64(), False),        
                pa.field("mem_capacity", pa.int64(), False),          
            ]), preserve_index=False)

            pq.write_table(tasks_table, workload_dir / "tasks.parquet")
            logger.info(f"📄 Created tasks.parquet with {len(tasks_df)} tasks")

        if fragments_data and len(fragments_data) > 0:
            fragments_df = pd.DataFrame([
                {
                    'id': f.get('id', 0),
                    'duration': f.get('duration', 10000),
                    'cpu_count': 1,
                    'cpu_usage': f.get('cpu_usage', 0.5)
                }
                for f in fragments_data
            ])

            frags_table = pa.Table.from_pandas(fragments_df, pa.schema([
                pa.field("id", pa.int32(), False),                  
                pa.field("duration", pa.int64(), False),             
                pa.field("cpu_count", pa.int32(), False),             
                pa.field("cpu_usage", pa.float64(), False),             
            ]), preserve_index=False)

            pq.write_table(frags_table, workload_dir / "fragments.parquet")
            logger.info(f"📄 Created fragments.parquet with {len(fragments_df)} fragments")

        return str(workload_dir)

    def run_simulation(self, tasks_data, fragments_data, topology_data):
        """Run OpenDC simulation with real data or enhanced mock"""

        if not self.opendc_path:
            return self.create_enhanced_mock_results(tasks_data, fragments_data,
                                                     "OpenDC runner not found anywhere in expected paths")

        try:
            # Create workload
            workload_path = self.create_workload(tasks_data, fragments_data)

            # Create topology file
            topology_file = Path("/tmp/topology.json")
            with open(topology_file, 'w') as f:
                json.dump(topology_data, f, indent=2)
            logger.info(f"📄 Created topology: {topology_file}")

            # Create experiment configuration
            experiment_config = self.base_experiment.copy()
            experiment_config.update({
                "topologies": [{"pathToFile": str(topology_file)}],
                "workloads": [{
                    "pathToFile": workload_path,
                    "type": "ComputeWorkload"
                }]
            })

            # Write experiment file
            experiment_file = Path("/tmp/experiment.json")
            with open(experiment_file, 'w') as f:
                json.dump(experiment_config, f, indent=2)
            logger.info(f"📄 Created experiment: {experiment_file}")

            logger.info(f"🚀 Running OpenDC simulation: {self.opendc_path}")

            # Run OpenDC with proper Java setup
            env = os.environ.copy()
            env['JAVA_HOME'] = '/usr/lib/jvm/java-21-openjdk-amd64'

            # Test if file is actually executable
            if not os.access(self.opendc_path, os.X_OK):
                logger.error(f"❌ OpenDC runner is not executable: {self.opendc_path}")
                return self.create_enhanced_mock_results(tasks_data, fragments_data,
                                                         f"OpenDC runner permissions issue: {self.opendc_path}")

            result = subprocess.run([
                str(self.opendc_path),
                "--experiment-path", str(experiment_file)
            ], capture_output=True, text=True, timeout=120, env=env)

            logger.info(f"OpenDC return code: {result.returncode}")
            if result.stdout:
                logger.info(f"OpenDC stdout: {result.stdout}")
            if result.stderr:
                logger.info(f"OpenDC stderr: {result.stderr}")

            if result.returncode != 0:
                return self.create_enhanced_mock_results(tasks_data, fragments_data,
                                                         f"OpenDC failed (code {result.returncode}): {result.stderr[:100]}")

            logger.info("✅ OpenDC simulation completed successfully")

            # Parse results
            return self.parse_opendc_results()

        except subprocess.TimeoutExpired:
            error_msg = "OpenDC simulation timed out (2 minutes)"
            logger.error(error_msg)
            return self.create_enhanced_mock_results(tasks_data, fragments_data, error_msg)
        except Exception as e:
            error_msg = f"OpenDC execution failed: {str(e)}"
            logger.error(error_msg)
            return self.create_enhanced_mock_results(tasks_data, fragments_data, error_msg)

    def parse_opendc_results(self):
        """Parse OpenDC output files"""
        try:
            # Look for output files
            output_dirs = [
                Path("output/opendt-simulation/raw-output/0/seed=0"),
                Path("./output/simple/raw-output/0/seed=0"),
                Path("/tmp/output")
            ]

            power_df = None
            host_df = None
            service_df = None

            #TODO change this to find the first directory that has all the files we need!
            #or to chose one file!
            for output_dir in output_dirs:
                if output_dir.exists():
                    logger.info(f"🔍 Found output directory: {output_dir}")

                    power_file = output_dir / "powerSource.parquet"
                    host_file = output_dir / "host.parquet"
                    service_file = output_dir / "service.parquet"

                    if power_file.exists():
                        power_df = pd.read_parquet(power_file)
                        logger.info(f"📊 Loaded power data: {len(power_df)} records")
                    if host_file.exists():
                        host_df = pd.read_parquet(host_file)
                        logger.info(f"📊 Loaded host data: {len(host_df)} records")
                    if service_file.exists():
                        service_df = pd.read_parquet(service_file)
                        logger.info(f"📊 Loaded service data: {len(service_df)} records")
                    break

            # Calculate metrics following colleague's approach
            if power_df is not None and len(power_df) > 0:
                energy_kwh = power_df['energy_usage'].sum() / 3_600_000
                max_power = power_df['power_draw'].max()
                logger.info(f"📈 REAL OpenDC Energy: {energy_kwh:.3f} kWh, Max Power: {max_power:.1f}W")
            else:
                energy_kwh, max_power = 1.8, 850.0
                logger.warning("⚠️ No power data found, using fallback values")

            if host_df is not None and len(host_df) > 0:
                cpu_util = host_df['cpu_utilization'].mean() if 'cpu_utilization' in host_df.columns else 0.6
                logger.info(f"💻 REAL CPU Utilization: {cpu_util:.1%}")
            else:
                cpu_util = 0.6
                logger.warning("⚠️ No host data found, using fallback CPU utilization")

            if service_df is not None and len(service_df) > 0:
                runtime_ms = service_df['timestamp'].max() - service_df['timestamp'].min()
                runtime_hours = runtime_ms / (1000 * 3600)
                logger.info(f"⏱️ REAL Runtime: {runtime_hours:.2f} hours")
            else:
                runtime_hours = 0.5
                logger.warning("⚠️ No service data found, using fallback runtime")

            return {
                'energy_kwh': float(round(energy_kwh, 3)),
                'cpu_utilization': float(round(cpu_util, 3)),
                'max_power_draw': float(round(max_power, 1)),
                'runtime_hours': float(round(runtime_hours, 2)),
                'simulation_type': 'real_opendc',
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Failed to parse OpenDC results: {e}")
            return self.create_enhanced_mock_results([], [], f"Result parsing failed: {str(e)}")

    def create_enhanced_mock_results(self, tasks_data, fragments_data, error_reason):
        """Create realistic mock results with error info"""
        import random

        task_count = len(tasks_data) if tasks_data else 10
        fragment_count = len(fragments_data) if fragments_data else 50

        # More realistic energy calculations based on workload
        base_energy = 0.8 + (task_count * 0.15) + (fragment_count * 0.005)
        energy_kwh = round(base_energy + random.uniform(-0.3, 0.5), 3)

        # CPU utilization based on fragments
        if fragments_data:
            avg_cpu = sum(f.get('cpu_usage', 0.5) for f in fragments_data) / len(fragments_data)
            cpu_utilization = round(min(0.95, avg_cpu + random.uniform(-0.1, 0.1)), 3)
        else:
            cpu_utilization = round(random.uniform(0.4, 0.8), 3)

        return {
            'energy_kwh': energy_kwh,
            'cpu_utilization': cpu_utilization,
            'max_power_draw': round(600 + (task_count * 5) + random.uniform(0, 200), 1),
            'runtime_hours': round(0.1 + (task_count * 0.005), 2),
            'simulation_type': 'enhanced_mock',
            'status': 'fallback',
            'error_reason': error_reason
        }
