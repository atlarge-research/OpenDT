"""Wrapper around the OpenDC experiment runner binary."""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow.parquet as pq

from .adapters import ensure_workload_dir, fragments_to_table, tasks_to_table

logger = logging.getLogger(__name__)


class OpenDCRunner:
    """OpenDC ExperimentRunner with comprehensive path detection and diagnostics."""

    def __init__(self) -> None:
        possible_paths = [
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "./opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
        ]

        self.opendc_path: str | None = None

        logger.info("🔍 Searching for OpenDC runner...")
        for path in possible_paths:
            candidate = Path(path)
            logger.info("Checking: %s", path)
            logger.info("  - Exists: %s", candidate.exists())
            if not candidate.exists():
                continue

            if candidate.is_file():
                if os.access(path, os.X_OK):
                    self.opendc_path = path
                    logger.info("✅ Found executable OpenDC runner: %s", path)
                    break
                logger.warning("⚠️ OpenDC found but not executable, fixing perms: %s", path)
                try:
                    os.chmod(path, 0o755)
                    if os.access(path, os.X_OK):
                        self.opendc_path = path
                        logger.info("✅ Fixed permissions for OpenDC runner: %s", path)
                        break
                except Exception as exc:  # pragma: no cover - defensive logging path
                    logger.error("❌ Failed to chmod OpenDC runner: %s", exc)

        logger.info("📁 Directory structure:")
        for base in ["/app/opendt-simulator", "/app/opendc"]:
            if Path(base).exists():
                logger.info("Contents of %s:", base)
                try:
                    for item in Path(base).rglob("*OpenDC*"):
                        if item.is_file():
                            size = item.stat().st_size
                            perms = oct(item.stat().st_mode)[-3:]
                            execb = os.access(str(item), os.X_OK)
                            logger.info("  📄 %s [%s bytes, %s, exec: %s]", item, size, perms, execb)
                except Exception as exc:  # pragma: no cover - defensive logging path
                    logger.error("Error listing %s: %s", base, exc)

        self.base_experiment = {
            "name": "opendt-simulation",
            "exportModels": [
                {
                    "exportInterval": 150,
                    "filesToExport": ["powerSource", "host", "task", "service"],
                    "computeExportConfig": {
                        "powerSourceExportColumns": ["energy_usage", "power_draw"]
                    },
                }
            ],
        }

    def create_workload(
        self,
        tasks_data: Iterable[Mapping[str, Any]] | None,
        fragments_data: Iterable[Mapping[str, Any]] | None,
    ) -> str:
        workload_dir = ensure_workload_dir()

        if tasks_data:
            tasks_table = tasks_to_table(tasks_data)
            pq.write_table(tasks_table, workload_dir / "tasks.parquet")
            logger.info("📄 Created tasks.parquet with %s tasks", tasks_table.num_rows)

        if fragments_data:
            frags_table = fragments_to_table(fragments_data)
            pq.write_table(frags_table, workload_dir / "fragments.parquet")
            logger.info("📄 Created fragments.parquet with %s fragments", frags_table.num_rows)

        return str(workload_dir)

    def run_simulation(
        self,
        tasks_data: Iterable[Mapping[str, Any]] | None,
        fragments_data: Iterable[Mapping[str, Any]] | None,
        topology_data: Mapping[str, Any] | None,
        expName: str = "simple",
    ) -> dict[str, Any]:
        if not self.opendc_path:
            return self._error_result("OpenDC runner not found or not executable")

        try:
            workload_path = self.create_workload(tasks_data, fragments_data)

            topology_file = Path("/tmp/topology.json")
            topology_file.write_text(json.dumps(topology_data, indent=2))
            logger.info("📄 Created topology: %s", topology_file)

            experiment = dict(self.base_experiment)
            experiment["name"] = expName
            experiment.update(
                {
                    "topologies": [{"pathToFile": str(topology_file)}],
                    "workloads": [{"pathToFile": workload_path, "type": "ComputeWorkload"}],
                }
            )
            experiment_file = Path("/tmp/experiment.json")
            experiment_file.write_text(json.dumps(experiment, indent=2))
            logger.info("📄 Created experiment: %s", experiment_file)

            logger.info("🚀 Running OpenDC simulation: %s", self.opendc_path)
            env = os.environ.copy()
            env.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")

            if not os.access(self.opendc_path, os.X_OK):
                logger.error("❌ OpenDC runner is not executable: %s", self.opendc_path)
                return self._error_result(
                    f"OpenDC runner permissions issue: {self.opendc_path}"
                )

            result = subprocess.run(
                [self.opendc_path, "--experiment-path", str(experiment_file)],
                capture_output=True,
                text=True,
                timeout=120,
                env=env,
            )
            logger.info("OpenDC return code: %s", result.returncode)
            if result.stdout:
                logger.info("OpenDC stdout: %s", result.stdout)
            if result.stderr:
                logger.info("OpenDC stderr: %s", result.stderr)

            if result.returncode != 0:
                return self._error_result(f"OpenDC failed (code {result.returncode})")

            logger.info("✅ OpenDC simulation completed successfully")
            return self.parse_opendc_results()

        except subprocess.TimeoutExpired:
            return self._error_result("OpenDC simulation timed out")
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("OpenDC execution failed: %s", exc)
            return self._error_result(f"OpenDC exec error: {exc}")

    def parse_opendc_results(self) -> dict[str, Any]:
        try:
            output_dirs = [
                Path("output/opendt-simulation/raw-output/0/seed=0"),
                Path("./output/simple/raw-output/0/seed=0"),
                Path("/tmp/output"),
                Path(os.environ.get("OPENDT_SIM_DIR") or "/app/output/opendt-simulation/raw-output"),
            ]

            power_df = host_df = service_df = None
            for odir in output_dirs:
                if not odir.exists():
                    continue
                pfile = odir / "powerSource.parquet"
                hfile = odir / "host.parquet"
                sfile = odir / "service.parquet"
                if pfile.exists():
                    power_df = pd.read_parquet(pfile)
                if hfile.exists():
                    host_df = pd.read_parquet(hfile)
                if sfile.exists():
                    service_df = pd.read_parquet(sfile)
                if power_df is not None or host_df is not None:
                    break

            if power_df is not None and len(power_df) > 0:
                energy_kwh = power_df["energy_usage"].sum() / 3_600_000
                max_power = float(power_df["power_draw"].max())
            else:
                energy_kwh, max_power = 0.0, 0.0

            if host_df is not None and len(host_df) > 0 and "cpu_utilization" in host_df.columns:
                cpu_util = float(host_df["cpu_utilization"].mean())
            else:
                cpu_util = 0.0

            if service_df is not None and len(service_df) > 0 and "timestamp" in service_df.columns:
                runtime_ms = service_df["timestamp"].max() - service_df["timestamp"].min()
                runtime_hours = float(runtime_ms) / (1000 * 3600)
            else:
                runtime_hours = 0.0

            return {
                "energy_kwh": round(float(energy_kwh), 4),
                "cpu_utilization": round(float(cpu_util), 3),
                "max_power_draw": round(float(max_power), 1),
                "runtime_hours": round(float(runtime_hours), 2),
                "status": "success",
            }
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Failed to parse OpenDC results: %s", exc)
            return {
                "energy_kwh": 0.0,
                "cpu_utilization": 0.0,
                "max_power_draw": 0.0,
                "runtime_hours": 0.0,
                "status": "error",
            }

    def _error_result(self, reason: str) -> dict[str, Any]:
        """Return a uniform error payload for failed simulations."""

        logger.error("Simulation failed: %s", reason)
        return {
            "energy_kwh": 0.0,
            "cpu_utilization": 0.0,
            "max_power_draw": 0.0,
            "runtime_hours": 0.0,
            "status": "error",
            "reason": reason,
        }
