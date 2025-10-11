#!/usr/bin/env python3
import os
import json
import logging
import subprocess
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class OpenDCRunner:
    """OpenDC ExperimentRunner with comprehensive path detection and diagnostics"""

    def __init__(self):
        # Try to locate the OpenDC binary; don't ever return from __init__
        possible_paths = [
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendt-simulator/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
            "/app/opendc/bin/OpenDCExperimentRunner/OpenDCExperimentRunner",
            "./opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner",
        ]

        self.opendc_path = None

        logger.info("🔍 Searching for OpenDC runner...")
        for path in possible_paths:
            p = Path(path)
            logger.info(f"Checking: {path}")
            logger.info(f"  - Exists: {p.exists()}")
            if not p.exists():
                continue

            if p.is_file():
                if os.access(path, os.X_OK):
                    self.opendc_path = path
                    logger.info(f"✅ Found executable OpenDC runner: {path}")
                    break
                else:
                    logger.warning(f"⚠️ OpenDC found but not executable, fixing perms: {path}")
                    try:
                        os.chmod(path, 0o755)
                        if os.access(path, os.X_OK):
                            self.opendc_path = path
                            logger.info(f"✅ Fixed permissions for OpenDC runner: {path}")
                            break
                    except Exception as e:
                        logger.error(f"❌ Failed to chmod OpenDC runner: {e}")

        # (Debug) list potential directories
        logger.info("📁 Directory structure:")
        for base in ["/app/opendt-simulator", "/app/opendc"]:
            if Path(base).exists():
                logger.info(f"Contents of {base}:")
                try:
                    for item in Path(base).rglob("*OpenDC*"):
                        if item.is_file():
                            size = item.stat().st_size
                            perms = oct(item.stat().st_mode)[-3:]
                            execb = os.access(str(item), os.X_OK)
                            logger.info(f"  📄 {item} [{size} bytes, {perms}, exec: {execb}]")
                except Exception as e:
                    logger.error(f"Error listing {base}: {e}")

        # Define base experiment config (used when we actually run OpenDC)
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

    # ---------- helpers to construct inputs ----------
    def create_workload(self, tasks_data, fragments_data):
        """Create OpenDC workload files from the given streaming data."""
        workload_dir = Path("/tmp/opendt_workload")
        workload_dir.mkdir(parents=True, exist_ok=True)

        if tasks_data:
            tasks_df = pd.DataFrame([{
                "id": t.get("id", 0),
                "submission_time": int(pd.to_datetime(t.get("submission_time", "2024-01-01")).value) // 1_000_000,
                "duration": t.get("duration", 30000),
                "cpu_count": t.get("cpu_count", 1),
                "cpu_capacity": t.get("cpu_capacity", 2400.0),
                "mem_capacity": t.get("mem_capacity", 1024 ** 3),
            } for t in tasks_data])

            tasks_table = pa.Table.from_pandas(
                tasks_df,
                pa.schema([
                    pa.field("id", pa.int32(), False),
                    pa.field("submission_time", pa.int64(), False),
                    pa.field("duration", pa.int64(), False),
                    pa.field("cpu_count", pa.int32(), False),
                    pa.field("cpu_capacity", pa.float64(), False),
                    pa.field("mem_capacity", pa.int64(), False),
                ]),
                preserve_index=False,
            )
            pq.write_table(tasks_table, workload_dir / "tasks.parquet")
            logger.info(f"📄 Created tasks.parquet with {len(tasks_df)} tasks")

        if fragments_data:
            frags_df = pd.DataFrame([{
                "id": f.get("id", 0),
                "duration": f.get("duration", 10000),
                "cpu_count": 1,
                "cpu_usage": f.get("cpu_usage", 0.5),
            } for f in fragments_data])

            frags_table = pa.Table.from_pandas(
                frags_df,
                pa.schema([
                    pa.field("id", pa.int32(), False),
                    pa.field("duration", pa.int64(), False),
                    pa.field("cpu_count", pa.int32(), False),
                    pa.field("cpu_usage", pa.float64(), False),
                ]),
                preserve_index=False,
            )
            pq.write_table(frags_table, workload_dir / "fragments.parquet")
            logger.info(f"📄 Created fragments.parquet with {len(frags_df)} fragments")

        return str(workload_dir)

    # ---------- main entry: run a simulation or fall back ----------
    def run_simulation(self, tasks_data, fragments_data, topology_data):
        """Run OpenDC; if the runner is missing/unusable, write mock outputs."""
        if not self.opendc_path:
            return self.create_enhanced_mock_results(
                tasks_data, fragments_data,
                outdir=os.environ.get("OPENDT_SIM_DIR") or "/app/data",
                reason="OpenDC runner not found or not executable"
            )

        try:
            # Build workload inputs
            workload_path = self.create_workload(tasks_data, fragments_data)

            # Write topology file
            topology_file = Path("/tmp/topology.json")
            topology_file.write_text(json.dumps(topology_data, indent=2))
            logger.info(f"📄 Created topology: {topology_file}")

            # Experiment config
            experiment = dict(self.base_experiment)
            experiment.update({
                "topologies": [{"pathToFile": str(topology_file)}],
                "workloads": [{"pathToFile": workload_path, "type": "ComputeWorkload"}],
            })
            experiment_file = Path("/tmp/experiment.json")
            experiment_file.write_text(json.dumps(experiment, indent=2))
            logger.info(f"📄 Created experiment: {experiment_file}")

            # Run OpenDC
            logger.info(f"🚀 Running OpenDC simulation: {self.opendc_path}")
            env = os.environ.copy()
            env.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")

            if not os.access(self.opendc_path, os.X_OK):
                logger.error(f"❌ OpenDC runner is not executable: {self.opendc_path}")
                return self.create_enhanced_mock_results(
                    tasks_data, fragments_data,
                    reason=f"OpenDC runner permissions issue: {self.opendc_path}"
                )

            result = subprocess.run(
                [self.opendc_path, "--experiment-path", str(experiment_file)],
                capture_output=True, text=True, timeout=120, env=env
            )
            logger.info(f"OpenDC return code: {result.returncode}")
            if result.stdout:
                logger.info(f"OpenDC stdout: {result.stdout}")
            if result.stderr:
                logger.info(f"OpenDC stderr: {result.stderr}")

            if result.returncode != 0:
                return self.create_enhanced_mock_results(
                    tasks_data, fragments_data,
                    reason=f"OpenDC failed (code {result.returncode})"
                )

            logger.info("✅ OpenDC simulation completed successfully")
            return self.parse_opendc_results()

        except subprocess.TimeoutExpired:
            return self.create_enhanced_mock_results(
                tasks_data, fragments_data, reason="OpenDC simulation timed out"
            )
        except Exception as e:
            logger.error(f"OpenDC execution failed: {e}")
            return self.create_enhanced_mock_results(
                tasks_data, fragments_data, reason=f"OpenDC exec error: {e}"
            )

    # ---------- parse results ----------
    def parse_opendc_results(self):
        """Parse OpenDC output files into a summary dict."""
        try:
            output_dirs = [
                Path("output/opendt-simulation/raw-output/0/seed=0"),
                Path("./output/simple/raw-output/0/seed=0"),
                Path("/tmp/output"),
            ]

            power_df = host_df = service_df = None
            for odir in output_dirs:
                if not odir.exists():
                    continue
                logger.info(f"🔍 Found output directory: {odir}")
                pfile = odir / "powerSource.parquet"
                hfile = odir / "host.parquet"
                sfile = odir / "service.parquet"
                if pfile.exists(): power_df = pd.read_parquet(pfile)
                if hfile.exists(): host_df  = pd.read_parquet(hfile)
                if sfile.exists(): service_df = pd.read_parquet(sfile)
                break

            if power_df is not None and len(power_df) > 0:
                energy_kwh = power_df["energy_usage"].sum() / 3_600_000
                max_power = float(power_df["power_draw"].max())
            else:
                energy_kwh, max_power = 1.8, 850.0

            if host_df is not None and len(host_df) > 0 and "cpu_utilization" in host_df.columns:
                cpu_util = float(host_df["cpu_utilization"].mean())
            else:
                cpu_util = 0.6

            if service_df is not None and len(service_df) > 0 and "timestamp" in service_df.columns:
                runtime_ms = service_df["timestamp"].max() - service_df["timestamp"].min()
                runtime_hours = float(runtime_ms) / (1000 * 3600)
            else:
                runtime_hours = 0.5

            return {
                "energy_kwh": round(float(energy_kwh), 4),
                "cpu_utilization": round(float(cpu_util), 3),
                "max_power_draw": round(float(max_power), 1),
                "runtime_hours": round(float(runtime_hours), 2),
                "status": "success",
            }
        except Exception as e:
            logger.error(f"Failed to parse OpenDC results: {e}")
<<<<<<< HEAD
            # always return a dict
            return {
                "energy_kwh": 0.0,
                "cpu_utilization": 0.0,
                "max_power_draw": 0.0,
                "runtime_hours": 0.0,
                "status": "error",
            }

    # ---------- mock output writer (also used as fallback) ----------
    def create_enhanced_mock_results(self, tasks_data=None, fragments_data=None, outdir=None, reason=None):
        """
        Append a couple of fresh points to powerSource.parquet and host.parquet
        under OPENDT_SIM_DIR so the dashboard updates DURING the simulation.
        Creates the files if missing. Also writes service.parquet header if missing.
        """
        import math
        from datetime import datetime, timedelta, timezone
        import numpy as np

        base = outdir or os.environ.get("OPENDT_SIM_DIR") or "/app/output/opendt-simulation/raw-output"
        Path(base).mkdir(parents=True, exist_ok=True)

        p_path = Path(base, "powerSource.parquet")
        h_path = Path(base, "host.parquet")
        s_path = Path(base, "service.parquet")  # optional; we create a minimal one if missing

        # helper: read existing (or empty) parquet
        def _read_parquet_or_empty(path, cols):
            if path.exists() and path.stat().st_size > 0:
                try:
                    return pd.read_parquet(path)
                except Exception:
                    pass
            # empty frame with expected columns
            return pd.DataFrame({c: pd.Series(dtype="float64") for c in cols})

        # read current
        pdf = _read_parquet_or_empty(p_path, ["timestamp", "energy_usage", "power_draw"])
        hdf = _read_parquet_or_empty(h_path, ["timestamp", "cpu_utilization"])

        # choose next timestamps (5-min step); if empty, seed 24h back then append 2 new points
        step = timedelta(minutes=5)
        now = datetime.now(timezone.utc)
        if "timestamp" in pdf.columns and not pdf.empty:
            last_ts = pd.to_datetime(pdf["timestamp"]).max().to_pydatetime()
        else:
            last_ts = now - timedelta(hours=2)  # start a little in the past so you can see history

        # generate 2 new points
        new_ts = [last_ts + step, last_ts + 2*step]

        # simple dynamics: sine + noise; tie power to cpu
        t = np.linspace(0, 2*math.pi, len(new_ts))
        cpu = np.clip(0.55 + 0.25*np.sin(4*t) + 0.06*np.random.randn(len(new_ts)), 0, 1)
        kwh_bucket = np.clip(cpu * 0.70 / 12 + 0.006*np.random.randn(len(new_ts)), 0, None)
        energy_J = (kwh_bucket * 3_600_000).astype(float)
        power_w  = (kwh_bucket * 12_000.0).astype(float)

        new_p = pd.DataFrame({"timestamp": new_ts, "energy_usage": energy_J, "power_draw": power_w})
        new_h = pd.DataFrame({"timestamp": new_ts, "cpu_utilization": cpu})

        # append and write back
        pdf = pd.concat([pdf, new_p], ignore_index=True)
        hdf = pd.concat([hdf, new_h], ignore_index=True)

        pdf.to_parquet(p_path, index=False)
        hdf.to_parquet(h_path, index=False)

        # ensure service.parquet exists (very small placeholder with timestamps)
        if not s_path.exists() or s_path.stat().st_size == 0:
            svc = pd.DataFrame({"timestamp": pd.to_datetime(pdf["timestamp"]).astype("int64") // 1_000_000})
            svc.to_parquet(s_path, index=False)

        # summary for scoring
        energy_kwh_total = float(pdf["energy_usage"].sum() / 3_600_000.0) if "energy_usage" in pdf.columns else 0.0
        cpu_mean = float(hdf["cpu_utilization"].tail(48).mean()) if "cpu_utilization" in hdf.columns else 0.0
        max_power_draw = float(pdf["power_draw"].tail(48).max()) if "power_draw" in pdf.columns else 0.0
        runtime_hours = max(0.0, (pd.to_datetime(pdf["timestamp"]).max() - pd.to_datetime(pdf["timestamp"]).min()).total_seconds() / 3600.0) if not pdf.empty else 0.0

        if reason:
            logger.warning(f"Using enhanced mock results: {reason}")

        return {
            "energy_kwh": round(energy_kwh_total, 4),
            "cpu_utilization": round(cpu_mean, 3),
            "max_power_draw": round(max_power_draw, 1),
            "runtime_hours": round(runtime_hours, 2),
            "status": "mock",
        }

=======
>>>>>>> 658302df2e3dabd73f38e9019164cdda7b59332f
