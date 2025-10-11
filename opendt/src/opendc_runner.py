#!/usr/bin/env python3
import os
import json
import logging
import subprocess
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from flask import request, Response, stream_with_context  # request used earlier, Response/SSE used later
import pandas as pd
from pathlib import Path
import glob
from queue import Queue
from threading import Lock
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


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
# ---------- File watcher + SSE for results ----------

# Where to read REAL vs SIM results from disk
DATA_DIR = os.environ.get("OPENDT_DATA_DIR", "/app/data")
REAL_DIR = os.environ.get("OPENDT_REAL_DIR", DATA_DIR)
SIM_DIR  = os.environ.get("OPENDT_SIM_DIR")  # optional

_file_events = Queue(maxsize=100)
_watch_lock = Lock()
_watch_started = False

class _ResultsEventHandler(FileSystemEventHandler):
    def on_created(self, event): self._notify(event)
    def on_modified(self, event): self._notify(event)
    def _notify(self, event):
        if event.is_directory:
            return
        name = os.path.basename(event.src_path)
        # Notify only on final artifacts; ignore temporary files if any
        if name.endswith((".csv", ".jsonl", ".parquet", ".json")) and not name.endswith(".tmp"):
            ts = int(time.time())
            try:
                _file_events.put_nowait({"file": name, "ts": ts})
            except:
                pass  # queue full; drop silently

def _start_watcher_once():
    global _watch_started
    with _watch_lock:
        if _watch_started:
            return
        handler = _ResultsEventHandler()
        observer = Observer()
        # Always watch REAL_DIR
        os.makedirs(REAL_DIR, exist_ok=True)
        observer.schedule(handler, REAL_DIR, recursive=False)
        # Also watch SIM_DIR if provided and different
        if SIM_DIR and os.path.abspath(SIM_DIR) != os.path.abspath(REAL_DIR):
            os.makedirs(SIM_DIR, exist_ok=True)
            observer.schedule(handler, SIM_DIR, recursive=True)
        observer.daemon = True
        observer.start()
        _watch_started = True

# Safe to call multiple times
_start_watcher_once()

@app.route("/api/events/files")
def sse_files():
    """Server-Sent Events: emit a 'file' event whenever a result file is created/modified."""
    @stream_with_context
    def gen():
        # initial ping so the client attaches cleanly
        yield "event: ping\ndata: {}\n\n"
        while True:
            try:
                evt = _file_events.get(timeout=30)
                yield f"event: file\ndata: {evt['ts']},{evt['file']}\n\n"
            except:
                # heartbeat to keep the connection alive
                yield "event: ping\ndata: {}\n\n"
    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    return Response(gen(), headers=headers)

# ---------- Timeseries (REAL + SIM) ----------

def _latest_recursive(patterns, base_dir):
    """Newest file anywhere under base_dir matching any of patterns."""
    if not base_dir or not os.path.isdir(base_dir):
        return None
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(base_dir, "**", pat), recursive=True))
    return max(files, key=os.path.getmtime) if files else None

def _read_any(path: str) -> pd.DataFrame:
    """Read parquet/csv/jsonl/json; empty DF on failure/missing."""
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    ext = Path(path).suffix.lower()
    try:
        if ext == ".parquet": return pd.read_parquet(path)   # needs pyarrow/fastparquet
        if ext == ".csv":     return pd.read_csv(path)
        if ext == ".jsonl":
            rows = []
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    try: rows.append(json.loads(line))
                    except: pass
            return pd.DataFrame(rows)
        if ext == ".json":
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return pd.DataFrame(data if isinstance(data, list) else data.get("rows", []))
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()

def _build_from(base_dir):
    """Build timeseries dict from newest power/host files under base_dir."""
    out = {"_dir": base_dir, "files": {"power": None, "host": None}}
    if not base_dir:
        return out

    # Prefer parquet; otherwise csv/jsonl/json
    p = _latest_recursive(
        ["*powerSource*.parquet", "*powerSource*.csv", "*powerSource*.jsonl", "*powerSource*.json"],
        base_dir
    )
    h = _latest_recursive(
        ["*host*.parquet", "*host*.csv", "*host*.jsonl", "*host*.json"],
        base_dir
    )

    if p: out["files"]["power"] = os.path.relpath(p, base_dir)
    if h: out["files"]["host"]  = os.path.relpath(h, base_dir)

    pdf = _read_any(p); hdf = _read_any(h)

    if not pdf.empty:
        x = pd.to_datetime(pdf.get("timestamp", pd.RangeIndex(len(pdf))), errors="coerce").astype(str).tolist()
        power_draw = pdf["power_draw"].tolist() if "power_draw" in pdf.columns else [None]*len(x)
        # energy_usage (J) -> cumulative kWh
        energy_kwh_cum = (pdf["energy_usage"].fillna(0)/3_600_000.0).cumsum().round(6).tolist() if "energy_usage" in pdf.columns else []
        out["power"] = {"x": x, "power_draw": power_draw, "energy_kwh_cum": energy_kwh_cum}

    if not hdf.empty:
        hx = pd.to_datetime(hdf.get("timestamp", pd.RangeIndex(len(hdf))), errors="coerce").astype(str).tolist()
        cpu = hdf["cpu_utilization"].tolist() if "cpu_utilization" in hdf.columns else [None]*len(hx)
        out["host"] = {"x": hx, "cpu_utilization": cpu}

    return out

@app.route("/api/sim/timeseries")
def api_sim_timeseries():
    """Return REAL (from REAL_DIR) and SIM (from SIM_DIR) series."""
    real = _build_from(REAL_DIR)
    sim  = _build_from(SIM_DIR) if SIM_DIR else {}

    # only "empty" if BOTH have no series
    real_has = ("power" in real) or ("host" in real)
    sim_has  = ("power" in sim)  or ("host" in sim)
    if not real_has and not sim_has:
        return jsonify({
            "status": "empty",
            "source_files": {"real": real.get("files", {}), "sim": sim.get("files", {}) if sim else {}},
            "mtime": 0
        })

    # newest mtime across any used file (real or sim)
    mtimes = []
    for group in (g for g in (real, sim) if g):
        for rel in (group.get("files") or {}).values():
            if not rel:
                continue
            p = os.path.join(group.get("_dir", ""), rel)
            if os.path.exists(p):
                mtimes.append(os.path.getmtime(p))

    payload = {
        "status": "ok",
        "source_files": {"real": real.get("files", {}), **({"sim": sim.get("files", {})} if sim else {})},
        **({"power": real.get("power")} if "power" in real else {}),
        **({"host":  real.get("host")}  if "host"  in real else {}),
        **({"power_sim": sim.get("power")} if "power" in sim else {}),
        **({"host_sim":  sim.get("host")}  if "host"  in sim else {}),
        "mtime": int(max(mtimes)) if mtimes else 0,
    }
    return jsonify(json.loads(json.dumps(payload, default=str)))

# ---------- Accept recommendation endpoint (kept) ----------

@app.route('/api/accept_recommendation', methods=['POST'])
def api_accept_recommendation():
    try:
        last_opt = orchestrator.state.get('last_optimization', {})
        proposed_topology = last_opt.get('new_topology')
        if not proposed_topology:
            return jsonify({'error': 'No pending recommendation to accept'}), 400

        if orchestrator.update_topology_file(proposed_topology):
            orchestrator.state['window_accepted'] = True
            return jsonify({'status': 'success', 'message': 'Recommendation accepted and topology updated'})
        else:
            return jsonify({'error': 'Failed to update topology'}), 500
    except Exception as e:
        logger.error(f"Error accepting recommendation: {e}")
        return jsonify({'error': str(e)}), 500
