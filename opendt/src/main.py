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
from llm import LLM

import glob
from pathlib import Path
from queue import Queue
from threading import Lock
import pandas as pd
from flask import Response, stream_with_context
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


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
        self.optimizer = LLM(self.openai_key)

        # Threading control
        self.stop_event = threading.Event()
        self.producer_thread = None
        self.consumer_thread = None

        # Topology file plumbing
        self.topology_path = '/app/config/topology.json'
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
                tasks_file='/../app/data/tasks.parquet',
                fragments_file='/../app/data/fragments.parquet'
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


def _build_series_for_dir(base_dir: str):
    def read_any(path: str) -> pd.DataFrame:
        if path is None: return pd.DataFrame()
        ext = Path(path).suffix.lower()
        if ext == ".parquet":  return pd.read_parquet(path)
        if ext == ".csv":      return pd.read_csv(path)
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
        return pd.DataFrame()

    power = _latest_file(["*powerSource*.parquet","*powerSource*.csv","*powerSource*.jsonl","*powerSource*.json"], base_dir)
    host  = _latest_file(["*host*.parquet","*host*.csv","*host*.jsonl","*host*.json"], base_dir)

    power_df = read_any(power)
    host_df  = read_any(host)

    def to_x(df: pd.DataFrame):
        if "timestamp" in df.columns:
            return pd.to_datetime(df["timestamp"], errors="coerce").astype(str).tolist()
        return list(range(len(df)))

    out = {
        "files": {
            "power": os.path.basename(power) if power else None,
            "host":  os.path.basename(host) if host else None,
        }
    }

    if not power_df.empty:
        energy_kwh_cum = []
        if "energy_usage" in power_df.columns:
            energy_kwh_cum = (power_df["energy_usage"].fillna(0) / 3_600_000.0).cumsum().round(6).tolist()
        out["power"] = {
            "x": to_x(power_df),
            "power_draw": power_df.get("power_draw", pd.Series([None]*len(power_df))).tolist(),
            "energy_kwh_cum": energy_kwh_cum,
        }

    if not host_df.empty:
        out["host"] = {
            "x": to_x(host_df),
            "cpu_utilization": host_df.get("cpu_utilization", pd.Series([None]*len(host_df))).tolist(),
        }

    return out


DATA_DIR = os.environ.get("OPENDT_DATA_DIR", "/app/data")
# Real vs Sim folders
REAL_DIR = os.environ.get("OPENDT_REAL_DIR", os.environ.get("OPENDT_DATA_DIR", "/app/data"))
SIM_DIR  = os.environ.get("OPENDT_SIM_DIR")  # optional; only used if set



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
        # Only notify on final artifacts; ignore temp files if you use *.tmp
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
        # always watch REAL_DIR
        os.makedirs(REAL_DIR, exist_ok=True)
        observer.schedule(handler, REAL_DIR, recursive=False)
        # also watch SIM_DIR if set and different
        if SIM_DIR and os.path.abspath(SIM_DIR) != os.path.abspath(REAL_DIR):
            os.makedirs(SIM_DIR, exist_ok=True)
            observer.schedule(handler, SIM_DIR, recursive=True)
        observer.daemon = True
        observer.start()
        _watch_started = True


# Safe to call multiple times
_start_watcher_once()

def _latest_file(patterns, base_dir=DATA_DIR):
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(base_dir, pat)))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

@app.route("/api/events/files")
def sse_files():
    """Server-Sent Events: emits a 'file' event whenever a result file is created/modified."""
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


# ===== real+sim parquet timeseries (TOP-LEVEL, only once) =====
import os, json, glob
from pathlib import Path
import pandas as pd
from flask import jsonify

DATA_DIR = os.environ.get("OPENDT_DATA_DIR", "/app/data")
SIM_DIR  = os.environ.get("OPENDT_SIM_DIR", None)

def _latest_recursive(patterns, base_dir):
    if not base_dir or not os.path.isdir(base_dir):
        return None
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(base_dir, "**", pat), recursive=True))
    return max(files, key=os.path.getmtime) if files else None

def _read_any(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    ext = Path(path).suffix.lower()
    try:
        if ext == ".parquet": return pd.read_parquet(path)
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
    out = {"_dir": base_dir, "files": {"power": None, "host": None}}
    if not base_dir:
        return out

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
        energy_kwh_cum = (pdf["energy_usage"].fillna(0)/3_600_000.0).cumsum().round(6).tolist() if "energy_usage" in pdf.columns else []
        out["power"] = {"x": x, "power_draw": power_draw, "energy_kwh_cum": energy_kwh_cum}

    if not hdf.empty:
        hx = pd.to_datetime(hdf.get("timestamp", pd.RangeIndex(len(hdf))), errors="coerce").astype(str).tolist()
        cpu = hdf["cpu_utilization"].tolist() if "cpu_utilization" in hdf.columns else [None]*len(hx)
        out["host"] = {"x": hx, "cpu_utilization": cpu}

    return out

@app.route("/api/sim/timeseries")
def api_sim_timeseries():
    real = _build_from(DATA_DIR)
    sim  = _build_from(SIM_DIR) if SIM_DIR else {}

    # Was: if ("power" not in real) and ("host" not in real): return empty
    # FIX: only return empty if BOTH real AND sim have no series.
    real_has = ("power" in real) or ("host" in real)
    sim_has  = ("power" in sim)  or ("host" in sim)
    if not real_has and not sim_has:
        return jsonify({
            "status": "empty",
            "source_files": {
                "real": real.get("files", {}),
                "sim":  sim.get("files",  {}) if sim else {}
            },
            "mtime": 0
        })

    # newest mtime across any used file (real or sim)
    mtimes = []
    for group in (g for g in (real, sim) if g):  # iterate real & sim
        for rel in (group.get("files") or {}).values():
            if not rel:
                continue
            p = os.path.join(group.get("_dir", ""), rel)
            if os.path.exists(p):
                mtimes.append(os.path.getmtime(p))

    payload = {
        "status": "ok",
        "source_files": {
            "real": real.get("files", {}),
            **({"sim": sim.get("files", {})} if sim else {})
        },
        # include REAL if present
        **({"power": real.get("power")} if "power" in real else {}),
        **({"host":  real.get("host")}  if "host"  in real else {}),
        # include SIM if present
        **({"power_sim": sim.get("power")} if "power" in sim else {}),
        **({"host_sim":  sim.get("host")}  if "host"  in sim else {}),
        "mtime": int(max(mtimes)) if mtimes else 0,
    }
    return jsonify(json.loads(json.dumps(payload, default=str)))
# ===== end =====




if __name__ == '__main__':
    # NOTE: ensure index.html is under ./templates/index.html
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
