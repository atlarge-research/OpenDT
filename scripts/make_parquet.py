# opendt/scripts/make_parquet.py
import os, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

def gen_series(minutes: int = 180, step_min: int = 5, seed: int = 0, skew: float = 0.0):
    """Return lists: ts[], cpu[0..1], power_w, energy_J (bucket per step)."""
    rng = np.random.default_rng(seed)
    points = minutes // step_min
    start = datetime.now(timezone.utc) - timedelta(minutes=(points-1)*step_min)
    ts = [start + timedelta(minutes=i*step_min) for i in range(points)]

    t = np.linspace(0, 2*np.pi, points)
    cpu = np.clip(0.55 + 0.20*np.sin(4*t) + 0.06*rng.standard_normal(points) + skew, 0, 1)
    # kWh in each 5-min bucket (roughly proportional to cpu)
    kwh_bucket = np.clip(cpu * 0.70/12 + 0.006*rng.standard_normal(points), 0, None)
    energy_J = (kwh_bucket * 3_600_000).astype(float)       # 1 kWh = 3.6e6 J
    power_w  = (kwh_bucket * (60/step_min) * 1000.0).astype(float)  # avg W over bucket
    return ts, cpu, power_w, energy_J

def write_parquet(outdir: Path, ts, cpu, power_w, energy_J):
    outdir.mkdir(parents=True, exist_ok=True)
    df_power = pd.DataFrame({"timestamp": ts, "energy_usage": energy_J, "power_draw": power_w})
    df_host  = pd.DataFrame({"timestamp": ts, "cpu_utilization": cpu})
    df_svc   = pd.DataFrame({"timestamp": ts})

    df_power.to_parquet(outdir/"powerSource.parquet", index=False)
    df_host.to_parquet(outdir/"host.parquet", index=False)
    df_svc.to_parquet(outdir/"service.parquet", index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=180)
    ap.add_argument("--step", type=int, default=5)
    ap.add_argument("--real", action="store_true", help="write real data to OPENDT_REAL_DIR (/app/data)")
    ap.add_argument("--sim", action="store_true", help="write sim data to OPENDT_SIM_DIR (/app/output/opendt-simulation/raw-output)")
    args = ap.parse_args()

    real_dir = Path(os.environ.get("OPENDT_REAL_DIR", os.environ.get("OPENDT_DATA_DIR", "/app/data")))
    sim_dir  = Path(os.environ.get("OPENDT_SIM_DIR", "/app/output/opendt-simulation/raw-output"))

    if args.real:
        ts, cpu, pw, ej = gen_series(minutes=args.minutes, step_min=args.step, seed=1, skew=0.00)
        write_parquet(real_dir, ts, cpu, pw, ej)
        print(f"[real] wrote: {real_dir}")

    if args.sim:
        # slightly different seed + slight skew so lines aren’t identical
        ts, cpu, pw, ej = gen_series(minutes=args.minutes, step_min=args.step, seed=2, skew=+0.03)
        write_parquet(sim_dir, ts, cpu, pw, ej)
        print(f"[sim ] wrote: {sim_dir}")

if __name__ == "__main__":
    main()
