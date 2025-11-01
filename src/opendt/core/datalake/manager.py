"""Data lake persistence utilities for OpenDT simulation outputs."""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

import pandas as pd


@dataclass(slots=True)
class DataLakeRecord:
    """Snapshot of a stored simulation run."""

    run_id: str
    timestamp: str
    run_type: str
    window_id: str | None
    cycle: int | None
    attempt: int | None
    status: str | None
    energy_kwh: float | None
    cpu_utilization: float | None
    runtime_hours: float | None
    max_power_draw: float | None
    exp_name: str | None
    score: float | None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "DataLakeRecord":
        return cls(
            run_id=str(payload.get("run_id")),
            timestamp=str(payload.get("timestamp")),
            run_type=str(payload.get("run_type")),
            window_id=payload.get("window_id"),
            cycle=int(payload["cycle"]) if payload.get("cycle") is not None else None,
            attempt=int(payload["attempt"]) if payload.get("attempt") is not None else None,
            status=payload.get("status"),
            energy_kwh=_safe_float(payload.get("energy_kwh")),
            cpu_utilization=_safe_float(payload.get("cpu_utilization")),
            runtime_hours=_safe_float(payload.get("runtime_hours")),
            max_power_draw=_safe_float(payload.get("max_power_draw")),
            exp_name=payload.get("exp_name"),
            score=_safe_float(payload.get("score")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:  # pragma: no cover - defensive conversion
        return None


class DataLake:
    """Persist simulation runs into an indexed on-disk structure."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root or "data/datalake").resolve()
        self.runs_dir = self.root / "runs"
        self.index_path = self.root / "index.parquet"
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    # Index helpers -----------------------------------------------------
    def _empty_index(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "run_id",
                "timestamp",
                "run_type",
                "window_id",
                "cycle",
                "attempt",
                "status",
                "energy_kwh",
                "cpu_utilization",
                "runtime_hours",
                "max_power_draw",
                "exp_name",
                "score",
            ]
        )

    def _load_index(self) -> pd.DataFrame:
        if self.index_path.exists():
            try:
                return pd.read_parquet(self.index_path)
            except Exception:  # pragma: no cover - fallback to empty
                return self._empty_index()
        return self._empty_index()

    def list_runs(self, limit: int | None = None) -> list[dict[str, Any]]:
        index = self._load_index()
        if index.empty:
            return []
        index = index.sort_values("timestamp", ascending=False)
        if limit is not None:
            index = index.head(limit)
        return [
            DataLakeRecord.from_payload(row._asdict()).to_dict()  # type: ignore[attr-defined]
            for row in index.itertuples(index=False)
        ]

    # Persistence -------------------------------------------------------
    def append_run(
        self,
        *,
        run_type: str,
        metrics: Mapping[str, Any],
        topology: Mapping[str, Any] | None,
        metadata: Mapping[str, Any] | None = None,
        timeseries: Mapping[str, Any] | None = None,
        artifacts: Mapping[str, Path] | None = None,
        window_id: str | None = None,
        cycle: int | None = None,
        attempt: int | None = None,
        exp_name: str | None = None,
        timestamp: datetime | None = None,
        score: float | None = None,
    ) -> DataLakeRecord:
        timestamp = timestamp or datetime.now(timezone.utc)
        run_id = _build_run_id(timestamp)

        summary = {
            "run_id": run_id,
            "timestamp": timestamp.isoformat(),
            "run_type": run_type,
            "window_id": window_id,
            "cycle": cycle,
            "attempt": attempt,
            "status": metrics.get("status"),
            "energy_kwh": metrics.get("energy_kwh"),
            "cpu_utilization": metrics.get("cpu_utilization"),
            "runtime_hours": metrics.get("runtime_hours"),
            "max_power_draw": metrics.get("max_power_draw"),
            "exp_name": exp_name,
            "score": score,
        }

        index = self._load_index()
        index = pd.concat([index, pd.DataFrame([summary])], ignore_index=True)
        index.sort_values("timestamp", ascending=False, inplace=True)
        index.to_parquet(self.index_path, index=False)

        run_dir = self.runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
        (run_dir / "metrics.json").write_text(json.dumps(dict(metrics), indent=2, sort_keys=True))

        if topology is not None:
            (run_dir / "topology.json").write_text(json.dumps(topology, indent=2, sort_keys=True))

        if metadata:
            (run_dir / "metadata.json").write_text(json.dumps(dict(metadata), indent=2, sort_keys=True))

        if timeseries:
            (run_dir / "timeseries.json").write_text(json.dumps(dict(timeseries), indent=2, sort_keys=True))

        if artifacts:
            artifacts_dir = run_dir / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            for name, src in artifacts.items():
                try:
                    if src and Path(src).exists():
                        shutil.copy2(src, artifacts_dir / Path(src).name)
                except Exception:  # pragma: no cover - best effort copy
                    continue

        return DataLakeRecord.from_payload(summary)

    # Retrieval ---------------------------------------------------------
    def load_run(self, run_id: str) -> dict[str, Any] | None:
        run_dir = self.runs_dir / run_id
        if not run_dir.exists():
            return None

        def _read_json(name: str) -> Any:
            path = run_dir / name
            if path.exists():
                return json.loads(path.read_text())
            return None

        summary = _read_json("summary.json") or {}
        metrics = _read_json("metrics.json") or {}
        topology = _read_json("topology.json")
        metadata = _read_json("metadata.json") or {}
        timeseries = _read_json("timeseries.json") or {}

        artifacts = {}
        artifacts_dir = run_dir / "artifacts"
        if artifacts_dir.exists():
            for file in artifacts_dir.iterdir():
                if file.is_file():
                    artifacts[file.name] = str(file)

        return {
            "summary": summary,
            "metrics": metrics,
            "topology": topology,
            "metadata": metadata,
            "timeseries": timeseries,
            "artifacts": artifacts,
        }


def _build_run_id(ts: datetime) -> str:
    prefix = ts.strftime("%Y%m%dT%H%M%S")
    suffix = uuid4().hex[:8]
    return f"{prefix}-{suffix}"
