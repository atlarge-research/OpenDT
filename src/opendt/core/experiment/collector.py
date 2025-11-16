"""Experiment data collector for parquet output."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ExperimentDataCollector:
    """Collects window-by-window simulation data for experiment analysis."""

    def __init__(self, experiment_name: str, output_path: str, flush_interval: int = 1) -> None:
        self.experiment_name = experiment_name
        self.output_path = output_path
        self.records: list[dict[str, Any]] = []
        self.flush_interval = flush_interval  # Flush after N windows
        self.windows_since_flush = 0
        
        # Generate experiment directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.experiment_dir_name = f"{experiment_name}_{timestamp}"
        self.experiment_dir = os.path.join(output_path, self.experiment_dir_name)
        self.windows_dir = os.path.join(self.experiment_dir, "windows")
        
        # Main parquet filename
        self.filename = "run.parquet"
        self.filepath = os.path.join(self.experiment_dir, self.filename)
        
        # Create directory structure
        Path(self.experiment_dir).mkdir(parents=True, exist_ok=True)
        Path(self.windows_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📊 Experiment data collector initialized: {experiment_name}")
        logger.info(f"📊 Experiment directory: {self.experiment_dir}")
        logger.info(f"📊 Data will be flushed every {flush_interval} windows to: {self.filepath}")

    def record_window(self, window_data: dict[str, Any]) -> None:
        """Record data for a single window."""
        try:
            record = {
                'window_number': window_data.get('window_number'),
                'window_start': window_data.get('window_start'),
                'window_end': window_data.get('window_end'),
                'task_count': window_data.get('task_count'),
                'fragment_count': window_data.get('fragment_count'),
                'avg_cpu_usage': window_data.get('avg_cpu_usage'),
                'baseline_energy_kwh': window_data.get('baseline_energy_kwh'),
                'baseline_runtime_hours': window_data.get('baseline_runtime_hours'),
                'baseline_cpu_utilization': window_data.get('baseline_cpu_utilization'),
                'baseline_max_power_draw': window_data.get('baseline_max_power_draw'),
                'baseline_mean_power_draw_kw': window_data.get('baseline_mean_power_draw_kw'),
                'baseline_score': window_data.get('baseline_score'),
                'baseline_topology_json': json.dumps(window_data.get('baseline_topology')) if window_data.get('baseline_topology') else None,
                'optimization_enabled': window_data.get('optimization_enabled'),
                'optimization_score': window_data.get('optimization_score'),
                'optimization_energy_kwh': window_data.get('optimization_energy_kwh'),
                'optimization_runtime_hours': window_data.get('optimization_runtime_hours'),
                'optimization_topology_json': json.dumps(window_data.get('optimization_topology')) if window_data.get('optimization_topology') else None,
                'slo_targets_json': json.dumps(window_data.get('slo_targets')) if window_data.get('slo_targets') else None,
            }
            self.records.append(record)
            self.windows_since_flush += 1
            logger.debug(f"Recorded window {window_data.get('window_number')}")
            
            # Flush to disk periodically
            if self.windows_since_flush >= self.flush_interval:
                self._flush_to_disk()
        except Exception as exc:
            logger.error(f"Failed to record window data: {exc}")

    def _flush_to_disk(self) -> None:
        """Flush buffered records to parquet file."""
        if not self.records:
            return
        
        try:
            # Create DataFrame from buffered records
            df = pd.DataFrame(self.records)
            
            # Append to existing file or create new one
            if os.path.exists(self.filepath):
                # Read existing data and concatenate
                existing_df = pd.read_parquet(self.filepath)
                df = pd.concat([existing_df, df], ignore_index=True)
            
            # Write to parquet
            df.to_parquet(self.filepath, index=False)
            
            logger.info(f"💾 Flushed {len(self.records)} window records to {self.filepath}")
            
            # Clear buffer and reset counter
            self.records.clear()
            self.windows_since_flush = 0
        except Exception as exc:
            logger.error(f"Failed to flush data to disk: {exc}")

    def get_window_dir(self, window_number: int) -> str:
        """Get the directory path for a specific window."""
        window_dir = os.path.join(self.windows_dir, str(window_number))
        Path(window_dir).mkdir(parents=True, exist_ok=True)
        return window_dir
    
    def get_window_inputs_dir(self, window_number: int) -> str:
        """Get the inputs directory path for a specific window."""
        inputs_dir = os.path.join(self.get_window_dir(window_number), "inputs")
        Path(inputs_dir).mkdir(parents=True, exist_ok=True)
        return inputs_dir
    
    def get_window_outputs_dir(self, window_number: int) -> str:
        """Get the outputs directory path for a specific window."""
        outputs_dir = os.path.join(self.get_window_dir(window_number), "outputs")
        Path(outputs_dir).mkdir(parents=True, exist_ok=True)
        return outputs_dir

    def finalize(self) -> str:
        """Flush any remaining buffered data and return the filepath."""
        try:
            # Flush any remaining records
            if self.records:
                self._flush_to_disk()
            
            # Check if file exists
            if os.path.exists(self.filepath):
                # Get total record count
                df = pd.read_parquet(self.filepath)
                total_records = len(df)
                logger.info(f"✅ Experiment complete: {total_records} total window records in {self.filepath}")
                logger.info(f"📁 Detailed window data saved to: {self.windows_dir}")
                return self.filepath
            else:
                logger.warning("No data was written during experiment")
                return ""
        except Exception as exc:
            logger.error(f"Failed to finalize experiment data: {exc}")
            return ""
