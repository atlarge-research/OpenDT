"""Rule-based optimization strategy used as a fallback and baseline."""
from __future__ import annotations

import copy
from typing import Any, Dict

def rule_based_optimization(
    sim_results: Dict[str, Any],
    batch_data: Dict[str, Any],
    slo_targets: Dict[str, Any],
    current_topology: Dict[str, Any] | None = None,
    reason: str = "",
    best_config: Dict[str, Any] | None = None,
    best_score: float | None = None,
) -> Dict[str, Any]:
    energy = sim_results.get('energy_kwh', 2.0)
    cpu_util = sim_results.get('cpu_utilization', 0.0)
    runtime_hours = sim_results.get('runtime_hours', 2)
    task_count = batch_data.get('task_count', 10)

    recommendations: list[str] = []
    new_topology = None
    action = "maintain"

    if current_topology:
        new_topology = copy.deepcopy(current_topology)

        if energy > slo_targets.get('energy_target', 10.0) * 1.15:
            recommendations.append("🔥 CRITICAL: Reduce host count - very high energy consumption")
            action = "massive downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    if host.get('count', 1) > 1:
                        host['count'] = max(1, host['count'] - 1)
        elif energy > slo_targets.get('energy_target', 10.0):
            recommendations.append("️ HIGH: Consider reducing core speed - high energy usage")
            action = "downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_speed = host.get('cpu', {}).get('coreSpeed', 2400)
                    if current_speed > 2000:
                        host['cpu']['coreSpeed'] = max(2000, int(current_speed * 0.9))
        elif runtime_hours > slo_targets.get('runtime_target', 2) * 1.15:
            recommendations.append("📈 SCALE UP: Add CPU cores - high utilization")
            action = "upscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_cores = host.get('cpu', {}).get('coreCount', 16)
                    if current_cores < 32:
                        host['cpu']['coreCount'] = min(32, current_cores + 4)
        elif runtime_hours > slo_targets.get('runtime_target', 2):
            recommendations.append("📉 CONSOLIDATE: Reduce cores - low utilization")
            action = "slightly downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_cores = host.get('cpu', {}).get('coreCount', 16)
                    if current_cores > 8:
                        host['cpu']['coreCount'] = max(8, current_cores - 2)
        else:
            recommendations.append("✅ OPTIMAL: Current configuration is efficient")

    return {
        'type': 'rule_based',
        'reason': reason,
        'energy_kwh': energy,
        'cpu_utilization': cpu_util,
        'task_count': task_count,
        'recommendations': recommendations,
        'action_taken': action,
        'action_type': [action],
        'new_topology': new_topology,
        'best_config': best_config,
        'best_score': best_score,
    }
