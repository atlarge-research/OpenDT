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

    try:
        energy = float(energy)
    except (TypeError, ValueError):
        energy = 0.0

    try:
        runtime_hours = float(runtime_hours)
    except (TypeError, ValueError):
        runtime_hours = 0.0

    recommendations: list[str] = []
    new_topology = None
    action = "maintain"

    if current_topology:
        new_topology = copy.deepcopy(current_topology)

        energy_target = slo_targets.get('energy_target', 10.0) or 10.0
        runtime_target = slo_targets.get('runtime_target', 2.0) or 2.0

        def relative_delta(actual: float, target: float) -> float:
            if target <= 0:
                return 0.0 if actual <= 0 else 1.0
            return (actual - target) / target

        energy_delta = relative_delta(energy, float(energy_target))
        runtime_delta = relative_delta(runtime_hours, float(runtime_target))

        if energy_delta >= 0.3:
            recommendations.append(
                f"🔥 CRITICAL: Energy usage is {energy_delta:.0%} above the target ({energy:.2f} kWh vs {energy_target:.2f} kWh)."
            )
            action = "massive downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    if host.get('count', 1) > 1:
                        host['count'] = max(1, host['count'] - 1)
        elif energy_delta >= 0.15:
            recommendations.append(
                f"⚠️ HIGH: Trim CPU frequency to curb energy usage ({energy:.2f} kWh vs target {energy_target:.2f} kWh)."
            )
            action = "downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_speed = host.get('cpu', {}).get('coreSpeed', 2400)
                    if current_speed > 1800:
                        host['cpu']['coreSpeed'] = max(1800, int(current_speed * 0.9))
        elif runtime_delta >= 0.25:
            recommendations.append(
                f"📈 SCALE UP: Runtime exceeds the SLO by {runtime_delta:.0%} ({runtime_hours:.2f}h vs {runtime_target:.2f}h)."
            )
            action = "upscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_cores = host.get('cpu', {}).get('coreCount', 16)
                    if current_cores < 48:
                        host['cpu']['coreCount'] = min(48, current_cores + 4)
        elif runtime_delta >= 0.1:
            recommendations.append(
                f"⚙️ Moderate runtime pressure detected ({runtime_hours:.2f}h vs target {runtime_target:.2f}h). Consider a light scale-up."
            )
            action = "light upscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_cores = host.get('cpu', {}).get('coreCount', 16)
                    if current_cores < 32:
                        host['cpu']['coreCount'] = min(32, current_cores + 2)
        elif energy_delta <= -0.2 and runtime_delta <= -0.1:
            recommendations.append(
                "📉 CONSOLIDATE: Metrics are comfortably under SLO; consider shedding idle capacity."
            )
            action = "slightly downscale"
            for cluster in new_topology.get('clusters', []):
                for host in cluster.get('hosts', []):
                    current_cores = host.get('cpu', {}).get('coreCount', 16)
                    if current_cores > 8:
                        host['cpu']['coreCount'] = max(8, current_cores - 2)
        else:
            recommendations.append("✅ OPTIMAL: Current configuration meets configured SLOs")

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
