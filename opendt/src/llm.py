#!/usr/bin/env python3
import json
import logging
import os
import copy

logger = logging.getLogger(__name__)


class LLM:
    """LLM-based topology optimizer with better error handling and topology updates"""

    def __init__(self, openai_key):
        self.openai_key = openai_key
        self.has_llm = bool(openai_key)
        self.best_config = None
        self.best_score = float('inf')  # Lower is better (energy-focused)
        logger.info(f"LLM Optimizer initialized. API Key present: {self.has_llm}")

    def calculate_performance_score(self, sim_results):
        """Calculate a performance score (lower is better)"""
        # TODO: update score to use a better metric, alligned with SLOs.
        #  Aka, the one that deviates the least (or the most, but in the good direction) from the SLOs.
        # equally penalize the energy usage and performance.

        energy = sim_results.get('energy_kwh', 5.0)
        performance = sim_results.get('runtime_hours', 1.0)
        score = (energy * 2.0) + (performance * 1.0)
        return score

    def update_best_configuration(self, sim_results, topology_data):
        """Track the best configuration seen so far"""
        score = self.calculate_performance_score(sim_results)

        if score < self.best_score:
            self.best_score = score
            self.best_config = copy.deepcopy(topology_data)
            logger.info(
                f"🏆 New best configuration! Score: {score:.2f} (Energy: {sim_results.get('energy_kwh', 0):.2f} kWh)")
            return True
        return False

    def optimize(self, simulation_results, batch_data, current_topology=None):
        """Optimize topology based on simulation results"""

        if not self.has_llm:
            return self.rule_based_optimization(simulation_results, batch_data, current_topology,
                                                reason="No OpenAI API key")

        try:
            return self.llm_optimization(simulation_results, batch_data, current_topology)
        except Exception as e:
            logger.error(f"LLM optimization failed: {e}")
            return self.rule_based_optimization(simulation_results, batch_data, current_topology,
                                                reason=f"LLM Error: {str(e)}")

    def rule_based_optimization(self, sim_results, batch_data, current_topology=None, reason=""):
        """Simple rule-based optimization with topology updates"""
        energy = sim_results.get('energy_kwh', 2.0)

        # TODO: instead of CPU_utilization, find out the total time elapsed in running the tasks
        cpu_util = sim_results.get('cpu_utilization', 0.5)
        task_count = batch_data.get('task_count', 10)

        recommendations = []
        new_topology = None

        if current_topology:
            # Update best configuration tracking
            is_new_best = self.update_best_configuration(sim_results, current_topology)

            # Generate new topology based on rules
            new_topology = copy.deepcopy(current_topology)

            # TODO: update these rules.
            # TODO: have a rule that says if it's already 15% over the SLO, then it's critical. Show purple.
            # TODO: if over SLO, it's bad, show red. Not cricial, but already danger zone.
            # TODO: if it's within 15% of SLO, it's warning, show orange.
            # TODO: else it's good, show green.

            # TODO: do this for energy and performance at the same time. e.g., if at least one of them is bad, then it's bad.
            if energy > 10.0:
                recommendations.append("🔥 CRITICAL: Reduce host count - very high energy consumption")
                action = "massive downscale"
                # Reduce host count if possible
                for cluster in new_topology.get('clusters', []):
                    for host in cluster.get('hosts', []):
                        if host.get('count', 1) > 1:
                            host['count'] = max(1, host['count'] - 1)
            elif energy > 5.0:
                recommendations.append("⚠️ HIGH: Consider reducing core speed - high energy usage")
                action = "downscale"
                # Reduce core speed
                for cluster in new_topology.get('clusters', []):
                    for host in cluster.get('hosts', []):
                        current_speed = host.get('cpu', {}).get('coreSpeed', 2400)
                        if current_speed > 2000:
                            host['cpu']['coreSpeed'] = max(2000, int(current_speed * 0.9))
            elif cpu_util > 0.8:
                recommendations.append("📈 SCALE UP: Add CPU cores - high utilization")
                action = "upscale"
                # Increase core count
                for cluster in new_topology.get('clusters', []):
                    for host in cluster.get('hosts', []):
                        current_cores = host.get('cpu', {}).get('coreCount', 16)
                        if current_cores < 32:
                            host['cpu']['coreCount'] = min(32, current_cores + 4)
            elif cpu_util < 0.3:
                recommendations.append("📉 CONSOLIDATE: Reduce cores - low utilization")
                action = "slightly downscale"
                # Reduce core count
                for cluster in new_topology.get('clusters', []):
                    for host in cluster.get('hosts', []):
                        current_cores = host.get('cpu', {}).get('coreCount', 16)
                        if current_cores > 8:
                            host['cpu']['coreCount'] = max(8, current_cores - 2)
            else:
                recommendations.append("✅ OPTIMAL: Current configuration is efficient")
                action = "maintain"

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
            'best_config': self.best_config,
            'best_score': self.best_score if self.best_config else None
        }

    def llm_optimization(self, sim_results, batch_data, current_topology=None):
        """LLM-based optimization with OpenAI and topology updates"""
        try:
            from langchain_openai import ChatOpenAI
            from langchain_core.output_parsers import JsonOutputParser
            from pydantic import BaseModel, Field
            from typing import List

            class TopologyRecommendation(BaseModel):
                cluster_name: List[str] = Field(description="List of cluster names")
                host_name: List[str] = Field(description="List of host names")
                coreCount: List[int] = Field(description="List of core counts")
                coreSpeed: List[int] = Field(description="List of core speeds in MHz")
                count: List[int] = Field(description="List of host counts")

            # Update best configuration tracking
            if current_topology:
                is_new_best = self.update_best_configuration(sim_results, current_topology)

            llm = ChatOpenAI(
                api_key=self.openai_key,
                model="gpt-3.5-turbo",
                temperature=0.3,
                timeout=15
            )

            parser = JsonOutputParser(pydantic_object=TopologyRecommendation)

            prompt = f"""You are an expert datacenter practitioner. 
Based on these simulation results, provide specific recommendations to optimize energy utilization and performance (execution time).

You will be provided data from OpenDC simulator which simulates datacenter energy usage and runtime.
You need to recommend next core count and core speed for simulation for each cluster and host.

You need to recommend similar configuration which helps to achieve objectives:
- Lesser runtime
- Less energy consumption

SIMULATION RESULTS:
- Energy Usage: {sim_results.get('energy_kwh', 'N/A')} kWh
- Runtime: {sim_results.get('runtime_hours', 'N/A')} hours
- CPU Utilization: {sim_results.get('cpu_utilization', 'N/A')}
- Task Count: {batch_data.get('task_count', 'N/A')}  
- Fragment Count: {batch_data.get('fragment_count', 'N/A')}
- Average CPU Usage: {batch_data.get('avg_cpu_usage', 'N/A')}

Current topology: {json.dumps(current_topology, indent=2) if current_topology else 'Not provided'}

{parser.get_format_instructions()}

Example:
{{
  "cluster_name": ["C01", "C01"],
  "host_name": ["H01", "H02"],
  "coreCount": [32, 16],
  "coreSpeed": [3200, 2100],
  "count": [2, 3]
}}
"""

            logger.info("🤖 Calling OpenAI for topology optimization...")
            response = llm.invoke(prompt)

            # Parse LLM response
            try:
                response_text = response.content.strip()
                logger.info(f"🤖 LLM Raw Response: {response_text[:300]}...")

                llm_result = parser.parse(response_text)
                logger.info(f"🤖 Parsed LLM recommendation: {llm_result}")

                # after: llm_result = parser.parse(response_text)
                try:
                    # If parse returned a dict, lift it into the model (no-op if already a model)
                    if isinstance(llm_result, dict):
                        from pydantic import BaseModel
                        class _T(BaseModel): pass  # just to type-check safely

                        llm_result = TopologyRecommendation(**llm_result)
                except Exception:
                    pass  # keep using dict; convert_llm_to_topology handles both

                # Convert LLM output to topology format
                new_topology = self.convert_llm_to_topology(llm_result, current_topology)

                return {
                    'type': 'llm_based',
                    'energy_kwh': sim_results.get('energy_kwh'),
                    'cpu_utilization': sim_results.get('cpu_utilization'),
                    'task_count': batch_data.get('task_count'),
                    'llm_recommendations': [
                        f"LLM optimized topology with {len(llm_result.cluster_name)} configurations"],
                    'priority': 'high' if sim_results.get('energy_kwh', 0) > 5 else 'medium',
                    'action_type': ['optimize'],
                    'action_taken': 'optimize',
                    'new_topology': new_topology,
                    'llm_raw': llm_result.__dict__,
                    'best_config': self.best_config,
                    'best_score': self.best_score if self.best_config else None
                }

            except Exception as e:
                logger.error(f"LLM response parsing failed: {e}")
                return self.rule_based_optimization(sim_results, batch_data, current_topology,
                                                    reason=f"LLM parsing error: {str(e)}")

        except ImportError:
            return self.rule_based_optimization(sim_results, batch_data, current_topology,
                                                reason="langchain-openai not installed")
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return self.rule_based_optimization(sim_results, batch_data, current_topology,
                                                reason=f"LLM API Error: {str(e)}")

    def convert_llm_to_topology(self, llm_result, current_topology):
        """Convert LLM output (pydantic OR dict) to OpenDC topology format."""
        import copy

        # Helper to read field from pydantic object or dict
        def field(obj, name, default=None):
            if hasattr(obj, name):  # pydantic / SimpleNamespace
                return getattr(obj, name, default)
            if isinstance(obj, dict):
                return obj.get(name, default)
            return default

        new_topology = copy.deepcopy(current_topology)

        try:
            clusters = field(llm_result, "cluster_name", []) or []
            hosts = field(llm_result, "host_name", []) or []
            counts = field(llm_result, "count", []) or []
            cores = field(llm_result, "coreCount", []) or []
            clocks = field(llm_result, "coreSpeed", []) or []

            n = min(len(clusters), len(hosts))
            for i in range(n):
                cluster_name = clusters[i]
                host_name = hosts[i]
                count = counts[i] if i < len(counts) else 1
                core_count = cores[i] if i < len(cores) else 16
                core_speed = clocks[i] if i < len(clocks) else 2400

                # find or create cluster
                cluster = next((c for c in new_topology["clusters"] if c["name"] == cluster_name), None)
                if not cluster:
                    cluster = {"name": cluster_name, "hosts": []}
                    new_topology["clusters"].append(cluster)

                # find or create host
                host = next((h for h in cluster["hosts"] if h["name"] == host_name), None)
                if not host:
                    host = {
                        "name": host_name,
                        "count": int(count),
                        "cpu": {"coreCount": int(core_count), "coreSpeed": int(core_speed)},
                        "memory": {"memorySize": 34359738368}
                    }
                    cluster["hosts"].append(host)
                else:
                    host["count"] = int(count)
                    host["cpu"]["coreCount"] = int(core_count)
                    host["cpu"]["coreSpeed"] = int(core_speed)

            logger.info("✅ Successfully converted LLM output to topology format")
        except Exception as e:
            logger.error(f"Error converting LLM output to topology: {e}")

        return new_topology
