"""LLM-powered optimization strategy with rule-based fallback."""
from __future__ import annotations

import copy
import json
import logging
from typing import Any, Dict

from .rule_based import rule_based_optimization
from .scoring import performance_score

logger = logging.getLogger(__name__)


class LLM:
    """LLM-based topology optimizer with better error handling and topology updates."""

    def __init__(self, openai_key: str | None) -> None:
        self.openai_key = openai_key
        self.has_llm = bool(openai_key)
        self.best_config: Dict[str, Any] | None = None
        self.best_score = float('inf')
        logger.info("LLM Optimizer initialized. API Key present: %s", self.has_llm)

    def calculate_performance_score(self, sim_results: Dict[str, Any]) -> float:
        return performance_score(sim_results)

    def update_best_configuration(self, sim_results: Dict[str, Any], topology_data: Dict[str, Any]) -> bool:
        score = self.calculate_performance_score(sim_results)

        if score < self.best_score:
            self.best_score = score
            self.best_config = copy.deepcopy(topology_data)
            logger.info(
                "🏆 New best configuration! Score: %.2f (Energy: %.2f kWh)",
                score,
                sim_results.get('energy_kwh', 0),
            )
            return True
        return False

    def optimize(
        self,
        simulation_results: Dict[str, Any],
        batch_data: Dict[str, Any],
        slo_targets: Dict[str, Any],
        current_topology: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if not self.has_llm:
            return rule_based_optimization(
                simulation_results,
                batch_data,
                slo_targets,
                current_topology,
                reason="No OpenAI API key",
                best_config=self.best_config,
                best_score=self.best_score if self.best_config else None,
            )

        try:
            return self.llm_optimization(simulation_results, batch_data, slo_targets, current_topology)
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("LLM optimization failed: %s", exc)
            return rule_based_optimization(
                simulation_results,
                batch_data,
                slo_targets,
                current_topology,
                reason=f"LLM Error: {str(exc)}",
                best_config=self.best_config,
                best_score=self.best_score if self.best_config else None,
            )

    def rule_based_optimization(
        self,
        sim_results: Dict[str, Any],
        batch_data: Dict[str, Any],
        slo_targets: Dict[str, Any],
        current_topology: Dict[str, Any] | None = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        result = rule_based_optimization(
            sim_results,
            batch_data,
            slo_targets,
            current_topology,
            reason,
            best_config=self.best_config,
            best_score=self.best_score if self.best_config else None,
        )
        if current_topology:
            self.update_best_configuration(sim_results, current_topology)
            result['best_config'] = self.best_config
            result['best_score'] = self.best_score if self.best_config else None
        return result

    def llm_optimization(
        self,
        sim_results: Dict[str, Any],
        batch_data: Dict[str, Any],
        slo_targets: Dict[str, Any],
        current_topology: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        from langchain_core.output_parsers import JsonOutputParser
        from langchain_openai import ChatOpenAI
        from pydantic import BaseModel, Field
        from typing import List

        class TopologyRecommendation(BaseModel):
            cluster_name: List[str] = Field(description="List of cluster names")
            host_name: List[str] = Field(description="List of host names")
            coreCount: List[int] = Field(description="List of core counts")
            coreSpeed: List[int] = Field(description="List of core speeds in MHz")
            count: List[int] = Field(description="List of host counts")

        if current_topology:
            self.update_best_configuration(sim_results, current_topology)

        llm = ChatOpenAI(
            api_key=self.openai_key,
            model="gpt-3.5-turbo",
            temperature=0.3,
            timeout=15,
        )

        parser = JsonOutputParser(pydantic_object=TopologyRecommendation)

        prompt = f"""You are an expert datacenter practitioner.
                    Based on these simulation results, provide specific recommendations to optimize energy utilization and performance (execution time).

                    You will be provided data from OpenDC simulator which simulates datacenter energy usage and runtime.
                    You need to recommend next core count and core speed for simulation for each cluster and host.

                    You need to recommend similar configuration which helps to achieve objectives:
                    - Lesser runtime
                    - Less energy consumption

                    Try to achieve both objectives(SLOs) at the same time as much as possible:
                    {slo_targets}

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
        logger.info("Received response from OpenAI")

        raw_content = self._extract_text_content(response)
        parsed = parser.parse(raw_content)

        if current_topology:
            new_topology = copy.deepcopy(current_topology)
        else:
            new_topology = {'clusters': []}

        new_topology = self.convert_llm_to_topology(parsed, current_topology)

        logger.info("Generated new topology from LLM recommendation")

        recommendations = parsed.dict() if hasattr(parsed, "dict") else parsed

        return {
            'type': 'llm',
            'reason': 'LLM recommendation applied',
            'new_topology': new_topology,
            'best_config': self.best_config,
            'best_score': self.best_score if self.best_config else None,
            'recommendations': recommendations,
        }

    def convert_llm_to_topology(self, llm_result, current_topology):
        """Convert LLM output (pydantic OR dict) to OpenDC topology format."""

        def field(obj, name, default=None):
            if hasattr(obj, name):
                return getattr(obj, name, default)
            if isinstance(obj, dict):
                return obj.get(name, default)
            return default

        new_topology = copy.deepcopy(current_topology) if current_topology else {'clusters': []}

        try:
            clusters = field(llm_result, "cluster_name", []) or []
            hosts = field(llm_result, "host_name", []) or []
            counts = field(llm_result, "count", []) or []
            cores = field(llm_result, "coreCount", []) or []
            clocks = field(llm_result, "coreSpeed", []) or []

            total = min(len(clusters), len(hosts))
            for index in range(total):
                cluster_name = clusters[index]
                host_name = hosts[index]
                count = counts[index] if index < len(counts) else 1
                core_count = cores[index] if index < len(cores) else 16
                core_speed = clocks[index] if index < len(clocks) else 2400

                cluster = next((c for c in new_topology["clusters"] if c["name"] == cluster_name), None)
                if not cluster:
                    cluster = {"name": cluster_name, "hosts": []}
                    new_topology["clusters"].append(cluster)

                host = next((h for h in cluster["hosts"] if h["name"] == host_name), None)
                if not host:
                    host = {
                        "name": host_name,
                        "count": int(count),
                        "cpu": {"coreCount": int(core_count), "coreSpeed": int(core_speed)},
                        "memory": {"memorySize": 34359738368},
                    }
                    cluster["hosts"].append(host)
                else:
                    host["count"] = int(count)
                    host["cpu"]["coreCount"] = int(core_count)
                    host["cpu"]["coreSpeed"] = int(core_speed)

            logger.info("✅ Successfully converted LLM output to topology format")
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.error("Error converting LLM output to topology: %s", exc)

        return new_topology

    @staticmethod
    def _extract_text_content(message: Any) -> str:
        """Normalize LangChain/OpenAI message payloads into plain text."""

        if message is None:
            return ""

        content = getattr(message, "content", message)

        if isinstance(content, str):
            return content.strip()

        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = item.get("text")
                    if text:
                        parts.append(str(text))
                else:
                    text = getattr(item, "text", None)
                    if text:
                        parts.append(str(text))
            return "".join(parts).strip()

        return str(content).strip()
