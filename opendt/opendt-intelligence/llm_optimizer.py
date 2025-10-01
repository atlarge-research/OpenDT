#!/usr/bin/env python3
"""LLM-powered topology optimization using OpenAI API"""
import pandas as pd
import json
import sys
import argparse
import os
from pathlib import Path
from typing import List
import time

from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field


class ClusterBatch(BaseModel):
    cluster_name: List[str] = Field(description="List of cluster names")
    host_name: List[str] = Field(description="List of host names")
    coreCount: List[int] = Field(description="List of core counts")
    coreSpeed: List[int] = Field(description="List of core speeds in MHz")
    count: List[int] = Field(description="List of host counts")


class OpenDTLLMOptimizer:
    def __init__(self, api_key: str = None):
        if api_key:
            os.environ["OPENAI_API_KEY"] = api_key
        elif "OPENAI_API_KEY" not in os.environ:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable.")

        self.llm_chat = ChatOpenAI(temperature=0, model="gpt-3.5-turbo")
        self.parser = JsonOutputParser(pydantic_object=ClusterBatch)

    def optimize_topology(self, simulation_results: dict, current_topology: dict,
                          experiment_history: pd.DataFrame = None) -> dict:
        """Generate topology recommendations based on simulation results"""

        # Prepare optimization prompt
        prompt_text = self._create_optimization_prompt(simulation_results, current_topology, experiment_history)

        try:
            # Create prompt template
            prompt_template = PromptTemplate.from_template(
                """You are an expert datacenter optimizer. Based on the simulation results and current topology,
                recommend an improved configuration to achieve:
                - Lower energy consumption
                - Better runtime performance
                - Optimal resource utilization

                {format_instructions}

                Analysis Data:
                {input}

                Provide your recommendation as structured JSON:"""
            )

            # Create optimization chain
            chain = prompt_template.partial(
                format_instructions=self.parser.get_format_instructions()
            ) | self.llm_chat | self.parser

            # Get LLM recommendation
            llm_response = chain.invoke({"input": prompt_text})
            print(f"🤖 LLM Recommendation: {llm_response}")

            # Apply recommendations to topology
            optimized_topology = self._apply_recommendations(current_topology, llm_response)

            return {
                "success": True,
                "optimized_topology": optimized_topology,
                "llm_recommendations": llm_response,
                "analysis": self._analyze_recommendations(llm_response)
            }

        except Exception as e:
            print(f"❌ LLM optimization failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "fallback_recommendations": self._get_fallback_recommendations(simulation_results)
            }

    def _create_optimization_prompt(self, sim_results: dict, topology: dict, history: pd.DataFrame) -> str:
        """Create detailed optimization prompt"""

        prompt_parts = [
            "SIMULATION RESULTS:",
            f"- Energy Usage: {sim_results.get('energy_kwh', 'N/A')} kWh",
            f"- Runtime: {sim_results.get('runtime_hours', 'N/A')} hours",
            f"- CPU Utilization: {sim_results.get('cpu_utilization', 'N/A')}",
            f"- Power Draw: {sim_results.get('max_power_draw', 'N/A')} W",
            "",
            "CURRENT TOPOLOGY:",
            json.dumps(topology, indent=2),
            ""
        ]

        if history is not None and not history.empty:
            prompt_parts.extend([
                "EXPERIMENT HISTORY:",
                history.to_string(),
                ""
            ])

        prompt_parts.extend([
            "OBJECTIVES:",
            "- Minimize energy consumption (target < 1.5 kWh)",
            "- Optimize runtime (target < 30 hours)",
            "- Maintain CPU utilization between 40-80%",
            "- Balance performance vs. energy efficiency",
            "",
            "Recommend specific coreCount, coreSpeed, and host count values."
        ])

        return "\n".join(prompt_parts)

    def _apply_recommendations(self, current_topology: dict, recommendations: dict) -> dict:
        """Apply LLM recommendations to topology"""
        optimized_topology = json.loads(json.dumps(current_topology))  # Deep copy

        try:
            for cluster in optimized_topology.get("clusters", []):
                cluster_name = cluster.get("name")

                for host in cluster.get("hosts", []):
                    host_name = host.get("name")

                    # Find matching recommendation
                    for i, rec_cluster in enumerate(recommendations.get("cluster_name", [])):
                        rec_host = recommendations["host_name"][i]

                        if rec_cluster == cluster_name and rec_host == host_name:
                            # Apply recommendations
                            host["cpu"]["coreCount"] = recommendations["coreCount"][i]
                            host["cpu"]["coreSpeed"] = recommendations["coreSpeed"][i]
                            host["count"] = recommendations["count"][i]

                            print(f"✅ Applied LLM recommendation to {cluster_name}/{host_name}")
                            break

        except Exception as e:
            print(f"⚠️ Error applying recommendations: {e}")

        return optimized_topology

    def _analyze_recommendations(self, recommendations: dict) -> dict:
        """Analyze LLM recommendations for insights"""
        total_cores = sum(
            recommendations["coreCount"][i] * recommendations["count"][i]
            for i in range(len(recommendations["coreCount"]))
        )

        avg_core_speed = sum(recommendations["coreSpeed"]) / len(recommendations["coreSpeed"])
        total_hosts = sum(recommendations["count"])

        return {
            "total_compute_cores": total_cores,
            "average_core_speed_mhz": round(avg_core_speed, 0),
            "total_hosts": total_hosts,
            "estimated_power_draw": round(total_cores * avg_core_speed * 0.1 / 1000, 2)  # Rough estimate
        }

    def _get_fallback_recommendations(self, sim_results: dict) -> dict:
        """Provide fallback recommendations when LLM fails"""
        energy = sim_results.get('energy_kwh', 1.0)
        cpu_util = sim_results.get('cpu_utilization', 0.5)

        if energy > 2.0:
            return {"action": "reduce_hosts", "reason": "High energy consumption"}
        elif cpu_util > 0.9:
            return {"action": "scale_up", "reason": "High CPU utilization"}
        elif cpu_util < 0.3:
            return {"action": "consolidate", "reason": "Low utilization"}
        else:
            return {"action": "maintain", "reason": "Configuration looks optimal"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--simulation-results', required=True, help='Simulation results JSON')
    parser.add_argument('--topology', help='Current topology JSON file path')
    parser.add_argument('--history', help='Experiment history CSV file path')
    parser.add_argument('--api-key', help='OpenAI API key (optional)')
    args = parser.parse_args()

    # Parse inputs
    sim_results = json.loads(args.simulation_results)

    current_topology = {}
    if args.topology and Path(args.topology).exists():
        with open(args.topology) as f:
            current_topology = json.load(f)

    experiment_history = None
    if args.history and Path(args.history).exists():
        experiment_history = pd.read_csv(args.history)

    # Initialize optimizer
    optimizer = OpenDTLLMOptimizer(api_key=args.api_key)

    # Get recommendations
    result = optimizer.optimize_topology(sim_results, current_topology, experiment_history)

    # Output results
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
