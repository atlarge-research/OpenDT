#!/usr/bin/env python3
import json
import logging
import os

logger = logging.getLogger(__name__)


class SimpleOptimizer:
    """LLM-based topology optimizer with better error handling"""

    def __init__(self, openai_key):
        self.openai_key = openai_key
        self.has_llm = bool(openai_key)
        logger.info(f"LLM Optimizer initialized. API Key present: {self.has_llm}")

    def optimize(self, simulation_results, batch_data):
        """Optimize topology based on simulation results"""

        if not self.has_llm:
            return self.rule_based_optimization(simulation_results, batch_data, reason="No OpenAI API key")

        try:
            return self.llm_optimization(simulation_results, batch_data)
        except Exception as e:
            logger.error(f"LLM optimization failed: {e}")
            return self.rule_based_optimization(simulation_results, batch_data, reason=f"LLM Error: {str(e)}")

    def rule_based_optimization(self, sim_results, batch_data, reason=""):
        """Simple rule-based optimization"""
        energy = sim_results.get('energy_kwh', 2.0)
        cpu_util = sim_results.get('cpu_utilization', 0.5)
        task_count = batch_data.get('task_count', 10)

        recommendations = []

        if energy > 10.0:
            recommendations.append("🔥 CRITICAL: Reduce host count - very high energy consumption")
            action = "scale_down"
        elif energy > 5.0:
            recommendations.append("⚠️ HIGH: Consider consolidating workload - high energy usage")
            action = "consolidate"
        elif cpu_util > 0.8:
            recommendations.append("📈 SCALE UP: Add CPU cores - high utilization")
            action = "scale_up"
        elif cpu_util < 0.3:
            recommendations.append("📉 CONSOLIDATE: Reduce resources - low utilization")
            action = "consolidate"
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
            'action_type': [action]
        }

    def llm_optimization(self, sim_results, batch_data):
        """LLM-based optimization with OpenAI"""
        try:
            from langchain_openai import ChatOpenAI

            llm = ChatOpenAI(
                api_key=self.openai_key,
                model="gpt-3.5-turbo",
                temperature=0.3,
                timeout=15
            )

            prompt = f"""You are an expert datacenter optimization consultant. Based on these simulation results, provide specific recommendations to optimize energy efficiency and performance.

SIMULATION RESULTS:
- Energy Usage: {sim_results.get('energy_kwh', 'N/A')} kWh
- CPU Utilization: {sim_results.get('cpu_utilization', 'N/A')}
- Task Count: {batch_data.get('task_count', 'N/A')}  
- Fragment Count: {batch_data.get('fragment_count', 'N/A')}
- Average CPU Usage: {batch_data.get('avg_cpu_usage', 'N/A')}

Please respond with ONLY a JSON object in this exact format:
{{
  "recommendations": ["specific recommendation 1", "specific recommendation 2"],
  "priority": "high" | "medium" | "low",
  "action_type": ["scale_up", "scale_down", "consolidate", "maintain"]
}}

Consider:
- Energy >5 kWh = HIGH consumption  
- CPU >80% = needs more cores
- CPU <30% = consolidation opportunity"""

            logger.info("🤖 Calling OpenAI for optimization recommendations...")
            response = llm.invoke(prompt)

            # Parse LLM response
            try:
                response_text = response.content.strip()
                logger.info(f"🤖 LLM Raw Response: {response_text[:200]}...")

                llm_result = json.loads(response_text)

                # Validate response format
                if not isinstance(llm_result.get('recommendations'), list):
                    raise ValueError("Invalid recommendations format")

            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"LLM response parsing failed: {e}")
                # Extract key insights from raw text
                response_text = response.content[:200]
                llm_result = {
                    'recommendations': [f"LLM suggests: {response_text}"],
                    'priority': 'medium',
                    'action_type': ['maintain']
                }

            return {
                'type': 'llm_based',
                'energy_kwh': sim_results.get('energy_kwh'),
                'cpu_utilization': sim_results.get('cpu_utilization'),
                'task_count': batch_data.get('task_count'),
                'llm_recommendations': llm_result.get('recommendations', []),
                'priority': llm_result.get('priority', 'medium'),
                'action_type': llm_result.get('action_type', ['maintain']),
                'action_taken': llm_result.get('action_type', ['maintain'])[0]
            }

        except ImportError:
            return self.rule_based_optimization(sim_results, batch_data, reason="langchain-openai not installed")
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return self.rule_based_optimization(sim_results, batch_data, reason=f"LLM API Error: {str(e)}")
