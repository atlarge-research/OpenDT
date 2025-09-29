import pandas as pd
import json
import subprocess
import os
import sys
import pathlib
import matplotlib.pyplot as plt
import shutil
import platform
from pathlib import Path

import os
from getpass import getpass
from langchain_openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from pydantic import BaseModel, Field
from typing import List

# Set up environment
os.environ[
    "OPENAI_API_KEY"] = "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"

# Get the script directory and set up paths using pathlib for cross-platform compatibility
script_dir = Path(__file__).parent.absolute()
data_dir = script_dir / "data"

# Use relative paths based on the data folder structure
path_output = data_dir / "outputs" / "raw-output" / "0" / "seed=0"
path_topology_file = data_dir / "topologies" / "datacenter.json"


def find_java():
    """Find Java executable on the system"""
    system = platform.system().lower()

    # Common Java paths
    java_paths = [
        "/usr/bin/java",  # Linux/macOS system Java
        "/usr/local/bin/java",  # Homebrew Java
        "/opt/homebrew/bin/java",  # Apple Silicon Homebrew
        "/Library/Java/JavaVirtualMachines/*/Contents/Home/bin/java",  # macOS Oracle/OpenJDK
        "/System/Library/Frameworks/JavaVM.framework/Versions/Current/Commands/java"  # macOS system Java
    ]

    # Check if java is in PATH
    try:
        result = subprocess.run(["which", "java"], capture_output=True, text=True)
        if result.returncode == 0:
            java_path = result.stdout.strip()
            return java_path
    except:
        pass

    # Check common locations
    for path_pattern in java_paths:
        if "*" in path_pattern:
            # Handle wildcard paths
            import glob
            matches = glob.glob(path_pattern)
            if matches:
                return matches[0]
        else:
            if Path(path_pattern).exists():
                return path_pattern

    # Fall back to just "java" and hope it's in PATH
    return "java"


def simulator():
    """Run the OpenDC simulator"""
    project_root = script_dir
    exp_file = data_dir / "experiment.json"

    # For cross-platform jar file handling
    lib_dir = project_root / "lib"  # Assuming jar files are in a lib directory

    # Find Java executable
    java_exe = find_java()

    # Set JAVA_HOME if possible
    java_home = Path(java_exe).parent.parent
    if java_home.exists():
        os.environ["JAVA_HOME"] = str(java_home)

    # Build classpath - use all jar files in lib directory
    if lib_dir.exists():
        jar_files = list(lib_dir.glob("*.jar"))
        classpath = os.pathsep.join(str(jar) for jar in jar_files)
    else:
        # Fallback - assume jars are in the main directory
        jar_files = list(project_root.glob("*.jar"))
        classpath = os.pathsep.join(str(jar) for jar in jar_files)

    # Verify files exist
    if not exp_file.exists():
        print(f"Warning: Experiment file not found: {exp_file}")
        # Create a dummy experiment file if needed
        exp_file.parent.mkdir(parents=True, exist_ok=True)
        exp_file.touch()

    if not classpath:
        print("Warning: No jar files found. Make sure OpenDC jar files are available.")
        return

    # Build command
    cmd = [
        str(java_exe),
        "-cp",
        classpath,
        "org.opendc.experiments.base.runner.ExperimentCli",
        "--experiment-path",
        str(exp_file)
    ]

    print("Running:", " ".join(cmd))

    try:
        res = subprocess.run(cmd, cwd=str(project_root), text=True, capture_output=True, timeout=300)
        print("Return code:", res.returncode)
        print("--- STDOUT ---\n", res.stdout)
        if res.stderr:
            print("--- STDERR ---\n", res.stderr)
    except subprocess.TimeoutExpired:
        print("Simulator timed out after 5 minutes")
    except Exception as e:
        print(f"Error running simulator: {e}")


class ClusterBatch(BaseModel):
    cluster_name: List[str] = Field(description="List of cluster names")
    host_name: List[str] = Field(description="List of host names")
    coreCount: List[int] = Field(description="List of core counts")
    coreSpeed: List[int] = Field(description="List of core speeds in MHz")
    count: List[int] = Field(description="List of host counts")


def main():
    """Main execution function"""
    # Ensure data directories exist
    data_dir.mkdir(parents=True, exist_ok=True)
    path_output.mkdir(parents=True, exist_ok=True)
    path_topology_file.parent.mkdir(parents=True, exist_ok=True)

    # Create sample topology file if it doesn't exist
    if not path_topology_file.exists():
        sample_topology = {
            "clusters": [
                {
                    "name": "C01",
                    "hosts": [
                        {
                            "name": "H01",
                            "cpu": {
                                "coreCount": 16,
                                "coreSpeed": 2400
                            },
                            "count": 1
                        },
                        {
                            "name": "H02",
                            "cpu": {
                                "coreCount": 8,
                                "coreSpeed": 2100
                            },
                            "count": 1
                        }
                    ]
                }
            ]
        }
        with open(path_topology_file, 'w') as f:
            json.dump(sample_topology, f, indent=4)
        print(f"Created sample topology file: {path_topology_file}")

    # Initialize results DataFrame
    df_exps = pd.DataFrame(columns=['cluster_name', 'host_name', 'coreCount', 'coreSpeed', 'count', 'expno'])

    row_count = 0
    iterations = 5

    for i in range(iterations):
        print(f"\nIteration {i + 1}/{iterations}")

        # Load topology data
        try:
            with open(path_topology_file, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f"Topology file not found: {path_topology_file}")
            break
        except json.JSONDecodeError as e:
            print(f"Error parsing topology JSON: {e}")
            break

        # Extract cluster and host information
        records = []
        for cluster in data['clusters']:
            cluster_name = cluster['name']
            for host in cluster['hosts']:
                record = {
                    'cluster_name': cluster_name,
                    'host_name': host['name'],
                    'coreCount': host['cpu']['coreCount'],
                    'coreSpeed': host['cpu']['coreSpeed'],
                    'count': host.get('count', None)
                }
                records.append(record)

        df = pd.DataFrame(records)
        df['expno'] = i
        df_exps = pd.concat([df_exps, df], axis=0, ignore_index=True)
        row_count = df_exps.shape[0]

        # Run simulator
        simulator()

        # Process simulation results (if available)
        try:
            host_file = path_output / "host.parquet"
            power_file = path_output / "powerSource.parquet"
            task_file = path_output / "task.parquet"
            service_file = path_output / "service.parquet"

            if all(f.exists() for f in [host_file, power_file, task_file, service_file]):
                df_host_small = pd.read_parquet(host_file)
                df_power_small = pd.read_parquet(power_file)
                df_task_small = pd.read_parquet(task_file)
                df_service_small = pd.read_parquet(service_file)

                runtime_small = pd.to_timedelta(
                    df_service_small.timestamp.max() - df_service_small.timestamp.min(),
                    unit="ms"
                )
                energy_small = df_power_small.energy_usage.sum() / 3_600_000

                df_exps.loc[df_exps['expno'] == i, 'runtime'] = runtime_small
                df_exps.loc[df_exps['expno'] == i, 'energy_usage'] = energy_small

                print("Simulation results processed successfully")
            else:
                print("Some output files are missing, skipping result processing")

        except Exception as e:
            print(f"Error processing simulation results: {e}")

        print(df_exps.tail())

        # LLM optimization (if API key is available)
        try:
            prompt_text = f"""You are an optimizer for datacenter configurations.
            You will be provided data from OpenDC simulator which simulates datacenter energy usage and runtime.
            You need to recommend next core count and core speed for simulation for each cluster and host.
            Each experiment has {row_count} rows which is the current configuration.
            You need to recommend similar configuration which helps to achieve objectives:
            - Lesser runtime
            - Less energy consumption

            Data: {df_exps.to_string()}

            Output format: JSON and it should contain same number of clusters and hosts.

            Example:
            {{
              "cluster_name": ["C01", "C01"],
              "host_name": ["H01", "H02"],
              "coreCount": [32, 16],
              "coreSpeed": [3200, 2100],
              "count": [2, 3]
            }}
            """

            # Initialize LLM
            llm = OpenAI()
            response = llm.invoke(prompt_text)

            # Parse response
            llm_chat = ChatOpenAI(temperature=0)
            parser = JsonOutputParser(pydantic_object=ClusterBatch)

            prompt_template = PromptTemplate.from_template(
                """Extract structured cluster data from the following description.
                {format_instructions}

                Text:
                {input}
                """
            )

            chain = prompt_template.partial(
                format_instructions=parser.get_format_instructions()
            ) | llm_chat | parser

            parsed_response = chain.invoke({"input": response})
            print("LLM Recommendation:", parsed_response)

            # Update topology with LLM recommendations
            input_json = data
            llm_output = parsed_response

            for cluster in input_json["clusters"]:
                cluster_name = cluster["name"]
                for host in cluster["hosts"]:
                    host_name = host["name"]
                    # Find matching index in LLM output
                    for j, (c_name, h_name) in enumerate(
                            zip(llm_output["cluster_name"], llm_output["host_name"])
                    ):
                        if c_name == cluster_name and h_name == host_name:
                            host["cpu"]["coreCount"] = llm_output["coreCount"][j]
                            host["cpu"]["coreSpeed"] = llm_output["coreSpeed"][j]
                            host["count"] = llm_output["count"][j]

            # Save updated topology
            with open(path_topology_file, "w") as f:
                json.dump(input_json, f, indent=4)

            print("Topology updated with LLM recommendations")

        except Exception as e:
            print(f"LLM optimization failed: {e}")
            print("Continuing without optimization...")

    # Save final results
    results_file = script_dir / "exp_results.csv"
    df_exps.to_csv(results_file, index=False)
    print(f"\nExperiment results saved to: {results_file}")
    print(df_exps)


if __name__ == "__main__":
    main()
