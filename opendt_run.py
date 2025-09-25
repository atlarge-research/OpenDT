import numpy as np
import pandas as pd
import json
import subprocess
import os
import sys
import pathlib
import matplotlib.pyplot as plt
import shutil

import os
from getpass import getpass
from langchain_openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser


from pydantic import BaseModel, Field
from typing import List


os.environ["OPENAI_API_KEY"] = "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"

path_output = r"D:\courses\DT\assignment\opendc-demos-main\output\new\raw-output\0\seed=0"
path_topology_file = r'D:\courses\DT\assignment\opendc-demos-main\topologies\new_datacenter.json'

def simulator():
    project = r"D:\courses\DT\assignment\opendc-demos-main"
    exp = rf"{project}\experiments\new_experiment.json"
    cp = rf"{project}\OpenDCExperimentRunner\lib\*"

    env_root = pathlib.Path(sys.executable).resolve().parent
    java_home = r"C:\Program Files\Common Files\Oracle\Java\javapath\\"
    java_exe = r"C:\Program Files\Common Files\Oracle\Java\javapath\java.exe"
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = str(java_home + r"/bin") + os.pathsep + os.environ["PATH"]
    assert pathlib.Path(exp).exists(), f"Missing file: {exp}"
    assert pathlib.Path(cp[:-1]).exists(), f"Missing folder: {cp[:-2]}"

    cmd = [str(java_exe), "-cp", cp, "org.opendc.experiments.base.runner.ExperimentCli",
           "--experiment-path", exp]
    print("Running:", cmd)
    res = subprocess.run(cmd, cwd=project, text=True, capture_output=True)
    print("Return code:", res.returncode)
    print("--- STDOUT ---\n", res.stdout)
    print("--- STDERR ---\n", res.stderr)

class ClusterBatch(BaseModel):
    cluster_name: List[str] = Field(description="List of cluster names")
    host_name: List[str] = Field(description="List of host names")
    coreCount: List[int] = Field(description="List of core counts")
    coreSpeed: List[int] = Field(description="List of core speeds in MHz")
    count: List[int] = Field(description="List of host counts")


df_exps = pd.DataFrame(columns=['cluster_name','host_name','coreCount','coreSpeed','count','expno'])

row_count = 0
iter = 5
for i in range(iter):
    with open(path_topology_file, 'r') as file:
        data = json.load(file)

    records = []
    for cluster in data['clusters']:
        cluster_name = cluster['name']
        for host in cluster['hosts']:
            record = {
                'cluster_name': cluster_name,
                'host_name': host['name'],
                'coreCount': host['cpu']['coreCount'],
                'coreSpeed': host['cpu']['coreSpeed'],
                'count': host.get('count', None)  # Use None if 'count' is missing
            }
            records.append(record)

    df = pd.DataFrame(records)

    df['expno'] = i
    df_exps = pd.concat([df_exps
                            , df], axis=0)
    row_count = df_exps.shape[0]

    simulator()


    df_host_small = pd.read_parquet(path_output + "\\host.parquet")
    df_power_small = pd.read_parquet(path_output + "\\powerSource.parquet")
    df_task_small = pd.read_parquet(path_output + "\\task.parquet")
    df_service_small = pd.read_parquet(path_output + "\\service.parquet")

    runtime_small = pd.to_timedelta(df_service_small.timestamp.max() - df_service_small.timestamp.min(), unit="ms")
    energy_small = df_power_small.energy_usage.sum() / 3_600_000

    df_exps.loc[df_exps['expno'] == i, 'runtime'] = runtime_small
    df_exps.loc[df_exps['expno'] == i, 'energy_usage'] = energy_small

    print(df_exps)

    prompt = f''' you are an optimiser 
    you will be provided data of opendc simulator which simulates the datacenter energy usage and runtime
    you need to recommend next corecount and corespeed for simulation for each clusters and hosts.
    each experiment has {row_count} rows which is the current configuration. you need to recommend similar configuration 
    which helps to achive objectives i.e lesser runtime with less consumption of energy_usage
    data:{df_exps}
    output_format=json and ***it should contain same number of clusters and hosts***
    eg:
    {{
    {{
      "cluster_name": ["C01","CO1"],
      "host_name": ["H01","H02"],
      "coreCount": [32,16],
      "coreSpeed": [3200,2100],
      "count": [2,3]
    }}
    }}
    '''

    # Instantiate the model
    llm = OpenAI()

    response = llm.invoke(prompt)
    # print(response)

    llm = ChatOpenAI(temperature=0)
    parser = JsonOutputParser(pydantic_object=ClusterBatch)

    prompt = PromptTemplate.from_template("""
    Extract structured cluster data from the following description.
    {format_instructions}

    Text:
    {input}
    """)

    chain = prompt.partial(format_instructions=parser.get_format_instructions()) | llm | parser

    # text = """
    # Cluster C01 has host H01 with 16 cores at 1600 MHz and 1 instance.
    # Cluster C01 has host H02 with 16 cores at 2100 MHz and 1 instance.
    # """

    text = response

    response = chain.invoke({"input": text})
    print(response)



    input_json = data
    llm_output = response
    for cluster in input_json["clusters"]:
        cluster_name = cluster["name"]
        for host in cluster["hosts"]:
            host_name = host["name"]
            # Find matching index in LLM output
            for j, (c_name, h_name) in enumerate(zip(llm_output["cluster_name"], llm_output["host_name"])):
                if c_name == cluster_name and h_name == host_name:
                    host["cpu"]["coreCount"] = llm_output["coreCount"][j]
                    host["cpu"]["coreSpeed"] = llm_output["coreSpeed"][j]
                    host["count"] = llm_output["count"][j]

    with open(path_topology_file, "w") as f:
        json.dump(input_json, f, indent=4)


print(df_exps.to_csv("exp_results.csv"))