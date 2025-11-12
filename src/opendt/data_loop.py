import time
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "OPENDT_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "opendt")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "opendt")

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=None)

fragments = pd.read_parquet("opendt/surf-workload/fragments.parquet")

def loop_forever():
    print("Starting ingestion loop")
    while True:
        for _, row in fragments.iterrows():
            point = (
                Point("surf_jobs")
                .tag("job_id", str(row["id"]))
                .field("cpu_count", int(row["cpu_count"]))
                .field("cpu_usage", float(row["cpu_usage"]))
                .time(time.time_ns(), WritePrecision.NS)
            )
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            time.sleep(1)
        print("Loop complete, restarting")
        time.sleep(3)

if __name__ == "__main__":
    loop_forever()