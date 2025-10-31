import pandas as pd

df = pd.DataFrame({
    "submission_time":      pd.to_datetime(["2025-10-07 09:00:00.000"]),
    "last_submission_time": pd.to_datetime(["2025-10-07 08:59:57.500"]),
})

df["delta_sec"] = (df["submission_time"] - df["last_submission_time"]).dt.total_seconds()
print(df["delta_sec"])
# 0    2.5