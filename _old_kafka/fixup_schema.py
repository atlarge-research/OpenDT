#!/usr/bin/env python3
#!/usr/bin/env python3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import sys
from pathlib import Path

def coerce_id_to_arrow_int32(src: Path, dst: Path):
    # read with pandas (no schema forcing)
    df = pd.read_parquet(src)

    if "id" not in df.columns:
        raise KeyError(f"'id' column not found in {src}")

    # build Arrow table from pandas
    tbl = pa.Table.from_pandas(df, preserve_index=False)

    # ensure 'id' is Arrow int32 (nullable). This does NOT check for nulls.
    idx = tbl.column_names.index("id")
    coerced_id = pc.cast(tbl["id"], pa.int32())
    tbl = tbl.set_column(idx, "id", coerced_id)

    # write a single Parquet file (not a dataset folder)
    pq.write_table(tbl, dst.as_posix())
    print(f"Wrote {dst} with id=int32")
    print("Arrow schema:\n", pq.read_schema(dst.as_posix()))

def main():

    if len(sys.argv) != 3:
        print("usage python3 fixup_schema.py <task_path> <fragments_path>")
        return
    
    tasks_path = Path(sys.argv[1])
    frags_path = Path(sys.argv[2])
    coerce_id_to_arrow_int32(tasks_path, tasks_path)
    coerce_id_to_arrow_int32(frags_path, frags_path)

    # quick pandas check
    print("\nPandas dtypes (verification):")
    print("tasks:", pd.read_parquet(tasks_path).dtypes)
    print("frags:", pd.read_parquet(frags_path).dtypes)

if __name__ == "__main__":
    main()
