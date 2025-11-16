"""Load Parquet files into Postgres.

Usage examples:

# using env var DATABASE_URL (recommended in docker-compose)
python load_parquet.py --files task.parquet fragments.parquet

# explicit db url
python load_parquet.py --db-url postgresql://myuser:mypassword@localhost:5500/mydb --files task.parquet fragments.parquet

You can optionally provide table names corresponding to the files:
python load_parquet.py --files task.parquet fragments.parquet --tables tasks fragments
"""

import os
import argparse
import logging
import glob
import sys
import time
from pathlib import Path
from typing import List


import pandas as pd


from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def load_parquet_to_db(path: str, table: str, engine, if_exists: str = "append", chunksize: int = 5000):
    """Read a parquet file and write it to the given SQL table using pandas.to_sql.

    - path: local path to parquet file
    - table: target table name
    - engine: SQLAlchemy engine
    - if_exists: 'append'|'replace'|'fail'
    - chunksize: rows per INSERT statement
    """
    logging.info("Reading parquet: %s", path)
    df = pd.read_parquet(path)
    logging.info("Loaded %d rows, writing to table '%s' (if_exists=%s)", len(df), table, if_exists)

    # Pandas to_sql with method='multi' is efficient for many DBs
    df.to_sql(name=table, con=engine, if_exists=if_exists, index=False, method='multi', chunksize=chunksize)
    logging.info("Finished writing %s", table)


def parse_args():
    p = argparse.ArgumentParser(description="Load Parquet files into Postgres")
    p.add_argument("--db-url", dest="db_url", default=os.environ.get("DATABASE_URL"),
                   help="Database URL (overrides env DATABASE_URL). Example: postgresql://user:pass@host:port/db")
    # Make --files optional. Accept zero or more values; support globs and default to *.parquet in cwd.
    p.add_argument("--files", dest="files", nargs='*', required=False, help="Parquet file paths or glob patterns to load. If omitted, loads all .parquet files in the CWD.")
    p.add_argument("--tables", dest="tables", nargs='*', help="Optional table names (one per file). Defaults to file basename without extension.")
    p.add_argument("--if-exists", dest="if_exists", choices=['append','replace','fail'], default='append',
                   help="Behavior if table exists")
    p.add_argument("--chunksize", dest="chunksize", type=int, default=5000, help="Rows per INSERT batch")
    return p.parse_args()


def main():
    # Configure logging to stdout and to /data/load_parquet.log (persisted when ./data is mounted)
    logger = logging.getLogger("load_parquet")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Try to add a file handler under /data for persistent logs
    log_file = Path("/data/load_parquet.log")
    try:
        fh = logging.FileHandler(str(log_file))
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        logger.warning("Could not create file log at %s", log_file)

    args = parse_args()

    if not args.db_url:
        logger.error("No database URL provided. Set DATABASE_URL env var or use --db-url.")
        raise SystemExit(1)

    engine = create_engine(args.db_url)

    # Wait for DB to become available with retries
    max_retries = 12
    delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            conn = engine.connect()
            conn.close()
            logger.info("Connected to database")
            break
        except OperationalError as e:
            logger.warning("Database not ready (attempt %d/%d): %s", attempt, max_retries, e)
            if attempt == max_retries:
                logger.error("Could not connect to database after %d attempts", max_retries)
                raise
            time.sleep(delay)
    # Resolve files: if user provided patterns, expand globs; if omitted, default to all .parquet files.
    files_to_process = []
    if args.files:
        for pattern in args.files:
            # expand glob patterns, or treat as literal if no matches
            matched = glob.glob(pattern)
            if matched:
                files_to_process.extend(matched)
            else:
                files_to_process.append(pattern)
    else:
        files_to_process = sorted(glob.glob("*.parquet"))

    if not files_to_process:
        logger.error("No parquet files to process. Provide --files or place .parquet files in the current directory.")
        raise SystemExit(2)

    for i, f in enumerate(files_to_process):
        if not os.path.exists(f):
            logger.error("File not found: %s", f)
            continue

        table = None
        if args.tables and i < len(args.tables):
            table = args.tables[i]
        else:
            table = os.path.splitext(os.path.basename(f))[0]

        try:
            print("loading......",table)
            load_parquet_to_db(f, table, engine, if_exists=args.if_exists, chunksize=args.chunksize)
        except Exception as e:
            logger.exception("Failed to load %s into %s: %s", f, table, e)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # Ensure any top-level error is logged to stdout and to the file log if available
        print("Fatal error in loader:", e, file=sys.stderr)
        try:
            logging.exception("Fatal error in loader: %s", e)
        except Exception:
            pass
        raise
