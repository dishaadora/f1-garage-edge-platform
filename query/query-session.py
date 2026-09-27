"""
query_session.py

Points DuckDB at the ingest service's Parquet output for one session and
lets you run SQL against it directly -- no import step, no server. Every
query re-scans whatever files exist on disk at that moment, so results
always reflect the latest flush.

Usage:
    python query_session.py --session-id 2024-monza-R
    python query_session.py --session-id 2024-monza-R --sql "SELECT driver, MAX(speed) FROM car_data GROUP BY driver"
"""

import argparse
import duckdb


def build_connection(session_id: str, data_dir: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    # One view per channel -- car_data and pos_data have different
    # schemas, so they can't share a single glob pattern.
    con.execute(f"""
        CREATE VIEW car_data AS
        SELECT * FROM read_parquet('{data_dir}/{session_id}/car_data/*.parquet')
    """)

    # Uncomment once pos_data is being ingested too:
    # con.execute(f"""
    #     CREATE VIEW pos_data AS
    #     SELECT * FROM read_parquet('{data_dir}/{session_id}/pos_data/*.parquet')
    # """)

    return con


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--sql", default="SELECT * FROM car_data ORDER BY session_time_seconds")
    args = parser.parse_args()

    con = build_connection(args.session_id, args.data_dir)
    result = con.execute(args.sql).fetchdf()
    print(result.to_string())


if __name__ == "__main__":
    main()