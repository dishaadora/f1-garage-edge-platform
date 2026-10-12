"""
query_api.py

A small, long-running HTTP service that lets a client POST SQL and get
JSON back, querying the same Parquet files the ingest service writes.
This replaces query_session.py's one-shot CLI for the containerized
deployment (Stage 2+): instead of starting a new process per query, the
service stays up, matching how the Go ingest service runs, and matching
how Kubernetes expects to manage a long-lived pod rather than a
run-once script.

Deliberately built on Python's standard library http.server rather
than a framework (Flask/FastAPI) -- this keeps the Docker image's
dependency list to exactly what was already needed (duckdb, pandas),
nothing more, for a two-endpoint service that doesn't need routing,
middleware, or templating.

Endpoints:
    GET  /health                 -> {"status": "ok"}
    POST /query  {"sql": "..."}  -> {"columns": [...], "rows": [[...], ...]}

Usage:
    python query_api.py --session-id 2024-monza-R --data-dir ./data --port 8000
"""

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("query_api")


def build_connection(session_id: str, data_dir: str) -> duckdb.DuckDBPyConnection:
    """Same view-per-channel logic as query_session.py's CLI version --
    car_data and pos_data have different schemas, so each channel gets
    its own view over its own glob pattern."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW car_data AS
        SELECT * FROM read_parquet('{data_dir}/{session_id}/car_data/*.parquet')
    """)
    return con


class QueryHandler(BaseHTTPRequestHandler):
    # Set by main() before the server starts -- shared across all
    # requests. DuckDB connections are safe to use from multiple
    # threads for read-only querying, which is all this does.
    #
    # NOTE: deliberately NOT named "connection" -- BaseHTTPRequestHandler's
    # own parent class (socketserver.BaseRequestHandler) already uses
    # self.connection internally for the client's raw socket, and
    # overwrites it per-request. Naming this the same thing silently
    # shadows the DuckDB connection with a socket object; found this
    # by actually testing the running server, not by inspection.
    db_connection: duckdb.DuckDBPyConnection = None

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/query":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length))
            sql = body["sql"]
        except (json.JSONDecodeError, KeyError) as e:
            self._send_json(400, {"error": f"expected JSON body with a 'sql' field: {e}"})
            return

        try:
            df = self.db_connection.execute(sql).fetchdf()
        except Exception as e:
            # Fail soft, same principle as the Go ingest service: a bad
            # query (typo, unknown column) shouldn't take the service
            # down for the next request.
            log.warning("query failed: %s", e)
            self._send_json(400, {"error": str(e)})
            return

        self._send_json(200, {
            "columns": list(df.columns),
            "rows": df.values.tolist(),
            "row_count": len(df),
        })

    def log_message(self, fmt, *args):
        # Replace BaseHTTPRequestHandler's default stderr logging with
        # the module's own logger, for consistent log formatting.
        log.info("%s - %s", self.address_string(), fmt % args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    QueryHandler.db_connection = build_connection(args.session_id, args.data_dir)

    server = ThreadingHTTPServer(("0.0.0.0", args.port), QueryHandler)
    log.info("query_api: listening on :%d, session=%s, data-dir=%s",
              args.port, args.session_id, args.data_dir)
    server.serve_forever()


if __name__ == "__main__":
    main()