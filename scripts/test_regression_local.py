#!/usr/bin/env python3
"""Local test for detect_regression.py using a local PostgreSQL instance.

Usage:
    # Start local Postgres first:
    #   docker run -d --name pg-test -e POSTGRES_PASSWORD=test -p 5432:5432 postgres
    python scripts/test_regression_local.py

The script will:
1. Create the benchmark_metrics table
2. Insert two fake commits: baseline (high RPS) and new (low RPS = regression)
3. Run detect_regression.py and verify it detects the regression
"""

import json
import subprocess
import sys
from datetime import datetime, timezone, timedelta

import psycopg2

DB = dict(host="localhost", port=5432, database="postgres", user="postgres", password="test")

BASELINE_COMMIT = "aaaaaaaabbbbbbbbccccccccdddddddd11111111"
NEW_COMMIT      = "eeeeeeeeffffffffgggggggghhhhhhhhiiiiiiii"

COMMANDS = ["GET", "SET"]
CONFIGS  = [
    dict(pipeline=1,  io_threads=1,  clients=1600, data_size=16),
    dict(pipeline=10, io_threads=9,  clients=1600, data_size=128),
]

BASELINE_RPS = 500_000.0
REGRESSION_FACTOR = 0.85  # 15% drop — above the 5% threshold
NUM_RUNS = 10  # need >1 run for CI-based significance test
RPS_NOISE = 0.005  # ±0.5% noise per run — tight enough that 15% drop is clearly significant


def insert_rows(cur, commit: str, rps_multiplier: float, ts: datetime) -> None:
    import random
    random.seed(42)
    for run in range(NUM_RUNS):
        run_ts = ts + timedelta(minutes=run)
        noise = 1 + random.uniform(-RPS_NOISE, RPS_NOISE)
        for cmd in COMMANDS:
            for cfg in CONFIGS:
                cur.execute(
                    """
                    INSERT INTO benchmark_metrics
                      (timestamp, commit, command, pipeline, io_threads, clients,
                       data_size, rps, avg_latency_ms, p50_latency_ms,
                       p95_latency_ms, p99_latency_ms, test_type)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        run_ts, commit, cmd,
                        cfg["pipeline"], cfg["io_threads"], cfg["clients"], cfg["data_size"],
                        round(BASELINE_RPS * rps_multiplier * noise, 2),
                        0.5, 0.4, 1.2, 2.0,
                        "core",
                    ),
                )


def setup_db() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_metrics (
                id              SERIAL PRIMARY KEY,
                timestamp       TIMESTAMPTZ NOT NULL,
                commit          VARCHAR(40) NOT NULL,
                command         VARCHAR(50) NOT NULL,
                pipeline        INTEGER,
                io_threads      INTEGER,
                clients         INTEGER,
                data_size       INTEGER,
                rps             DECIMAL(15,2),
                avg_latency_ms  DECIMAL(10,3),
                p50_latency_ms  DECIMAL(10,3),
                p95_latency_ms  DECIMAL(10,3),
                p99_latency_ms  DECIMAL(10,3),
                test_type       VARCHAR(50) DEFAULT 'core',
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("TRUNCATE benchmark_metrics")

        now = datetime.now(timezone.utc)
        insert_rows(cur, BASELINE_COMMIT, 1.0,               now - timedelta(hours=8))
        insert_rows(cur, NEW_COMMIT,      REGRESSION_FACTOR, now)

    print(f"Inserted baseline commit : {BASELINE_COMMIT[:8]}")
    print(f"Inserted new commit      : {NEW_COMMIT[:8]}")
    print(f"Expected RPS drop        : {round((1 - REGRESSION_FACTOR) * 100)}%")
    return conn


def run_detection() -> int:
    result = subprocess.run(
        [
            sys.executable, "-m", "utils.detect_regression",
            "--host",       DB["host"],
            "--port",       str(DB["port"]),
            "--database",   DB["database"],
            "--username",   DB["user"],
            "--password",   DB["password"],
            "--table-name", "benchmark_metrics",
            "--threshold",  "5",
            "--sslmode",    "disable",
        ],
        capture_output=True,
        text=True,
    )
    print("\n--- detect_regression.py output ---")
    print(result.stdout)
    if result.stderr:
        print("--- stderr ---")
        print(result.stderr)
    return result.returncode


def main() -> None:
    print("=== Setting up local test database ===")
    try:
        conn = setup_db()
        conn.close()
    except psycopg2.OperationalError as e:
        print(f"\nFailed to connect to local Postgres: {e}")
        print("Make sure Docker is running:")
        print("  docker run -d --name pg-test -e POSTGRES_PASSWORD=test -p 5432:5432 postgres")
        sys.exit(1)

    print("\n=== Running detect_regression.py ===")
    exit_code = run_detection()

    print("\n=== Result ===")
    if exit_code == 1:
        print("PASS: regression detected as expected (exit code 1)")
    elif exit_code == 0:
        print("FAIL: no regression detected — check the data or threshold")
        sys.exit(1)
    else:
        print(f"FAIL: script crashed with exit code {exit_code} — check stderr above")
        sys.exit(1)


if __name__ == "__main__":
    main()
