#!/usr/bin/env python3
"""Convert benchmark metrics from JSON to PostgreSQL.

This script supports both traditional password authentication and
AWS IAM database authentication for RDS PostgreSQL instances.
When using IAM authentication, provide an IAM-generated token as the password.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values


def create_tables(conn):
    """Create benchmark metrics table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                commit VARCHAR(40) NOT NULL,
                command VARCHAR(50) NOT NULL,
                data_size INTEGER,
                pipeline INTEGER,
                clients INTEGER,
                requests INTEGER,
                rps DECIMAL(12,2),
                avg_latency_ms DECIMAL(10,3),
                min_latency_ms DECIMAL(10,3),
                p50_latency_ms DECIMAL(10,3),
                p95_latency_ms DECIMAL(10,3),
                p99_latency_ms DECIMAL(10,3),
                max_latency_ms DECIMAL(10,3),
                cluster_mode BOOLEAN,
                tls BOOLEAN,
                io_threads INTEGER,
                benchmark_threads INTEGER,
                benchmark_mode VARCHAR(50),
                duration INTEGER,
                warmup INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_commit ON benchmark_metrics(commit);
            CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_timestamp ON benchmark_metrics(timestamp);
            CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_command ON benchmark_metrics(command);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_benchmark_metrics_unique 
                ON benchmark_metrics(timestamp, commit, command, data_size, pipeline);
        """
        )
    conn.commit()


def convert_metrics_to_rows(metrics_data):
    """Convert JSON metrics to PostgreSQL rows."""
    rows = []
    for metric in metrics_data:
        row = (
            datetime.fromisoformat(metric["timestamp"].replace("Z", "+00:00")),
            metric.get("commit"),
            metric.get("command"),
            metric.get("data_size"),
            metric.get("pipeline"),
            metric.get("clients"),
            metric.get("requests"),
            metric.get("rps"),
            metric.get("avg_latency_ms"),
            metric.get("min_latency_ms"),
            metric.get("p50_latency_ms"),
            metric.get("p95_latency_ms"),
            metric.get("p99_latency_ms"),
            metric.get("max_latency_ms"),
            metric.get("cluster_mode"),
            metric.get("tls"),
            metric.get("io_threads"),
            metric.get("valkey-benchmark-threads"),
            metric.get("benchmark_mode"),
            metric.get("duration"),
            metric.get("warmup"),
        )
        rows.append(row)
    return rows


def push_to_postgres(metrics_data, conn, dry_run=False):
    """Push metrics to PostgreSQL."""
    print(f"  Converting {len(metrics_data)} metrics to rows...")
    rows = convert_metrics_to_rows(metrics_data)

    if dry_run:
        print(f"Would insert {len(rows)} rows:")
        for i, row in enumerate(rows[:3]):
            print(f"  [{i+1}] {row}")
        if len(rows) > 3:
            print(f"  ... and {len(rows) - 3} more")
        return len(rows)

    print(f"  Inserting {len(rows)} rows into database...")
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO benchmark_metrics (
                timestamp, commit, command, data_size, pipeline, clients, requests,
                rps, avg_latency_ms, min_latency_ms, p50_latency_ms, p95_latency_ms,
                p99_latency_ms, max_latency_ms, cluster_mode, tls, io_threads,
                benchmark_threads, benchmark_mode, duration, warmup
            ) VALUES %s
            ON CONFLICT (timestamp, commit, command, data_size, pipeline) DO NOTHING
            """,
            rows,
        )
    print(f"  Committing transaction...")
    conn.commit()
    print(f"  ✓ Successfully inserted {len(rows)} rows")
    return len(rows)


def process_commit_metrics(commit_dir, conn, dry_run=False):
    """Process metrics for a single commit directory."""
    metrics_file = commit_dir / "metrics.json"
    if not metrics_file.exists():
        print(f"Skipping {commit_dir.name}: no metrics.json found")
        return 0

    with open(metrics_file) as f:
        metrics_data = json.load(f)

    print(f"\n=== Processing {commit_dir.name} ===")
    count = push_to_postgres(metrics_data, conn, dry_run)

    status = "Would insert" if dry_run else "Inserted"
    print(f"{status} {count} metrics")
    return count


def main():
    parser = argparse.ArgumentParser(description="Push benchmark metrics to PostgreSQL")
    parser.add_argument(
        "--results-dir", required=True, help="Path to results directory"
    )
    parser.add_argument("--host", help="PostgreSQL host (not required for dry-run)")
    parser.add_argument("--port", default=5432, type=int, help="PostgreSQL port")
    parser.add_argument("--database", help="Database name (not required for dry-run)")
    parser.add_argument(
        "--username", help="Database username (not required for dry-run)"
    )
    parser.add_argument(
        "--password", help="Database password (not required for dry-run)"
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be inserted"
    )

    args = parser.parse_args()

    if not args.dry_run:
        if not all([args.host, args.database, args.username]):
            parser.error(
                "--host, --database, and --username are required unless --dry-run is specified"
            )
        if not args.password:
            parser.error("--password is required unless --dry-run is specified")

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    conn = None
    if not args.dry_run:
        password = args.password
        print(f"Connecting as {args.username}@{args.host}")

        # Connect to PostgreSQL
        try:
            print(f"Attempting connection to {args.host}:{args.port}...")
            print(f"Database: {args.database}, User: {args.username}")

            conn = psycopg2.connect(
                host=args.host,
                port=args.port,
                database=args.database,
                user=args.username,
                password=password,
                connect_timeout=30,
                sslmode="require",
            )
            print(f"✓ Connected to PostgreSQL at {args.host}:{args.port}")
        except psycopg2.OperationalError as e:
            if "timeout expired" in str(e) or "Connection timed out" in str(e):
                print(f"\n❌ Connection timeout to RDS instance.", file=sys.stderr)
                print(f"This indicates a network connectivity issue.", file=sys.stderr)
                print(f"\nTroubleshooting steps:", file=sys.stderr)
                print(
                    f"1. Check RDS Security Group allows inbound port 5432 from GitHub Actions IP",
                    file=sys.stderr,
                )
                print(f"2. Verify RDS is in correct VPC/subnets", file=sys.stderr)
                print(
                    f"3. Check if RDS is publicly accessible (if needed)",
                    file=sys.stderr,
                )
                print(
                    f"4. Consider using RDS Proxy for better connectivity",
                    file=sys.stderr,
                )
            else:
                print(f"❌ PostgreSQL connection error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected error connecting to PostgreSQL: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        if not args.dry_run:
            print("Creating/verifying database tables...")
            create_tables(conn)
            print("✓ Created/verified database tables")

        # Process all commit directories
        print(f"Scanning {results_dir} for commit directories...")
        commit_dirs = [
            d
            for d in results_dir.iterdir()
            if d.is_dir() and (d / "metrics.json").exists()
        ]
        commit_dirs.sort()

        print(f"Found {len(commit_dirs)} commit directories to process")

        total_processed = 0
        for i, commit_dir in enumerate(commit_dirs, 1):
            print(f"\n[{i}/{len(commit_dirs)}] Processing {commit_dir.name}...")
            try:
                count = process_commit_metrics(commit_dir, conn, args.dry_run)
                total_processed += count
                print(f"✓ Completed {commit_dir.name} ({count} metrics)")
            except Exception as e:
                print(f"✗ Error processing {commit_dir.name}: {e}", file=sys.stderr)

        status = "[DRY RUN] Would process" if args.dry_run else "Successfully processed"
        print(f"\n{status} {total_processed} total metrics")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
