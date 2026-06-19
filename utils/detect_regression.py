#!/usr/bin/env python3
"""Detect performance regressions by comparing the latest two benchmarked commits in PostgreSQL.

Exits with code 1 if regressions are found, 0 otherwise.
Prints a JSON summary to stdout for use by downstream notification steps.
"""

import argparse
import json
import sys
from typing import List, Dict, Any, Optional

import psycopg2

from utils.compare_benchmark_results import (
    average_multiple_runs,
    create_comparison_table_data,
    _generate_summary,
    _extract_common_and_unique_config,
)


def fetch_last_two_commits(
    conn: psycopg2.extensions.connection,
    table_name: str,
    test_type: str = "core",
) -> List[str]:
    """Return the two most recently benchmarked commit SHAs (newest first)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT commit, MAX(timestamp) as latest
            FROM {table_name}
            WHERE test_type = %s OR test_type IS NULL
            GROUP BY commit
            ORDER BY latest DESC
            LIMIT 2
            """,
            (test_type,),
        )
        rows = cur.fetchall()
    if len(rows) < 2:
        print(
            f"Not enough commits in '{table_name}' to compare (found {len(rows)}).",
            file=sys.stderr,
        )
        sys.exit(0)
    return [row[0] for row in rows]


def fetch_metrics_for_commit(
    conn: psycopg2.extensions.connection,
    table_name: str,
    commit: str,
) -> List[Dict[str, Any]]:
    """Fetch all metric rows for a given commit as a list of dicts."""
    from decimal import Decimal

    # Exclude id and created_at — they are unique per row and would prevent
    # average_multiple_runs from grouping runs with identical configurations.
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {table_name} WHERE commit = %s",
            (commit,),
        )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    skip = {"id", "created_at"}
    result = []
    for row in rows:
        d = {}
        for col, val in zip(columns, row):
            if col in skip:
                continue
            d[col] = float(val) if isinstance(val, Decimal) else val
        result.append(d)
    return result


def detect(
    conn: psycopg2.extensions.connection,
    table_name: str,
    threshold_pct: float,
    test_type: str = "core",
) -> Dict[str, Any]:
    """Compare the two most recent commits and return a regression report dict."""
    new_sha, baseline_sha = fetch_last_two_commits(conn, table_name, test_type)

    baseline_data = average_multiple_runs(
        fetch_metrics_for_commit(conn, table_name, baseline_sha)
    )
    new_data = average_multiple_runs(
        fetch_metrics_for_commit(conn, table_name, new_sha)
    )

    config_groups, baseline_version, new_version, _, _ = create_comparison_table_data(
        baseline_data, new_data, metrics_filter="rps"
    )
    _, groups_with_unique = _extract_common_and_unique_config(config_groups)
    improvements, regressions, _, _ = _generate_summary(groups_with_unique)

    # Filter regressions by threshold
    significant_regressions = [
        r for r in regressions if r["change_magnitude"] >= threshold_pct
    ]

    return {
        "baseline_commit": baseline_sha,
        "new_commit": new_sha,
        "threshold_pct": threshold_pct,
        "regressions": significant_regressions,
        "improvements": improvements,
        "has_regression": len(significant_regressions) > 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect performance regressions from PostgreSQL benchmark data"
    )
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Minimum RPS regression %% to trigger an alert (default: 5.0)",
    )
    parser.add_argument("--test-type", default="core")
    parser.add_argument("--sslmode", default="require")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.username,
            password=args.password,
            connect_timeout=30,
            sslmode=args.sslmode,
        )
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        report = detect(conn, args.table_name, args.threshold, args.test_type)
    except Exception as e:
        print(f"Error during regression detection: {e}", file=sys.stderr)
        print(json.dumps({"has_regression": False, "error": str(e)}))
        sys.exit(0)
    finally:
        conn.close()

    print(json.dumps(report, indent=2))

    if report["has_regression"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
