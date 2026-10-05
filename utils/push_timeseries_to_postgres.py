#!/usr/bin/env python3
"""Push per-second sampler rows from timeseries.jsonl files to PostgreSQL.

This script accepts database credentials including password.
For AWS IAM authentication, generate the token externally and pass it as the password.

Rows are stored as the sampler wrote them: identity columns plus one JSONB
column per source. The target table must already exist. Create it from
dashboards/schema.sql, which also defines the view that derives per-second
numbers from the raw readings.
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json

DEFAULT_TABLE = "benchmark_timeseries_tiering"
TIMESERIES_FILENAME = "timeseries.jsonl"
IDENTITY_COLUMNS = (
    "commit",
    "timestamp",
    "repository",
    "module_commit",
    "test_id",
    "scenario",
    "command",
    "data_size",
    "pipeline",
    "clients",
    "io_threads",
    "architecture",
    "cluster_mode",
    "tls",
    "run",
    "sample_time",
    "elapsed_sec",
)
JSON_COLUMNS = (
    "config_set",
    "profiling_set",
    "valkey_info",
    "latency_histogram",
    "process_cpu",
    "disk",
)
COLUMNS = ("config_name", "run_started_at") + IDENTITY_COLUMNS + JSON_COLUMNS
BATCH_SIZE = 500

TABLE_MISSING_MESSAGE = (
    "Table '{table}' does not exist. Create it from dashboards/schema.sql "
    "before pushing timeseries rows."
)
COLUMNS_MISSING_MESSAGE = (
    "Table '{table}' has no column(s) {columns}. Recreate it from "
    "dashboards/schema.sql."
)


def validate_table_name(table_name: str) -> str:
    """Return the table name when it is a plain SQL identifier."""
    if not re.match(r"^[a-z_][a-z0-9_]{0,62}$", table_name):
        raise ValueError(f"Invalid table name: '{table_name}'")
    return table_name


def group_key(row: Dict[str, Any]) -> Tuple[str, str, Any, str, Any]:
    """Return the run group key of a sampler row."""
    return (
        row.get("commit"),
        row.get("test_id"),
        row.get("run"),
        json.dumps(row.get("config_set"), sort_keys=True),
        row.get("io_threads"),
    )


def group_runs(
    rows: List[Dict[str, Any]],
) -> Dict[Tuple[Any, ...], List[Dict[str, Any]]]:
    """Group sampler rows by run."""
    groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(group_key(row), []).append(row)
    return groups


def run_started_at(group_rows: List[Dict[str, Any]]) -> Optional[datetime]:
    """Return the earliest sample_time of a run group."""
    stamps = [
        datetime.fromisoformat(row["sample_time"])
        for row in group_rows
        if row.get("sample_time")
    ]
    return min(stamps) if stamps else None


def get_table_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> Set[str]:
    """Read column names of a table from information_schema."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            """,
            (table_name,),
        )
        return {row[0] for row in cur.fetchall()}


def build_row(
    row: Dict[str, Any], config_name: str, started_at: Optional[datetime]
) -> Tuple[Any, ...]:
    """Convert one sampler row into a tuple in COLUMNS order."""
    values: List[Any] = [config_name, started_at]
    values.extend(row.get(column) for column in IDENTITY_COLUMNS)
    for column in JSON_COLUMNS:
        value = row.get(column)
        values.append(Json(value) if value is not None else None)
    return tuple(values)


def collect_rows(rows: List[Dict[str, Any]], config_name: str) -> List[Tuple[Any, ...]]:
    """Build insert tuples for every row, stamping each with its run start."""
    built: List[Tuple[Any, ...]] = []
    for group_rows in group_runs(rows).values():
        started_at = run_started_at(group_rows)
        built.extend(build_row(row, config_name, started_at) for row in group_rows)
    return built


def insert_rows(
    conn: psycopg2.extensions.connection,
    table_name: str,
    rows: List[Tuple[Any, ...]],
) -> int:
    """Insert rows in batches, ignoring conflicts, and return the inserted count."""
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(col) for col in COLUMNS),
    )
    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), BATCH_SIZE):
            batch = rows[start : start + BATCH_SIZE]
            execute_values(cur, insert_sql, batch, page_size=BATCH_SIZE)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def load_timeseries(path: Path) -> List[Dict[str, Any]]:
    """Read a timeseries.jsonl file, one JSON object per non-blank line."""
    rows: List[Dict[str, Any]] = []
    with open(path) as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{path} line {number} is not valid JSON: {e.msg}")
            if not isinstance(row, dict):
                raise ValueError(f"{path} line {number} is not a JSON object")
            rows.append(row)
    return rows


def process_file(
    path: Path,
    conn: Optional[psycopg2.extensions.connection],
    table_name: str,
    config_name: str,
    dry_run: bool,
) -> Tuple[int, int]:
    """Push one timeseries.jsonl file and return inserted and skipped counts."""
    rows = load_timeseries(path)
    if not rows:
        print(f"  No rows in {path}")
        return 0, 0

    groups = group_runs(rows)
    built = collect_rows(rows, config_name)
    print(f"  {len(rows)} rows in {len(groups)} run groups")

    if dry_run:
        for key, group_rows in sorted(groups.items(), key=lambda item: str(item[0])):
            commit, test_id, run, config_set, io_threads = key
            print(
                f"  [dry-run] commit={commit} test_id={test_id} run={run} "
                f"config_set={config_set} io_threads={io_threads} "
                f"rows={len(group_rows)} run_started_at={run_started_at(group_rows)}"
            )
        print(f"  [dry-run] would insert {len(built)} rows into {table_name}")
        return 0, 0

    assert conn is not None
    inserted = insert_rows(conn, table_name, built)
    skipped = len(built) - inserted
    print(f"  Inserted {inserted} rows, skipped {skipped} existing rows")
    return inserted, skipped


def find_timeseries_files(results_dir: Path) -> List[Path]:
    """Return every timeseries.jsonl under the immediate subdirectories."""
    return sorted(results_dir.glob(f"*/{TIMESERIES_FILENAME}"))


def connect(args: argparse.Namespace) -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection or exit on failure."""
    print(f"Connecting as {args.username}@{args.host}")
    try:
        print(f"Attempting connection to {args.host}:{args.port}...")
        print(f"Database: {args.database}, User: {args.username}")
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.username,
            password=args.password,
            connect_timeout=30,
            sslmode=args.sslmode,
        )
        print(f"Connected to PostgreSQL at {args.host}:{args.port}")
        return conn
    except psycopg2.OperationalError as e:
        if "timeout expired" in str(e) or "Connection timed out" in str(e):
            print(f"Connection timeout to RDS: {e}", file=sys.stderr)
        else:
            print(f"PostgreSQL connection error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error connecting to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
    parser = argparse.ArgumentParser(
        description="Push per-second sampler rows to PostgreSQL"
    )
    parser.add_argument(
        "--results-dir", required=True, help="Path to results directory"
    )
    parser.add_argument(
        "--config-name", required=True, help="Config name stored with every row"
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
        "--sslmode", default="require", help="PostgreSQL sslmode. Defaults to require"
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Target table name. Defaults to '{DEFAULT_TABLE}'",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be inserted"
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        table_name = validate_table_name(args.table)
    except ValueError as e:
        parser.error(str(e))

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

    files = find_timeseries_files(results_dir)
    print(f"Found {len(files)} {TIMESERIES_FILENAME} files under {results_dir}")

    conn = None
    if not args.dry_run:
        conn = connect(args)
        table_columns = get_table_columns(conn, table_name)
        missing = sorted(set(COLUMNS) - table_columns)
        if not table_columns or missing:
            message = (
                COLUMNS_MISSING_MESSAGE.format(table=table_name, columns=missing)
                if table_columns
                else TABLE_MISSING_MESSAGE.format(table=table_name)
            )
            print(message, file=sys.stderr)
            conn.close()
            sys.exit(1)

    total_inserted = 0
    total_skipped = 0
    try:
        for i, path in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}] Processing {path.parent.name}...")
            try:
                inserted, skipped = process_file(
                    path, conn, table_name, args.config_name, args.dry_run
                )
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)
            total_inserted += inserted
            total_skipped += skipped
    finally:
        if conn:
            conn.close()

    if args.dry_run:
        print(f"\n[DRY RUN] Read {len(files)} files")
    else:
        print(
            f"\nInserted {total_inserted} rows, skipped {total_skipped} existing rows"
        )


if __name__ == "__main__":
    main()
