#!/usr/bin/env python3
"""Convert benchmark metrics from JSON to PostgreSQL.

This script accepts database credentials including password.
For AWS IAM authentication, generate the token externally and pass it as the password.

metrics.json rows support dynamic schema evolution: new metrics in JSON files
automatically create new database columns.

timeseries.jsonl rows from the per-second sampler go to a separate table that
must already exist. Create it from dashboards/schema.sql, which also defines
the view that derives per-second numbers from the raw readings.
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json

DESCRIPTION_MAX_LENGTH = 500
CONFIG_NAME_MAX_LENGTH = 50

METRICS_FILENAME = "metrics.json"
TIMESERIES_FILENAME = "timeseries.jsonl"
TIMESERIES_IDENTITY_COLUMNS = (
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
TIMESERIES_JSON_COLUMNS = (
    "config_set",
    "profiling_set",
    "valkey_info",
    "latency_histogram",
    "process_cpu",
    "disk",
)
TIMESERIES_COLUMNS = (
    ("config_name", "run_started_at")
    + TIMESERIES_IDENTITY_COLUMNS
    + TIMESERIES_JSON_COLUMNS
)
TIMESERIES_BATCH_SIZE = 500

TABLE_MISSING_MESSAGE = (
    "Table '{table}' does not exist. Create it from dashboards/schema.sql "
    "before pushing timeseries rows."
)
COLUMNS_MISSING_MESSAGE = (
    "Table '{table}' has no column(s) {columns}. Recreate it from "
    "dashboards/schema.sql."
)


def detect_field_type(value: Any) -> str:
    """Detect PostgreSQL column type from a sample value."""
    if value is None:
        return "TEXT"  # Default for unknown types
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "DECIMAL(15,6)"  # Accommodate precision for metrics
    elif isinstance(value, str):
        # Special handling for timestamp fields
        if "timestamp" in str(value).lower():
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return "TIMESTAMPTZ"
            except:
                pass
        # Determine string length for VARCHAR
        if len(value) <= 50:
            return "VARCHAR(50)"
        elif len(value) <= 255:
            return "VARCHAR(255)"
        else:
            return "TEXT"
    else:
        return "TEXT"


def analyze_metrics_schema(metrics_data: List[Dict[str, Any]]) -> Dict[str, str]:
    """Analyze metrics data to determine schema requirements.

    Args:
        metrics_data: List of metric dictionaries

    Returns:
        Dictionary mapping field names to PostgreSQL column types
    """
    schema = {}

    # Always include core fields
    schema["id"] = "SERIAL PRIMARY KEY"
    schema["created_at"] = "TIMESTAMPTZ DEFAULT NOW()"

    # Analyze all fields in the data
    all_fields = set()
    field_samples = {}

    for metric in metrics_data:
        for field, value in metric.items():
            all_fields.add(field)
            if field not in field_samples and value is not None:
                field_samples[field] = value

    # Determine types for each field
    for field in sorted(all_fields):
        if field == "timestamp":
            schema[field] = "TIMESTAMPTZ NOT NULL"
        elif field == "commit":
            schema[field] = "VARCHAR(255) NOT NULL"
        elif field == "command":
            schema[field] = "TEXT NOT NULL"
        elif field == "module_commit":
            schema[field] = "VARCHAR(255)"
        elif field == "module_commit_timestamp":
            schema[field] = "TIMESTAMPTZ"
        elif field == "config_set":
            schema[field] = "JSONB"
        elif field == "config_name":
            schema[field] = f"VARCHAR({CONFIG_NAME_MAX_LENGTH})"
        elif field in ["group_description", "scenario_description"]:
            schema[field] = f"VARCHAR({DESCRIPTION_MAX_LENGTH})"
        elif field in ["error", "dataset"]:
            schema[field] = "TEXT"
        else:
            sample_value = field_samples.get(field)
            column_type = detect_field_type(sample_value)
            schema[field] = column_type

    return schema


def get_existing_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> Set[str]:
    """Get existing column names from the specified table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND table_schema = 'public'
        """,
            (table_name,),
        )
        result = cur.fetchall()
        if result is None:
            return set()
        return {row[0] for row in result}


def create_or_update_table(
    conn: psycopg2.extensions.connection,
    required_schema: Dict[str, str],
    table_name: str,
) -> None:
    """Create table or add missing columns dynamically."""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """,
            (table_name,),
        )
        result = cur.fetchone()
        if result is None:
            table_exists = False
        else:
            table_exists = result[0]

        if not table_exists:
            # Create new table with all required columns
            columns_def = []
            for field, column_type in required_schema.items():
                columns_def.append(
                    sql.SQL("{} {}").format(sql.Identifier(field), sql.SQL(column_type))
                )

            create_sql = sql.SQL("CREATE TABLE {} ({})").format(
                sql.Identifier(table_name), sql.SQL(", ").join(columns_def)
            )
            cur.execute(create_sql)
            print(
                f"Created new table '{table_name}' with {len(required_schema)} columns"
            )

            # Create indexes for performance
            create_indexes(cur, table_name)
        else:
            # Table exists, check for missing columns
            existing_columns = get_existing_columns(conn, table_name)
            missing_columns = []

            for field, column_type in required_schema.items():
                if field not in existing_columns:
                    missing_columns.append((field, column_type))

            # Add missing columns
            for field, column_type in missing_columns:
                alter_sql = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier(table_name),
                    sql.Identifier(field),
                    sql.SQL(column_type),
                )
                cur.execute(alter_sql)
                print(f"Added new column: {field} ({column_type})")

            if missing_columns:
                print(f"Added {len(missing_columns)} new columns to existing table")

    conn.commit()


def create_indexes(cur, table_name: str) -> None:
    """Create performance indexes on the table."""
    indexes = [
        (
            f"idx_{table_name}_commit",
            [sql.Identifier("commit")],
        ),
        (
            f"idx_{table_name}_timestamp",
            [sql.Identifier("timestamp")],
        ),
        (
            f"idx_{table_name}_command",
            [sql.Identifier("command")],
        ),
        (
            f"idx_{table_name}_config",
            [
                sql.Identifier("commit"),
                sql.Identifier("command"),
                sql.Identifier("data_size"),
                sql.Identifier("pipeline"),
                sql.Identifier("clients"),
            ],
        ),
    ]

    for index_name, columns in indexes:
        index_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(index_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns),
        )
        cur.execute(index_sql)


def convert_metrics_to_rows(
    metrics_data: List[Dict[str, Any]], column_order: List[str]
) -> Tuple[List[Tuple[Any, ...]], int]:
    """Convert JSON metrics to PostgreSQL rows dynamically.

    Args:
        metrics_data: List of benchmark metric dictionaries from metrics.json file.
        column_order: Ordered list of column names for the INSERT statement.

    Returns:
        Tuple of (list of tuples ready for PostgreSQL insertion, number of skipped entries).
    """
    rows = []
    skipped_count = 0

    for i, metric in enumerate(metrics_data):
        # Skip entries that are None or empty
        if not metric or not isinstance(metric, dict):
            print(f"  Skipping entry {i+1}: invalid data")
            skipped_count += 1
            continue

        # Skip entries missing required fields
        if not metric.get("timestamp") or not metric.get("commit"):
            print(f"  Skipping entry {i+1}: missing required fields")
            skipped_count += 1
            continue

        row = []
        for column in column_order:
            if column in ["id", "created_at"]:
                # Skip auto-generated columns
                continue
            elif column in ("timestamp", "module_commit_timestamp"):
                timestamp_str = metric.get(column, "")
                if timestamp_str:
                    try:
                        timestamp_obj = datetime.fromisoformat(
                            timestamp_str.replace("Z", "+00:00")
                        )
                        row.append(timestamp_obj)
                    except:
                        row.append(None)
                else:
                    row.append(None)
            elif column in ["group_description", "scenario_description"]:
                value = metric.get(column)
                if isinstance(value, str) and len(value) > DESCRIPTION_MAX_LENGTH:
                    value = value[:DESCRIPTION_MAX_LENGTH]
                row.append(value)
            elif column == "config_set":
                value = metric.get(column)
                row.append(Json(value) if value is not None else None)
            elif column == "config_name":
                value = metric.get(column)
                if isinstance(value, str) and len(value) > CONFIG_NAME_MAX_LENGTH:
                    value = value[:CONFIG_NAME_MAX_LENGTH]
                row.append(value)
            else:
                # Direct field mapping since field names are now normalized
                row.append(metric.get(column))
        rows.append(tuple(row))
    return rows, skipped_count


def push_to_postgres(
    metrics_data: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection],
    table_name: str,
    dry_run: bool = False,
) -> int:
    """Push metrics to PostgreSQL with dynamic schema support.

    Args:
        metrics_data: List of benchmark metric dictionaries from metrics.json file.
        conn: PostgreSQL database connection.
        table_name: Name of the PostgreSQL table to insert into.
        dry_run: If True, only show what would be inserted without actually inserting.

    Returns:
        Number of rows that would be/were processed.
    """
    if not metrics_data:
        print("  No metrics data to process")
        return 0

    print(f"  Analyzing schema for {len(metrics_data)} metrics...")

    # Analyze the schema requirements from the data
    required_schema = analyze_metrics_schema(metrics_data)

    if not dry_run:
        if conn is None:
            raise ValueError(
                "Database connection is required for non-dry-run operations"
            )
        # Create or update table schema
        create_or_update_table(conn, required_schema, table_name)

    # Get column order (excluding auto-generated columns)
    column_order = [
        col for col in required_schema.keys() if col not in ["id", "created_at"]
    ]

    print(f"  Converting {len(metrics_data)} metrics to rows...")
    rows, skipped_entries = convert_metrics_to_rows(metrics_data, column_order)

    if skipped_entries > 0:
        print(f"  Skipped {skipped_entries} invalid entries (expected 0)")

    if dry_run:
        print(f"Would insert {len(rows)} rows with {len(column_order)} columns:")
        print(f"  Columns: {', '.join(column_order)}")
        for i, row in enumerate(rows[:3]):
            print(f"  [{i+1}] {row}")
        if len(rows) > 3:
            print(f"  ... and {len(rows) - 3} more")
        return len(rows)

    # Build dynamic INSERT statement
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(col) for col in column_order),
    )

    if conn is None:
        raise ValueError("Database connection is required for inserting data")

    print(f"  Inserting {len(rows)} rows into {table_name}...")
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows)
        inserted_count = cur.rowcount

    print("  Committing transaction...")
    conn.commit()

    if inserted_count != len(rows):
        print(f"  Skipped {len(rows) - inserted_count} rows during insert (expected 0)")

    print(f"Successfully inserted {inserted_count} rows into {table_name}")
    return len(rows)


def process_commit_metrics(
    commit_dir: Path,
    conn: Optional[psycopg2.extensions.connection],
    table_name: str,
    dry_run: bool = False,
    test_type: str = "core",
) -> Tuple[int, bool]:
    """Process metrics for a single commit directory.

    Args:
        commit_dir: Path to directory containing metrics.json file.
        conn: PostgreSQL database connection.
        table_name: Name of the PostgreSQL table to insert into.
        dry_run: If True, only show what would be inserted without actually inserting.
        test_type: Test type identifier (e.g., 'core', 'fts') for filtering in dashboards.

    Returns:
        Tuple of (number of metrics processed, whether any records were skipped).
    """
    metrics_file = commit_dir / "metrics.json"
    if not metrics_file.exists():
        print(f"Skipping {commit_dir.name}: no metrics.json found")
        return 0, True

    with open(metrics_file) as f:
        metrics_data = json.load(f)

    if not metrics_data:
        print(f"Skipping {commit_dir.name}: empty metrics")
        return 0, True

    # Augment metrics with test_type at push time
    for metric in metrics_data:
        metric["test_type"] = test_type

    print(f"\n=== Processing {commit_dir.name} ===")
    count = push_to_postgres(metrics_data, conn, table_name, dry_run)

    status = "Would insert" if dry_run else "Inserted"
    print(f"{status} {count} metrics")
    return count, False


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


def build_timeseries_row(
    row: Dict[str, Any], config_name: str, started_at: Optional[datetime]
) -> Tuple[Any, ...]:
    """Convert one sampler row into a tuple in TIMESERIES_COLUMNS order."""
    values: List[Any] = [config_name, started_at]
    values.extend(row.get(column) for column in TIMESERIES_IDENTITY_COLUMNS)
    for column in TIMESERIES_JSON_COLUMNS:
        value = row.get(column)
        values.append(Json(value) if value is not None else None)
    return tuple(values)


def collect_timeseries_rows(
    rows: List[Dict[str, Any]], config_name: str
) -> List[Tuple[Any, ...]]:
    """Build insert tuples for every row, stamping each with its run start."""
    built: List[Tuple[Any, ...]] = []
    for group_rows in group_runs(rows).values():
        started_at = run_started_at(group_rows)
        built.extend(
            build_timeseries_row(row, config_name, started_at) for row in group_rows
        )
    return built


def insert_timeseries_rows(
    conn: psycopg2.extensions.connection,
    table_name: str,
    rows: List[Tuple[Any, ...]],
) -> int:
    """Insert rows in batches, ignoring conflicts, and return the inserted count."""
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(col) for col in TIMESERIES_COLUMNS),
    )
    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), TIMESERIES_BATCH_SIZE):
            batch = rows[start : start + TIMESERIES_BATCH_SIZE]
            execute_values(cur, insert_sql, batch, page_size=TIMESERIES_BATCH_SIZE)
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


def process_commit_timeseries(
    commit_dir: Path,
    conn: Optional[psycopg2.extensions.connection],
    table_name: str,
    config_name: str,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Push the timeseries.jsonl of a commit directory.

    Returns:
        Tuple of (rows inserted, rows skipped as already present).
    """
    path = commit_dir / TIMESERIES_FILENAME
    rows = load_timeseries(path)
    if not rows:
        print(f"  No rows in {path}")
        return 0, 0

    groups = group_runs(rows)
    built = collect_timeseries_rows(rows, config_name)
    print(f"  {len(rows)} timeseries rows in {len(groups)} run groups")

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

    if conn is None:
        raise ValueError("Database connection is required for inserting data")
    inserted = insert_timeseries_rows(conn, table_name, built)
    skipped = len(built) - inserted
    print(
        f"  Inserted {inserted} rows into {table_name}, skipped {skipped} existing rows"
    )
    return inserted, skipped


def timeseries_table_error(
    conn: psycopg2.extensions.connection, table_name: str
) -> Optional[str]:
    """Return why the timeseries table cannot take rows, or None when it can."""
    table_columns = get_existing_columns(conn, table_name)
    if not table_columns:
        return TABLE_MISSING_MESSAGE.format(table=table_name)
    missing = sorted(set(TIMESERIES_COLUMNS) - table_columns)
    if missing:
        return COLUMNS_MISSING_MESSAGE.format(table=table_name, columns=missing)
    return None


CORE_METRICS_TABLE = "benchmark_metrics"
TAGS_METRICS_TABLE = "benchmark_tags_metrics"
CORE_TIMESERIES_TABLE = "benchmark_timeseries"
TAGS_TIMESERIES_TABLE = "benchmark_tags_timeseries"


def _resolve_table(table_id: str, core_table: str, tags_table: str) -> str:
    """Map a table identifier onto a table family."""
    if table_id == "core":
        return core_table
    if table_id == "tag":
        return tags_table
    if not re.match(r"^[a-z][a-z0-9_]{0,30}$", table_id):
        raise ValueError(f"Invalid table identifier: '{table_id}'")
    return f"{core_table}_{table_id}"


def resolve_table_name(table_id: str) -> str:
    """Resolve metrics table name from table identifier.

    Args:
        table_id: Table identifier (e.g., 'search', 'core', 'tag').

    Returns:
        'benchmark_metrics' for 'core',
        'benchmark_tags_metrics' for 'tag',
        or 'benchmark_metrics_{table_id}' otherwise.

    Raises:
        ValueError: If table_id is invalid.
    """
    return _resolve_table(table_id, CORE_METRICS_TABLE, TAGS_METRICS_TABLE)


def resolve_timeseries_table_name(table_id: str) -> str:
    """Resolve per-second timeseries table name from table identifier.

    Returns:
        'benchmark_timeseries' for 'core',
        'benchmark_tags_timeseries' for 'tag',
        or 'benchmark_timeseries_{table_id}' otherwise.

    Raises:
        ValueError: If table_id is invalid.
    """
    return _resolve_table(table_id, CORE_TIMESERIES_TABLE, TAGS_TIMESERIES_TABLE)


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
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
        "--sslmode", default="require", help="PostgreSQL sslmode. Defaults to require"
    )
    parser.add_argument(
        "--table",
        default="core",
        help="Table identifier (e.g., 'core', 'search', 'tag'). Defaults to 'core' "
        "which uses 'benchmark_metrics'. 'tag' uses 'benchmark_tags_metrics'. "
        "Other values use 'benchmark_metrics_{table}'. timeseries.jsonl rows go to "
        "'benchmark_timeseries', 'benchmark_tags_timeseries' or "
        "'benchmark_timeseries_{table}' in the same way.",
    )
    parser.add_argument(
        "--test-type",
        default="core",
        help="Test type identifier (e.g., 'core', 'fts') for filtering in dashboards. "
        "Also stored as config_name on timeseries rows.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be inserted"
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    args.table_name = resolve_table_name(args.table)
    args.timeseries_table_name = resolve_timeseries_table_name(args.table)

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
                sslmode=args.sslmode,
            )
            print(f"Connected to PostgreSQL at {args.host}:{args.port}")
        except psycopg2.OperationalError as e:
            if "timeout expired" in str(e) or "Connection timed out" in str(e):
                print(f"Connection timeout to RDS: {e}", file=sys.stderr)
            else:
                print(f"PostgreSQL connection error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error connecting to PostgreSQL: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        # Table creation/updates for metrics happen dynamically during processing
        print(f"Scanning {results_dir} for commit directories...")
        commit_dirs = [
            d
            for d in results_dir.iterdir()
            if d.is_dir()
            and ((d / METRICS_FILENAME).exists() or (d / TIMESERIES_FILENAME).exists())
        ]
        commit_dirs.sort()

        print(f"Found {len(commit_dirs)} commit directories to process")

        timeseries_dirs = [d for d in commit_dirs if (d / TIMESERIES_FILENAME).exists()]
        if timeseries_dirs and conn is not None:
            error = timeseries_table_error(conn, args.timeseries_table_name)
            if error:
                print(error, file=sys.stderr)
                sys.exit(1)

        total_processed = 0
        total_inserted = 0
        total_skipped = 0

        for i, commit_dir in enumerate(commit_dirs, 1):
            print(f"\n[{i}/{len(commit_dirs)}] Processing {commit_dir.name}...")
            try:
                if (commit_dir / METRICS_FILENAME).exists():
                    count, was_skipped = process_commit_metrics(
                        commit_dir,
                        conn,
                        args.table_name,
                        args.dry_run,
                        test_type=args.test_type,
                    )
                    total_processed += count
                    if was_skipped:
                        print(f"Warning: Skipped {commit_dir.name} (no valid metrics)")
                    print(f"Completed {commit_dir.name} ({count} metrics)")
                if commit_dir in timeseries_dirs:
                    inserted, skipped = process_commit_timeseries(
                        commit_dir,
                        conn,
                        args.timeseries_table_name,
                        args.test_type,
                        args.dry_run,
                    )
                    total_inserted += inserted
                    total_skipped += skipped
            except Exception as e:
                print(f"Error processing {commit_dir.name}: {e}", file=sys.stderr)
                sys.exit(1)

        status = "[DRY RUN] Would process" if args.dry_run else "Successfully processed"
        print(f"\n{status} {total_processed} total metrics")
        if timeseries_dirs:
            if args.dry_run:
                print(f"[DRY RUN] Read {len(timeseries_dirs)} timeseries files")
            else:
                print(
                    f"Inserted {total_inserted} timeseries rows, "
                    f"skipped {total_skipped} existing rows"
                )

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
