#!/usr/bin/env python3
"""PostgreSQL-based module commit tracking for benchmarks.

Tracks which (core_sha, module_sha) pairs have been benchmarked,
enabling 2D commit discovery across both valkey and module repositories.
"""

import argparse
import json
import platform
import sys
from pathlib import Path
from typing import List, Optional

import psycopg2
from psycopg2.extras import Json

from postgres_track_commits import _git_rev_list, _git_commit_time


def get_config_name(config_file_path: str) -> str:
    """Extract the config file name from a full path.

    Args:
        config_file_path: Full or relative path to config file
            (e.g., '../valkey-search/.github/benchmark_configs/fts-benchmarks-arm.json')

    Returns:
        Just the filename (e.g., 'fts-benchmarks-arm.json')
    """
    return Path(config_file_path).name


def _module_table_name(module_name: str) -> str:
    """Return the tracking table name for a given module."""
    return f"benchmark_module_commits_{module_name}"


def _is_config_sets_subset(subset: List[dict], superset: List[dict]) -> bool:
    """Check if every element in subset has an exact match in superset.

    Each element in subset must match exactly (same keys and values) with
    at least one element in superset. Superset can have extra elements.

    Example:
        subset =   [{"reader-threads": 1}]
        superset = [{"reader-threads": 1}, {"reader-threads": 8}]
        → True (subset's single element matches one in superset)

        subset =   [{"reader-threads": 1}, {"reader-threads": 8}]
        superset = [{"reader-threads": 1}]
        → False (subset has an element with no match in superset)
    """
    if not isinstance(subset, list) or not isinstance(superset, list):
        return False
    for item in subset:
        if item not in superset:
            return False
    return True


def create_module_table(conn, module_name: str) -> None:
    """Create module benchmark queue table if it doesn't exist.

    Queue-based design: pairs are inserted as 'pending', classified with
    priority (1=forward, 2=fallback), and fetched in priority order.
    """
    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                sha VARCHAR(40) NOT NULL,
                module_sha VARCHAR(40) NOT NULL,
                core_timestamp TIMESTAMPTZ NOT NULL,
                module_timestamp TIMESTAMPTZ NOT NULL,
                max_commit_timestamp TIMESTAMPTZ NOT NULL,
                min_commit_timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'new'
                    CHECK (status IN ('new', 'pending', 'in_progress', 'completed', 'completed_as_subset')),
                priority INTEGER,
                config_name VARCHAR(255) NOT NULL,
                config_sets JSONB NOT NULL,
                architecture VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT unique_{table}_sha_module_config_arch
                    UNIQUE(sha, module_sha, config_name, config_sets, architecture)
            );

            CREATE INDEX IF NOT EXISTS idx_{table}_sha ON {table}(sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_module_sha ON {table}(module_sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_status ON {table}(status);
            CREATE INDEX IF NOT EXISTS idx_{table}_config_name ON {table}(config_name);
            CREATE INDEX IF NOT EXISTS idx_{table}_sha_status ON {table}(sha, status);
            CREATE INDEX IF NOT EXISTS idx_{table}_fetch_order
                ON {table}(status, created_at DESC, priority ASC,
                           max_commit_timestamp DESC, min_commit_timestamp DESC);
        """)
    conn.commit()
    print(f"Created/verified {table} table", file=sys.stderr)


def populate_module_commits(
    conn,
    repo: Path,
    branch: str,
    module_repo: Path,
    module_branch: str,
    architecture: str,
    module_name: str,
    config_name: str,
    config_sets: List[dict],
    config_sets_json,
) -> int:
    """Insert all new (core_sha, module_sha, config_set) combos into the queue table.

    Computes the cartesian product of core commits, module commits, and config_sets,
    then inserts any combos not already in the table. Existing rows
    (any status) are skipped via ON CONFLICT DO NOTHING.
    Also clears any stale in_progress entries from previous failed runs.

    Args:
        conn: PostgreSQL connection
        repo: Path to valkey (core) git repository
        branch: Git branch for valkey (e.g., 'unstable')
        module_repo: Path to module git repository
        module_branch: Git branch for module (e.g., 'main')
        architecture: Architecture (e.g., 'aarch64')
        module_name: Module name (determines table)
        config_name: Config file name (e.g., 'fts-benchmarks-arm.json')
        config_sets: List of module runtime configs to track individually

    Returns:
        Number of new rows inserted
    """
    create_module_table(conn, module_name)

    _check_null_priorities(conn, module_name, "start of populate_module_commits")

    # Clean up stale in_progress entries from previous failed runs
    cleanup_module_commits(
        conn, module_name, config_name, config_sets_json, architecture
    )

    # Get all commits from both git repos (newest first)
    core_shas = _git_rev_list(repo, branch)
    module_shas = _git_rev_list(module_repo, module_branch)

    # Get existing (sha, module_sha) pairs for this config+arch to avoid redundant inserts
    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT sha, module_sha FROM {table}
            WHERE config_name = %s AND config_sets = %s AND architecture = %s
        """,
            (config_name, config_sets_json, architecture),
        )
        existing = {(row[0], row[1]) for row in cur.fetchall()}

    # Insert new rows from the cartesian product of commits
    inserted = 0
    with conn.cursor() as cur:
        for core_sha in core_shas:
            core_ts = _git_commit_time(repo, core_sha)
            for module_sha in module_shas:
                if (core_sha, module_sha) in existing:
                    continue

                module_ts = _git_commit_time(module_repo, module_sha)
                max_ts = max(core_ts, module_ts)
                min_ts = min(core_ts, module_ts)

                cur.execute(
                    f"""
                    INSERT INTO {table} (sha, module_sha, core_timestamp,
                                         module_timestamp, max_commit_timestamp,
                                         min_commit_timestamp,
                                         config_name, config_sets, architecture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sha, module_sha, config_name, config_sets, architecture)
                    DO NOTHING
                """,
                    (
                        core_sha,
                        module_sha,
                        core_ts,
                        module_ts,
                        max_ts,
                        min_ts,
                        config_name,
                        config_sets_json,
                        architecture,
                    ),
                )
                inserted += cur.rowcount

    conn.commit()
    print(
        f"Populated {table}: {inserted} new pairs inserted "
        f"({len(core_shas)} core × {len(module_shas)} module commits)",
        file=sys.stderr,
    )

    # Classify newly inserted pairs with priority
    _determine_priority(
        conn, module_name, config_name, config_sets, config_sets_json, architecture
    )

    _check_null_priorities(conn, module_name, "end of populate_module_commits")

    return inserted


def _check_null_priorities(conn, module_name: str, context: str) -> int:
    """Check for NULL priority values in the table and print a warning.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        context: Description of when this check is happening (for the message)

    Returns:
        Number of rows with NULL priority
    """
    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE priority IS NULL AND status = 'pending'"
        )
        null_count = cur.fetchone()[0]

    if null_count > 0:
        print(
            f"WARNING [{context}]: {null_count} pending rows with NULL priority in {table}",
            file=sys.stderr,
        )
    return null_count


def _mark_subset_rows(
    conn,
    module_name: str,
    config_name: str,
    config_sets: List[dict],
    config_sets_json,
    architecture: str,
) -> int:
    """Mark 'new' rows as 'completed_as_subset' if a superset config_sets already exists.

    Batch approach:
      1. Find all distinct config_sets with 'completed' status for this (config_name, arch)
      2. Check if our config_sets is a subset of any of them
      3. If yes, batch-UPDATE all 'new' rows whose (sha, module_sha) has a completed
         superset row

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        config_name: Config file name to scope
        config_sets: The config_sets array (raw Python list for subset comparison)
        config_sets_json: Pre-wrapped Json(config_sets) for SQL queries
        architecture: Architecture to scope

    Returns:
        Number of rows marked as completed_as_subset
    """
    table = _module_table_name(module_name)

    with conn.cursor() as cur:
        # Step 1: Get all distinct completed config_sets for this (config_name, arch)
        cur.execute(
            f"""
            SELECT DISTINCT config_sets FROM {table}
            WHERE status IN ('completed', 'completed_as_subset')
              AND config_name = %s AND architecture = %s
        """,
            (config_name, architecture),
        )
        completed_config_sets_list = [row[0] for row in cur.fetchall()]

        # Step 2: Find all completed config_sets that are supersets of ours
        superset_config_sets_list = []
        for completed_cs in completed_config_sets_list:
            if _is_config_sets_subset(config_sets, completed_cs):
                superset_config_sets_list.append(completed_cs)

        if not superset_config_sets_list:
            return 0

        # Step 3: Batch-mark all 'new' rows whose pair exists in ANY superset's completed rows
        subset_count = 0
        for superset_cs in superset_config_sets_list:
            cur.execute(
                f"""
                UPDATE {table}
                SET status = 'completed_as_subset', updated_at = NOW()
                WHERE status = 'new'
                  AND config_name = %s AND architecture = %s
                  AND config_sets = %s
                  AND (sha, module_sha) IN (
                      SELECT sha, module_sha FROM {table}
                      WHERE status = 'completed'
                        AND config_name = %s AND architecture = %s
                        AND config_sets = %s
                  )
            """,
                (
                    config_name,
                    architecture,
                    config_sets_json,
                    config_name,
                    architecture,
                    Json(superset_cs),
                ),
            )
            subset_count += cur.rowcount

    conn.commit()
    if subset_count > 0:
        print(
            f"Subset detection: {subset_count} rows marked completed_as_subset",
            file=sys.stderr,
        )
    return subset_count


def _determine_priority(
    conn,
    module_name: str,
    config_name: str,
    config_sets: List[dict],
    config_sets_json,
    architecture: str,
) -> int:
    """Process 'new' rows: detect subsets, then assign priority to the rest.

    Steps:
      1. Call _mark_subset_rows to batch-mark any rows covered by a superset.
      2. For remaining 'new' rows, derive priority from the newest completed pair:
         - Forward (1): core_timestamp >= pointer AND module_timestamp >= pointer
         - Fallback (2): everything else
      3. Move processed rows from 'new' → 'pending'.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        config_name: Config file name to scope
        config_sets: The config_sets array being processed (raw Python list)
        config_sets_json: Pre-wrapped Json(config_sets) for SQL queries
        architecture: Architecture to scope

    Returns:
        Number of rows processed
    """
    table = _module_table_name(module_name)

    # Step 1: Subset detection (batch)
    subset_count = _mark_subset_rows(
        conn, module_name, config_name, config_sets, config_sets_json, architecture
    )

    # Step 2: Assign priority to remaining 'new' rows
    updated = 0
    with conn.cursor() as cur:
        # Find pointer from newest completed pair for this config+config_sets+arch
        cur.execute(
            f"""
            SELECT core_timestamp, module_timestamp FROM {table}
            WHERE status IN ('completed', 'completed_as_subset')
              AND config_name = %s AND config_sets = %s AND architecture = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT 1
        """,
            (config_name, config_sets_json, architecture),
        )
        pointer_row = cur.fetchone()

        if pointer_row is None:
            # No completed pairs — all 'new' with our config_sets get priority=1
            cur.execute(
                f"""
                UPDATE {table}
                SET status = 'pending', priority = 1, updated_at = NOW()
                WHERE status = 'new'
                  AND config_name = %s AND config_sets = %s AND architecture = %s
            """,
                (config_name, config_sets_json, architecture),
            )
            updated = cur.rowcount
        else:
            pointer_core_ts, pointer_module_ts = pointer_row

            # Forward (1): both timestamps >= pointer
            cur.execute(
                f"""
                UPDATE {table}
                SET status = 'pending', priority = 1, updated_at = NOW()
                WHERE status = 'new'
                  AND config_name = %s AND config_sets = %s AND architecture = %s
                  AND core_timestamp >= %s
                  AND module_timestamp >= %s
            """,
                (
                    config_name,
                    config_sets_json,
                    architecture,
                    pointer_core_ts,
                    pointer_module_ts,
                ),
            )
            forward_count = cur.rowcount

            # Fallback (2): remaining 'new' rows with our config_sets
            cur.execute(
                f"""
                UPDATE {table}
                SET status = 'pending', priority = 2, updated_at = NOW()
                WHERE status = 'new'
                  AND config_name = %s AND config_sets = %s AND architecture = %s
            """,
                (config_name, config_sets_json, architecture),
            )
            fallback_count = cur.rowcount

            updated = forward_count + fallback_count
            print(
                f"Priority assigned: {forward_count} forward, {fallback_count} fallback",
                file=sys.stderr,
            )

    conn.commit()
    total = subset_count + updated
    print(
        f"determine_priority: {total} rows processed ({subset_count} subset, {updated} prioritized)",
        file=sys.stderr,
    )
    return total


def fetch_next_module_commits(
    conn,
    module_name: str,
    config_name: str,
    config_sets_json,
    architecture: str,
    max_pairs: int = 1,
) -> List[str]:
    """Fetch the next batch of pending pairs and mark them as in_progress.

    Only fetches pairs matching the given config_name, config_sets, and architecture.

    Selects pending pairs sorted by:
      1. created_at DESC (newest inserts first)
      2. priority ASC (forward before fallback)
      3. max_commit_timestamp DESC (freshest combo first)
      4. min_commit_timestamp DESC (tiebreak)

    Marks selected pairs as in_progress and returns them.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        config_name: Config file name to match
        config_sets_json: Pre-wrapped Json(config_sets) for SQL queries
        architecture: Architecture to match
        max_pairs: Maximum number of pairs to fetch

    Returns:
        List of 'core_sha:module_sha' strings marked as in_progress
    """
    create_module_table(conn, module_name)
    table = _module_table_name(module_name)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, sha, module_sha FROM {table}
            WHERE status = 'pending'
              AND config_name = %s AND config_sets = %s AND architecture = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT %s
        """,
            (config_name, config_sets_json, architecture, max_pairs),
        )
        rows = cur.fetchall()

        if not rows:
            print("No pending pairs to fetch", file=sys.stderr)
            return []

        ids = [row[0] for row in rows]
        pairs = [f"{row[1]}:{row[2]}" for row in rows]

        # Mark selected pairs as in_progress
        cur.execute(
            f"""
            UPDATE {table}
            SET status = 'in_progress', updated_at = NOW()
            WHERE id = ANY(%s)
        """,
            (ids,),
        )

    conn.commit()
    print(
        f"Fetched {len(pairs)} pairs from {table}: {' '.join(pairs)}",
        file=sys.stderr,
    )
    return pairs


def mark_module_commits(
    conn,
    module_name: str,
    pairs: List[str],
    config_name: str,
    config_sets_json,
    architecture: str,
) -> int:
    """Mark specific pairs as completed.

    Updates status from 'in_progress' to 'completed' for the given pairs.
    Only matches rows with the same config_name, config_sets, and architecture.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        pairs: List of 'core_sha:module_sha' strings to mark complete
        config_name: Config file name to match
        config_sets_json: Pre-wrapped Json(config_sets) for SQL queries
        architecture: Architecture to match

    Returns:
        Number of rows updated
    """
    create_module_table(conn, module_name)
    table = _module_table_name(module_name)
    updated = 0

    with conn.cursor() as cur:
        for pair in pairs:
            core_sha, module_sha = pair.split(":")
            cur.execute(
                f"""
                UPDATE {table}
                SET status = 'completed', updated_at = NOW()
                WHERE sha = %s AND module_sha = %s
                  AND config_name = %s AND config_sets = %s AND architecture = %s
            """,
                (core_sha, module_sha, config_name, config_sets_json, architecture),
            )
            if cur.rowcount == 0:
                print(
                    f"WARNING: No matching row for {core_sha[:8]}:{module_sha[:8]} "
                    f"with config={config_name}, arch={architecture}",
                    file=sys.stderr,
                )
            else:
                updated += cur.rowcount
                print(
                    f"Marked {core_sha[:8]}:{module_sha[:8]} ({module_name}) as complete",
                    file=sys.stderr,
                )

    conn.commit()
    print(
        f"mark_module_commits: {updated} pairs marked complete in {table}",
        file=sys.stderr,
    )
    return updated


def cleanup_module_commits(
    conn, module_name: str, config_name: str, config_sets_json, architecture: str
) -> int:
    """Reset 'in_progress' entries back to 'pending' for a specific config+config_sets+arch.

    Called at the start of each run to retry pairs from previous
    failed runs. Unlike core (which deletes all), we reset to pending
    so the pair stays in the queue and gets retried. Scoped to the
    current config+config_sets+arch to avoid affecting other configs.

    Returns:
        Number of entries reset
    """
    # Ensure table exists (mirrors: create_tables in core cleanup)
    create_module_table(conn, module_name)

    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table}
            SET status = 'pending', updated_at = NOW()
            WHERE status = 'in_progress'
              AND config_name = %s AND config_sets = %s AND architecture = %s
        """,
            (config_name, config_sets_json, architecture),
        )
        count = cur.rowcount

    conn.commit()

    if count > 0:
        print(
            f"Cleaned up {count} in_progress entries (reset to pending) in {table}",
            file=sys.stderr,
        )

    return count


def main():
    parser = argparse.ArgumentParser(
        description="Module benchmark commit tracking (queue-based)"
    )

    parser.add_argument(
        "operation",
        choices=["populate", "fetch-next", "mark-complete", "cleanup"],
        help="Operation to perform",
    )

    # Database connection arguments
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--username", required=True, help="Database username")
    parser.add_argument("--password", required=True, help="Database password")

    # Module arguments
    parser.add_argument(
        "--module-name", required=True, help="Module name (e.g., 'search')"
    )
    parser.add_argument(
        "--repo", type=Path, help="Path to valkey (core) git repository"
    )
    parser.add_argument(
        "--branch", default="unstable", help="Git branch for core (default: unstable)"
    )
    parser.add_argument(
        "--module-repo", type=Path, help="Path to module git repository"
    )
    parser.add_argument(
        "--module-branch", default="main", help="Git branch for module (default: main)"
    )
    parser.add_argument(
        "--config-file",
        type=str,
        help="Path to config file (name extracted for tracking)",
    )
    parser.add_argument(
        "--architecture",
        type=str,
        help="Architecture (e.g., x86_64, aarch64). Auto-detected if not provided.",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=1,
        help="Max pairs to fetch (for fetch-next, default: 1)",
    )

    args, remaining_args = parser.parse_known_args()

    # mark-complete takes positional pairs
    if args.operation == "mark-complete":
        parser.add_argument("pairs", nargs="+", help="core_sha:module_sha pairs")
        args = parser.parse_args()
    elif remaining_args:
        parser.error(f"unrecognized arguments: {' '.join(remaining_args)}")

    # Auto-detect architecture
    if not args.architecture:
        args.architecture = platform.machine()
        print(f"Auto-detected architecture: {args.architecture}", file=sys.stderr)

    # Extract config name from path and load config_sets if present
    config_name = get_config_name(args.config_file) if args.config_file else ""
    config_sets = None
    if args.config_file and Path(args.config_file).exists():
        with open(args.config_file) as f:
            cfg_data = json.load(f)
        if isinstance(cfg_data, list) and cfg_data:
            cfg_data = cfg_data[0]
        config_sets = cfg_data.get("config_sets")
    if not config_sets:
        print("Error: config_sets not found in config file", file=sys.stderr)
        sys.exit(1)
    config_sets_json = Json(config_sets)

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.username,
            password=args.password,
            connect_timeout=30,
            sslmode="require",
        )
        print(f"Connected to PostgreSQL at {args.host}:{args.port}", file=sys.stderr)
    except Exception as err:
        print(f"Failed to connect to PostgreSQL: {err}", file=sys.stderr)
        sys.exit(1)

    try:
        if args.operation == "populate":
            if not args.repo:
                print("Error: --repo is required for populate", file=sys.stderr)
                sys.exit(1)
            if not args.module_repo:
                print("Error: --module-repo is required for populate", file=sys.stderr)
                sys.exit(1)
            if not config_name:
                print("Error: --config-file is required for populate", file=sys.stderr)
                sys.exit(1)
            if not args.architecture:
                print("Error: architecture could not be determined", file=sys.stderr)
                sys.exit(1)

            populate_module_commits(
                conn=conn,
                repo=args.repo,
                branch=args.branch,
                module_repo=args.module_repo,
                module_branch=args.module_branch,
                architecture=args.architecture,
                module_name=args.module_name,
                config_name=config_name,
                config_sets=config_sets,
                config_sets_json=config_sets_json,
            )

        elif args.operation == "fetch-next":
            if args.max_pairs < 1:
                print("Error: --max-pairs must be >= 1", file=sys.stderr)
                sys.exit(1)
            if not config_name:
                print(
                    "Error: --config-file is required for fetch-next", file=sys.stderr
                )
                sys.exit(1)
            if not args.architecture:
                print("Error: architecture could not be determined", file=sys.stderr)
                sys.exit(1)

            pairs = fetch_next_module_commits(
                conn=conn,
                module_name=args.module_name,
                config_name=config_name,
                config_sets_json=config_sets_json,
                architecture=args.architecture,
                max_pairs=args.max_pairs,
            )
            # Output pairs to stdout for workflow to capture
            print(" ".join(pairs))

        elif args.operation == "mark-complete":
            if not args.pairs:
                print(
                    "Error: core_sha:module_sha pairs are required for mark-complete",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not config_name:
                print(
                    "Error: --config-file is required for mark-complete",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not args.architecture:
                print("Error: architecture could not be determined", file=sys.stderr)
                sys.exit(1)

            # Validate pair format
            for pair in args.pairs:
                if ":" not in pair:
                    print(
                        f"Error: invalid pair format '{pair}', expected 'core_sha:module_sha'",
                        file=sys.stderr,
                    )
                    sys.exit(1)

            mark_module_commits(
                conn=conn,
                module_name=args.module_name,
                pairs=args.pairs,
                config_name=config_name,
                config_sets_json=config_sets_json,
                architecture=args.architecture,
            )

        elif args.operation == "cleanup":
            if not config_name:
                print("Error: --config-file is required for cleanup", file=sys.stderr)
                sys.exit(1)
            if not args.architecture:
                print("Error: architecture could not be determined", file=sys.stderr)
                sys.exit(1)

            cleanup_module_commits(
                conn=conn,
                module_name=args.module_name,
                config_name=config_name,
                config_sets_json=config_sets_json,
                architecture=args.architecture,
            )

    except psycopg2.IntegrityError as e:
        print(
            f"Database integrity error (likely NULL in NOT NULL field): {e}",
            file=sys.stderr,
        )
        conn.rollback()
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
