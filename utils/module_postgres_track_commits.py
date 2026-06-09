#!/usr/bin/env python3
"""PostgreSQL-based module commit tracking for benchmarks.

Tracks which (core_sha, module_sha) pairs have been benchmarked,
enabling 2D commit discovery across both valkey and module repositories.
"""

import argparse
import platform
import sys
from pathlib import Path
from typing import List, Optional

import psycopg2

from utils.postgres_track_commits import _git_rev_list, _git_commit_time


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


def create_module_table(conn, module_name: str) -> None:
    """Create module benchmark queue table if it doesn't exist.

    Queue-based design: pairs are inserted as 'pending', classified with
    priority (1=forward, 2=fallback), and fetched in priority order.
    """
    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                sha VARCHAR(40) NOT NULL,
                module_sha VARCHAR(40) NOT NULL,
                core_timestamp TIMESTAMPTZ NOT NULL,
                module_timestamp TIMESTAMPTZ NOT NULL,
                max_commit_timestamp TIMESTAMPTZ NOT NULL,
                min_commit_timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_progress', 'complete')),
                priority INTEGER,
                config_name VARCHAR(255) NOT NULL,
                config_set JSONB,
                architecture VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT unique_{table}_sha_module_config_arch
                    UNIQUE(sha, module_sha, config_name, architecture)
            );

            CREATE INDEX IF NOT EXISTS idx_{table}_sha ON {table}(sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_module_sha ON {table}(module_sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_status ON {table}(status);
            CREATE INDEX IF NOT EXISTS idx_{table}_config_name ON {table}(config_name);
            CREATE INDEX IF NOT EXISTS idx_{table}_sha_status ON {table}(sha, status);
            CREATE INDEX IF NOT EXISTS idx_{table}_fetch_order
                ON {table}(status, created_at DESC, priority ASC,
                           max_commit_timestamp DESC, min_commit_timestamp DESC);
        """
        )
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
) -> int:
    """Insert all new (core_sha, module_sha) pairs into the queue table.

    Computes the cartesian product of core and module git histories,
    then inserts any pairs not already in the table. Existing rows
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

    Returns:
        Number of new pairs inserted
    """
    create_module_table(conn, module_name)

    _check_null_priorities(conn, module_name, "start of populate_module_commits")

    # Clean up stale in_progress entries from previous failed runs
    cleanup_module_commits(conn, module_name, config_name, architecture)

    # Get all commits from both git repos (newest first)
    core_shas = _git_rev_list(repo, branch)
    module_shas = _git_rev_list(module_repo, module_branch)

    # Get existing pairs for this config+arch to avoid redundant inserts
    table = _module_table_name(module_name)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT sha, module_sha FROM {table}
            WHERE config_name = %s AND architecture = %s
        """,
            (config_name, architecture),
        )
        existing = {(row[0], row[1]) for row in cur.fetchall()}

    # Insert new pairs from the cartesian product
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
                                         config_name, architecture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sha, module_sha, config_name, architecture)
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
    _determine_priority(conn, module_name, config_name, architecture)

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
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE priority IS NULL")
        null_count = cur.fetchone()[0]

    if null_count > 0:
        print(
            f"WARNING [{context}]: {null_count} rows with NULL priority in {table}",
            file=sys.stderr,
        )
    return null_count


def _determine_priority(
    conn, module_name: str, config_name: str, architecture: str
) -> int:
    """Classify newly inserted pairs as forward (1) or fallback (2).

    Derives pointers from the newest completed pair for this config+arch:
      - pointer_core_ts = core_timestamp of newest completed row
      - pointer_module_ts = module_timestamp of newest completed row

    Classification (for rows where priority IS NULL):
      - Forward (1): core_timestamp >= pointer_core_ts AND
                     module_timestamp >= pointer_module_ts
      - Fallback (2): everything else

    If no completed pairs exist for this config+arch, all rows get
    priority=1 (treated as first run for this config).

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        config_name: Config file name to scope the pointer
        architecture: Architecture to scope the pointer

    Returns:
        Number of rows updated
    """
    table = _module_table_name(module_name)

    _check_null_priorities(conn, module_name, "before determine_priority")

    with conn.cursor() as cur:
        # Find pointers from the newest completed pair for this config+arch
        cur.execute(
            f"""
            SELECT core_timestamp, module_timestamp FROM {table}
            WHERE status = 'complete'
              AND config_name = %s AND architecture = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT 1
        """,
            (config_name, architecture),
        )
        pointer_row = cur.fetchone()

        if pointer_row is None:
            # No completed pairs — all get priority=1 (forward)
            cur.execute(
                f"""
                UPDATE {table}
                SET priority = 1, updated_at = NOW()
                WHERE priority IS NULL
            """
            )
            updated = cur.rowcount
        else:
            pointer_core_ts, pointer_module_ts = pointer_row

            # Forward: both timestamps >= pointer
            cur.execute(
                f"""
                UPDATE {table}
                SET priority = 1, updated_at = NOW()
                WHERE priority IS NULL
                  AND core_timestamp >= %s
                  AND module_timestamp >= %s
            """,
                (pointer_core_ts, pointer_module_ts),
            )
            forward_count = cur.rowcount

            # Fallback: everything else still NULL
            cur.execute(
                f"""
                UPDATE {table}
                SET priority = 2, updated_at = NOW()
                WHERE priority IS NULL
            """
            )
            fallback_count = cur.rowcount

            updated = forward_count + fallback_count
            print(
                f"Priority assigned: {forward_count} forward, {fallback_count} fallback",
                file=sys.stderr,
            )

    conn.commit()

    _check_null_priorities(conn, module_name, "after determine_priority")

    print(f"determine_priority: updated {updated} rows in {table}", file=sys.stderr)
    return updated


def fetch_next_module_commits(
    conn, module_name: str, config_name: str, architecture: str, max_pairs: int = 1
) -> List[str]:
    """Fetch the next batch of pending pairs and mark them as in_progress.

    Only fetches pairs matching the given config_name and architecture.

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
              AND config_name = %s AND architecture = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT %s
        """,
            (config_name, architecture, max_pairs),
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
    conn, module_name: str, pairs: List[str], config_name: str, architecture: str
) -> int:
    """Mark specific pairs as complete.

    Updates status from 'in_progress' to 'complete' for the given pairs.
    Only matches rows with the same config_name and architecture to avoid
    accidentally marking another config/arch's row.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        pairs: List of 'core_sha:module_sha' strings to mark complete
        config_name: Config file name to match
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
                SET status = 'complete', updated_at = NOW()
                WHERE sha = %s AND module_sha = %s
                  AND config_name = %s AND architecture = %s
            """,
                (core_sha, module_sha, config_name, architecture),
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
    conn, module_name: str, config_name: str, architecture: str
) -> int:
    """Reset 'in_progress' entries back to 'pending' for a specific config+arch.

    Called at the start of each run to retry pairs from previous
    failed runs. Unlike core (which deletes all), we reset to pending
    so the pair stays in the queue and gets retried. Scoped to the
    current config+arch to avoid affecting other configs.

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
              AND config_name = %s AND architecture = %s
        """,
            (config_name, architecture),
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

    # Extract config name from path
    config_name = get_config_name(args.config_file) if args.config_file else ""

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
