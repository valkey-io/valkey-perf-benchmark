#!/usr/bin/env python3
"""PostgreSQL-based module commit tracking for benchmarks.

Tracks which (core_sha, module_sha) pairs have been benchmarked,
enabling 2D commit discovery across both valkey and module repositories.
"""

import argparse
import json
import platform
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import psycopg2
from psycopg2.extras import Json, execute_values

from datetime import datetime

from git_utils import git_rev_list_with_timestamps

SKIP_CONFIG_SETS_SENTINEL = [{"config": "default"}]


def _parse_timestamp(ts) -> datetime:
    """Convert a timestamp (string or datetime) to a timezone-aware datetime.

    Raises ValueError if the result has no timezone info or input is malformed.
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            raise ValueError(f"Naive datetime not allowed (missing timezone): {ts}")
        return ts
    if not isinstance(ts, str) or not ts.strip():
        raise ValueError(f"Invalid timestamp: {ts!r}")
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp missing timezone: {ts!r}")
    return dt


@dataclass
class CommitPair:
    """Represents a (core_sha, module_sha) pair to be benchmarked.

    Validates required fields on creation. Status and priority are assigned
    in-memory before any DB insert, ensuring no row is ever incomplete.
    """

    core_sha: str
    module_sha: str
    core_timestamp: datetime
    module_timestamp: datetime
    max_commit_timestamp: datetime
    min_commit_timestamp: datetime
    config_name: str
    config_sets: List[dict]
    architecture: str
    cluster_mode: List[bool]
    status: str = "pending"
    priority: Optional[int] = None

    def __post_init__(self):
        required = {
            "core_sha": self.core_sha,
            "module_sha": self.module_sha,
            "core_timestamp": self.core_timestamp,
            "module_timestamp": self.module_timestamp,
            "max_commit_timestamp": self.max_commit_timestamp,
            "min_commit_timestamp": self.min_commit_timestamp,
            "config_name": self.config_name,
            "config_sets": self.config_sets,
            "architecture": self.architecture,
            "cluster_mode": self.cluster_mode,
        }
        missing = [k for k, v in required.items() if v is None or v == ""]
        if missing:
            raise ValueError(
                f"CommitPair missing required fields: {missing} "
                f"(pair={self.core_sha}:{self.module_sha})"
            )

    def is_ready_to_insert(self) -> bool:
        """Check if all fields including status and priority are set."""
        return self.priority is not None and self.status is not None

    def to_insert_tuple(self) -> tuple:
        """Return tuple for SQL INSERT. Wraps config_sets and cluster_mode as Json() at insert time."""
        return (
            self.core_sha,
            self.module_sha,
            self.core_timestamp,
            self.module_timestamp,
            self.max_commit_timestamp,
            self.min_commit_timestamp,
            self.status,
            self.priority,
            self.config_name,
            Json(self.config_sets),
            self.architecture,
            Json(self.cluster_mode),
        )


def resolve_cluster_modes(
    raw_cluster_mode, cli_cluster_mode: Optional[str] = None
) -> Optional[List[bool]]:
    """Resolve cluster_modes from config file value and optional CLI override.

    Args:
        raw_cluster_mode: Value from config JSON's "cluster_mode" key (bool, list, or None).
        cli_cluster_mode: Optional CLI argument string ("true" or "false").
            If provided, overrides the config value with a single-element list.

    Returns:
        List of booleans (e.g., [False], [True], [False, True]), or None if
        neither source provides a value.
    """
    # CLI override takes precedence (only accepts "true" or "false")
    if cli_cluster_mode is not None:
        return [cli_cluster_mode == "true"]

    # Fall back to config file value
    if isinstance(raw_cluster_mode, bool):
        return [raw_cluster_mode]
    elif isinstance(raw_cluster_mode, list):
        return [bool(v) for v in raw_cluster_mode]
    return None


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
    if not re.match(r"^[a-z][a-z0-9_]{0,30}$", module_name):
        raise ValueError(f"Invalid module_name: {module_name}")
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


def _create_module_table(conn, module_name: str) -> None:
    """Create module benchmark queue table if it doesn't exist.

    Queue-based design: pairs are inserted as 'pending', classified with
    priority (1=forward, 2=fallback, 99=for completed as subset), and fetched in priority order.
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
                status VARCHAR(20) NOT NULL
                    CHECK (status IN ('pending', 'in_progress', 'completed', 'completed_as_subset')),
                priority INTEGER,
                config_name VARCHAR(255) NOT NULL,
                config_sets JSONB NOT NULL,
                architecture VARCHAR(50),
                cluster_mode JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT unique_{table}_sha_module_config_arch
                    UNIQUE(sha, module_sha, config_name, config_sets, architecture, cluster_mode)
            );

            CREATE INDEX IF NOT EXISTS idx_{table}_sha ON {table}(sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_module_sha ON {table}(module_sha);
            CREATE INDEX IF NOT EXISTS idx_{table}_status ON {table}(status);
            CREATE INDEX IF NOT EXISTS idx_{table}_config_name ON {table}(config_name);
            CREATE INDEX IF NOT EXISTS idx_{table}_sha_status ON {table}(sha, status);
            CREATE INDEX IF NOT EXISTS idx_{table}_fetch_order
                ON {table}(status, created_at DESC, priority ASC,
                           max_commit_timestamp DESC, min_commit_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_{table}_created_order
                ON {table}(created_at DESC, priority ASC,
                           max_commit_timestamp DESC, min_commit_timestamp DESC);
        """
        )
    conn.commit()
    print(f"Created/verified {table} table", file=sys.stderr)


def _mark_subset_pairs_in_memory(
    conn,
    pairs: List[CommitPair],
    table: str,
    config_name: str,
    config_sets: List[dict],
    architecture: str,
    cluster_mode: List[bool],
) -> int:
    """Mark pairs as completed_as_subset if a superset config already exists.

    Checks if any completed row in the DB has both config_sets and cluster_mode
    that are supersets of the current values. If yes, marks matching
    (sha, module_sha) pairs as completed_as_subset.

    Args:
        conn: PostgreSQL connection
        pairs: List of CommitPair objects to check
        table: Table name
        config_name: Config file name
        config_sets: Current config_sets (raw list for subset comparison)
        architecture: Architecture
        cluster_mode: Current cluster_mode list (e.g., [false] or [false, true])

    Returns:
        Number of pairs marked as completed_as_subset
    """
    with conn.cursor() as cur:
        # Find all distinct completed (config_sets, cluster_mode) combos
        cur.execute(
            f"""
            SELECT DISTINCT config_sets, cluster_mode FROM {table}
            WHERE status IN ('completed', 'completed_as_subset')
              AND config_name = %s AND architecture = %s
        """,
            (config_name, architecture),
        )
        completed_combos = [(row[0], row[1]) for row in cur.fetchall()]

    print(
        f"  Subset check: found {len(completed_combos)} distinct completed (config_sets, cluster_mode) combos in DB",
        file=sys.stderr,
    )

    # Find combos where BOTH config_sets and cluster_mode are supersets of ours
    superset_combos = [
        (cs, cm)
        for cs, cm in completed_combos
        if _is_config_sets_subset(config_sets, cs)
        and _is_config_sets_subset(cluster_mode, cm)
    ]

    print(
        f"  Subset check: {len(superset_combos)} of those are supersets of current config",
        file=sys.stderr,
    )

    if not superset_combos:
        return 0

    # Get (sha, module_sha) pairs that are completed with a superset combo
    completed_pairs = set()
    with conn.cursor() as cur:
        combo_tuples = tuple((Json(cs), Json(cm)) for cs, cm in superset_combos)
        cur.execute(
            f"""
            SELECT sha, module_sha FROM {table}
            WHERE status = 'completed'
              AND config_name = %s AND architecture = %s
              AND (config_sets, cluster_mode) IN %s
        """,
            (config_name, architecture, combo_tuples),
        )
        completed_pairs = {(row[0], row[1]) for row in cur.fetchall()}

    print(
        f"  Subset check: {len(completed_pairs)} completed pairs found from superset configs",
        file=sys.stderr,
    )

    # Mark matching pairs in memory
    count = 0
    for pair in pairs:
        if (pair.core_sha, pair.module_sha) in completed_pairs:
            pair.status = "completed_as_subset"
            pair.priority = 99
            count += 1

    return count


def _assign_priority_in_memory(
    conn,
    pairs: List[CommitPair],
    table: str,
    config_name: str,
    config_sets: List[dict],
    architecture: str,
    cluster_mode: List[bool],
) -> None:
    """Assign priority to pairs that are still pending (not subset-completed).

    Derives pointer from the newest completed pair for this config+arch+cluster_mode.
    - Forward (priority=1): both timestamps >= pointer (includes pointer row & column)
    - Fallback (priority=2): at least one timestamp < pointer (old core × new module, or new core × old module)
    - No pointer (first run): all get priority=1
    - Completed as subset pairs (priority=99) are skipped

    Args:
        conn: PostgreSQL connection
        pairs: List of CommitPair objects (modifies in-place)
        table: Table name
        config_name: Config file name
        config_sets: List of module runtime configs
        architecture: Architecture
        cluster_mode: List of cluster mode booleans
    """
    config_sets_json = Json(config_sets)
    cluster_mode_json = Json(cluster_mode)
    # Find pointer: max_commit_timestamp of the newest completed pair
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT core_timestamp, module_timestamp FROM {table}
            WHERE status IN ('completed', 'completed_as_subset')
              AND config_name = %s AND config_sets = %s AND architecture = %s
              AND cluster_mode = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT 1
        """,
            (config_name, config_sets_json, architecture, cluster_mode_json),
        )
        pointer_row = cur.fetchone()

    forward_count = 0
    fallback_count = 0

    for pair in pairs:
        # Skip already-assigned pairs (e.g., completed_as_subset)
        if pair.priority is not None:
            continue

        if pointer_row is None:
            # No pointer — all forward
            pair.priority = 1
            forward_count += 1
        else:
            pointer_core_ts = _parse_timestamp(pointer_row[0])
            pointer_module_ts = _parse_timestamp(pointer_row[1])
            # Forward: both core and module at or newer than pointer
            if (
                pair.core_timestamp >= pointer_core_ts
                and pair.module_timestamp >= pointer_module_ts
            ):
                pair.priority = 1
                forward_count += 1
            else:
                pair.priority = 2
                fallback_count += 1

    print(
        f"Priority assigned in memory: {forward_count} forward, {fallback_count} fallback",
        file=sys.stderr,
    )


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
    cluster_mode: List[bool],
    max_core_commits: Optional[int] = None,
    max_module_commits: Optional[int] = None,
) -> int:
    """Insert all new (core_sha, module_sha) combos into the queue table.

    Computes the cartesian product of core commits and module commits,
    filters out pairs already in the table, assigns priority in memory,
    then batch-inserts all new pairs in a single transaction.

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
        cluster_mode: List of cluster mode booleans (e.g., [false] or [false, true])

    Returns:
        Number of new rows inserted
    """

    # Get commits and timestamps from both repos in one subprocess each
    core_timestamps = git_rev_list_with_timestamps(
        repo, branch, max_count=max_core_commits
    )
    module_timestamps = git_rev_list_with_timestamps(
        module_repo, module_branch, max_count=max_module_commits
    )
    core_shas = list(core_timestamps.keys())
    module_shas = list(module_timestamps.keys())
    print(
        f"Scanned {len(core_shas)} core commits "
        f"(limited to most recent {max_core_commits}), "
        f"{len(module_shas)} module commits "
        f"(limited to most recent {max_module_commits})",
        file=sys.stderr,
    )
    print(
        f"Cached {len(core_timestamps)} core + {len(module_timestamps)} module timestamps",
        file=sys.stderr,
    )

    # Get existing (sha, module_sha) pairs for this config+arch+cluster_mode to avoid redundant inserts
    table = _module_table_name(module_name)
    config_sets_json = Json(config_sets)
    cluster_mode_json = Json(cluster_mode)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT sha, module_sha FROM {table}
            WHERE config_name = %s AND config_sets = %s AND architecture = %s
              AND cluster_mode = %s
        """,
            (config_name, config_sets_json, architecture, cluster_mode_json),
        )
        existing = {(row[0], row[1]) for row in cur.fetchall()}
    print(f"Found {len(existing)} existing pairs in {table}", file=sys.stderr)

    # Step 1: Build CommitPair objects in memory
    pairs: List[CommitPair] = []
    for core_sha in core_shas:
        core_ts = core_timestamps[core_sha]
        for module_sha in module_shas:
            if (core_sha, module_sha) in existing:
                continue
            module_ts = module_timestamps[module_sha]
            core_dt = _parse_timestamp(core_ts)
            module_dt = _parse_timestamp(module_ts)
            pairs.append(
                CommitPair(
                    core_sha=core_sha,
                    module_sha=module_sha,
                    core_timestamp=core_dt,
                    module_timestamp=module_dt,
                    max_commit_timestamp=max(core_dt, module_dt),
                    min_commit_timestamp=min(core_dt, module_dt),
                    config_name=config_name,
                    config_sets=config_sets,
                    architecture=architecture,
                    cluster_mode=cluster_mode,
                )
            )
    print(f"Built {len(pairs)} new CommitPair objects", file=sys.stderr)

    if not pairs:
        print("No new pairs to insert", file=sys.stderr)
        return 0

    # Step 2: Check for subset — find completed superset config_sets and cluster_mode
    completed_as_subset = _mark_subset_pairs_in_memory(
        conn, pairs, table, config_name, config_sets, architecture, cluster_mode
    )
    print(
        f"Subset detection: {completed_as_subset} pairs marked as completed_as_subset",
        file=sys.stderr,
    )

    # Step 3: Assign priority in memory (for pairs not marked as subset)
    _assign_priority_in_memory(
        conn, pairs, table, config_name, config_sets, architecture, cluster_mode
    )

    # Step 4: Validate — all pairs must be ready before insert
    not_ready = [p for p in pairs if not p.is_ready_to_insert()]
    if not_ready:
        raise ValueError(
            f"{len(not_ready)} pairs not ready to insert "
            f"(missing priority or status). Aborting."
        )

    # Step 5: Batch insert — single transaction, all or nothing
    insert_sql = f"""
        INSERT INTO {table} (sha, module_sha, core_timestamp,
                             module_timestamp, max_commit_timestamp,
                             min_commit_timestamp, status, priority,
                             config_name, config_sets, architecture,
                             cluster_mode)
        VALUES %s
    """
    data = [pair.to_insert_tuple() for pair in pairs]

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, data)
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        raise ValueError(
            f"Unexpected duplicate row during insert. "
            f"This should not happen — existing pairs were filtered beforehand. "
            f"Error: {e}"
        ) from e
    print(
        f"Populated {table}: {len(pairs)} new pairs inserted "
        f"({len(core_shas)} core × {len(module_shas)} module commits)",
        file=sys.stderr,
    )

    return len(pairs)


def fetch_next_module_commits(
    conn,
    module_name: str,
    config_name: str,
    config_sets: List[dict],
    architecture: str,
    cluster_mode: List[bool],
    max_pairs: int = 1,
) -> List[str]:
    """Fetch the next batch of pending pairs and mark them as in_progress.

    Only fetches pairs matching the given config_name, config_sets, architecture,
    and cluster_mode.

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
        config_sets: List of module runtime configs
        architecture: Architecture to match
        cluster_mode: List of cluster mode booleans to match
        max_pairs: Maximum number of pairs to fetch

    Returns:
        Tuple of (pairs, max_timestamps) where:
            pairs: List of 'core_sha:module_sha' strings marked as in_progress
            max_timestamps: List of max_commit_timestamp ISO strings (same order)
    """
    table = _module_table_name(module_name)
    config_sets_json = Json(config_sets)
    cluster_mode_json = Json(cluster_mode)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, sha, module_sha, max_commit_timestamp FROM {table}
            WHERE status = 'pending'
              AND config_name = %s AND config_sets = %s AND architecture = %s
              AND cluster_mode = %s
            ORDER BY created_at DESC, priority ASC,
                     max_commit_timestamp DESC, min_commit_timestamp DESC
            LIMIT %s
        """,
            (config_name, config_sets_json, architecture, cluster_mode_json, max_pairs),
        )
        rows = cur.fetchall()

        if not rows:
            print("No pending pairs to fetch", file=sys.stderr)
            return [], []

        ids = [row[0] for row in rows]
        pairs = [f"{row[1]}:{row[2]}" for row in rows]
        max_timestamps = [row[3].isoformat() for row in rows]

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
    return pairs, max_timestamps


def mark_module_commits(
    conn,
    module_name: str,
    pairs: List[str],
    config_name: str,
    config_sets: List[dict],
    architecture: str,
    cluster_mode: List[bool],
) -> int:
    """Mark specific pairs as completed.

    Updates status from 'in_progress' to 'completed' for the given pairs.
    Only matches rows with the same config_name, config_sets, architecture,
    and cluster_mode.

    Args:
        conn: PostgreSQL connection
        module_name: Module name (determines table)
        pairs: List of 'core_sha:module_sha' strings to mark complete
        config_name: Config file name to match
        config_sets: List of module runtime configs
        architecture: Architecture to match
        cluster_mode: List of cluster mode booleans to match

    Returns:
        Number of rows updated
    """
    table = _module_table_name(module_name)
    config_sets_json = Json(config_sets)
    cluster_mode_json = Json(cluster_mode)
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
                  AND cluster_mode = %s
            """,
                (
                    core_sha,
                    module_sha,
                    config_name,
                    config_sets_json,
                    architecture,
                    cluster_mode_json,
                ),
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
    conn,
    module_name: str,
    config_name: str,
    config_sets: List[dict],
    architecture: str,
    cluster_mode: List[bool],
) -> int:
    """Reset 'in_progress' entries back to 'pending' for a specific config+config_sets+arch+cluster_mode.

    Called at the start of each run to retry pairs from previous
    failed runs. Unlike core (which deletes all), we reset to pending
    so the pair stays in the queue and gets retried. Scoped to the
    current config+config_sets+arch+cluster_mode to avoid affecting other configs.

    Returns:
        Number of entries reset
    """
    table = _module_table_name(module_name)
    config_sets_json = Json(config_sets)
    cluster_mode_json = Json(cluster_mode)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table}
            SET status = 'pending', updated_at = NOW()
            WHERE status = 'in_progress'
              AND config_name = %s AND config_sets = %s AND architecture = %s
              AND cluster_mode = %s
        """,
            (config_name, config_sets_json, architecture, cluster_mode_json),
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
        choices=[
            "populate",
            "fetch-next",
            "mark-complete",
            "cleanup",
        ],
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
    parser.add_argument(
        "--max-core-commits",
        type=int,
        default=None,
        help="Max core commits to scan during populate (default: all)",
    )
    parser.add_argument(
        "--max-module-commits",
        type=int,
        default=None,
        help="Max module commits to scan during populate (default: all)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=60000,
        help="Idle-in-transaction timeout in ms (default: 60000)",
    )
    parser.add_argument(
        "--cluster-mode",
        type=str,
        choices=["true", "false"],
        default=None,
        help="Cluster mode (true/false). Overwrites cluster_mode from config file.",
    )
    parser.add_argument(
        "--skip-config-set",
        action="store_true",
        help="Skip config_sets from config file and use [{'default': true}] instead. "
        "Use when benchmarking with server defaults (no CONFIG SET commands).",
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
    raw_cluster_mode = None
    if args.config_file and Path(args.config_file).exists():
        with open(args.config_file) as f:
            cfg_data = json.load(f)
        if isinstance(cfg_data, list) and cfg_data:
            cfg_data = cfg_data[0]
        config_sets = cfg_data.get("config_sets")
        raw_cluster_mode = cfg_data.get("cluster_mode")

    # --skip-config-set overrides config_sets with a sentinel value
    if args.skip_config_set:
        config_sets = SKIP_CONFIG_SETS_SENTINEL
    elif not config_sets:
        print("Error: config_sets not found in config file", file=sys.stderr)
        sys.exit(1)

    # Resolve cluster_modes: CLI overrides config file
    cluster_modes = resolve_cluster_modes(raw_cluster_mode, args.cluster_mode)

    if not cluster_modes:
        print(
            "Error: cluster_mode not found in config file and --cluster-mode not provided",
            file=sys.stderr,
        )
        sys.exit(1)

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
            options=f"-c idle_in_transaction_session_timeout={args.idle_timeout}",
        )
        print(f"Connected to PostgreSQL at {args.host}:{args.port}", file=sys.stderr)
    except Exception as err:
        print(f"Failed to connect to PostgreSQL: {err}", file=sys.stderr)
        sys.exit(1)

    # Create table + indexes once, before dispatching to any operation
    _create_module_table(conn, args.module_name)

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
                cluster_mode=cluster_modes,
                max_core_commits=args.max_core_commits,
                max_module_commits=args.max_module_commits,
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

            pairs, max_timestamps = fetch_next_module_commits(
                conn=conn,
                module_name=args.module_name,
                config_name=config_name,
                config_sets=config_sets,
                architecture=args.architecture,
                cluster_mode=cluster_modes,
                max_pairs=args.max_pairs,
            )
            # Output pairs and timestamps on separate lines for workflow
            print(" ".join(pairs))
            print(" ".join(max_timestamps))

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
                config_sets=config_sets,
                architecture=args.architecture,
                cluster_mode=cluster_modes,
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
                config_sets=config_sets,
                architecture=args.architecture,
                cluster_mode=cluster_modes,
            )

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
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
