#!/usr/bin/env python3
"""PostgreSQL-based commit tracking for benchmarks."""

import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional

import psycopg2
from psycopg2.extras import Json


def create_tables(conn):
    """Create benchmark tracking tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_commits (
                id SERIAL PRIMARY KEY,
                sha VARCHAR(40) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL CHECK (status IN ('in_progress', 'complete')),
                config JSONB NOT NULL,
                architecture VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                
                -- Unique constraint: same commit + config + architecture can only exist once
                CONSTRAINT unique_sha_config_arch UNIQUE(sha, config, architecture)
            );
            
            -- Indexes for fast lookups
            CREATE INDEX IF NOT EXISTS idx_commits_sha ON benchmark_commits(sha);
            CREATE INDEX IF NOT EXISTS idx_commits_status ON benchmark_commits(status);
            CREATE INDEX IF NOT EXISTS idx_commits_timestamp ON benchmark_commits(timestamp);
            CREATE INDEX IF NOT EXISTS idx_commits_config ON benchmark_commits USING GIN(config);
            CREATE INDEX IF NOT EXISTS idx_commits_sha_status ON benchmark_commits(sha, status);
        """)
    conn.commit()
    print("Created/verified benchmark_commits table", file=sys.stderr)


def _git_rev_list(repo: Path, branch: str) -> List[str]:
    """Get list of commit SHAs from git."""
    proc = subprocess.run(
        ["git", "rev-list", branch],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip().splitlines()


def _git_commit_time(repo: Path, sha: str) -> str:
    """Get commit timestamp."""
    proc = subprocess.run(
        ["git", "show", "-s", "--format=%cI", sha],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def mark_commits(
    conn,
    repo: Path,
    shas: List[str],
    status: str,
    architecture: str,
    config: Optional[dict] = None,
) -> None:
    """Mark commits with status, architecture, and config.

    Args:
        conn: PostgreSQL connection
        repo: Path to git repository
        shas: List of commit SHAs to mark
        status: Status to set ('in_progress', 'complete')
        architecture: Architecture (e.g., 'x86_64', 'arm64')
        config: Config content (dict/list) to track
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        for sha in shas:
            # Resolve HEAD to actual commit SHA
            if sha == "HEAD":
                sha = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=repo, text=True
                ).strip()

            ts = _git_commit_time(repo, sha)

            # Insert or update
            cur.execute(
                """
                INSERT INTO benchmark_commits (sha, status, config, timestamp, architecture)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sha, config, architecture) 
                DO UPDATE SET 
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """,
                (sha, status, Json(config) if config else Json({}), ts, architecture),
            )

            # Format config for display
            config_display = ""
            if config:
                if isinstance(config, list) and len(config) > 0:
                    first_cfg = config[0]
                    config_display = f" (config: io-threads={first_cfg.get('io-threads', 'N/A')}, cluster={first_cfg.get('cluster_mode', 'N/A')})"

            print(
                f"Marked {sha} (on {architecture}) as {status} with timestamp {ts}{config_display}",
                file=sys.stderr,
            )

    conn.commit()


def cleanup_incomplete_commits(conn) -> int:
    """Remove all 'in_progress' entries.

    Returns:
        Number of entries cleaned up
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM benchmark_commits 
            WHERE status = 'in_progress'
            RETURNING id
        """)
        count = cur.rowcount

    conn.commit()

    if count > 0:
        print(f"Cleaned up {count} incomplete commits", file=sys.stderr)

    return count


def _is_list_subset(subset_list: List, superset_list: List) -> bool:
    """Check if all elements in subset_list exist in superset_list."""
    if not isinstance(subset_list, list) or not isinstance(superset_list, list):
        return False
    return all(item in superset_list for item in subset_list)


def _is_config_subset(subset_config: dict, superset_config: dict) -> bool:
    """Check if subset_config is a subset of superset_config.

    A config is considered a subset if:
    1. All non-list fields match exactly
    2. All list fields in subset are subsets of corresponding superset lists

    Args:
        subset_config: The config to check if it's a subset
        superset_config: The config to check against

    Returns:
        True if subset_config is a subset of superset_config
    """
    if not isinstance(subset_config, dict) or not isinstance(superset_config, dict):
        return False

    # Check each field in subset_config
    for key, subset_value in subset_config.items():
        if key not in superset_config:
            return False

        superset_value = superset_config[key]

        # If both are lists, check if subset is contained in superset
        if isinstance(subset_value, list) and isinstance(superset_value, list):
            if not _is_list_subset(subset_value, superset_value):
                return False
        # For non-list values, they must match exactly
        elif subset_value != superset_value:
            return False

    return True


def _is_config_array_subset(
    subset_config: List[dict], superset_config: List[dict]
) -> bool:
    """Check if subset config array is a subset of superset config array.

    For config arrays (like benchmark configs), we check if each config object
    in the subset has a corresponding superset in the superset config array.

    Args:
        subset_config: List of config objects to check
        superset_config: List of config objects to check against

    Returns:
        True if all subset configs have corresponding supersets
    """
    if not isinstance(subset_config, list) or not isinstance(superset_config, list):
        return False

    # Each config in subset must have a superset match
    for subset_cfg in subset_config:
        found_superset = False
        for superset_cfg in superset_config:
            if _is_config_subset(subset_cfg, superset_cfg):
                found_superset = True
                break

        if not found_superset:
            return False

    return True


def _find_superset_configs(
    conn, sha: str, target_config: dict, architecture: str
) -> List[dict]:
    """Find completed configs for a commit that are supersets of target_config.

    Args:
        conn: PostgreSQL connection
        sha: Commit SHA to check
        target_config: Config to find supersets for
        architecture: Architecture to filter by

    Returns:
        List of superset configs found
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT config FROM benchmark_commits
            WHERE sha = %s AND status = 'complete' AND architecture = %s
        """,
            (sha, architecture),
        )

        completed_configs = [row[0] for row in cur.fetchall()]
        superset_configs = []

        for completed_config in completed_configs:
            # Handle both single config objects and config arrays
            if isinstance(target_config, list) and isinstance(completed_config, list):
                if _is_config_array_subset(target_config, completed_config):
                    superset_configs.append(completed_config)
            elif isinstance(target_config, dict) and isinstance(completed_config, dict):
                if _is_config_subset(target_config, completed_config):
                    superset_configs.append(completed_config)

        return superset_configs


def determine_commits_to_benchmark(
    conn,
    repo: Path,
    branch: str,
    max_commits: int,
    architecture: str,
    config: Optional[dict] = None,
    enable_subset_detection: bool = True,
) -> List[str]:
    """Return up to max_commits SHAs not benchmarked with the given config and architecture.

    Args:
        conn: PostgreSQL connection
        repo: Path to git repository
        branch: Git branch to examine
        max_commits: Maximum number of commits to return
        architecture: Architecture to filter by
        config: Config content to check against
        enable_subset_detection: If True, skip commits that have superset configs completed

    Returns:
        List of commit SHAs that need benchmarking
    """
    # Ensure tables exist
    create_tables(conn)

    # Clean up incomplete commits first
    cleanup_incomplete_commits(conn)

    # Get all commits from git
    all_shas = _git_rev_list(repo, branch)

    # Get completed commits for exact config match
    with conn.cursor() as cur:
        if config:
            cur.execute(
                """
                SELECT DISTINCT sha FROM benchmark_commits
                WHERE status = 'complete' AND config = %s AND architecture = %s
            """,
                (Json(config), architecture),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT sha FROM benchmark_commits
                WHERE status = 'complete' AND architecture = %s
            """,
                (architecture,),
            )

        exact_completed = {row[0] for row in cur.fetchall()}

    # Find commits that need benchmarking
    commits = []
    subset_skipped = 0

    for sha in all_shas:
        # Skip if exact config match exists
        if sha in exact_completed:
            continue

        # Check for subset detection if enabled and config is provided
        if enable_subset_detection and config:
            superset_configs = _find_superset_configs(conn, sha, config, architecture)
            if superset_configs:
                subset_skipped += 1
                # Format superset info for display
                superset_info = ""
                if (
                    isinstance(config, list)
                    and len(config) > 0
                    and len(superset_configs) > 0
                ):
                    subset_cfg = config[0]
                    superset_cfg = superset_configs[0]
                    if isinstance(superset_cfg, list) and len(superset_cfg) > 0:
                        superset_cfg = superset_cfg[0]

                    subset_data_sizes = subset_cfg.get("data_sizes", [])
                    superset_data_sizes = superset_cfg.get("data_sizes", [])

                    if subset_data_sizes and superset_data_sizes:
                        superset_info = f" (subset {subset_data_sizes} found in superset {superset_data_sizes})"

                print(
                    f"Skipping {sha[:8]} - subset config already benchmarked{superset_info}",
                    file=sys.stderr,
                )
                continue

        commits.append(sha)
        if len(commits) >= max_commits:
            break

    if subset_skipped > 0:
        print(
            f"Subset detection: skipped {subset_skipped} commits with existing superset configs",
            file=sys.stderr,
        )

    return commits


def get_commits_by_config(
    conn, architecture: str, config: Optional[dict] = None
) -> List[Dict]:
    """Get commits filtered by architecture and config.

    Args:
        conn: PostgreSQL connection
        architecture: Architecture to filter by
        config: Config to filter by (None returns all for the architecture)

    Returns:
        List of commit entries
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        if config:
            cur.execute(
                """
                SELECT sha, timestamp, status, config, architecture
                FROM benchmark_commits
                WHERE config = %s AND architecture = %s
                ORDER BY timestamp DESC
            """,
                (Json(config), architecture),
            )
        else:
            cur.execute(
                """
                SELECT sha, timestamp, status, config, architecture
                FROM benchmark_commits
                WHERE architecture = %s
                ORDER BY timestamp DESC
            """,
                (architecture,),
            )

        results = []
        for row in cur.fetchall():
            results.append(
                {
                    "sha": row[0],
                    "timestamp": row[1].isoformat(),
                    "status": row[2],
                    "config": row[3],
                    "architecture": row[4],
                }
            )

        return results


def get_unique_configs(conn) -> List[dict]:
    """Get list of unique config objects used.

    Args:
        conn: PostgreSQL connection

    Returns:
        List of unique configs
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT config
            FROM benchmark_commits
        """)
        return [row[0] for row in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(
        description="PostgreSQL-based commit tracking for benchmarks"
    )

    parser.add_argument(
        "operation",
        choices=["determine", "mark", "query", "cleanup"],
        help="Operation to perform",
    )

    # Database connection arguments
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--username", required=True, help="Database username")
    parser.add_argument(
        "--password", required=True, help="Database password (or use IAM auth)"
    )

    # Arguments for determine operation
    parser.add_argument(
        "--repo", type=Path, help="Git repository path (for determine/mark)"
    )
    parser.add_argument(
        "--branch", default="unstable", help="Git branch (for determine)"
    )
    parser.add_argument(
        "--max-commits",
        type=int,
        default=3,
        help="Max commits to return (for determine)",
    )
    parser.add_argument("--config-file", type=str, help="Config file to load")
    parser.add_argument(
        "--disable-subset-detection",
        action="store_true",
        help="Disable subset config detection (for determine)",
    )
    parser.add_argument(
        "--architecture",
        type=str,
        help="Architecture (e.g., x86_64, arm64). Auto-detected if not provided.",
    )

    # Arguments for mark operation
    parser.add_argument(
        "--status", choices=["in_progress", "complete"], help="Status to set (for mark)"
    )

    # Arguments for query operation
    parser.add_argument(
        "--list-configs",
        action="store_true",
        help="List all unique configs (for query)",
    )

    # Parse known args first to get the operation
    args, remaining_args = parser.parse_known_args()

    # Add shas argument only for mark operation
    if args.operation == "mark":
        parser.add_argument("shas", nargs="+", help="Commit SHAs (required for mark)")
        args = parser.parse_args()
    elif remaining_args:
        # If there are remaining args for non-mark operations, it's an error
        parser.error(f"unrecognized arguments: {' '.join(remaining_args)}")

    # Auto-detect architecture if not provided
    if not args.architecture:
        args.architecture = platform.machine()
        print(f"Auto-detected architecture: {args.architecture}", file=sys.stderr)

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
        if args.operation == "determine":
            if not args.repo:
                print(
                    "Error: --repo is required for determine operation", file=sys.stderr
                )
                sys.exit(1)

            config = None
            if args.config_file:
                with open(args.config_file, "r") as f:
                    config = json.load(f)

            enable_subset_detection = not args.disable_subset_detection
            commits = determine_commits_to_benchmark(
                conn=conn,
                repo=args.repo,
                branch=args.branch,
                max_commits=args.max_commits,
                architecture=args.architecture,
                config=config,
                enable_subset_detection=enable_subset_detection,
            )
            print(" ".join(commits))

        elif args.operation == "mark":
            if not args.repo:
                print("Error: --repo is required for mark operation", file=sys.stderr)
                sys.exit(1)
            if not args.status:
                print("Error: --status is required for mark operation", file=sys.stderr)
                sys.exit(1)
            if not args.shas:
                print(
                    "Error: commit SHAs are required for mark operation",
                    file=sys.stderr,
                )
                sys.exit(1)

            config = None
            if args.config_file:
                with open(args.config_file, "r") as f:
                    config = json.load(f)

            mark_commits(
                conn=conn,
                repo=args.repo,
                shas=args.shas,
                status=args.status,
                architecture=args.architecture,
                config=config,
            )

        elif args.operation == "query":
            config = None
            if args.config_file:
                with open(args.config_file, "r") as f:
                    config = json.load(f)

            if args.list_configs:
                configs = get_unique_configs(conn)
                print(f"Unique configs used: {len(configs)}", file=sys.stderr)
                for i, cfg in enumerate(configs, 1):
                    summary = ""
                    if isinstance(cfg, list) and len(cfg) > 0:
                        first = cfg[0]
                        summary = f"(io-threads={first.get('io-threads', 'N/A')}, cluster={first.get('cluster_mode', 'N/A')}, tls={first.get('tls_mode', 'N/A')})"
                    print(f"  Config {i}: {summary}", file=sys.stderr)
            else:
                commits = get_commits_by_config(conn, args.architecture, config)
                count = len(commits)
                if config:
                    summary = ""
                    if isinstance(config, list) and len(config) > 0:
                        cfg = config[0]
                        summary = f" (io-threads={cfg.get('io-threads', 'N/A')}, cluster={cfg.get('cluster_mode', 'N/A')}, tls={cfg.get('tls_mode', 'N/A')})"
                    print(
                        f"Config{summary} on {args.architecture}: {count} commits",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"All commits on {args.architecture}: {count}", file=sys.stderr
                    )

        elif args.operation == "cleanup":
            cleanup_incomplete_commits(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
