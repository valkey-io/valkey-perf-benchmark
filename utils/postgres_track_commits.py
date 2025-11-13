#!/usr/bin/env python3
"""PostgreSQL-based commit tracking for benchmarks."""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    print("Please install psycopg2 to use this script.")

def create_tables(conn):
    """Create benchmark tracking tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_commits (
                id SERIAL PRIMARY KEY,
                sha VARCHAR(40) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL CHECK (status IN ('in_progress', 'complete')),
                config JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                
                -- Unique constraint: same commit + config can only exist once
                CONSTRAINT unique_sha_config UNIQUE(sha, config)
            );
            
            -- Indexes for fast lookups
            CREATE INDEX IF NOT EXISTS idx_commits_sha ON benchmark_commits(sha);
            CREATE INDEX IF NOT EXISTS idx_commits_status ON benchmark_commits(status);
            CREATE INDEX IF NOT EXISTS idx_commits_timestamp ON benchmark_commits(timestamp);
            CREATE INDEX IF NOT EXISTS idx_commits_config ON benchmark_commits USING GIN(config);
            CREATE INDEX IF NOT EXISTS idx_commits_sha_status ON benchmark_commits(sha, status);
        """
        )
    conn.commit()
    print("Created/verified benchmark_commits table")


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
    conn, repo: Path, shas: List[str], status: str, config: Optional[dict] = None
) -> None:
    """Mark commits with status and config.

    Args:
        conn: PostgreSQL connection
        repo: Path to git repository
        shas: List of commit SHAs to mark
        status: Status to set ('in_progress', 'complete')
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
                INSERT INTO benchmark_commits (sha, status, config, timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (sha, config) 
                DO UPDATE SET 
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """,
                (sha, status, Json(config) if config else Json({}), ts),
            )

            # Format config for display
            config_display = ""
            if config:
                if isinstance(config, list) and len(config) > 0:
                    first_cfg = config[0]
                    config_display = f" (config: io-threads={first_cfg.get('io-threads', 'N/A')}, cluster={first_cfg.get('cluster_mode', 'N/A')})"

            print(f"Marked {sha} as {status} with timestamp {ts}{config_display}")

    conn.commit()


def cleanup_incomplete_commits(conn) -> int:
    """Remove all 'in_progress' entries.

    Returns:
        Number of entries cleaned up
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM benchmark_commits 
            WHERE status = 'in_progress'
            RETURNING id
        """
        )
        count = cur.rowcount

    conn.commit()

    if count > 0:
        print(f"Cleaned up {count} incomplete commits")

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


def _find_superset_configs(conn, sha: str, target_config: dict) -> List[dict]:
    """Find completed configs for a commit that are supersets of target_config.

    Args:
        conn: PostgreSQL connection
        sha: Commit SHA to check
        target_config: Config to find supersets for

    Returns:
        List of superset configs found
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT config FROM benchmark_commits
            WHERE sha = %s AND status = 'complete'
        """,
            (sha,),
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
    config: Optional[dict] = None,
    enable_subset_detection: bool = True,
) -> List[str]:
    """Return up to max_commits SHAs not benchmarked with the given config.

    Args:
        conn: PostgreSQL connection
        repo: Path to git repository
        branch: Git branch to examine
        max_commits: Maximum number of commits to return
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
                WHERE status = 'complete' AND config = %s
            """,
                (Json(config),),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT sha FROM benchmark_commits
                WHERE status = 'complete'
            """
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
            superset_configs = _find_superset_configs(conn, sha, config)
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
                    f"Skipping {sha[:8]} - subset config already benchmarked{superset_info}"
                )
                continue

        commits.append(sha)
        if len(commits) >= max_commits:
            break

    if subset_skipped > 0:
        print(
            f"Subset detection: skipped {subset_skipped} commits with existing superset configs"
        )

    return commits


def get_commits_by_config(conn, config: Optional[dict] = None) -> List[Dict]:
    """Get commits filtered by config content.

    Args:
        conn: PostgreSQL connection
        config: Config to filter by (None returns all)

    Returns:
        List of commit entries
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        if config:
            cur.execute(
                """
                SELECT sha, timestamp, status, config
                FROM benchmark_commits
                WHERE config = %s
                ORDER BY timestamp DESC
            """,
                (Json(config),),
            )
        else:
            cur.execute(
                """
                SELECT sha, timestamp, status, config
                FROM benchmark_commits
                ORDER BY timestamp DESC
            """
            )

        results = []
        for row in cur.fetchall():
            results.append(
                {
                    "sha": row[0],
                    "timestamp": row[1].isoformat(),
                    "status": row[2],
                    "config": row[3],
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
        cur.execute(
            """
            SELECT DISTINCT config
            FROM benchmark_commits
        """
        )
        return [row[0] for row in cur.fetchall()]


def get_commit_stats(conn) -> Dict:
    """Get statistics about tracked commits.

    Args:
        conn: PostgreSQL connection

    Returns:
        Dict with statistics
    """
    # Ensure tables exist
    create_tables(conn)

    with conn.cursor() as cur:
        # Total entries
        cur.execute("SELECT COUNT(*) FROM benchmark_commits")
        total = cur.fetchone()[0]

        # By status
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM benchmark_commits
            GROUP BY status
        """
        )
        by_status = {row[0]: row[1] for row in cur.fetchall()}

        # Unique commits
        cur.execute("SELECT COUNT(DISTINCT sha) FROM benchmark_commits")
        unique_commits = cur.fetchone()[0]

        # Unique configs
        cur.execute("SELECT COUNT(DISTINCT config) FROM benchmark_commits")
        unique_configs = cur.fetchone()[0]

    return {
        "total": total,
        "by_status": by_status,
        "unique_commits": unique_commits,
        "unique_configs": unique_configs,
    }


def main():
    parser = argparse.ArgumentParser(
        description="PostgreSQL-based commit tracking for benchmarks"
    )

    parser.add_argument(
        "operation", 
        choices=["determine", "mark", "query", "cleanup"],
        help="Operation to perform"
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
    parser.add_argument("--repo", type=Path, help="Git repository path (for determine/mark)")
    parser.add_argument("--branch", default="unstable", help="Git branch (for determine)")
    parser.add_argument("--max-commits", type=int, default=3, help="Max commits to return (for determine)")
    parser.add_argument("--config-file", type=str, help="Config file to load")
    parser.add_argument(
        "--disable-subset-detection",
        action="store_true",
        help="Disable subset config detection (for determine)",
    )

    # Arguments for mark operation
    parser.add_argument(
        "--status", choices=["in_progress", "complete"], help="Status to set (for mark)"
    )

    # Arguments for query operation
    parser.add_argument(
        "--list-configs", action="store_true", help="List all unique configs (for query)"
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
        print(f"Connected to PostgreSQL at {args.host}:{args.port}")
    except Exception as err:
        print(f"Failed to connect to PostgreSQL: {err}", file=sys.stderr)
        sys.exit(1)

    try:
        if args.operation == "determine":
            if not args.repo:
                print("Error: --repo is required for determine operation", file=sys.stderr)
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
                print("Error: commit SHAs are required for mark operation", file=sys.stderr)
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
                config=config,
            )

        elif args.operation == "query":
            config = None
            if args.config_file:
                with open(args.config_file, "r") as f:
                    config = json.load(f)

            if args.list_configs:
                configs = get_unique_configs(conn)
                print(f"Unique configs used: {len(configs)}")
                for i, cfg in enumerate(configs, 1):
                    summary = ""
                    if isinstance(cfg, list) and len(cfg) > 0:
                        first = cfg[0]
                        summary = f"(io-threads={first.get('io-threads', 'N/A')}, cluster={first.get('cluster_mode', 'N/A')}, tls={first.get('tls_mode', 'N/A')})"
                    print(f"  Config {i}: {summary}")
            else:
                commits = get_commits_by_config(conn, config)
                count = len(commits)
                if config:
                    summary = ""
                    if isinstance(config, list) and len(config) > 0:
                        cfg = config[0]
                        summary = f" (io-threads={cfg.get('io-threads', 'N/A')}, cluster={cfg.get('cluster_mode', 'N/A')}, tls={cfg.get('tls_mode', 'N/A')})"
                    print(f"Config{summary}: {count} commits")
                else:
                    print(f"All commits: {count}")

        elif args.operation == "cleanup":
            cleanup_incomplete_commits(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
