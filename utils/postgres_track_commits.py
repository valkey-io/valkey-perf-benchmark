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


CORE_TABLE_NAME = "benchmark_commits"
DEFAULT_CONFIG_SETS = [{}]
DEFAULT_PROFILING_SETS = [{"enabled": False}]


def _resolve_module_table_name(module_name: Optional[str]) -> str:
    """Resolve tracking table name from module name.

    Args:
        module_name: Module identifier (e.g., 'search'), or None for core.

    Returns:
        Table name: 'benchmark_module_commits_search' for module,
        or CORE_TABLE_NAME ('benchmark_commits') if None.

    Raises:
        ValueError: If module_name is empty string (likely accidental omission).
    """
    if module_name is None:
        return CORE_TABLE_NAME
    if not module_name.strip():
        raise ValueError("Module name cannot be empty.")
    return f"benchmark_module_commits_{module_name}"


def _extract_config_name(config_file: Optional[str]) -> Optional[str]:
    """Extract config name from config file path (basename with extension).

    Args:
        config_file: Path to config file (e.g., 'configs/fts-benchmarks-arm.json').

    Returns:
        Config filename (e.g., 'fts-benchmarks-arm.json'), or None if no config_file.
    """
    if config_file is None:
        return None
    return Path(config_file).name


def _apply_config_overrides(
    cfg: dict,
    cluster_mode: Optional[str] = None,
    skip_config_set: bool = False,
    skip_profiling: bool = False,
) -> dict:
    """Apply runtime overrides to a config dict.

    Args:
        cfg: Config dictionary to modify.
        cluster_mode: If provided, overwrites 'cluster_mode' field ('true' or 'false').
        skip_config_set: If True, sets 'config_sets' to default [{}].
        skip_profiling: If True, sets 'profiling_sets' to default [{"enabled": False}].

    Returns:
        Modified config dict.
    """
    if cluster_mode is not None:
        if cluster_mode.lower() == "true":
            cfg["cluster_mode"] = True
        elif cluster_mode.lower() == "false":
            cfg["cluster_mode"] = False
    if skip_config_set:
        cfg["config_sets"] = DEFAULT_CONFIG_SETS
    if skip_profiling:
        if "profiling_sets" not in cfg:
            print(
                "Warning: --skip-profiling passed but no 'profiling_sets' in config",
                file=sys.stderr,
            )
        cfg["profiling_sets"] = DEFAULT_PROFILING_SETS
    return cfg


def _load_config(
    config_file: Optional[str],
    module_name: Optional[str] = None,
    cluster_mode: Optional[str] = None,
    skip_config_set: bool = False,
    skip_profiling: bool = False,
) -> Optional[dict]:
    """Load config from file, apply runtime overrides, and optionally transform for module.

    Args:
        config_file: Path to config JSON file, or None.
        module_name: If provided, strips large keys and injects config_name.
        cluster_mode: If provided, overwrites 'cluster_mode' field in config dicts.
        skip_config_set: If True, sets 'config_sets' to default [{}].
        skip_profiling: If True, sets 'profiling_sets' to default [{"enabled": False}].

    Returns:
        Loaded config (dict or list), or None if no config_file.
    """
    if not config_file:
        return None

    with open(config_file, "r") as f:
        config = json.load(f)

    # Apply runtime overrides to config dicts (both core and module)
    if isinstance(config, dict):
        config = _apply_config_overrides(
            config, cluster_mode, skip_config_set, skip_profiling
        )
    elif isinstance(config, list):
        config = [
            _apply_config_overrides(cfg, cluster_mode, skip_config_set, skip_profiling)
            for cfg in config
            if isinstance(cfg, dict)
        ]

    # For module tracking, strip large keys and add config_name
    if module_name:
        config_name = _extract_config_name(config_file)
        skip_keys = {"test_groups", "dataset_generation", "query_generation"}

        if isinstance(config, dict):
            config = {k: v for k, v in config.items() if k not in skip_keys}
            config["config_name"] = config_name
        elif isinstance(config, list):
            config = [
                {k: v for k, v in cfg.items() if k not in skip_keys}
                for cfg in config
                if isinstance(cfg, dict)
            ]
            config.insert(0, {"config_name": config_name})

        print(
            f"Injected config_name='{config_name}' into config set",
            file=sys.stderr,
        )

    return config


def create_tables(conn, table_name: str = CORE_TABLE_NAME):
    """Create benchmark tracking table if it doesn't exist.

    Args:
        conn: PostgreSQL connection.
        table_name: Table name to create. Defaults to core 'benchmark_commits'.
    """
    if table_name == CORE_TABLE_NAME:
        prefix = "_"
    else:
        prefix = f"_{table_name}_"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                sha VARCHAR(40) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                status VARCHAR(20) NOT NULL CHECK (status IN ('in_progress', 'complete')),
                config JSONB NOT NULL,
                architecture VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                -- Unique constraint: same commit + config + architecture can only exist once                
                CONSTRAINT unique{prefix}sha_config_arch UNIQUE(sha, config, architecture)
            );
            
            CREATE INDEX IF NOT EXISTS idx{prefix}commits_sha ON {table_name}(sha);
            CREATE INDEX IF NOT EXISTS idx{prefix}commits_status ON {table_name}(status);
            CREATE INDEX IF NOT EXISTS idx{prefix}commits_timestamp ON {table_name}(timestamp);
            CREATE INDEX IF NOT EXISTS idx{prefix}commits_config ON {table_name} USING GIN(config);
            CREATE INDEX IF NOT EXISTS idx{prefix}commits_sha_status ON {table_name}(sha, status);
        """
        )
    conn.commit()
    print(f"Created/verified {table_name} table", file=sys.stderr)


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
    table_name: str = CORE_TABLE_NAME,
) -> None:
    """Mark commits with status, architecture, and config.

    Args:
        conn: PostgreSQL connection
        repo: Path to git repository
        shas: List of commit SHAs to mark
        status: Status to set ('in_progress', 'complete')
        architecture: Architecture (e.g., 'x86_64', 'arm64')
        config: Config content (dict/list) to track
        table_name: Target table name. Defaults to core table.
    """
    # Ensure tables exist
    create_tables(conn, table_name)

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
                f"""
                INSERT INTO {table_name} (sha, status, config, timestamp, architecture)
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


def cleanup_incomplete_commits(conn, table_name: str = CORE_TABLE_NAME) -> int:
    """Remove all 'in_progress' entries.

    Args:
        conn: PostgreSQL connection
        table_name: Target table name. Defaults to core table.

    Returns:
        Number of entries cleaned up
    """
    # Ensure tables exist
    create_tables(conn, table_name)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {table_name} 
            WHERE status = 'in_progress'
            RETURNING id
        """
        )
        count = cur.rowcount

    conn.commit()

    if count > 0:
        print(
            f"Cleaned up {count} incomplete commits from {table_name}", file=sys.stderr
        )

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
    conn,
    sha: str,
    target_config: dict,
    architecture: str,
    table_name: str = CORE_TABLE_NAME,
) -> List[dict]:
    """Find completed configs for a commit that are supersets of target_config.

    Args:
        conn: PostgreSQL connection
        sha: Commit SHA to check
        target_config: Config to find supersets for
        architecture: Architecture to filter by
        table_name: Target table name. Defaults to core table.

    Returns:
        List of superset configs found
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT config FROM {table_name}
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
    table_name: str = CORE_TABLE_NAME,
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
        table_name: Target table name. Defaults to core table.

    Returns:
        List of commit SHAs that need benchmarking
    """
    # Ensure tables exist
    create_tables(conn, table_name)

    # Clean up incomplete commits first
    cleanup_incomplete_commits(conn, table_name)

    # Get all commits from git
    all_shas = _git_rev_list(repo, branch)

    # Get completed commits for exact config match
    with conn.cursor() as cur:
        if config:
            cur.execute(
                f"""
                SELECT DISTINCT sha FROM {table_name}
                WHERE status = 'complete' AND config = %s AND architecture = %s
            """,
                (Json(config), architecture),
            )
        else:
            cur.execute(
                f"""
                SELECT DISTINCT sha FROM {table_name}
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
            superset_configs = _find_superset_configs(
                conn, sha, config, architecture, table_name
            )
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
    conn,
    architecture: str,
    config: Optional[dict] = None,
    table_name: str = CORE_TABLE_NAME,
) -> List[Dict]:
    """Get commits filtered by architecture and config.

    Args:
        conn: PostgreSQL connection
        architecture: Architecture to filter by
        config: Config to filter by (None returns all for the architecture)
        table_name: Target table name. Defaults to core table.

    Returns:
        List of commit entries
    """
    # Ensure tables exist
    create_tables(conn, table_name)

    with conn.cursor() as cur:
        if config:
            cur.execute(
                f"""
                SELECT sha, timestamp, status, config, architecture
                FROM {table_name}
                WHERE config = %s AND architecture = %s
                ORDER BY timestamp DESC
            """,
                (Json(config), architecture),
            )
        else:
            cur.execute(
                f"""
                SELECT sha, timestamp, status, config, architecture
                FROM {table_name}
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


def get_unique_configs(conn, table_name: str = CORE_TABLE_NAME) -> List[dict]:
    """Get list of unique config objects used.

    Args:
        conn: PostgreSQL connection
        table_name: Target table name. Defaults to core table.

    Returns:
        List of unique configs
    """
    # Ensure tables exist
    create_tables(conn, table_name)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT config
            FROM {table_name}
        """
        )
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

    # Module argument (optional — when provided, uses module-specific table)
    parser.add_argument(
        "--module-name",
        type=str,
        default=None,
        help="Module name (e.g., 'search'). When provided, operations target "
        "'benchmark_module_commits_{module_name}' table instead of core.",
    )

    # Runtime config overrides (applied to stored config for accurate tracking)
    parser.add_argument(
        "--cluster-mode-filter",
        choices=["false", "true"],
        default=None,
        help="Filter which cluster_mode to run. "
        "'false' runs only non-cluster tests, 'true' runs only cluster tests. "
        "If not specified, runs all modes in config. "
        "Used with configs that have cluster_mode as array (e.g., [false, true]).",
    )
    parser.add_argument(
        "--skip-config-set",
        action="store_true",
        help="Remove config_sets from stored config (when benchmarking without config sets).",
    )
    parser.add_argument(
        "--skip-profiling",
        action="store_true",
        help="Skip profiling and run single test pass only. "
        "Overrides profiling_sets from config file. "
        "Use for quick benchmarks or when profiling overhead is unwanted.",
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

    # Resolve module table name (None means core table)
    module_name = args.module_name
    table_name = _resolve_module_table_name(module_name)
    print(f"Using table: {table_name}", file=sys.stderr)

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

            config = _load_config(
                args.config_file,
                module_name,
                args.cluster_mode_filter,
                args.skip_config_set,
                args.skip_profiling,
            )

            enable_subset_detection = not args.disable_subset_detection
            commits = determine_commits_to_benchmark(
                conn=conn,
                repo=args.repo,
                branch=args.branch,
                max_commits=args.max_commits,
                architecture=args.architecture,
                config=config,
                enable_subset_detection=enable_subset_detection,
                table_name=table_name,
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

            config = _load_config(
                args.config_file,
                module_name,
                args.cluster_mode_filter,
                args.skip_config_set,
                args.skip_profiling,
            )

            mark_commits(
                conn=conn,
                repo=args.repo,
                shas=args.shas,
                status=args.status,
                architecture=args.architecture,
                config=config,
                table_name=table_name,
            )

        elif args.operation == "query":
            config = _load_config(
                args.config_file,
                module_name,
                args.cluster_mode_filter,
                args.skip_config_set,
                args.skip_profiling,
            )

            if args.list_configs:
                configs = get_unique_configs(conn, table_name)
                print(f"Unique configs used: {len(configs)}", file=sys.stderr)
                for i, cfg in enumerate(configs, 1):
                    summary = ""
                    if isinstance(cfg, list) and len(cfg) > 0:
                        first = cfg[0]
                        summary = f"(io-threads={first.get('io-threads', 'N/A')}, cluster={first.get('cluster_mode', 'N/A')}, tls={first.get('tls_mode', 'N/A')})"
                    print(f"  Config {i}: {summary}", file=sys.stderr)
            else:
                commits = get_commits_by_config(
                    conn, args.architecture, config, table_name
                )
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
            cleanup_incomplete_commits(conn, table_name)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
