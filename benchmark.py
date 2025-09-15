#!/usr/bin/env python3
"""Command-line interface to run Valkey benchmarks."""

import argparse
import json
import logging
from pathlib import Path
from typing import List
import sys


from valkey_build import ServerBuilder
from valkey_server import ServerLauncher
from valkey_benchmark import ClientRunner
from utils.workflow_commits import mark_commits

# ---------- Constants --------------------------------------------------------
DEFAULT_RESULTS_ROOT = Path("results")
REQUIRED_KEYS = [
    "requests",
    "keyspacelen",
    "data_sizes",
    "pipelines",
    "clients",
    "commands",
    "cluster_mode",
    "tls_mode",
    "warmup",
]

OPTIONAL_CONF_KEYS = [
    "io-threads",
    "server_cpu_range",
    "client_cpu_range",
]


# ---------- CLI --------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Valkey Benchmarking Tool", allow_abbrev=False
    )

    parser.add_argument(
        "--mode",
        choices=["server", "client", "both"],
        default="both",
        help="Execution mode: 'server' to only setup and run Valkey server, 'client' to only run benchmark tests against an existing server, or 'both' to run server and benchmarks on the same host.",
    )
    parser.add_argument(
        "--commits",
        nargs="+",
        default=["HEAD"],
        metavar="COMMITS",
        help="Git SHA(s) or ref(s) to benchmark (default: HEAD).",
    )
    parser.add_argument(
        "--valkey-path",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to an existing Valkey checkout. If omitted a fresh clone is created per commit.",
    )
    parser.add_argument(
        "--valkey-benchmark-path",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to a custom valkey-benchmark executable. If omitted, uses the default 'src/valkey-benchmark' relative to valkey-path.",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        metavar="REF",
        help="Extra commit to include for comparison (e.g. 'unstable').",
    )
    parser.add_argument(
        "--use-running-server",
        action="store_true",
        help="Assumes the Valkey servers are already running; "
        "skip build / launch / cleanup steps.",
    )
    parser.add_argument(
        "--target-ip",
        default="127.0.0.1",
        help="Server IP visible to the client (ignored for --mode=server).",
    )
    parser.add_argument(
        "--config",
        default="./configs/benchmark-configs.json",
        help=(
            "Path to benchmark-configs.json. Each entry is an explicit benchmark "
            "configuration and combinations are not generated automatically."
        ),
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="Root folder for benchmark outputs.",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    parser.add_argument(
        "--completed-file",
        type=Path,
        default="./completed_commits.json",
        help="Path to completed_commits.json used for tracking progress",
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error(f"Unrecognized arguments: {' '.join(unknown)}")
    return args


# ---------- Helpers ----------------------------------------------------------


def validate_config(cfg: dict) -> None:
    """Ensure all required keys exist and have valid values in ``cfg``."""
    for k in REQUIRED_KEYS:
        if k not in cfg:
            raise ValueError(f"Missing required config key: {k}")

    # Validate data types and ranges
    if not isinstance(cfg["requests"], list) or not all(
        isinstance(x, int) and x > 0 for x in cfg["requests"]
    ):
        raise ValueError("'requests' must be a list of positive integers")

    if not isinstance(cfg["keyspacelen"], list) or not all(
        isinstance(x, int) and x > 0 for x in cfg["keyspacelen"]
    ):
        raise ValueError("'keyspacelen' must be a list of positive integers")

    if not isinstance(cfg["data_sizes"], list) or not all(
        isinstance(x, int) and x > 0 for x in cfg["data_sizes"]
    ):
        raise ValueError("'data_sizes' must be a list of positive integers")

    if not isinstance(cfg["pipelines"], list) or not all(
        isinstance(x, int) and x > 0 for x in cfg["pipelines"]
    ):
        raise ValueError("'pipelines' must be a list of positive integers")

    if not isinstance(cfg["clients"], list) or not all(
        isinstance(x, int) and x > 0 for x in cfg["clients"]
    ):
        raise ValueError("'clients' must be a list of positive integers")

    if (
        not isinstance(cfg["commands"], list)
        or not cfg["commands"]
        or not all(isinstance(x, str) and x.strip() for x in cfg["commands"])
    ):
        raise ValueError("'commands' must be a non-empty list of non-empty strings")

    if not isinstance(cfg["warmup"], int) or cfg["warmup"] < 0:
        raise ValueError("'warmup' must be a non-negative integer")

    for k in OPTIONAL_CONF_KEYS:
        if k in cfg:
            # Validate optional io-threads
            if k == "io-threads":
                if not isinstance(cfg["io-threads"], int) or cfg["io-threads"] <= 0:
                    raise ValueError("'io-threads' must be a positive integer")
            # Validate optional CPU ranges
            elif k in ["server_cpu_range", "client_cpu_range"]:
                if not isinstance(cfg[k], str):
                    raise ValueError(f"'{k}' must be a string")
                try:
                    parse_core_range(cfg[k])
                except ValueError as e:
                    raise ValueError(f"Invalid {k}: {e}")


def load_configs(path: str) -> List[dict]:
    """Load benchmark configurations from a JSON file."""
    with open(path, "r") as fp:
        configs = json.load(fp)
    for c in configs:
        validate_config(c)
        c["cluster_mode"] = parse_bool(c["cluster_mode"])
        c["tls_mode"] = parse_bool(c["tls_mode"])
    return configs


def ensure_results_dir(root: Path, commit_id: str) -> Path:
    """Return directory path for a commit's results."""
    d = root / commit_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def init_logging(log_path: Path) -> None:
    """Set up logging to both file and stdout/stderr."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_core_range(range_str: str) -> None:
    """Validate CPU core range string format.

    ``range_str`` can be a simple range like ``"0-3"`` or a comma separated
    list such as ``"0,2,4"``.
    """
    if not range_str or not isinstance(range_str, str):
        raise ValueError("Core range must be a non-empty string")

    try:
        if "-" in range_str:
            parts = range_str.split("-")
            if len(parts) != 2:
                raise ValueError("Range format should be 'start-end'")
            start, end = int(parts[0]), int(parts[1])
            if start < 0 or end < 0 or start > end:
                raise ValueError("Invalid core range values")
        else:
            cores = [int(c.strip()) for c in range_str.split(",") if c.strip()]
            if not cores or any(c < 0 for c in cores):
                raise ValueError("Core numbers must be non-negative")
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(f"Invalid core range format: {range_str}")
        raise


def parse_bool(value) -> bool:
    """Return ``value`` converted to ``bool``.

    Accepts booleans directly or common string representations like
    ``"yes"``/``"no"``, "1"/"0" and ``"true"``/``"false"``.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("yes", "true", "1")
    return bool(value)


def run_benchmark_matrix(
    *, commit_id: str, cfg: dict, args: argparse.Namespace
) -> None:
    """Run benchmarks for all tls and cluster mode combinations."""
    results_dir = ensure_results_dir(args.results_dir, commit_id)
    init_logging(results_dir / "logs.txt")
    logging.info(f"Loaded config: {cfg}")

    server_core_range = cfg.get("server_cpu_range")
    bench_core_range = cfg.get("client_cpu_range")

    valkey_dir = (
        Path(args.valkey_path) if args.valkey_path else Path(f"../valkey_{commit_id}")
    )

    builder = ServerBuilder(
        commit_id=commit_id,
        tls_mode=cfg["tls_mode"],
        valkey_path=str(valkey_dir),
    )
    if not args.use_running_server:
        builder.build()
    else:
        logging.info("Using pre-built Valkey instance.")

    logging.info(
        f"Commit {commit_id[:10]} | "
        f"TLS={'on' if cfg['tls_mode'] else 'off'} | "
        f"Cluster={'on' if cfg['cluster_mode'] else 'off'}"
    )
    # ---- server section -----------------
    if (not args.use_running_server) and args.mode in ("server", "both"):
        launcher = ServerLauncher(
            results_dir=results_dir,
            valkey_path=str(valkey_dir),
            cores=server_core_range,
        )
        launcher.launch(
            cluster_mode=cfg["cluster_mode"],
            tls_mode=cfg["tls_mode"],
            io_threads=cfg.get("io-threads"),
        )

    # ---- benchmarking client section -----------------
    if args.mode in ("client", "both"):
        runner = ClientRunner(
            commit_id=commit_id,
            config=cfg,
            cluster_mode=cfg["cluster_mode"],
            tls_mode=cfg["tls_mode"],
            target_ip=args.target_ip,
            results_dir=results_dir,
            valkey_path=str(valkey_dir),
            cores=bench_core_range,
            io_threads=cfg.get("io-threads"),
            valkey_benchmark_path=(
                str(args.valkey_benchmark_path) if args.valkey_benchmark_path else None
            ),
        )
        runner.wait_for_server_ready()
        runner.run_benchmark_config()

        # Mark commit as complete when done
        try:
            mark_commits(
                completed_file=Path(args.completed_file),
                repo=valkey_dir,
                shas=[commit_id],
                status="complete",
            )
        except Exception as exc:
            logging.warning(f"Failed to update completed_commits.json: {exc}")

        if not args.use_running_server:
            if args.valkey_path:
                builder.terminate_valkey()
            else:
                builder.terminate_and_clean_valkey()


# ---------- Entry point ------------------------------------------------------
def main() -> None:
    """Entry point for the benchmark CLI."""
    args = parse_args()

    if args.use_running_server and (
        args.mode in ("server", "both") or not args.valkey_path
    ):
        print(
            "ERROR: --use-running-server implies the valkey is already built and running, "
            "so --mode must be 'client' and `valkey_path` must be provided."
        )
        sys.exit(1)

    commits = args.commits.copy()
    if args.baseline and args.baseline not in commits:
        commits.append(args.baseline)

    for cfg in load_configs(args.config):
        for commit in commits:
            print(f"=== Processing commit: {commit} ===")
            run_benchmark_matrix(commit_id=commit, cfg=cfg, args=args)


if __name__ == "__main__":
    main()
