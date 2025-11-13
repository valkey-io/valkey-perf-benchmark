#!/usr/bin/env python3
"""Command-line interface to run Valkey benchmarks."""

import argparse
import json
import logging
import platform
from pathlib import Path
from typing import List, Union
import sys


from valkey_build import ServerBuilder
from valkey_server import ServerLauncher
from valkey_benchmark import ClientRunner
from benchmark_build import BenchmarkBuilder

# ---------- Constants --------------------------------------------------------
DEFAULT_RESULTS_ROOT = Path("results")
REQUIRED_KEYS = [
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
    "benchmark-threads",
    "requests",
    "duration",
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
        help="Path to a custom valkey-benchmark executable. If omitted, automatically clones and builds the latest valkey-benchmark from unstable branch.",
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
        "--runs",
        type=int,
        default=1,
        help="Number of times to run each benchmark configuration (default: 1)",
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

    # Validate that either requests or duration is provided
    has_requests = "requests" in cfg and cfg["requests"] is not None
    has_duration = "duration" in cfg and cfg["duration"] is not None

    if not has_requests and not has_duration:
        raise ValueError("Either 'requests' or 'duration' must be provided")

    if has_requests and has_duration:
        raise ValueError("Cannot specify both 'requests' and 'duration' - use only one")

    # Validate required data types and ranges
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
                if isinstance(cfg["io-threads"], int):
                    if cfg["io-threads"] <= 0:
                        raise ValueError("'io-threads' must be a positive integer")
                elif isinstance(cfg["io-threads"], list):
                    if not all(isinstance(x, int) and x > 0 for x in cfg["io-threads"]):
                        raise ValueError(
                            "'io-threads' must be a list of positive integers"
                        )
                else:
                    raise ValueError(
                        "'io-threads' must be a positive integer or list of positive integers"
                    )
            # Validate optional benchmark-threads
            elif k == "benchmark-threads":
                if (
                    not isinstance(cfg["benchmark-threads"], int)
                    or cfg["benchmark-threads"] <= 0
                ):
                    raise ValueError("'benchmark-threads' must be a positive integer")
            # Validate optional requests
            elif k == "requests":
                if cfg["requests"] is not None:
                    if not isinstance(cfg["requests"], list) or not all(
                        isinstance(x, int) and x > 0 for x in cfg["requests"]
                    ):
                        raise ValueError(
                            "'requests' must be a list of positive integers or null"
                        )
            # Validate optional duration
            elif k == "duration":
                if cfg["duration"] is not None:
                    if not isinstance(cfg["duration"], int) or cfg["duration"] <= 0:
                        raise ValueError(
                            "'duration' must be a positive integer or null"
                        )
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

    ``range_str`` can be:
    - A simple range like ``"0-3"``
    - A comma separated list such as ``"0,2,4"``
    - Multiple ranges like ``"0-3,8-11"`` or ``"144-191,48-95"``
    """
    if not range_str or not isinstance(range_str, str):
        raise ValueError("Core range must be a non-empty string")

    # Check for leading/trailing commas or empty parts
    if range_str.startswith(",") or range_str.endswith(","):
        raise ValueError("Core range cannot start or end with comma")

    if ",," in range_str:
        raise ValueError("Core range cannot contain consecutive commas")

    try:
        # Split by comma to handle multiple ranges or individual cores
        parts = [part.strip() for part in range_str.split(",")]
        if not parts or any(not part for part in parts):
            raise ValueError("Core range must contain at least one core or range")

        for part in parts:
            if "-" in part:
                # Handle range format like "0-3" or "144-191"
                range_parts = part.split("-")
                if len(range_parts) != 2:
                    raise ValueError(f"Range format should be 'start-end', got: {part}")
                start, end = int(range_parts[0]), int(range_parts[1])
                if start < 0 or end < 0 or start > end:
                    raise ValueError(f"Invalid core range values in: {part}")
            else:
                # Handle individual core number
                core = int(part)
                if core < 0:
                    raise ValueError(f"Core numbers must be non-negative, got: {core}")
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
    *, commit_id: str, cfg: dict, args: argparse.Namespace, config_data: Union[dict, None] = None
) -> None:
    """Run benchmarks for all tls and cluster mode combinations.

    Args:
        commit_id: Git commit SHA to benchmark
        cfg: Benchmark configuration dictionary
        args: Command line arguments
        config_data: Full config data including file path and content for tracking
    """
    results_dir = ensure_results_dir(args.results_dir, commit_id)
    init_logging(results_dir / "logs.txt")
    logging.info(f"Loaded config: {cfg}")

    # Detect system architecture
    architecture = platform.machine()
    logging.info(f"Detected architecture: {architecture}")

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

    # Get io_threads values - handle both single int and list
    io_threads_values = cfg.get("io-threads")
    if io_threads_values is None:
        io_threads_list = [None]
    elif isinstance(io_threads_values, int):
        io_threads_list = [io_threads_values]
    else:
        io_threads_list = io_threads_values

    # Run benchmark for each io_threads value
    for io_threads in io_threads_list:
        logging.info(f"Running benchmark with io_threads={io_threads}")

        # ---- server section -----------------
        launcher = None
        if (not args.use_running_server) and args.mode in ("server", "both"):
            launcher = ServerLauncher(
                results_dir=str(results_dir),
                valkey_path=str(valkey_dir),
                cores=server_core_range,
            )
            launcher.launch(
                cluster_mode=cfg["cluster_mode"],
                tls_mode=cfg["tls_mode"],
                io_threads=io_threads,
            )

        # ---- benchmarking client section -----------------
        if args.mode in ("client", "both"):
            # Determine valkey-benchmark path
            if args.valkey_benchmark_path:
                benchmark_path = str(args.valkey_benchmark_path)
                logging.info(f"Using custom valkey-benchmark path: {benchmark_path}")
            else:
                logging.info(
                    "No custom valkey-benchmark path provided, building latest unstable..."
                )
                benchmark_builder = BenchmarkBuilder(tls_enabled=cfg["tls_mode"])
                benchmark_path = benchmark_builder.build_benchmark()
                logging.info(f"Built fresh valkey-benchmark at: {benchmark_path}")

            runner = ClientRunner(
                commit_id=commit_id,
                config=cfg,
                cluster_mode=cfg["cluster_mode"],
                tls_mode=cfg["tls_mode"],
                target_ip=args.target_ip,
                results_dir=results_dir,
                valkey_path=str(valkey_dir),
                cores=bench_core_range,
                io_threads=io_threads,
                valkey_benchmark_path=benchmark_path,
                benchmark_threads=cfg.get("benchmark-threads"),
                runs=args.runs,
                server_launcher=launcher,
                architecture=architecture,
            )
            runner.wait_for_server_ready()
            runner.run_benchmark_config()

        # Shutdown server after each io_threads test
        if launcher and not args.use_running_server:
            launcher.shutdown(cfg["tls_mode"])

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

    # Validate runs parameter
    if args.runs < 1:
        print("ERROR: --runs must be a positive integer")
        sys.exit(1)

    commits = args.commits.copy()
    if args.baseline and args.baseline not in commits:
        commits.append(args.baseline)

    configs = load_configs(args.config)
    for cfg in configs:
        # Prepare config data for tracking (just the config content, not file path)
        config_data = cfg

        for commit in commits:
            print(f"=== Processing commit: {commit} ===")
            run_benchmark_matrix(
                commit_id=commit, cfg=cfg, args=args, config_data=config_data
            )


if __name__ == "__main__":
    main()
