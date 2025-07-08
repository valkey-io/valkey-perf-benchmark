#!/usr/bin/env python3
"""Command-line interface to run Valkey benchmarks."""

import argparse
import json
import logging
from pathlib import Path
from typing import List

from logger import Logger
from valkey_build import ServerBuilder
from valkey_server import ServerLauncher
from valkey_benchmark import ClientRunner

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
        help="Where to run: only server setup, only client tests, or both on one host.",
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
        default="../valkey",
        metavar="PATH",
        help="Use this pre-built Valkey directory instead of building from source.",
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
        help="Path to benchmark-configs.json.",
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
        "--cpu-range",
        help=(
            "Comma-separated CPU ranges for server and benchmark, e.g. "
            "'0-1,2-3'. If omitted, processes are not pinned."
        ),
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error(f"Unrecognized arguments: {' '.join(unknown)}")
    return args


# ---------- Helpers ----------------------------------------------------------
def validate_config(cfg: dict) -> None:
    """Ensure all required keys exist in ``cfg``."""
    for k in REQUIRED_KEYS:
        if k not in cfg:
            raise ValueError(f"Missing required config key: {k}")


def load_configs(path: str) -> List[dict]:
    """Load benchmark configurations from a JSON file."""
    with open(path, "r") as fp:
        configs = json.load(fp)
    for c in configs:
        validate_config(c)
    return configs


def ensure_results_dir(root: Path, commit_id: str) -> Path:
    """Return directory path for a commit's results."""
    d = root / commit_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def parse_core_range(range_str: str) -> List[int]:
    """Return a list of CPU cores from a range string like '0-3'."""
    if "-" in range_str:
        start, end = range_str.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(c) for c in range_str.split(",") if c]


def run_benchmark_matrix(
    *, commit_id: str, cfg: dict, args: argparse.Namespace
) -> None:
    """Run benchmarks for all tls and cluster mode combinations."""
    results_dir = ensure_results_dir(args.results_dir, commit_id)
    Logger.init_logging(results_dir / "logs.txt")
    logging.getLogger().setLevel(args.log_level)

    server_core_range = None
    bench_core_range = None
    if args.cpu_range:
        try:
            server_str, bench_str = [s.strip() for s in args.cpu_range.split(',')]
        except ValueError:
            raise ValueError(
                "--cpu-range must be two comma-separated ranges, e.g. '0-1,2-3'"
            )
        parse_core_range(server_str)
        parse_core_range(bench_str)
        server_core_range = server_str
        bench_core_range = bench_str

    builder = ServerBuilder(
        commit_id=commit_id,
        tls_mode=cfg["tls_mode"],
        valkey_path=args.valkey_path,
    )
    if not args.use_running_server:
        builder.build()
    else:
        Logger.info("Using pre-built Valkey instance.")

    Logger.info(
        f"Commit {commit_id[:10]} | "
        f"TLS={'on' if cfg['tls_mode'] == 'yes' else 'off'} | "
        f"Cluster={'on' if cfg['cluster_mode'] == 'yes' else 'off'}"
    )
    # ---- server section -----------------
    if (not args.use_running_server) and args.mode in ("server", "both"):
        launcher = ServerLauncher(
            commit_id=commit_id,
            valkey_path=args.valkey_path,
            cores=server_core_range,
        )
        launcher.launch(
            cluster_mode=cfg["cluster_mode"],
            tls_mode=cfg["tls_mode"],
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
            valkey_path=args.valkey_path,
            cores=bench_core_range,
        )
        runner.wait_for_server_ready()
        runner.run_benchmark_config()

        if not args.use_running_server:
            runner.cleanup_terminate()


# ---------- Entry point ------------------------------------------------------
def main() -> None:
    """Entry point for the benchmark CLI."""
    args = parse_args()

    if args.use_running_server and args.mode in ("server", "both"):
        Logger.error(
            "ERROR: --use-running-server implies the valkey is already build and running, "
            "so --mode must be 'client'."
        )

    commits = args.commits.copy()
    if args.baseline and args.baseline not in commits:
        commits.append(args.baseline)

    for cfg in load_configs(args.config):
        Logger.info(f"Loaded config: {cfg}")
        for commit in commits:
            run_benchmark_matrix(commit_id=commit, cfg=cfg, args=args)


if __name__ == "__main__":
    main()
