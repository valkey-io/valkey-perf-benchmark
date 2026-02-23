#!/usr/bin/env python3
"""Command-line interface to run Valkey benchmarks."""

import argparse
import json
import logging
import os
import platform
from pathlib import Path
from typing import List, Optional, Union
import sys


from valkey_build import ServerBuilder
from valkey_server import ServerLauncher
from valkey_benchmark import ClientRunner
from benchmark_build import BenchmarkBuilder
from utils.cpu_utils import (
    parse_core_range,
    calculate_cpu_ranges,
    calculate_server_cpu_ranges,
    calculate_client_cpu_ranges,
    validate_explicit_cpu_ranges,
)

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
    "test_groups",
    "cpu_allocation",
    "cluster_nodes",
    "cluster_ports",
    "bind_ip",
    "config_sets",
    "profiling_sets",
    "monitoring",
    "dataset_generation",
    "query_generation",
    "port",
    "module_startup_args",
]


# ---------- CLI --------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Valkey Benchmarking Tool", allow_abbrev=False
    )

    parser.add_argument(
        "--mode",
        choices=["client", "both"],
        default="both",
        help="Execution mode: 'client' to only run benchmark tests against an existing server, or 'both' to run server and benchmarks on the same host.",
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
        help="Server IP visible to the client.",
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

    parser.add_argument(
        "--module",
        type=str,
        default=None,
        help="Module name for results directory (e.g., 'search', 'json', 'bloom'). "
        "Optional label - results saved to {module}_tests/. "
        "If not specified, auto-detects from --module-path or uses commit_id.",
    )

    parser.add_argument(
        "--groups",
        default=None,
        help="Test groups to run (e.g., '1,2,3'). "
        "If not specified, runs all test groups. "
        "Requires configuration with 'test_groups' structure.",
    )

    parser.add_argument(
        "--scenarios",
        default=None,
        help="Specific scenarios to run within groups (e.g., 'a,b,c'). "
        "If not specified, runs all scenarios. "
        "Requires configuration with 'test_groups' structure.",
    )

    parser.add_argument(
        "--module-path",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to pre-built module .so file (e.g., ../valkey-search/.build-release/libsearch.so). "
        "REQUIRED for module testing unless --use-running-server is set. "
        "Build your module with its native build system (build.sh, make, cmake) before running benchmarks.",
    )

    parser.add_argument(
        "--skip-config-set",
        action="store_true",
        help="Skip CONFIG SET commands during benchmark initialization. "
        "Use this flag when testing against servers that don't support the CONFIG SET parameters in your config. "
        "When enabled, all CONFIG SET operations are skipped and the server runs with its default configuration.",
    )

    parser.add_argument(
        "--skip-profiling",
        action="store_true",
        help="Skip profiling and run single test pass only. "
        "Overrides profiling_sets and config_sets from config file. "
        "Use for quick benchmarks or when profiling overhead is unwanted.",
    )

    parser.add_argument(
        "--cluster-mode-filter",
        choices=["false", "true"],
        default=None,
        help="Filter which cluster_mode to run. "
        "'false' runs only non-cluster tests, 'true' runs only cluster tests. "
        "If not specified, runs all modes in config. "
        "Used with configs that have cluster_mode as array (e.g., [false, true]).",
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error(f"Unrecognized arguments: {' '.join(unknown)}")
    return args


# ---------- Validation Helpers -----------------------------------------------


def _validate_positive_int_list(value, key_name: str) -> None:
    """Validate value is a list of positive integers."""
    if not isinstance(value, list) or not all(
        isinstance(x, int) and x > 0 for x in value
    ):
        raise ValueError(f"'{key_name}' must be a list of positive integers")


def _validate_positive_int(value, key_name: str) -> None:
    """Validate value is a positive integer."""
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"'{key_name}' must be a positive integer")


def _validate_non_negative_int(value, key_name: str) -> None:
    """Validate value is a non-negative integer."""
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"'{key_name}' must be a non-negative integer")


def _validate_positive_int_or_list(value, key_name: str) -> None:
    """Validate value is positive int or list of positive ints."""
    if isinstance(value, int):
        if value <= 0:
            raise ValueError(f"'{key_name}' must be positive")
    elif isinstance(value, list):
        if not all(isinstance(x, int) and x > 0 for x in value):
            raise ValueError(f"'{key_name}' must be list of positive integers")
    else:
        raise ValueError(f"'{key_name}' must be int or list")


def _validate_cpu_range(value, key_name: str) -> None:
    """Validate CPU range string."""
    if not isinstance(value, str):
        raise ValueError(f"'{key_name}' must be a string")
    try:
        parse_core_range(value)
    except ValueError as e:
        raise ValueError(f"Invalid {key_name}: {e}")


# ---------- Helpers ----------------------------------------------------------


def validate_config(cfg: dict) -> None:
    """Validate config (commands or test_groups format)."""
    if "scenarios" in cfg and "test_groups" not in cfg:
        cfg["test_groups"] = [{"scenarios": cfg["scenarios"]}]
        del cfg["scenarios"]

    has_commands = "commands" in cfg
    has_test_groups = "test_groups" in cfg

    if not (has_commands or has_test_groups):
        raise ValueError("Config must have either 'commands' or 'test_groups'")

    if has_commands:
        for k in REQUIRED_KEYS:
            if k not in cfg:
                raise ValueError(f"Missing required key: {k}")

        has_requests = "requests" in cfg and cfg["requests"] is not None
        has_duration = "duration" in cfg and cfg["duration"] is not None

        if not has_requests and not has_duration:
            raise ValueError("Either 'requests' or 'duration' must be provided")
        if has_requests and has_duration:
            raise ValueError("Cannot specify both 'requests' and 'duration'")

        # Use helpers for validation
        _validate_positive_int_list(cfg["keyspacelen"], "keyspacelen")
        _validate_positive_int_list(cfg["data_sizes"], "data_sizes")
        _validate_positive_int_list(cfg["pipelines"], "pipelines")
        _validate_positive_int_list(cfg["clients"], "clients")
        _validate_non_negative_int(cfg["warmup"], "warmup")

        # Validate commands (special case: non-empty strings)
        if (
            not isinstance(cfg["commands"], list)
            or not cfg["commands"]
            or not all(isinstance(x, str) and x.strip() for x in cfg["commands"])
        ):
            raise ValueError("'commands' must be a non-empty list of non-empty strings")

    if has_test_groups:
        validate_test_groups(cfg)

    # Validate optional keys using helpers
    if "io-threads" in cfg:
        _validate_positive_int_or_list(cfg["io-threads"], "io-threads")
    if "benchmark-threads" in cfg:
        _validate_positive_int(cfg["benchmark-threads"], "benchmark-threads")
    if "requests" in cfg and cfg["requests"] is not None:
        _validate_positive_int_list(cfg["requests"], "requests")
    if "duration" in cfg and cfg["duration"] is not None:
        _validate_positive_int(cfg["duration"], "duration")
    if "server_cpu_range" in cfg:
        _validate_cpu_range(cfg["server_cpu_range"], "server_cpu_range")
    if "client_cpu_range" in cfg:
        _validate_cpu_range(cfg["client_cpu_range"], "client_cpu_range")
    if "module_startup_args" in cfg:
        if not isinstance(cfg["module_startup_args"], str):
            raise ValueError("'module_startup_args' must be string")
    if "port" in cfg:
        if not isinstance(cfg["port"], int) or cfg["port"] <= 0 or cfg["port"] > 65535:
            raise ValueError("'port' must be between 1 and 65535")

    if "cluster_mode" in cfg and not isinstance(cfg["cluster_mode"], list):
        cfg["cluster_mode"] = parse_bool(cfg["cluster_mode"])
    if "tls_mode" in cfg:
        cfg["tls_mode"] = parse_bool(cfg["tls_mode"])


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


def init_logging(log_path: Path, log_level: str = "INFO") -> None:
    """Set up logging to both file and stdout/stderr."""

    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Clear any existing handlers to force reconfiguration
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )


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


def _get_active_ports(cfg: dict) -> List[int]:
    """Return ports based on actual cluster mode (not config)."""
    if cfg.get("cluster_mode") and "cluster_ports" in cfg:
        return cfg["cluster_ports"]
    return [cfg.get("port", 6379)]


def validate_cpu_allocation(cfg: dict) -> None:
    """Validate CPU configuration (new cpu_allocation or old individual fields)."""
    has_cpu_allocation = "cpu_allocation" in cfg
    has_old_fields = "server_cpu_range" in cfg or "client_cpu_range" in cfg

    # Mutually exclusive
    if has_cpu_allocation and has_old_fields:
        raise ValueError(
            "Cannot use both cpu_allocation and server_cpu_range/client_cpu_range"
        )

    # Validate cpu_allocation (new)
    if has_cpu_allocation:
        cpu_alloc = cfg["cpu_allocation"]

        if "cores_per_server" not in cpu_alloc or "cores_per_client" not in cpu_alloc:
            raise ValueError(
                "cpu_allocation requires both 'cores_per_server' and 'cores_per_client'"
            )

        if cpu_alloc["cores_per_server"] <= 0 or cpu_alloc["cores_per_client"] <= 0:
            raise ValueError("cores_per_server and cores_per_client must be positive")

    # Validate explicit ranges
    if has_old_fields and "server_cpu_range" in cfg and "client_cpu_range" in cfg:
        validate_explicit_cpu_ranges(cfg["server_cpu_range"], cfg["client_cpu_range"])


def validate_test_groups(cfg: dict) -> None:
    """Validate test_groups structure."""
    if "test_groups" not in cfg:
        return

    test_groups = cfg["test_groups"]
    if not isinstance(test_groups, list) or len(test_groups) == 0:
        raise ValueError("'test_groups' must be a non-empty list")

    for i, group in enumerate(test_groups):
        if not isinstance(group, dict):
            raise ValueError(f"test_groups[{i}] must be a dict")

        if "scenarios" not in group:
            raise ValueError(f"test_groups[{i}] missing 'scenarios' field")

        if not isinstance(group["scenarios"], list) or len(group["scenarios"]) == 0:
            raise ValueError(f"test_groups[{i}].scenarios must be a non-empty list")


def run_benchmark_matrix(
    *,
    commit_id: str,
    cfg: dict,
    args: argparse.Namespace,
    module_path: Optional[str] = None,
    uses_test_groups: bool = False,
) -> None:
    """Orchestrate benchmark execution for all configurations."""
    if args.module:
        results_dir = args.results_dir / f"{args.module}_tests"
    else:
        results_dir = args.results_dir / commit_id

    results_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Loaded config: {cfg}")

    architecture = platform.machine()
    logging.info(f"Detected architecture: {architecture}")

    valkey_dir = (
        Path(args.valkey_path) if args.valkey_path else Path(f"../valkey_{commit_id}")
    )

    builder = ServerBuilder(
        commit_id=commit_id, tls_mode=cfg["tls_mode"], valkey_path=str(valkey_dir)
    )
    if not args.use_running_server:
        server_binary = valkey_dir / "src" / "valkey-server"
        if server_binary.exists():
            logging.info("Using existing valkey-server binary")
        else:
            logging.info("valkey-server binary not found, building...")
            builder.build()
    else:
        logging.info("Using pre-built Valkey instance.")

    logging.info(
        f"Commit {commit_id[:10]} | TLS={'on' if cfg['tls_mode'] else 'off'} | Cluster={'on' if cfg['cluster_mode'] else 'off'}"
    )

    client_cpu_ranges = calculate_client_cpu_ranges(cfg)

    for exec_config in _iterate_execution_configs(cfg, args):
        _execute_benchmark_run(
            exec_config,
            args,
            results_dir,
            valkey_dir,
            module_path,
            uses_test_groups,
            architecture,
            client_cpu_ranges,
        )

    # Cleanup
    if not args.use_running_server:
        if args.valkey_path:
            builder.terminate_valkey()
        else:
            builder.terminate_and_clean_valkey()


def _iterate_execution_configs(cfg: dict, args: argparse.Namespace):
    """Generate all execution configurations from config and CLI args."""
    # Normalize cluster_modes
    cluster_modes = cfg.get("cluster_mode")
    if args.cluster_mode_filter:
        cluster_modes = [parse_bool(args.cluster_mode_filter)]
    elif not isinstance(cluster_modes, list):
        cluster_modes = [cluster_modes]

    # Normalize profiling_sets
    profiling_sets = cfg.get("profiling_sets", [{"enabled": False}])
    if args.skip_profiling:
        profiling_sets = [{"enabled": False}]

    # Normalize config_sets
    config_sets = cfg.get("config_sets", [{}])
    if args.skip_config_set:
        config_sets = [{}]

    # Normalize io_threads
    io_threads_list = cfg.get("io-threads")
    if io_threads_list is None:
        io_threads_list = [None]
    elif isinstance(io_threads_list, int):
        io_threads_list = [io_threads_list]

    # Generate all combinations
    for cluster_mode in cluster_modes:
        for profiling_set in profiling_sets:
            for config_set in config_sets:
                config_suffix = (
                    "_".join([f"{k.split('.')[-1]}{v}" for k, v in config_set.items()])
                    if config_set
                    else "default"
                )

                for io_threads in io_threads_list:
                    # Create modified config for this iteration
                    exec_cfg = cfg.copy()
                    exec_cfg["cluster_mode"] = cluster_mode

                    yield {
                        "cfg": exec_cfg,
                        "cluster_mode": cluster_mode,
                        "profiling_set": profiling_set,
                        "config_set": config_set,
                        "config_suffix": config_suffix,
                        "io_threads": io_threads,
                    }


def _execute_benchmark_run(
    exec_config,
    args,
    results_dir,
    valkey_dir,
    module_path,
    uses_test_groups,
    architecture,
    client_cpu_ranges,
):
    """Execute a single benchmark run with specific configuration."""
    cfg = exec_config["cfg"]
    io_threads = exec_config["io_threads"]

    logging.info(f"Running benchmark with io_threads={io_threads}")

    # Setup server
    launcher = None
    if not args.use_running_server and args.mode == "both":
        server_cpu_ranges = calculate_server_cpu_ranges(cfg)
        if server_cpu_ranges:
            cfg["server_cpu_ranges"] = server_cpu_ranges

        launcher = ServerLauncher(
            results_dir=str(results_dir),
            valkey_path=str(valkey_dir),
            cores=(
                cfg.get("server_cpu_ranges", [cfg.get("server_cpu_range")])[0]
                if cfg.get("server_cpu_ranges") or cfg.get("server_cpu_range")
                else None
            ),
            target_ip=args.target_ip,
        )
        launcher.launch(
            cluster_mode=cfg["cluster_mode"],
            tls_mode=cfg["tls_mode"],
            io_threads=io_threads,
            module_path=module_path,
            config=cfg,
        )

    # Apply config set
    if exec_config["config_set"] and not args.skip_config_set:
        _apply_config_to_servers(exec_config["config_set"], cfg, args.target_ip)

    # Run benchmark client
    if args.mode in ("client", "both"):
        if args.valkey_benchmark_path:
            benchmark_path = str(args.valkey_benchmark_path)
            logging.info(f"Using custom valkey-benchmark: {benchmark_path}")
        elif args.valkey_path:
            benchmark_path = str(valkey_dir / "src" / "valkey-benchmark")
            logging.info(f"Using valkey-benchmark from valkey-path: {benchmark_path}")
        else:
            logging.info("Building latest valkey-benchmark...")
            benchmark_builder = BenchmarkBuilder(tls_enabled=cfg["tls_mode"])
            benchmark_path = benchmark_builder.build_benchmark()
            logging.info(f"Built valkey-benchmark: {benchmark_path}")

        runner = ClientRunner(
            commit_id=exec_config["cfg"].get("commit_id", "HEAD"),
            config=cfg,
            cluster_mode=cfg["cluster_mode"],
            tls_mode=cfg["tls_mode"],
            target_ip=args.target_ip,
            results_dir=results_dir,
            valkey_path=str(valkey_dir),
            cores=cfg.get("client_cpu_range"),
            io_threads=io_threads,
            valkey_benchmark_path=benchmark_path,
            benchmark_threads=cfg.get("benchmark-threads"),
            runs=args.runs,
            server_launcher=launcher,
            architecture=architecture,
            uses_test_groups=uses_test_groups,
        )

        runner.current_profiling_set = exec_config["profiling_set"]
        runner.current_config_set = exec_config["config_set"]
        runner.config_suffix = exec_config["config_suffix"]

        if client_cpu_ranges:
            runner.client_cpu_ranges = client_cpu_ranges

        runner.wait_for_server_ready()
        runner.run_benchmark_config()

    # Shutdown server
    if launcher and not args.use_running_server:
        launcher.shutdown(cfg["tls_mode"])


def _apply_config_to_servers(config_set: dict, cfg: dict, target_ip: str) -> None:
    """Apply CONFIG SET commands to all server nodes."""
    import valkey

    for port in _get_active_ports(cfg):
        client = valkey.Valkey(host=target_ip, port=port)
        try:
            for k, v in config_set.items():
                client.execute_command("CONFIG", "SET", k, str(v))
                logging.info(f"Set {k} = {v} on port {port}")
        finally:
            client.close()


def get_module_binary_path(args: argparse.Namespace, config: dict) -> Optional[str]:
    """Validate and return module binary path if module testing requested."""
    # Check if module testing (CLI or config)
    if not args.module_path and not config.get("modules"):
        return None

    # Require --module name for module testing
    if (args.module_path or config.get("modules")) and not args.module:
        raise ValueError(
            "--module <name> required when using --module-path or config modules"
        )

    if args.use_running_server:
        logging.info("Using running server with pre-loaded module")
        return None

    if args.module_path:
        module_binary = Path(args.module_path)
        if not module_binary.exists():
            raise FileNotFoundError(f"Module binary not found: {module_binary}")
        if not module_binary.suffix == ".so":
            raise ValueError(
                f"--module-path must point to .so file, got: {module_binary}"
            )
        return str(module_binary.absolute())

    return None


# ---------- Entry point ------------------------------------------------------
def main() -> None:
    """Entry point for the benchmark CLI."""
    args = parse_args()

    if args.use_running_server and not args.valkey_path:
        print(
            "ERROR: --use-running-server implies the valkey is already built and running, "
            "so `valkey_path` must be provided."
        )
        sys.exit(1)

    # Validate runs parameter
    if args.runs < 1:
        print("ERROR: --runs must be a positive integer")
        sys.exit(1)

    # Load and validate configs
    configs_list = load_configs(args.config)

    if not configs_list:
        print("ERROR: No configurations found in config file")
        sys.exit(1)

    # Use first config for initial setup
    config = configs_list[0]
    validate_cpu_allocation(config)

    uses_test_groups = "test_groups" in config

    module_path = get_module_binary_path(args, config)

    # Module testing requires valkey-path
    if (args.module_path or config.get("modules")) and not args.valkey_path:
        print("ERROR: Module testing requires --valkey-path")
        sys.exit(1)

    if uses_test_groups and (
        config.get("dataset_generation") or config.get("query_generation")
    ):
        import subprocess

        required_datasets = set()
        for test_group in config["test_groups"]:
            for scenario in test_group.get("scenarios", []):
                if "dataset" in scenario:
                    required_datasets.add(scenario["dataset"])

        missing = [Path(d) for d in required_datasets if not Path(d).exists()]
        if missing:
            print(f"Missing datasets: {[f.name for f in missing]}")
            cmd = [
                "python3",
                "scripts/setup_datasets.py",
                "--config",
                args.config,
                "--files",
            ] + [f.name for f in missing]
            subprocess.run(cmd, check=True)

    if args.groups:
        config["groups_to_run"] = set(int(g.strip()) for g in args.groups.split(","))
    if args.scenarios:
        config["scenario_filter"] = set(s.strip() for s in args.scenarios.split(","))

    commits = args.commits.copy()
    if args.baseline and args.baseline not in commits:
        commits.append(args.baseline)

    # Setup logging ONCE before processing configs
    if args.module:
        log_dir = args.results_dir / f"{args.module}_tests"
    else:
        log_dir = args.results_dir / commits[0]
    log_dir.mkdir(parents=True, exist_ok=True)
    init_logging(log_dir / "logs.txt", args.log_level)

    # Process all configs
    for cfg in configs_list:
        validate_cpu_allocation(cfg)
        uses_test_groups = "test_groups" in cfg

        # Apply CLI filters to this config
        if args.groups:
            cfg["groups_to_run"] = set(int(g.strip()) for g in args.groups.split(","))
        if args.scenarios:
            cfg["scenario_filter"] = set(s.strip() for s in args.scenarios.split(","))

        for commit in commits:
            print(f"=== Processing commit: {commit} ===")
            run_benchmark_matrix(
                commit_id=commit,
                cfg=cfg,
                args=args,
                module_path=module_path,
                uses_test_groups=uses_test_groups,
            )


if __name__ == "__main__":
    main()
