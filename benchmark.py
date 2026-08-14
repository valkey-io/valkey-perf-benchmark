#!/usr/bin/env python3
"""Command-line interface to run Valkey benchmarks."""

import argparse
import json
import logging
import platform
from itertools import product
from pathlib import Path
from typing import List, Optional
import sys


from valkey_build import ServerBuilder
from valkey_server import ServerLauncher, apply_config_to_servers
from valkey_benchmark import (
    ClientRunner,
    ORIGIN_FIELD,
    ORIGIN_SIMPLE,
    READ_COMMANDS,
    READ_POPULATE_MAP,
    WRITE_COMMANDS,
)
from benchmark_build import BenchmarkBuilder
from utils.cpu_utils import (
    parse_core_range,
    calculate_server_cpu_ranges,
    calculate_client_cpu_ranges,
    validate_explicit_cpu_ranges,
)

# ---------- Constants --------------------------------------------------------
DEFAULT_RESULTS_ROOT = Path("results")
DEFAULT_CONFIG_FILE = "./configs/benchmark-configs.json"
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
    "custom-server-configs",
    "custom-server-config-file",
]


# ---------- CLI --------------------------------------------------------------
def _validate_repository_format(value: str) -> str:
    """Validate repository is in 'owner/repo' format."""
    if value.count("/") != 1:
        raise argparse.ArgumentTypeError(
            f"Invalid repository format: '{value}'. Expected 'owner/repo' format."
        )
    owner, repo = value.split("/")
    if not owner or not repo:
        raise argparse.ArgumentTypeError(
            f"Invalid repository format: '{value}'. Owner and repo cannot be empty."
        )
    return value


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
        default=None,
        help=(
            "Path to benchmark-configs.json. Each entry is an explicit benchmark "
            "configuration and combinations are not generated automatically."
            "Defaults to './configs/benchmark-configs.json' if not provided."
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
        "--module-commit",
        type=str,
        default=None,
        help="Module commit SHA (written to metrics for tracking module versions).",
    )
    parser.add_argument(
        "--module-commit-timestamp",
        type=str,
        default=None,
        help="Module commit timestamp ISO 8601 (written to metrics for module tracking).",
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
        "--repository",
        type=_validate_repository_format,
        default=None,
        help="GitHub repository in 'owner/repo' format (e.g., 'valkey-io/valkey'). "
        "Used to generate commit links in comparison reports.",
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


def compile_simple_config(cfg: dict) -> None:
    """Compile the basic 'commands' format into generated test_groups.

    Each cartesian combination of requests x keyspacelen x data_sizes x
    pipelines x clients x commands becomes ONE generated test group holding a
    single scenario, so the group-level runs loop reproduces the basic
    format's consecutive-run ordering exactly (A,A then B,B for runs=2).

    Generated scenarios are tagged with an internal origin marker
    (``_origin: simple``) so the runner keeps the basic metrics schema and
    the one-shared-seed-per-execution behavior.

    Commands outside the supported allowlist are dropped here with a warning.
    The MSET/MGET cluster-mode skip intentionally does NOT happen here:
    compilation runs once at config load, before cluster_mode arrays are
    scalarized per execution config, so that skip lives at execution time in
    ``ClientRunner._iterate_test_groups_scenarios``.
    """
    requests_list = (
        cfg["requests"] if cfg.get("requests") is not None else [None]
    )  # duration mode has no requests

    groups = []
    for requests, keyspacelen, data_size, pipeline, clients, command in product(
        requests_list,
        cfg["keyspacelen"],
        cfg["data_sizes"],
        cfg["pipelines"],
        cfg["clients"],
        cfg["commands"],
    ):
        if command not in READ_COMMANDS + WRITE_COMMANDS:
            logging.warning(f"Unsupported command: {command}, skipping.")
            continue

        scenario = {
            ORIGIN_FIELD: ORIGIN_SIMPLE,
            "id": command,
            "test": command,
            "data_size": data_size,
            "pipeline": pipeline,
            "clients": clients,
            "keyspacelen": keyspacelen,
            "warmup_inline": cfg["warmup"],
            "restart_before": True,
        }
        if cfg.get("duration") is not None:
            scenario["duration"] = cfg["duration"]
        else:
            scenario["requests"] = requests
        if command in READ_COMMANDS:
            # Write-equivalent used to seed the keyspace (run sequentially
            # with the same seed as the main run).
            scenario["populate_with"] = READ_POPULATE_MAP[command]

        groups.append({"group": len(groups) + 1, "scenarios": [scenario]})

    cfg["test_groups"] = groups


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
    if "custom-server-configs" in cfg:
        if not isinstance(cfg["custom-server-configs"], dict):
            raise ValueError("'custom-server-configs' must be a dictionary")
        for key, value in cfg["custom-server-configs"].items():
            if not isinstance(key, str):
                raise ValueError(
                    f"'custom-server-configs' keys must be strings, got: {type(key)}"
                )
            # Note: bool is a subclass of int in Python, so check bool first.
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ValueError(
                    f"'custom-server-configs' values must be strings or numbers, got: {type(value)}"
                )
    if "custom-server-config-file" in cfg:
        if not isinstance(cfg["custom-server-config-file"], str):
            raise ValueError("'custom-server-config-file' must be a string path")

    if "cluster_mode" in cfg and not isinstance(cfg["cluster_mode"], list):
        cfg["cluster_mode"] = parse_bool(cfg["cluster_mode"])
    if "tls_mode" in cfg:
        cfg["tls_mode"] = parse_bool(cfg["tls_mode"])

    # Basic 'commands' configs compile down to generated test_groups so the
    # scenario model is the only execution path. Configs that already define
    # test_groups keep them (commands are ignored there, as before).
    if has_commands and not has_test_groups:
        compile_simple_config(cfg)


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

        for j, scenario in enumerate(group["scenarios"]):
            if not isinstance(scenario, dict):
                raise ValueError(f"test_groups[{i}].scenarios[{j}] must be a dict")

            if scenario.get("type") == "mixed":
                # Mixed scenarios carry writes/reads sub-scenarios instead of
                # a top-level test/command, so the checks below do not apply.
                # populate_with does not either: a mixed scenario seeds through
                # its own writes, and with no test/command the populate pass
                # would fall back to running populate_with as a predefined -t
                # workload.
                if "populate_with" in scenario:
                    raise ValueError(
                        f"test_groups[{i}].scenarios[{j}] combines 'mixed' "
                        "with 'populate_with'; mixed scenarios seed the "
                        "keyspace through their own 'writes' sub-scenarios"
                    )
                continue

            if ("test" in scenario) == ("command" in scenario):
                raise ValueError(
                    f"test_groups[{i}].scenarios[{j}] must have exactly one "
                    "of 'test' or 'command'"
                )

            if "test" in scenario and "options" in scenario:
                raise ValueError(
                    f"test_groups[{i}].scenarios[{j}] combines 'test' with "
                    "'options'; options append flags to the command string "
                    "and are only valid with 'command', not 'test'"
                )

            if "populate_with" in scenario:
                populate_with = scenario["populate_with"]
                if not isinstance(populate_with, str) or not populate_with:
                    raise ValueError(
                        f"test_groups[{i}].scenarios[{j}] 'populate_with' must "
                        "be a non-empty string"
                    )
                # A 'test:' scenario seeds via a predefined -t write workload,
                # so populate_with must name one. Rejecting an unsupported name
                # closes a footgun: the old runtime silently skipped populate
                # for anything outside READ_POPULATE_MAP, benchmarking reads
                # against an empty keyspace. A 'command:' scenario treats
                # populate_with as an arbitrary write command string, which
                # cannot be validated against a fixed list.
                if "test" in scenario and populate_with not in WRITE_COMMANDS:
                    raise ValueError(
                        f"test_groups[{i}].scenarios[{j}] 'populate_with' "
                        f"{populate_with!r} is not a supported write command; "
                        f"for a 'test' scenario it must be one of {WRITE_COMMANDS}"
                    )


def run_benchmark_matrix(
    *,
    commit_id: str,
    cfg: dict,
    args: argparse.Namespace,
    module_path: Optional[str] = None,
    config_name: Optional[str] = None,
    module_commit: Optional[str] = None,
    module_commit_timestamp: Optional[str] = None,
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
        if args.valkey_path and commit_id != "HEAD":
            # Shared directory with explicit commit: checkout and rebuild
            builder.build()
        elif server_binary.exists():
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
            commit_id,
            module_path,
            architecture,
            client_cpu_ranges,
            config_name,
            module_commit,
            module_commit_timestamp,
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
    commit_id,
    module_path,
    architecture,
    client_cpu_ranges,
    config_name=None,
    module_commit=None,
    module_commit_timestamp=None,
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
        apply_config_to_servers(
            exec_config["config_set"],
            _get_active_ports(cfg),
            args.target_ip,
            tls_mode=cfg.get("tls_mode", False),
            valkey_dir=valkey_dir,
        )

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
            commit_id=commit_id,
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
            repository=args.repository,
            config_name=config_name,
            module_commit=module_commit,
            module_commit_timestamp=module_commit_timestamp,
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
    if args.config is None:
        args.config = DEFAULT_CONFIG_FILE
        print(
            f"WARNING: --config not specified, using default: '{DEFAULT_CONFIG_FILE}'",
            file=sys.stderr,
        )

    configs_list = load_configs(args.config)

    if not configs_list:
        print("ERROR: No configurations found in config file")
        sys.exit(1)

    # Use first config for initial setup
    config = configs_list[0]
    validate_cpu_allocation(config)

    module_path = get_module_binary_path(args, config)

    # Module testing requires valkey-path
    if (args.module_path or config.get("modules")) and not args.valkey_path:
        print("ERROR: Module testing requires --valkey-path")
        sys.exit(1)

    # Every validated config has test_groups (basic configs are compiled)
    if config.get("dataset_generation") or config.get("query_generation"):
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
                config_name=Path(args.config).name if args.module else None,
                module_commit=args.module_commit,
                module_commit_timestamp=args.module_commit_timestamp,
            )


if __name__ == "__main__":
    main()
