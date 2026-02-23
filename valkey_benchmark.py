"""Client-side benchmark execution logic."""

import copy
import logging
import random
import shlex
import subprocess
import time
import csv
from contextlib import contextmanager
from itertools import product
from pathlib import Path
from typing import Iterable, List, Optional

import valkey

from process_metrics import MetricsProcessor
from valkey_server import ServerLauncher
from profiler import PerformanceProfiler

# Constants
VALKEY_BENCHMARK = "src/valkey-benchmark"
DEFAULT_PORT = 6379
DEFAULT_TIMEOUT = 30

# Supported Valkey benchmark commands
READ_COMMANDS = ["GET", "MGET", "LRANGE", "SPOP", "ZPOPMIN", "XRANGE"]
WRITE_COMMANDS = [
    "SET",
    "MSET",
    "INCR",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "SADD",
    "HSET",
    "ZADD",
    "XADD",
]

# Map for read commands to populate equivalents
READ_POPULATE_MAP = {
    "GET": "SET",
    "MGET": "MSET",
    "LRANGE": "LPUSH",
    "SPOP": "SADD",
    "ZPOPMIN": "ZADD",
}


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base, returning new dict."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ClientRunner:
    """Run ``valkey-benchmark`` for a given commit and configuration."""

    def __init__(
        self,
        commit_id: str,
        config: dict,
        cluster_mode: bool,
        tls_mode: bool,
        target_ip: str,
        results_dir: Path,
        valkey_path: str,
        cores: Optional[str] = None,
        io_threads: Optional[int] = None,
        valkey_benchmark_path: Optional[str] = None,
        benchmark_threads: Optional[int] = None,
        runs: int = 1,
        server_launcher: Optional[ServerLauncher] = None,
        architecture: Optional[str] = None,
        uses_test_groups: bool = False,
    ) -> None:
        self.commit_id = commit_id
        self.config = config
        self.cluster_mode = cluster_mode
        self.tls_mode = tls_mode
        self.target_ip = target_ip
        self.results_dir = results_dir
        self.valkey_path = Path(valkey_path)
        self.cores = cores
        self.io_threads = io_threads
        self.valkey_benchmark_path = valkey_benchmark_path or VALKEY_BENCHMARK
        self.benchmark_threads = benchmark_threads
        self.runs = runs
        self.server_launcher = server_launcher
        self.architecture = architecture
        self.uses_test_groups = uses_test_groups
        self.current_profiling_set = {"enabled": False}
        self.current_config_set = {}
        self.config_suffix = "default"
        self.client_cpu_ranges = []

    def _create_client(self, port: Optional[int] = None) -> valkey.Valkey:
        """Return a Valkey client configured for TLS or plain mode."""
        if port is None:
            port = self.config.get("port", DEFAULT_PORT)
        logging.info(f"Connecting to {self.target_ip}:{port}")
        kwargs = {
            "host": self.target_ip,
            "port": port,
            "decode_responses": True,
            "socket_timeout": 10,
            "socket_connect_timeout": 10,
        }
        if self.tls_mode:
            tls_cert_path = Path(self.valkey_path) / "tests" / "tls"
            if not tls_cert_path.exists():
                raise FileNotFoundError(
                    f"TLS certificates not found at {tls_cert_path}"
                )

            kwargs.update(
                {
                    "ssl": True,
                    "ssl_certfile": str(tls_cert_path / "valkey.crt"),
                    "ssl_keyfile": str(tls_cert_path / "valkey.key"),
                    "ssl_ca_certs": str(tls_cert_path / "ca.crt"),
                }
            )
        return valkey.Valkey(**kwargs)

    @contextmanager
    def _client_context(self):
        """Context manager for Valkey client connections."""
        client = None
        try:
            client = self._create_client()
            yield client
        finally:
            if client:
                try:
                    client.close()
                except Exception as e:
                    logging.warning(f"Error closing client connection: {e}")

    def _run(
        self,
        command: Iterable[str],
        cwd: Optional[Path] = None,
        capture_output: bool = False,
        text: bool = True,
        timeout: Optional[int] = 300,
    ) -> Optional[subprocess.CompletedProcess]:
        """Execute a command with proper error handling and timeout."""
        cmd_list = list(command)
        cmd_str = shlex.join(cmd_list)
        logging.info(f"Running: {cmd_str}")

        try:
            result = subprocess.run(
                cmd_list,
                shell=False,
                cwd=cwd,
                capture_output=capture_output,
                text=text,
                check=True,
                timeout=timeout,
            )
            if result.stderr:
                logging.warning(f"Command stderr: {result.stderr}")
            return result if capture_output else None
        except subprocess.TimeoutExpired as e:
            logging.error(f"Command timed out after {timeout}s: {cmd_str}")
            raise RuntimeError(f"Command timed out: {cmd_str}") from e
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed with exit code {e.returncode}: {cmd_str}")
            if e.stderr:
                logging.error(f"Command stderr: {e.stderr}")
            raise RuntimeError(f"Command failed: {cmd_str}") from e
        except Exception as e:
            logging.error(f"Unexpected error while running: {cmd_str}")
            raise RuntimeError(f"Unexpected error: {cmd_str}") from e

    def wait_for_server_ready(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        """Poll until the Valkey server responds to PING or timeout expires."""
        logging.info(
            "Waiting for Valkey server to be ready from the benchmark client..."
        )
        start = time.time()
        last_error = None

        while time.time() - start < timeout:
            try:
                with self._client_context() as client:
                    client.ping()
                    logging.info("Valkey server is ready.")
                    return
            except Exception as e:
                last_error = e
                time.sleep(1)

        logging.error(f"Valkey server did not become ready within {timeout} seconds.")
        if last_error:
            logging.error(f"Last connection error: {last_error}")
        raise RuntimeError(f"Server failed to start in time. Last error: {last_error}")

    def get_commit_time(self, commit_id: str) -> str:
        """Return timestamp for a commit."""
        try:
            result = self._run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                cwd=self.valkey_path,
                capture_output=True,
            )
            if result is None:
                raise RuntimeError("Failed to get commit time: no result returned")
            return result.stdout.strip()
        except Exception as e:
            logging.exception(f"Failed to get commit time for {commit_id}: {e}")
            raise

    def _get_active_ports(self) -> List[int]:
        """Return ports based on actual cluster mode."""
        if self.cluster_mode and "cluster_ports" in self.config:
            return self.config["cluster_ports"]
        return [self.config.get("port", 6379)]

    def _flush_database(self) -> None:
        """Flush all data from the database before benchmark runs."""
        logging.info(
            "Flushing database before benchmark run (may take several minutes for large indexes)"
        )
        try:
            ports = self._get_active_ports()

            # Drop indexes first with extended timeout (large indexes take time)
            try:
                # Extended timeout for index operations
                first_client = self._create_client(port=ports[0])
                first_client.connection_pool.connection_kwargs["socket_timeout"] = 300
                try:
                    indexes = first_client.execute_command("FT._LIST")
                    for idx in indexes:
                        try:
                            logging.info(f"Dropping index {idx}...")
                            first_client.execute_command("FT.DROPINDEX", idx)
                            logging.info(f"Dropped index {idx}")
                        except Exception as e:
                            logging.warning(f"Could not drop index {idx}: {e}")
                finally:
                    first_client.close()
            except Exception as e:
                logging.warning(f"Could not list/drop indexes: {e}")

            # Flush all nodes with extended timeout
            for port in ports:
                client = self._create_client(port=port)
                client.connection_pool.connection_kwargs["socket_timeout"] = 300
                try:
                    logging.info(f"Flushing database on port {port}...")
                    client.flushall(asynchronous=False)
                    logging.info(f"Flushed database on port {port}")
                finally:
                    client.close()
        except Exception as e:
            logging.error(f"Failed to flush database: {e}")
            raise RuntimeError(f"Database flush failed: {e}")

    def _populate_keyspace(
        self,
        read_command: str,
        requests: int,
        keyspacelen: int,
        data_size: int,
        pipeline: int,
        clients: int,
        seed_val: int,
    ) -> None:
        """Populate keyspace for a read command using its write equivalent."""
        write_cmd = READ_POPULATE_MAP.get(read_command)
        if not write_cmd:
            logging.info(f"No populate needed for {read_command}")
            return

        logging.info(f"Populating keyspace for {read_command} using {write_cmd}")

        bench_cmd = self._build_benchmark_command(
            tls=self.tls_mode,
            requests=requests,
            keyspacelen=keyspacelen,
            data_size=data_size,
            pipeline=pipeline,
            clients=clients,
            command=write_cmd,
            seed_val=seed_val,
            sequential=True,
        )

        self._run(command=bench_cmd, cwd=self.valkey_path, timeout=None)
        logging.info(f"Keyspace populated for {read_command} with {requests} keys")

    def run_benchmark_config(self) -> None:
        """Orchestrate benchmark execution for both config formats."""
        commit_time = self.get_commit_time(self.commit_id)

        # Setup profiling/metrics infrastructure
        (
            profiler,
            metrics_processor,
            profiling_enabled,
        ) = self._setup_profiling_and_metrics(self.current_profiling_set, commit_time)

        # Execute all scenarios and collect results
        metric_json = []
        for scenario_data in self._iterate_scenarios():
            result = self._execute_scenario(
                scenario_data,
                profiler,
                metrics_processor,
                profiling_enabled,
                commit_time,
            )
            if result:
                metric_json.append(result)

        # Finalize and write results
        self._finalize_metrics(metrics_processor, metric_json, profiling_enabled)

    def _iterate_scenarios(self):
        """Generate scenario execution data from either config format."""
        if self.uses_test_groups:
            yield from self._iterate_test_groups_scenarios()
        else:
            yield from self._iterate_simple_scenarios()

    def _iterate_simple_scenarios(self):
        """Generate scenarios from simple command-based configuration."""
        for (
            requests,
            keyspacelen,
            data_size,
            pipeline,
            clients,
            command,
            warmup,
            duration,
        ) in self._generate_combinations():
            # Validate command
            if command not in READ_COMMANDS + WRITE_COMMANDS:
                logging.warning(f"Unsupported command: {command}, skipping.")
                continue

            if command in ["MSET", "MGET"] and self.cluster_mode:
                logging.warning(
                    f"Command {command} not supported in cluster mode, skipping."
                )
                continue

            # Run multiple times if requested
            for run_num in range(self.runs):
                seed_val = random.randint(0, 1000000)

                yield {
                    "format": "simple",
                    "run_num": run_num,
                    "requests": requests,
                    "keyspacelen": keyspacelen,
                    "data_size": data_size,
                    "pipeline": pipeline,
                    "clients": clients,
                    "command": command,
                    "warmup": warmup,
                    "duration": duration,
                    "seed": seed_val,
                    "needs_population": command in READ_COMMANDS,
                    "populate_command": READ_POPULATE_MAP.get(command),
                }

    def _iterate_test_groups_scenarios(self):
        """Generate scenarios from test_groups configuration."""
        groups_to_run = self.config.get("groups_to_run")
        scenario_filter = self.config.get("scenario_filter")

        for test_group in self.config.get("test_groups", []):
            group_id = test_group.get("group", "unknown")

            # Skip filtered groups
            if groups_to_run and group_id not in groups_to_run:
                logging.info(
                    f"Skipping group {group_id} (not in filter: {groups_to_run})"
                )
                continue

            logging.info(
                f"=== Group {group_id}: {test_group.get('description', '')} ==="
            )

            for scenario in test_group.get("scenarios", []):
                # Expand scenario options (e.g., with/without flags)
                for expanded_scenario in self._expand_scenario_options(scenario):
                    # Skip filtered scenarios
                    if (
                        scenario_filter
                        and expanded_scenario.get("id") not in scenario_filter
                    ):
                        logging.info(
                            f"Skipping scenario {expanded_scenario.get('id')} (filtered)"
                        )
                        continue

                    yield {
                        "format": "test_groups",
                        "scenario": expanded_scenario,
                        "group_id": group_id,
                        "config_set": self.current_config_set,
                        "config_suffix": self.config_suffix,
                    }

    def _execute_scenario(
        self, scenario_data, profiler, metrics_processor, profiling_enabled, commit_time
    ):
        """Execute a single scenario regardless of format."""
        if scenario_data["format"] == "simple":
            return self._execute_simple_scenario(scenario_data, metrics_processor)
        else:
            return self._execute_test_groups_scenario(
                scenario_data,
                profiler,
                metrics_processor,
                profiling_enabled,
                commit_time,
            )

    def _execute_simple_scenario(self, data, metrics_processor):
        """Execute a simple format scenario."""
        if self.runs > 1:
            logging.info(f"=== Run {data['run_num'] + 1}/{self.runs} ===")

        mode_info = (
            f"duration={data['duration']}s"
            if data["duration"] is not None
            else f"requests={data['requests']}"
        )
        logging.info(
            f"--> Running {data['command']} | size={data['data_size']} | "
            f"pipeline={data['pipeline']} | clients={data['clients']} | {mode_info} | "
            f"keyspacelen={data['keyspacelen']} | warmup={data['warmup']}"
        )
        logging.info(f"Using seed value: {data['seed']}")

        # Restart/flush
        if self.server_launcher:
            self._restart_server()
        else:
            self._flush_database()

        # Populate if needed
        if data["needs_population"]:
            populate_requests = (
                data["requests"]
                if data["requests"] is not None
                else data["keyspacelen"]
            )
            self._populate_keyspace(
                data["command"],
                populate_requests,
                data["keyspacelen"],
                data["data_size"],
                data["pipeline"],
                data["clients"],
                data["seed"],
            )

        # Run benchmark
        bench_cmd = self._build_benchmark_command(
            tls=self.tls_mode,
            requests=data["requests"],
            keyspacelen=data["keyspacelen"],
            data_size=data["data_size"],
            pipeline=data["pipeline"],
            clients=data["clients"],
            command=data["command"],
            seed_val=data["seed"],
            sequential=False,
            duration=data["duration"],
            warmup=data["warmup"],
        )

        proc = self._run(
            bench_cmd, cwd=self.valkey_path, capture_output=True, timeout=None
        )
        if proc is None:
            logging.error("Benchmark command failed to return results")
            return None

        logging.info(f"Benchmark output:\n{proc.stdout}")
        if proc.stderr:
            logging.warning(f"Benchmark stderr:\n{proc.stderr}")

        # Parse metrics
        try:
            reader = csv.DictReader(proc.stdout.splitlines())
            for row in reader:
                test_name = row.get("test", "")
                if not test_name.startswith(data["command"]):
                    continue

                metrics = metrics_processor.create_metrics(
                    row,
                    test_name,
                    data["data_size"],
                    data["pipeline"],
                    data["clients"],
                    data["requests"],
                    data["warmup"],
                    data["duration"],
                )
                if metrics:
                    logging.info(f"Parsed metrics for {test_name}: {metrics}")
                    return metrics
        except Exception as e:
            logging.error(f"Failed to parse benchmark results: {e}")

        return None

    def _execute_test_groups_scenario(
        self, data, profiler, metrics_processor, profiling_enabled, commit_time
    ):
        """Execute a test_groups format scenario."""
        return self._run_single_scenario(
            data["scenario"],
            data["group_id"],
            profiler,
            metrics_processor,
            profiling_enabled,
            commit_time,
            data["config_set"],
            data["config_suffix"],
        )

    def _generate_combinations(self) -> List[tuple]:
        """Cartesian product of parameters within a single config item."""
        # Use requests if available, otherwise None for duration mode
        requests_list = self.config.get("requests", [None])

        return list(
            product(
                requests_list,
                self.config["keyspacelen"],
                self.config["data_sizes"],
                self.config["pipelines"],
                self.config["clients"],
                self.config["commands"],
                [self.config["warmup"]],
                [self.config.get("duration")],
            )
        )

    def _build_benchmark_command(
        self,
        tls: Optional[bool] = None,
        requests: Optional[int] = None,
        keyspacelen: Optional[int] = None,
        data_size: Optional[int] = None,
        pipeline: Optional[int] = None,
        clients: Optional[int] = None,
        command: Optional[str] = None,
        seed_val: Optional[int] = None,
        *,
        sequential: bool = False,
        duration: Optional[int] = None,
        warmup: Optional[int] = None,
        scenario: Optional[dict] = None,
        warmup_mode: bool = False,
        port: Optional[int] = None,
        cpu_range: Optional[str] = None,
    ) -> List[str]:
        """Unified command builder for both simple and test_groups formats.

        Usage:
            # Simple format (positional args)
            _build_benchmark_command(tls=True, requests=1000, keyspacelen=1000, ...)

            # Test groups format (scenario dict)
            _build_benchmark_command(scenario={"command": "FT.SEARCH ...", ...})
        """
        cmd = []

        # Determine format
        is_test_groups = scenario is not None

        # CPU pinning
        cores = cpu_range or self.cores
        if cores:
            cmd += ["taskset", "-c", cores]

        cmd.append(self.valkey_benchmark_path)

        # TLS configuration
        use_tls = tls if tls is not None else self.tls_mode
        if use_tls:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"]
            cmd += ["--cacert", "./tests/tls/ca.crt"]

        # Connection settings
        cmd += ["-h", self.target_ip]
        cmd += ["-p", str(port or self.config.get("port", DEFAULT_PORT))]

        if is_test_groups:
            # Test groups format: extract from scenario
            if scenario.get("dataset"):
                dataset_path = Path(scenario["dataset"])
                if not dataset_path.is_absolute():
                    dataset_path = Path.cwd() / dataset_path
                cmd += ["--dataset", str(dataset_path)]

                if scenario.get("xml_root_element"):
                    cmd += ["--xml-root-element", scenario["xml_root_element"]]

                if scenario.get("maxdocs") and scenario.get("type") == "write":
                    cmd += ["--maxdocs", str(scenario["maxdocs"])]

            # Duration/requests
            if warmup_mode:
                warmup_duration = scenario.get("warmup", 60)
                cmd += ["--duration", str(warmup_duration)]
            else:
                if scenario.get("duration"):
                    cmd += ["--duration", str(scenario["duration"])]
                elif scenario.get("requests"):
                    cmd += ["-n", str(scenario["requests"])]
                elif scenario.get("maxdocs"):
                    cmd += ["-n", str(scenario["maxdocs"])]
                else:
                    cmd += ["--duration", str(self.config.get("duration", 60))]

            cmd += ["-c", str(scenario.get("clients", 1))]
            cmd += ["-P", str(scenario.get("pipeline", 1))]

            keyspacelen_val = self.config.get("keyspacelen", [1000000])[0]
            cmd += ["-r", str(keyspacelen_val)]

            if scenario.get("sequential", False):
                cmd += ["--sequential"]

            if scenario.get("cluster_execution") == "single":
                if self.cluster_mode and self.config.get("cluster_nodes"):
                    cmd += ["--cluster"]

            # Seed: Default ON unless explicitly disabled with "seed": false
            if (
                scenario.get("seed") is not False
                and self.config.get("seed") is not False
            ):
                seed = seed_val if seed_val is not None else random.randint(0, 1000000)
                cmd += ["--seed", str(seed)]

            cmd += ["--csv"]
            cmd += ["--"]
            cmd += shlex.split(scenario["command"])
        else:
            # Simple format: use positional args
            if duration is not None:
                cmd += ["--duration", str(duration)]
            else:
                cmd += ["-n", str(requests)]

            cmd += ["-r", str(keyspacelen)]
            cmd += ["-d", str(data_size)]
            cmd += ["-P", str(pipeline)]
            cmd += ["-c", str(clients)]
            cmd += ["-t", command]

            if self.benchmark_threads is not None:
                cmd += ["--threads", str(self.benchmark_threads)]

            if warmup is not None and warmup > 0:
                cmd += ["--warmup", str(warmup)]

            if sequential:
                cmd += ["--sequential"]

            # Unified seed logic: Default ON unless config disables
            if self.config.get("seed") is not False:
                cmd += ["--seed", str(seed_val)]

            cmd += ["--csv"]

        return cmd

    def _find_csv_start(self, lines: List[str]) -> Optional[int]:
        """Find CSV header line index."""
        for i, line in enumerate(lines):
            if line.startswith('"test","rps"') or line.startswith("test,rps"):
                return i
        return None

    def _parse_csv_row(self, stdout: str) -> Optional[dict]:
        """Parse benchmark CSV output, return first row."""
        if not stdout:
            return None
        lines = stdout.splitlines()
        csv_start = self._find_csv_start(lines)
        if csv_start is None:
            return None
        reader = csv.DictReader(lines[csv_start:])
        for row in reader:
            return row
        return None

    def _is_cme(self) -> bool:
        """Check if cluster mode is enabled with multiple nodes."""
        return self.cluster_mode and self.config.get("cluster_nodes", 1) > 1

    def _should_use_parallel(self, scenario: dict) -> bool:
        """Determine if scenario should use parallel execution."""
        return (
            self._is_cme() and scenario.get("cluster_execution", "single") == "parallel"
        )

    def _expand_scenario_options(self, scenario: dict) -> List[dict]:
        """Expand scenario with options to create variants."""
        options = scenario.get("options")

        # No options: return scenario as-is
        if not options:
            return [scenario]

        # Options provided: create variant for each option
        scenarios = []
        for flag, suffix in options.items():
            variant = copy.deepcopy(scenario)
            variant["id"] = scenario["id"] + suffix
            variant["command"] = scenario["command"] + (f" {flag}" if flag else "")
            if "description" in variant and flag:
                variant["description"] += f" + {flag}"
            scenarios.append(variant)

        return scenarios

    def _create_failure_marker(
        self,
        group_id: int,
        scenario_id: str,
        scenario_type: str,
        error: str,
        command: str,
        timestamp: str,
        config_set: dict,
    ) -> dict:
        """Create failure marker dict for failed scenarios."""
        return {
            "test_id": f"{group_id}_{scenario_id}",
            "test_phase": scenario_type,
            "status": "failed",
            "error": error,
            "command": command,
            "timestamp": timestamp,
            "config_set": config_set,
        }

    def _setup_profiling_and_metrics(self, profiling_set: dict, commit_time: str):
        """Setup profiler and metrics processor based on profiling_set."""
        profiling_enabled = profiling_set.get("enabled", False)

        profiler = None
        if profiling_enabled:
            profiler = PerformanceProfiler(
                results_dir=self.results_dir,
                enabled=True,
                config={"profiling": profiling_set},
                commit_id="",
            )

        metrics_processor = None
        if not profiling_enabled:
            metrics_processor = MetricsProcessor(
                self.commit_id,
                self.cluster_mode,
                self.tls_mode,
                commit_time,
                self.io_threads,
                self.benchmark_threads,
                self.architecture,
            )

        return profiler, metrics_processor, profiling_enabled

    def _finalize_metrics(self, metrics_processor, metric_json, profiling_enabled):
        """Write metrics and log completion status."""
        if metrics_processor and metric_json:
            metrics_processor.write_metrics(self.results_dir, metric_json)
            logging.info(
                f"=== Benchmark Complete: {len(metric_json)} metrics collected ==="
            )
        elif profiling_enabled:
            logging.info(
                "=== Benchmark Complete: Profiling mode, no metrics collected ==="
            )
        else:
            logging.warning("No metrics collected")

    def _run_single_scenario(
        self,
        scenario,
        group_id,
        profiler,
        metrics_processor,
        profiling_enabled,
        commit_time,
        config_set,
        config_suffix,
    ):
        """Run a single scenario."""
        scenario_type = scenario.get("type", "test")
        scenario_id = scenario.get("id", "unknown")

        logging.info(f"Running scenario: {scenario_id} (type: {scenario_type})")

        if scenario.get("flush_before", False):
            self._flush_database()

        for setup_cmd in scenario.get("setup_commands", []):
            self._execute_setup_command(setup_cmd)

        if scenario.get("profiling"):
            effective_profiling = deep_merge(
                self.current_profiling_set, scenario["profiling"]
            )
        else:
            effective_profiling = self.current_profiling_set

        scenario_profiling_enabled = effective_profiling.get("enabled", False)
        profile_id = f"group{group_id}_{scenario_type}_{scenario_id}_{config_suffix}"

        warmup_duration = scenario.get("warmup", 0)
        try:
            if warmup_duration > 0:
                if self._should_use_parallel(scenario):
                    logging.info(
                        f"Running parallel warmup on {len(self._get_active_ports())} nodes: {warmup_duration}s"
                    )
                    # Warm up all nodes that will be queried
                    self._run_parallel_search(
                        scenario,
                        self._get_active_ports(),
                        self.client_cpu_ranges,
                        warmup_mode=True,
                    )
                else:
                    logging.info(f"Running warmup: {warmup_duration}s")
                    cpu = self.client_cpu_ranges[0] if self.client_cpu_ranges else None
                    self._run(
                        self._build_benchmark_command(
                            scenario=scenario, warmup_mode=True, cpu_range=cpu
                        ),
                        cwd=self.valkey_path,
                        capture_output=True,
                        timeout=None,
                    )

            if profiler and scenario_profiling_enabled:
                target_port = self._get_active_ports()[0] if self._is_cme() else None
                if target_port:
                    logging.info(
                        f"CME profiling: targeting node 0 on port {target_port}"
                    )

                # Pass scenario delays override
                profiler.delays = effective_profiling.get("delays", profiler.delays)
                profiler.start_profiling(
                    profile_id, target_process="valkey-server", target_port=target_port
                )

            if self._should_use_parallel(scenario):
                logging.info(f"Using parallel execution for scenario {scenario_id}")
                aggregated_row = self._run_parallel_search(
                    scenario, self._get_active_ports(), self.client_cpu_ranges
                )
                proc = None
            else:
                cpu = self.client_cpu_ranges[0] if self.client_cpu_ranges else None
                proc = self._run(
                    self._build_benchmark_command(scenario=scenario, cpu_range=cpu),
                    cwd=self.valkey_path,
                    capture_output=True,
                    timeout=None,
                )
                aggregated_row = None

            if profiler and scenario_profiling_enabled:
                profiler.stop_profiling(profile_id)

            if proc is None and aggregated_row is None:
                logging.error(f"Benchmark failed for scenario {scenario_id}")
                if metrics_processor:
                    return self._create_failure_marker(
                        group_id,
                        scenario_id,
                        scenario_type,
                        "No results",
                        scenario["command"],
                        commit_time,
                        config_set,
                    )
                return None

            if proc:
                logging.info(f"Benchmark output:\n{proc.stdout}")

            if metrics_processor:
                requests_value = scenario.get("requests") or scenario.get("maxdocs")
                row = aggregated_row or self._parse_csv_row(proc.stdout if proc else "")

                if not row:
                    logging.warning(f"No metrics data for scenario {scenario_id}")
                    return None

                metrics = metrics_processor.create_metrics(
                    row,
                    scenario["command"],
                    scenario.get("data_size", 100),
                    scenario.get("pipeline", 1),
                    scenario.get("clients", 1),
                    requests_value,
                    warmup_duration,
                    scenario.get("duration"),
                )

                if metrics:
                    metrics["status"] = "success"
                    metrics["test_id"] = f"{group_id}_{scenario_id}"
                    metrics["test_phase"] = scenario_type
                    metrics["config_set"] = config_set
                    if scenario.get("dataset"):
                        metrics["dataset"] = scenario["dataset"]
                    return metrics

        except Exception as e:
            logging.error(f"Scenario {group_id}_{scenario_id} failed: {e}")
            if metrics_processor:
                return self._create_failure_marker(
                    group_id,
                    scenario_id,
                    scenario_type,
                    str(e),
                    scenario["command"],
                    commit_time,
                    config_set,
                )

        return None

    def _execute_setup_command(self, cmd_str: str) -> None:
        """Execute a setup command via valkey client."""
        logging.info(f"Executing setup command: {cmd_str}")
        try:
            with self._client_context() as client:
                cmd_parts = shlex.split(cmd_str)
                result = client.execute_command(*cmd_parts)
                logging.info(f"Setup command result: {result}")
        except Exception as e:
            logging.error(f"Failed to execute setup command '{cmd_str}': {e}")
            raise

    def _run_parallel_search(
        self,
        scenario: dict,
        ports: List[int],
        client_cpu_ranges: List[str],
        warmup_mode: bool = False,
    ) -> dict:
        """Run search benchmarks in parallel to all cluster nodes."""
        # Check for custom parallel client count
        parallel_clients = scenario.get("parallel_clients")
        if parallel_clients:
            # Custom: Spawn N clients distributed across nodes
            logging.info(
                f"Starting parallel execution: {parallel_clients} clients across {len(ports)} nodes"
            )
            # Distribute clients across nodes round-robin
            port_assignments = [ports[i % len(ports)] for i in range(parallel_clients)]
            cpu_assignments = [
                client_cpu_ranges[i % len(client_cpu_ranges)]
                for i in range(parallel_clients)
            ]
        else:
            # Default: 1 client per node
            logging.info(f"Starting parallel execution on {len(ports)} nodes")
            port_assignments = ports
            cpu_assignments = client_cpu_ranges

        processes = []
        for i, (port, cpu_range) in enumerate(zip(port_assignments, cpu_assignments)):
            cmd = self._build_benchmark_command(
                scenario=scenario,
                port=port,
                cpu_range=cpu_range,
                warmup_mode=warmup_mode,
            )
            if warmup_mode:
                logging.info(f"Launching warmup client {i} on port {port}")
            else:
                logging.info(
                    f"Launching client {i} on port {port} with CPU range {cpu_range}"
                )
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.valkey_path,
            )
            processes.append((proc, port))

        # Wait for all to complete and collect results
        results = []
        for proc, port in processes:
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                logging.error(f"Benchmark failed on port {port}: {stderr}")
                continue
            results.append((stdout, stderr, port))
            logging.info(f"Completed benchmark on port {port}")

        if not results:
            raise RuntimeError("All parallel benchmarks failed")

        # Aggregate results
        return self._aggregate_parallel_results(results, scenario)

    def _aggregate_parallel_results(
        self,
        results: List[tuple],
        scenario: dict,
    ) -> dict:
        """Aggregate results from parallel benchmarks."""
        metrics_list = []

        for stdout, stderr, port in results:
            row = self._parse_csv_row(stdout)
            if not row:
                logging.warning(f"No CSV data in output for port {port}")
                continue

            try:
                metrics = {
                    "rps": float(row.get("rps", 0)),
                    "avg_latency_ms": float(row.get("avg_latency_ms", 0)),
                    "min_latency_ms": float(row.get("min_latency_ms", 999999)),
                    "p50_latency_ms": float(row.get("p50_latency_ms", 0)),
                    "p95_latency_ms": float(row.get("p95_latency_ms", 0)),
                    "p99_latency_ms": float(row.get("p99_latency_ms", 0)),
                    "max_latency_ms": float(row.get("max_latency_ms", 0)),
                    "port": port,
                }
                metrics_list.append(metrics)
                logging.info(
                    f"Parsed metrics from port {port}: RPS={metrics['rps']:.2f}"
                )
            except (ValueError, KeyError) as e:
                logging.error(f"Failed to parse metrics from port {port}: {e}")
                continue

        if not metrics_list:
            raise RuntimeError("No valid metrics parsed from parallel results")

        # Aggregate: Sum RPS, weighted-average latencies
        total_rps = sum(m["rps"] for m in metrics_list)

        if total_rps > 0:
            # Weighted average: sum(rps_i * latency_i) / total_rps
            avg_latency = (
                sum(m["rps"] * m["avg_latency_ms"] for m in metrics_list) / total_rps
            )
            p50_latency = (
                sum(m["rps"] * m["p50_latency_ms"] for m in metrics_list) / total_rps
            )
            p95_latency = (
                sum(m["rps"] * m["p95_latency_ms"] for m in metrics_list) / total_rps
            )
            p99_latency = (
                sum(m["rps"] * m["p99_latency_ms"] for m in metrics_list) / total_rps
            )
        else:
            avg_latency = p50_latency = p95_latency = p99_latency = 0

        # Min/max across all nodes
        min_latency = min(m["min_latency_ms"] for m in metrics_list)
        max_latency = max(m["max_latency_ms"] for m in metrics_list)

        # Build aggregated result dict (CSV-like format)
        aggregated = {
            "test": scenario["command"],
            "rps": str(total_rps),
            "avg_latency_ms": str(avg_latency),
            "min_latency_ms": str(min_latency),
            "p50_latency_ms": str(p50_latency),
            "p95_latency_ms": str(p95_latency),
            "p99_latency_ms": str(p99_latency),
            "max_latency_ms": str(max_latency),
        }

        logging.info(
            f"Aggregated parallel results: Total RPS={total_rps:.2f}, Avg Latency={avg_latency:.2f}ms"
        )
        return aggregated

    def _restart_server(self) -> None:
        """Restart the Valkey server for a clean state."""
        if self.server_launcher is None:
            logging.error("No server launcher available for restart")
            return

        logging.info("Restarting Valkey server for clean state...")

        # Shutdown current server
        self.server_launcher.shutdown(self.tls_mode)

        # Start fresh server (module_path is stored in launcher)
        self.server_launcher.launch(
            cluster_mode=self.cluster_mode,
            tls_mode=self.tls_mode,
            io_threads=self.io_threads,
            module_path=self.server_launcher.module_path,
        )

        # Wait for server to be ready
        self.wait_for_server_ready()
        logging.info("Server restarted successfully")
