"""Client-side benchmark execution logic."""

import copy
import logging
import random
import shlex
import subprocess
import time
import csv
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Optional

import valkey

from process_metrics import MetricsProcessor
from valkey_server import ServerLauncher, apply_config_to_servers
from profiler import PerformanceProfiler
from utils.git_utils import resolve_ref, get_commit_timestamp
from utils.cpu_utils import format_core_list, parse_core_range
from environment_metadata import collect_environment_metadata

# Constants
VALKEY_BENCHMARK = "src/valkey-benchmark"
DEFAULT_PORT = 6379
DEFAULT_TIMEOUT = 30

# Supported Valkey benchmark commands
READ_COMMANDS = ["GET", "MGET", "LRANGE", "SISMEMBER", "ZSCORE", "ZRANGE"]
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
    "SPOP",
    "ZPOPMIN",
]

# Map for read commands to populate equivalents
READ_POPULATE_MAP = {
    "GET": "SET",
    "MGET": "MSET",
    "LRANGE": "LPUSH",
    "SISMEMBER": "SADD",
    "ZSCORE": "ZADD",
    "ZRANGE": "ZADD",
}

# Compiled basic scenarios retain the basic metrics schema and shared seed.
ORIGIN_FIELD = "_origin"
ORIGIN_SIMPLE = "simple"


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base, returning new dict."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _iterate_group_scenarios(test_group: dict) -> Iterable[dict]:
    """Yield initial scenarios followed by the configured iteration sequence."""
    yield from test_group.get("scenarios", [])

    iterations = test_group.get("iterations")
    if iterations is None:
        return

    for iteration in range(1, iterations["count"] + 1):
        for scenario in iterations["scenarios"]:
            on_iterations = scenario.get("on_iterations")
            if on_iterations is not None and iteration not in on_iterations:
                continue

            repeated_scenario = copy.deepcopy(scenario)
            repeated_scenario["iteration"] = iteration
            for field in ("command", "dataset"):
                value = repeated_scenario.get(field)
                if isinstance(value, str):
                    repeated_scenario[field] = value.replace(
                        "{iteration}",
                        str(iteration),
                    )
            yield repeated_scenario


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
        repository: Optional[str] = None,
        config_name: Optional[str] = None,
        module_commit: Optional[str] = None,
        module_commit_timestamp: Optional[str] = None,
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
        self.repository = repository
        self.config_name = config_name
        self.module_commit = module_commit
        self.module_commit_timestamp = module_commit_timestamp
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
            sha = resolve_ref(commit_id, self.valkey_path)
            return get_commit_timestamp(sha, self.valkey_path)
        except Exception as e:
            logging.exception(f"Failed to get commit time for {commit_id}: {e}")
            raise

    def _get_active_ports(self) -> List[int]:
        """Return ports based on actual cluster mode."""
        if self.cluster_mode and "cluster_ports" in self.config:
            return self.config["cluster_ports"]
        return [self.config.get("port", 6379)]

    def _should_add_cluster_flag(self, scenario: Optional[dict] = None) -> bool:
        """Return whether the valkey-benchmark command should include --cluster."""
        if not self.cluster_mode:
            return False
        if scenario is None:
            return True
        return scenario.get("cluster_execution", "single") == "single"

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

    def _apply_config_set(self, config_set: dict) -> None:
        """Apply CONFIG SET commands to all server nodes after restart."""
        apply_config_to_servers(
            config_set,
            self._get_active_ports(),
            self.target_ip,
            tls_mode=self.tls_mode,
            valkey_dir=self.valkey_path,
        )

    def _populate_keyspace(
        self,
        workload_key: str,
        write_workload: str,
        requests: int,
        keyspacelen: int,
        data_size: int,
        pipeline: int,
        clients: int,
        seed_val: int,
    ) -> None:
        """Run a sequential write workload to seed the keyspace."""
        logging.info(f"Populating keyspace using {write_workload}")

        populate_scenario = {
            workload_key: write_workload,
            "requests": requests,
            "keyspacelen": keyspacelen,
            "data_size": data_size,
            "pipeline": pipeline,
            "clients": clients,
            "sequential": True,
        }
        bench_cmd = self._build_benchmark_command(
            populate_scenario, tls=self.tls_mode, seed_val=seed_val
        )

        self._run(command=bench_cmd, cwd=self.valkey_path, timeout=None)
        logging.info(f"Keyspace populated using {write_workload} with {requests} keys")

    def run_benchmark_config(self) -> None:
        """Execute the configured scenarios and persist their metrics."""
        commit_time = self.get_commit_time(self.commit_id)

        (
            profiler,
            metrics_processor,
            profiling_enabled,
        ) = self._setup_profiling_and_metrics(self.current_profiling_set, commit_time)

        metric_json = []
        for scenario_data in self._iterate_test_groups_scenarios():
            result = self._run_single_scenario(
                scenario_data["scenario"],
                scenario_data["group_id"],
                profiler,
                metrics_processor,
                scenario_data["config_set"],
                scenario_data["config_suffix"],
                scenario_data.get("group_description"),
            )
            if result:
                if isinstance(result, list):
                    metric_json.extend(result)
                else:
                    metric_json.append(result)

        self._finalize_metrics(metrics_processor, metric_json, profiling_enabled)

    def _get_effective_runs(self) -> int:
        """Return one run while profiling, otherwise the configured count."""
        if self.current_profiling_set.get("enabled", False) and self.runs > 1:
            logging.info("Profiling enabled: forcing runs=1 (profiling runs only once)")
            return 1
        return self.runs

    def _iterate_test_groups_scenarios(self):
        """Yield scenarios in group/run/scenario order."""
        effective_runs = self._get_effective_runs()
        groups_to_run = self.config.get("groups_to_run")
        scenario_filter = self.config.get("scenario_filter")

        for test_group in self.config.get("test_groups", []):
            group_id = test_group.get("group", "unknown")
            group_description = test_group.get("description")

            if groups_to_run and group_id not in groups_to_run:
                logging.info(
                    f"Skipping group {group_id} (not in filter: {groups_to_run})"
                )
                continue

            for run_num in range(effective_runs):
                if effective_runs > 1:
                    logging.info(
                        f"=== Group {group_id}: {group_description or ''} "
                        f"(run {run_num + 1}/{effective_runs}) ==="
                    )
                else:
                    logging.info(f"=== Group {group_id}: {group_description or ''} ===")

                for scenario in _iterate_group_scenarios(test_group):
                    # Cluster mode is scalarized only at execution time.
                    test_cmd = scenario.get("test")
                    if test_cmd in ("MSET", "MGET") and self.cluster_mode:
                        logging.warning(
                            f"Command {test_cmd} not supported in cluster mode, skipping."
                        )
                        continue

                    for expanded_scenario in self._expand_scenario_options(scenario):
                        if (
                            scenario_filter
                            and expanded_scenario.get("id") not in scenario_filter
                        ):
                            logging.info(
                                f"Skipping scenario {expanded_scenario.get('id')} (filtered)"
                            )
                            continue

                        yield {
                            "scenario": expanded_scenario,
                            "group_id": group_id,
                            "group_description": group_description,
                            "config_set": self.current_config_set,
                            "config_suffix": self.config_suffix,
                        }

    def _build_benchmark_command(
        self,
        scenario: dict,
        *,
        tls: Optional[bool] = None,
        seed_val: Optional[int] = None,
        warmup_mode: bool = False,
        port: Optional[int] = None,
        cpu_range: Optional[str] = None,
    ) -> List[str]:
        """Build argv for a predefined ``test`` or arbitrary ``command``.

        ``seed_val`` shares a seed across related invocations; when omitted,
        each invocation draws one unless seeding is disabled.
        """
        cmd = []

        cores = cpu_range or self.cores
        if cores:
            cmd += ["taskset", "-c", cores]

        cmd.append(self.valkey_benchmark_path)

        use_tls = tls if tls is not None else self.tls_mode
        if use_tls:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"]
            cmd += ["--cacert", "./tests/tls/ca.crt"]

        cmd += ["-h", self.target_ip]
        cmd += ["-p", str(port or self.config.get("port", DEFAULT_PORT))]

        keyspacelen_val = scenario.get(
            "keyspacelen", self.config.get("keyspacelen", [1000000])[0]
        )

        if "test" in scenario:
            if warmup_mode:
                cmd += ["--duration", str(scenario.get("warmup", 60))]
            elif scenario.get("duration") is not None:
                cmd += ["--duration", str(scenario["duration"])]
            elif scenario.get("requests") is not None:
                cmd += ["-n", str(scenario["requests"])]
            else:
                raise ValueError(
                    f"test scenario {scenario.get('id')!r} requires "
                    "'requests' or 'duration'"
                )

            cmd += ["-r", str(keyspacelen_val)]
            if scenario.get("data_size") is not None:
                cmd += ["-d", str(scenario["data_size"])]
            cmd += ["-P", str(scenario.get("pipeline", 1))]
            cmd += ["-c", str(scenario.get("clients", 1))]
            cmd += ["-t", scenario["test"]]

            if self.benchmark_threads is not None:
                cmd += ["--threads", str(self.benchmark_threads)]

            # Inline warmup is distinct from the scenario's pre-run warmup.
            warmup_inline = scenario.get("warmup_inline")
            if not warmup_mode and warmup_inline is not None and warmup_inline > 0:
                cmd += ["--warmup", str(warmup_inline)]
        else:
            if scenario.get("dataset"):
                dataset_path = Path(scenario["dataset"])
                if not dataset_path.is_absolute():
                    dataset_path = Path.cwd() / dataset_path
                cmd += ["--dataset", str(dataset_path)]

                if scenario.get("xml_root_element"):
                    cmd += ["--xml-root-element", scenario["xml_root_element"]]

                if scenario.get("maxdocs") and scenario.get("type") == "write":
                    cmd += ["--maxdocs", str(scenario["maxdocs"])]

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
            cmd += ["-r", str(keyspacelen_val)]
            if scenario.get("data_size") is not None:
                cmd += ["-d", str(scenario["data_size"])]

            if self.benchmark_threads is not None:
                cmd += ["--threads", str(self.benchmark_threads)]

        if scenario.get("sequential", False):
            cmd += ["--sequential"]

        if self._should_add_cluster_flag(scenario):
            cmd += ["--cluster"]

        if scenario.get("seed") is not False and self.config.get("seed") is not False:
            seed = seed_val if seed_val is not None else random.randint(0, 1000000)
            cmd += ["--seed", str(seed)]

        cmd += ["--csv"]

        if "command" in scenario:
            cmd += ["--"]
            cmd += shlex.split(scenario["command"])

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

    def _parse_csv_row_for_test(self, stdout: str, test_name: str) -> Optional[dict]:
        """Parse CSV output of a predefined ``-t`` workload.

        ``valkey-benchmark -t CMD`` emits rows whose test name may be a
        variant of the command (e.g. ``MSET (10 keys)``), so the first row
        whose test name starts with the benchmarked name is returned.
        """
        if not stdout:
            return None
        lines = stdout.splitlines()
        csv_start = self._find_csv_start(lines)
        if csv_start is None:
            return None
        for row in csv.DictReader(lines[csv_start:]):
            if row.get("test", "").startswith(test_name):
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
        """Expand option variants, applying mixed options to read children."""
        options = scenario.get("options")

        if not options:
            return [scenario]

        scenarios = []
        for flag, suffix in options.items():
            variant = copy.deepcopy(scenario)
            variant["id"] = scenario["id"] + suffix

            if variant.get("type") == "mixed":
                for read in variant.get("reads", []):
                    # options append a benchmark flag to an arbitrary command
                    # string; a predefined ``test:`` read has no command string
                    # to extend, so leave it untouched.
                    if flag and "command" in read:
                        read["command"] = read["command"] + f" {flag}"
            else:
                variant["command"] = scenario["command"] + (f" {flag}" if flag else "")

            if "description" in variant and flag:
                variant["description"] += f" + {flag}"
            scenarios.append(variant)

        return scenarios

    def _apply_row_metadata(
        self,
        metrics: dict,
        *,
        test_id: str,
        test_phase: str,
        group_id,
        scenario_id: str,
        config_set: dict,
        group_description: Optional[str] = None,
        scenario_description: Optional[str] = None,
        dataset: Optional[str] = None,
        iteration: Optional[int] = None,
    ) -> None:
        """Stamp shared scenario identity fields onto ``metrics`` in place.

        Optional fields remain absent when unset so success and failure rows
        have identical comparison keys.
        """
        metrics["test_id"] = test_id
        metrics["test_phase"] = test_phase
        metrics["group"] = group_id
        metrics["scenario"] = scenario_id
        metrics["config_set"] = config_set
        if group_description:
            metrics["group_description"] = group_description
        if scenario_description:
            metrics["scenario_description"] = scenario_description
        if self.config_name:
            metrics["config_name"] = self.config_name
        if self.module_commit:
            metrics["module_commit"] = self.module_commit
        if self.module_commit_timestamp:
            metrics["module_commit_timestamp"] = self.module_commit_timestamp
        if dataset:
            metrics["dataset"] = dataset
        if iteration is not None:
            metrics["iteration"] = iteration

    def _create_failure_marker(
        self,
        metrics_processor,
        workload: dict,
        *,
        group_id,
        scenario_id: str,
        test_id: str,
        test_phase: str,
        error: str,
        config_set: Optional[dict] = None,
        requests: Optional[int] = None,
        warmup: Optional[int] = None,
        parent_scenario: Optional[dict] = None,
        group_description: Optional[str] = None,
    ) -> dict:
        """Build a failed row with identity metadata but no performance fields.

        Mixed children inherit duration and description from their parent.
        """
        parent = parent_scenario or workload
        marker = metrics_processor.build_base_metadata(
            workload.get("command") or workload.get("test", ""),
            workload.get("data_size", 100),
            workload.get("pipeline", 1),
            workload.get("clients", 1),
            requests=requests,
            warmup=warmup,
            duration=workload.get("duration") or parent.get("duration"),
        )
        marker["status"] = "failed"
        marker["error"] = error
        self._apply_row_metadata(
            marker,
            test_id=test_id,
            test_phase=test_phase,
            group_id=group_id,
            scenario_id=scenario_id,
            config_set=config_set if config_set is not None else {},
            group_description=group_description,
            scenario_description=parent.get("description"),
            dataset=workload.get("dataset"),
            iteration=parent.get("iteration"),
        )
        return marker

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
            env_metadata = collect_environment_metadata(
                benchmark_path=self.valkey_benchmark_path,
                server_cpu_range=self.config.get("server_cpu_range"),
                client_cpu_range=self.cores,
            )
            metrics_processor = MetricsProcessor(
                self.commit_id,
                self.cluster_mode,
                self.tls_mode,
                commit_time,
                self.io_threads,
                self.benchmark_threads,
                self.architecture,
                self.repository,
                environment_metadata=env_metadata,
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
        config_set,
        config_suffix,
        group_description=None,
    ):
        """Run one scenario and return its metric row(s)."""
        scenario_type = scenario.get("type", "test")
        scenario_id = scenario.get("id", "unknown")
        origin_simple = scenario.get(ORIGIN_FIELD) == ORIGIN_SIMPLE
        iteration = scenario.get("iteration", None)

        logging.info(f"Running scenario: {scenario_id} (type: {scenario_type})")

        self._prepare_server_state(scenario, config_set)

        seed_val = self._draw_scenario_seed(scenario, origin_simple)

        effective_profiling = self._resolve_effective_profiling(scenario)
        scenario_profiling_enabled = effective_profiling.get("enabled", False)
        profile_id = f"group{group_id}_{scenario_type}_{scenario_id}_{config_suffix}"
        if iteration is not None:
            profile_id += f"_iteration_{iteration}"

        warmup_duration = scenario.get("warmup", 0)
        try:
            # Population failures follow the scenario's normal error policy.
            self._populate_scenario_keyspace(scenario, seed_val)

            self._run_scenario_warmup(scenario, group_id, config_set)

            self._start_scenario_profiling(
                profiler, scenario_profiling_enabled, effective_profiling, profile_id
            )

            # This finally is the single profiling teardown path.
            try:
                if scenario_type == "mixed":
                    logging.info(f"Running mixed workload for scenario {scenario_id}")
                    metrics_list = self._run_mixed_workload(
                        scenario,
                        group_id,
                        config_set,
                        metrics_processor,
                        warmup_duration,
                        group_description=group_description,
                    )
                    return metrics_list if metrics_list else None

                # Invocation errors reach the outer scenario error policy.
                proc, aggregated_row = self._execute_benchmark_run(scenario, seed_val)

                if proc is None and aggregated_row is None:
                    logging.error(f"Benchmark failed for scenario {scenario_id}")
                    # Basic metrics omit scenario-schema failure markers.
                    if metrics_processor and not origin_simple:
                        return self._create_failure_marker(
                            metrics_processor,
                            scenario,
                            group_id=group_id,
                            scenario_id=scenario_id,
                            test_id=f"{group_id}_{scenario_id}",
                            test_phase=scenario_type,
                            error="No results",
                            config_set=config_set,
                            requests=scenario.get("requests")
                            or scenario.get("maxdocs"),
                            warmup=scenario.get("warmup_inline", warmup_duration),
                            group_description=group_description,
                        )
                    return None

                if proc:
                    logging.info(f"Benchmark output:\n{proc.stdout}")

                # Basic parse failures skip one combination; other scenarios
                # emit a failure marker through the outer handler.
                try:
                    return self._build_scenario_metrics(
                        scenario,
                        proc,
                        aggregated_row,
                        group_id,
                        config_set,
                        warmup_duration,
                        group_description,
                        metrics_processor,
                    )
                except Exception as e:
                    if origin_simple:
                        logging.error(
                            f"Failed to parse benchmark results for scenario "
                            f"{group_id}_{scenario_id}: {e}"
                        )
                        return None
                    raise
            finally:
                self._stop_scenario_profiling(
                    profiler, scenario_profiling_enabled, profile_id
                )

        except Exception as e:
            if origin_simple:
                raise
            logging.error(f"Scenario {group_id}_{scenario_id} failed: {e}")
            if metrics_processor:
                return self._create_failure_marker(
                    metrics_processor,
                    scenario,
                    group_id=group_id,
                    scenario_id=scenario_id,
                    test_id=f"{group_id}_{scenario_id}",
                    test_phase=scenario_type,
                    error=str(e),
                    config_set=config_set,
                    requests=scenario.get("requests") or scenario.get("maxdocs"),
                    warmup=scenario.get("warmup_inline", warmup_duration),
                    group_description=group_description,
                )

        return None

    def _prepare_server_state(self, scenario, config_set):
        """Clean server state before a scenario runs, then run setup commands.

        Restart when a launcher is available; otherwise flush the database.
        """
        if scenario.get("restart_before", False) or scenario.get("flush_before", False):
            if self.server_launcher:
                self._restart_server()
                # Re-apply config_set after restart since CONFIG SET values are lost
                if config_set:
                    self._apply_config_set(config_set)
            else:
                self._flush_database()

        for setup_cmd in scenario.get("setup_commands", []):
            self._execute_setup_command(setup_cmd)

    def _draw_scenario_seed(self, scenario, origin_simple):
        """Draw one seed shared by a populate pass and its main run."""
        if origin_simple or scenario.get("populate_with"):
            seed_val = random.randint(0, 1000000)
            logging.info(f"Using seed value: {seed_val}")
            return seed_val
        return None

    def _populate_scenario_keyspace(self, scenario, seed_val):
        """Seed a scenario's keyspace through its configured write workload."""
        populate_with = scenario.get("populate_with")
        if not populate_with:
            return

        keyspacelen_val = scenario.get(
            "keyspacelen", self.config.get("keyspacelen", [1000000])[0]
        )
        populate_requests = (
            scenario["requests"]
            if scenario.get("requests") is not None
            else keyspacelen_val
        )
        workload_key = "command" if "command" in scenario else "test"
        self._populate_keyspace(
            workload_key,
            populate_with,
            populate_requests,
            keyspacelen_val,
            scenario.get("data_size", 100),
            scenario.get("pipeline", 1),
            scenario.get("clients", 1),
            seed_val,
        )

    def _resolve_effective_profiling(self, scenario):
        """Merge a scenario's profiling override onto the current profiling set."""
        if scenario.get("profiling"):
            return deep_merge(self.current_profiling_set, scenario["profiling"])
        return self.current_profiling_set

    def _run_scenario_warmup(self, scenario, group_id, config_set):
        """Run the scenario-shaped warmup pass and discard its results."""
        warmup_duration = scenario.get("warmup", 0)
        if warmup_duration <= 0:
            return

        scenario_type = scenario.get("type", "test")
        if scenario_type == "mixed":
            warmup_scenario = copy.deepcopy(scenario)
            warmup_scenario["duration"] = warmup_duration
            # Opt-in: warm only the write side. Reads during warmup query a cold
            # keyspace, are discarded anyway, and consume client capacity that
            # could be populating. Absent/false keeps today's full mixed warmup
            # exactly, so configs relying on it to warm read-path state (e.g. the
            # FTS scenario "j") are unaffected.
            if warmup_scenario.get("warmup_writes_only"):
                warmup_scenario["reads"] = []
                logging.info(f"Running mixed warmup (writes only): {warmup_duration}s")
            else:
                logging.info(f"Running mixed warmup: {warmup_duration}s")
            self._run_mixed_workload(
                warmup_scenario,
                group_id,
                config_set,
                metrics_processor=None,
                warmup_duration=0,
            )
        elif self._should_use_parallel(scenario):
            logging.info(
                f"Running parallel warmup on {len(self._get_active_ports())} nodes: {warmup_duration}s"
            )
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

    def _start_scenario_profiling(
        self, profiler, scenario_profiling_enabled, effective_profiling, profile_id
    ):
        """Start profiling for a scenario when a profiler is enabled."""
        if profiler and scenario_profiling_enabled:
            target_port = self._get_active_ports()[0] if self._is_cme() else None
            if target_port:
                logging.info(f"CME profiling: targeting node 0 on port {target_port}")

            # Pass scenario delays override
            profiler.delays = effective_profiling.get("delays", profiler.delays)
            profiler.start_profiling(
                profile_id, target_process="valkey-server", target_port=target_port
            )

    def _stop_scenario_profiling(
        self, profiler, scenario_profiling_enabled, profile_id
    ):
        """Stop profiling for a scenario when a profiler is enabled."""
        if profiler and scenario_profiling_enabled:
            profiler.stop_profiling(profile_id)

    def _execute_benchmark_run(self, scenario, seed_val):
        """Return a process or an aggregated row for a non-mixed scenario."""
        if self._should_use_parallel(scenario):
            logging.info(
                f"Using parallel execution for scenario {scenario.get('id', 'unknown')}"
            )
            aggregated_row = self._run_parallel_search(
                scenario,
                self._get_active_ports(),
                self.client_cpu_ranges,
                seed_val=seed_val,
            )
            return None, aggregated_row

        cpu = self.client_cpu_ranges[0] if self.client_cpu_ranges else None
        proc = self._run(
            self._build_benchmark_command(
                scenario=scenario, cpu_range=cpu, seed_val=seed_val
            ),
            cwd=self.valkey_path,
            capture_output=True,
            timeout=None,
        )
        return proc, None

    def _build_scenario_metrics(
        self,
        scenario,
        proc,
        aggregated_row,
        group_id,
        config_set,
        warmup_duration,
        group_description,
        metrics_processor,
    ):
        """Parse output and construct a metric row when one is available.

        Compiled basic scenarios retain the basic schema; other rows receive
        scenario identity fields.
        """
        if not metrics_processor:
            return None

        scenario_id = scenario.get("id", "unknown")
        scenario_type = scenario.get("type", "test")
        origin_simple = scenario.get(ORIGIN_FIELD) == ORIGIN_SIMPLE

        requests_value = scenario.get("requests") or scenario.get("maxdocs")

        if aggregated_row:
            row = aggregated_row
            command_label = (
                scenario["command"]
                if "command" in scenario
                else row.get("test") or scenario["test"]
            )
        elif "test" in scenario:
            row = self._parse_csv_row_for_test(
                proc.stdout if proc else "", scenario["test"]
            )
            command_label = row.get("test") if row else None
        else:
            row = self._parse_csv_row(proc.stdout if proc else "")
            command_label = scenario["command"]

        if not row:
            logging.warning(f"No metrics data for scenario {scenario_id}")
            return None

        warmup_metrics = (
            scenario["warmup_inline"]
            if "warmup_inline" in scenario
            else warmup_duration
        )

        metrics = metrics_processor.create_metrics(
            row,
            command_label,
            scenario.get("data_size", 100),
            scenario.get("pipeline", 1),
            scenario.get("clients", 1),
            requests_value,
            warmup_metrics,
            scenario.get("duration"),
        )

        if not metrics:
            return None

        if origin_simple:
            logging.info(f"Parsed metrics for {command_label}: {metrics}")
            return metrics

        metrics["status"] = "success"
        self._apply_row_metadata(
            metrics,
            test_id=f"{group_id}_{scenario_id}",
            test_phase=scenario_type,
            group_id=group_id,
            scenario_id=scenario_id,
            config_set=config_set,
            group_description=group_description,
            scenario_description=scenario.get("description"),
            dataset=scenario.get("dataset"),
            iteration=scenario.get("iteration"),
        )
        return metrics

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

    def _normalize_mixed_configs(self, scenario: dict):
        """Apply inherited parent parameters to mixed children."""
        write_scenarios = scenario.get("writes", [])
        read_scenarios = scenario.get("reads", [])

        for cfg in write_scenarios + read_scenarios:
            # Inherit the parent's run bound / shape only where the sub omits it.
            # duration-over-requests precedence stays in _build_benchmark_command.
            if "duration" not in cfg and scenario.get("duration") is not None:
                cfg["duration"] = scenario["duration"]
            if "requests" not in cfg and scenario.get("requests") is not None:
                cfg["requests"] = scenario["requests"]
            if "pipeline" not in cfg:
                cfg["pipeline"] = scenario.get("pipeline", 1)
            # A parent data_size must reach subs, else _create_mixed_metric
            # misreports the payload as its data_size=100 default.
            if "data_size" not in cfg and scenario.get("data_size") is not None:
                cfg["data_size"] = scenario["data_size"]
            if "cluster_execution" not in cfg and scenario.get("cluster_execution"):
                cfg["cluster_execution"] = scenario["cluster_execution"]

        return write_scenarios, read_scenarios

    def _get_cpu_for_mixed_process(self, process_idx: int) -> Optional[str]:
        """Allocate ``cores_per_client`` cores to each mixed-workload process.

        Cores inside the configured client pool are handed out by indexing the
        flattened core list, so a non-contiguous pool like "10-11,20-21" gives
        process 0 -> "10-11" and process 1 -> "20-21" (never the absent
        "12-13"). Once the pool is exhausted the allocation continues
        arithmetically past the last pool core and logs a warning, so an
        undersized pool (e.g. the shipped FTS config) spills onto extra cores
        rather than aborting the run.
        """
        if not self.client_cpu_ranges:
            return None

        cpu_alloc = self.config.get("cpu_allocation", {})
        cores_per_client = cpu_alloc.get("cores_per_client", 1)

        # Flatten the pool across every range string, in order. Indexing the
        # real core list (not first..last arithmetic) keeps a non-contiguous
        # pool like "10-11,20-21" from handing process 1 the absent cores 12-13.
        pool = []
        for range_str in self.client_cpu_ranges:
            pool.extend(parse_core_range(range_str))

        # Processes that fit fully inside the pool get their real cores. A
        # partial trailing slice counts as pool exhaustion for that process.
        num_pool_processes = len(pool) // cores_per_client
        if process_idx < num_pool_processes:
            start = process_idx * cores_per_client
            cores = pool[start : start + cores_per_client]
            return format_core_list(cores)

        # Pool exhausted: continue arithmetically past the last pool core and
        # warn that processes may overlap CPU cores.
        last_core = pool[-1]
        spill_idx = process_idx - num_pool_processes
        proc_start = last_core + 1 + (spill_idx * cores_per_client)
        proc_end = proc_start + cores_per_client - 1
        logging.warning(
            f"Mixed process {process_idx} pinned to cores {proc_start}-{proc_end} "
            f"which exceeds the client CPU pool ending at core {last_core}. "
            f"Processes may overlap CPU cores."
        )
        return f"{proc_start}-{proc_end}" if proc_end > proc_start else str(proc_start)

    def _launch_mixed_process(
        self, sub_scenario: dict, port: int, process_idx: int, label: str
    ):
        """Launch a single benchmark subprocess for a mixed sub-scenario."""
        cpu = self._get_cpu_for_mixed_process(process_idx)
        cmd = self._build_benchmark_command(
            scenario=sub_scenario, port=port, cpu_range=cpu
        )
        cmd_str = shlex.join(cmd)
        logging.info(f"{label} [port {port}]: {cmd_str[:200]}...")
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=self.valkey_path,
        )

    def _launch_mixed_processes(
        self, write_scenarios: List[dict], read_scenarios: List[dict], ports: List[int]
    ):
        """Launch all mixed write + read processes across the given ports."""
        process_idx = 0
        write_procs = []
        read_procs = []
        try:
            for write_cfg in write_scenarios:
                sub = copy.deepcopy(write_cfg)
                write_id = write_cfg.get("id", "w")
                for port in ports:
                    proc = self._launch_mixed_process(
                        sub, port, process_idx, f"write-{write_id}"
                    )
                    write_procs.append((proc, port, write_id))
                    process_idx += 1

            for read_cfg in read_scenarios:
                sub = copy.deepcopy(read_cfg)
                read_id = read_cfg.get("id", "r")
                for port in ports:
                    proc = self._launch_mixed_process(
                        sub, port, process_idx, f"read-{read_id}"
                    )
                    read_procs.append((proc, port, read_id))
                    process_idx += 1
        except Exception:
            self._terminate_mixed_processes(write_procs + read_procs)
            raise

        return write_procs, read_procs

    @staticmethod
    def _terminate_mixed_processes(procs: List[tuple]) -> None:
        """Stop every still-running mixed client and reap it."""
        running = []
        for proc, _port, _sub_id in procs:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    running.append(proc)
            except Exception:
                logging.exception("Failed to terminate mixed benchmark process")

        for proc in running:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except Exception:
                    logging.exception("Failed to kill mixed benchmark process")
            except Exception:
                logging.exception("Failed to reap mixed benchmark process")

    def _collect_mixed_results(self, procs: List[tuple], label: str) -> dict:
        """Wait for mixed workload processes and group results by sub-scenario id."""
        results_by_id: dict = {}
        for proc, port, sub_id in procs:
            stdout, stderr = proc.communicate()
            if proc.returncode == 0:
                results_by_id.setdefault(sub_id, []).append((stdout, stderr, port))
                logging.info(f"{label}-{sub_id} on port {port} completed")
            else:
                logging.error(f"{label}-{sub_id} on port {port} failed: {stderr}")
        return results_by_id

    def _create_mixed_metric(
        self,
        row: dict,
        sub_cfg: dict,
        parent_scenario: dict,
        group_id,
        scenario_id: str,
        sub_id: str,
        phase: str,
        config_set: dict,
        warmup_duration: int,
        metrics_processor,
        group_description: Optional[str] = None,
    ) -> Optional[dict]:
        """Build a metrics dict for a single mixed sub-scenario result."""
        if not metrics_processor:
            return None

        # A sub-scenario carries EITHER an arbitrary ``command`` (recorded
        # verbatim) or a predefined ``test`` workload. For a ``test:`` sub the
        # command field records the CSV row's own test name, falling back to the
        # sub's test name -- the same convention _build_scenario_metrics uses for
        # a predefined workload's aggregated row (row["test"] is set to the
        # canonical test name by _aggregate_parallel_results).
        command_label = (
            sub_cfg["command"]
            if "command" in sub_cfg
            else row.get("test") or sub_cfg["test"]
        )

        metrics = metrics_processor.create_metrics(
            row,
            command_label,
            sub_cfg.get("data_size", 100),
            sub_cfg.get("pipeline", 1),
            sub_cfg.get("clients", 1),
            sub_cfg.get("requests") or parent_scenario.get("requests"),
            warmup_duration,
            sub_cfg.get("duration") or parent_scenario.get("duration"),
        )
        if not metrics:
            return None

        metrics["status"] = "success"
        self._apply_row_metadata(
            metrics,
            test_id=f"{group_id}_{scenario_id}_{phase}_{sub_id}",
            test_phase=f"mixed_{phase}",
            group_id=group_id,
            scenario_id=scenario_id,
            config_set=config_set,
            group_description=group_description,
            scenario_description=parent_scenario.get("description"),
            dataset=sub_cfg.get("dataset"),
            iteration=parent_scenario.get("iteration"),
        )
        return metrics

    def _run_mixed_workload(
        self,
        scenario: dict,
        group_id,
        config_set: dict,
        metrics_processor,
        warmup_duration: int,
        group_description: Optional[str] = None,
    ) -> Optional[List[dict]]:
        """Run concurrent mixed children and return one row per expected child.

        Missing results become child-specific failure rows. Warmups drain all
        processes without recording rows and propagate launch failures.
        """
        write_scenarios, read_scenarios = self._normalize_mixed_configs(scenario)

        if not write_scenarios and not read_scenarios:
            logging.warning(
                f"Mixed scenario {scenario.get('id')} has no writes or reads"
            )
            return None

        sid = scenario.get("id", "unknown")
        warmup_mode = metrics_processor is None
        write_procs: List[tuple] = []
        read_procs: List[tuple] = []

        expected = [(w, "write", w.get("id", "w")) for w in write_scenarios] + [
            (r, "read", r.get("id", "r")) for r in read_scenarios
        ]

        def _marker(sub_cfg, phase, sub_id, error):
            return self._create_failure_marker(
                metrics_processor,
                sub_cfg,
                group_id=group_id,
                scenario_id=sid,
                test_id=f"{group_id}_{sid}_{phase}_{sub_id}",
                test_phase=f"mixed_{phase}",
                error=error,
                config_set=config_set,
                requests=sub_cfg.get("requests") or scenario.get("requests"),
                warmup=warmup_duration,
                parent_scenario=scenario,
                group_description=group_description,
            )

        try:
            if scenario.get("cluster_execution") == "single" or not self._is_cme():
                ports = [self._get_active_ports()[0]]
            else:
                ports = self._get_active_ports()

            total_writes = sum(w.get("clients", 1) for w in write_scenarios) * len(
                ports
            )
            total_reads = sum(r.get("clients", 1) for r in read_scenarios) * len(ports)
            logging.info(
                f"Mixed workload: {total_writes} write clients + {total_reads} read clients "
                f"across {len(ports)} node(s)"
            )

            write_procs, read_procs = self._launch_mixed_processes(
                write_scenarios, read_scenarios, ports
            )
            write_results = self._collect_mixed_results(write_procs, "write")
            read_results = self._collect_mixed_results(read_procs, "read")

            if warmup_mode:
                return None

            metrics_list: List[dict] = []
            for sub_cfg, phase, sub_id in expected:
                results = write_results if phase == "write" else read_results
                # A mixed sub-scenario carries EITHER ``command`` or ``test``;
                # hand _aggregate_parallel_results the key it actually has so the
                # aggregated row's test name is the workload the sub ran.
                workload_key = "command" if "command" in sub_cfg else "test"
                row = (
                    self._aggregate_parallel_results(
                        results[sub_id], {workload_key: sub_cfg[workload_key]}
                    )
                    if results.get(sub_id)
                    else None
                )
                metric = (
                    self._create_mixed_metric(
                        row,
                        sub_cfg,
                        scenario,
                        group_id,
                        sid,
                        sub_id,
                        phase,
                        config_set,
                        warmup_duration,
                        metrics_processor,
                        group_description,
                    )
                    if row is not None
                    else None
                )
                metrics_list.append(
                    metric
                    if metric is not None
                    else _marker(
                        sub_cfg,
                        phase,
                        sub_id,
                        "Mixed sub-scenario produced no successful result",
                    )
                )
        except Exception as e:
            self._terminate_mixed_processes(write_procs + read_procs)
            if warmup_mode:
                raise
            logging.error(f"Mixed workload for scenario {sid} raised: {e}")
            metrics_list = [
                _marker(sub_cfg, phase, sub_id, str(e))
                for sub_cfg, phase, sub_id in expected
            ]

        logging.info(f"Mixed workload produced {len(metrics_list)} metric entries")
        return metrics_list if metrics_list else None

    def _run_parallel_search(
        self,
        scenario: dict,
        ports: List[int],
        client_cpu_ranges: List[str],
        warmup_mode: bool = False,
        seed_val: Optional[int] = None,
    ) -> dict:
        """Run search on all cluster nodes, optionally sharing ``seed_val``."""
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
                seed_val=seed_val,
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

        workload_key = "command" if "command" in scenario else "test"
        aggregated = {
            "test": scenario[workload_key],
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

        # Flush database before shutdown to eliminate RDB/index cleanup delays
        # that can block the server from releasing the port in time.
        try:
            self._flush_database()
        except Exception as e:
            logging.warning(
                f"Pre-shutdown flush failed (proceeding with shutdown): {e}"
            )

        # Shutdown current server
        self.server_launcher.shutdown(self.tls_mode)

        # Start fresh server (module_path and config are stored in launcher)
        self.server_launcher.launch(
            cluster_mode=self.cluster_mode,
            tls_mode=self.tls_mode,
            io_threads=self.io_threads,
            module_path=self.server_launcher.module_path,
            config=self.server_launcher.config,
        )

        # Wait for server to be ready
        self.wait_for_server_ready()
        logging.info("Server restarted successfully")
