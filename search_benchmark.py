"""Search module benchmark execution (FTS, vector, numeric, tag)."""

import logging
import time
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

import valkey
from valkey_benchmark import ClientRunner
from profiler import PerformanceProfiler
from process_metrics import MetricsProcessor
from cpu_monitor import CPUMonitor
from per_cpu_monitor import PerCPUMonitor


class SearchBenchmarkRunner(ClientRunner):
    """Search module benchmark runner (FTS, vector, numeric, tag)."""

    def __init__(self, *args, **kwargs):
        # Extract parameters before calling parent __init__
        profiling_enabled = kwargs.pop("profiling_enabled", False)
        commit_id = kwargs.get("commit_id", "HEAD")
        server_type = kwargs.pop("server_type", "auto")
        config = kwargs.get("config", {})

        super().__init__(*args, **kwargs)

        # Search-specific state
        self.current_index = None

        # Generic components - handle their own config
        self.profiler = PerformanceProfiler(
            self.results_dir, profiling_enabled, config=config, commit_id=commit_id
        )

        # Monitoring configuration
        monitoring_config = config.get("monitoring", {})
        cpu_enabled = monitoring_config.get("cpu_enabled", True)
        per_cpu_enabled = monitoring_config.get("per_cpu_enabled", True)

        # Server CPU monitoring
        self.cpu_monitor = CPUMonitor(server_type=server_type, enabled=cpu_enabled)

        # Per-CPU monitoring
        server_cpu_range = config.get("server_cpu_range", "0-7")
        self.per_cpu_monitor = PerCPUMonitor(
            cpu_cores=server_cpu_range, enabled=per_cpu_enabled
        )

        # Metrics processing
        self.commit_id = commit_id
        self.commit_time = self._get_commit_time(commit_id)
        self.metrics_processor = MetricsProcessor(
            commit_id=commit_id,
            cluster_mode=kwargs.get("cluster_mode", False),
            tls_mode=kwargs.get("tls_mode", False),
            commit_time=self.commit_time,
            architecture=kwargs.get("architecture", "x86_64"),
        )
        self.all_metrics = []

    def _get_commit_time(self, commit_id: str) -> str:
        """Get commit timestamp."""
        try:
            result = subprocess.run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                cwd=self.valkey_path,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except Exception:
            from datetime import datetime

            return datetime.now().isoformat()

    def run_fts_benchmark_config(self, fts_config: Dict[str, Any]) -> None:
        """Run FTS benchmark tests according to configuration."""
        logging.info("=== Starting FTS Benchmark Testing ===")

        # Run compound ingestion+search tests
        for test_group in fts_config.get("fts_tests", []):
            self._run_compound_test_group(test_group)

        # Save results using MetricsProcessor with commit-based directory
        if self.all_metrics:
            commit_dir = self.results_dir / self.metrics_processor.commit_id
            self.metrics_processor.write_metrics(commit_dir, self.all_metrics)
            logging.info(
                f"Wrote {len(self.all_metrics)} metrics to {commit_dir}/metrics.json"
            )

        logging.info("=== FTS Benchmark Testing Complete ===")

    def _run_compound_test_group(self, test_group: Dict[str, Any]) -> None:
        """Run test group scenarios."""
        group_id = test_group["group"]
        description = test_group["description"]

        logging.info(f"=== Group {group_id}: {description} ===")

        # Get scenario filter from config if specified
        scenario_filter = self.config.get("scenario_filter")

        # Run scenarios
        for scenario_config in test_group.get("scenarios", []):
            scenario_id = scenario_config["id"]

            # Apply filter if specified
            if scenario_filter and scenario_id not in scenario_filter:
                logging.info(
                    f"Skipping scenario {scenario_id} (not in filter: {scenario_filter})"
                )
                continue

            scenario_type = scenario_config.get("type")

            if scenario_type == "ingestion":
                # Handle index creation + ingestion
                if "create_index_command" in scenario_config:
                    self._flush_database()
                    create_cmd = scenario_config["create_index_command"].split()
                    index_name = create_cmd[1] if len(create_cmd) > 1 else "rd0"
                    logging.info(f"Creating index '{index_name}'")
                    self._execute_fts_command(create_cmd)
                    self.current_index = index_name

                scenario_id = scenario_config["id"]
                logging.info(f"Scenario: Ingestion ({scenario_id})")
                self._run_ingestion_phase(scenario_config, f"{group_id}_{scenario_id}")

            elif scenario_type == "search":
                # Expand search options
                scenarios = self._expand_search_options(scenario_config)
                for scenario in scenarios:
                    scenario_id = scenario["id"]
                    search_desc = scenario.get("description", "")
                    logging.info(
                        f"Scenario: Search {group_id}{scenario_id} ({search_desc})"
                    )
                    self._run_search_phase(scenario, f"{group_id}{scenario_id}")

        logging.info(f"Completed Group {group_id}")

    def _expand_search_options(
        self, search_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Expand search scenario with options."""
        options = search_config.get("options")

        # No options: return base scenario as-is
        if not options:
            return [search_config]

        # Options provided: iterate and generate only those
        scenarios = []
        for flag, suffix in options.items():
            variant = search_config.copy()
            variant["id"] = search_config["id"] + suffix
            variant["command"] = search_config["command"] + (f" {flag}" if flag else "")
            if "description" in variant and flag:
                variant["description"] += f" + {flag}"
            scenarios.append(variant)

        return scenarios

    def _drop_fts_index(self) -> None:
        """Drop the current FTS index."""
        if self.current_index:
            logging.info(
                f"Dropping FTS index: {self.current_index} (may take time for large indexes)"
            )
            # Use extended timeout for large index drops
            try:
                client = valkey.Valkey(
                    host=self.target_ip,
                    port=self.config.get("port", 6379),
                    decode_responses=True,
                    socket_timeout=120,  # 2 minute timeout for large index drops
                    socket_connect_timeout=10,
                )
                result = client.execute_command("FT.DROPINDEX", self.current_index)
                logging.info(f"Index dropped successfully: {result}")
                client.close()
                self.current_index = None
            except Exception as e:
                logging.error(f"Failed to drop index {self.current_index}: {e}")
                raise

    def _execute_fts_command(self, cmd: List[str]) -> Any:
        """Execute FTS command via Valkey client with extended timeout."""
        try:
            # Use extended timeout for FTS commands (index creation can be slow)
            client = valkey.Valkey(
                host=self.target_ip,
                port=self.config.get("port", 6379),
                decode_responses=True,
                socket_timeout=120,  # 2 minute timeout for FTS operations
                socket_connect_timeout=10,
            )
            result = client.execute_command(*cmd)
            client.close()
            return result
        except Exception as e:
            logging.error(f"FTS command failed: {' '.join(cmd)}, Error: {e}")
            raise

    def _flush_database(self) -> None:
        """Flush database between test groups."""
        port = self.config.get("port", 6379)
        logging.info(
            f"Flushing database on {self.target_ip}:{port} for clean state (may take 30+ seconds for large datasets)"
        )
        try:
            # Create client with extended timeout for large flushes
            client = valkey.Valkey(
                host=self.target_ip,
                port=port,
                decode_responses=True,
                socket_timeout=120,  # 2 minute timeout for large flushes
                socket_connect_timeout=10,
            )

            # Test connection first
            try:
                client.ping()
                logging.info(f"Connected to server on port {port}")
            except Exception as e:
                logging.error(
                    f"Cannot connect to server on {self.target_ip}:{port}: {e}"
                )
                client.close()
                raise ConnectionError(f"Server not accessible on port {port}")

            # Execute FLUSHALL synchronously
            result = client.execute_command("FLUSHALL", "SYNC")
            logging.info(f"Database flushed successfully: {result}")
            client.close()
        except Exception as e:
            logging.error(f"Database flush failed: {e}")
            raise

    def _run_monitored_benchmark(
        self,
        config: Dict[str, Any],
        test_id: str,
        bench_cmd: List[str],
        phase: str,
        metric_params: Dict[str, Any],
    ) -> None:
        """Run benchmark with monitoring and metrics collection."""
        scenario_type = config.get("type", phase)
        monitor_id = f"{scenario_type}_{test_id}"

        self.profiler.start_profiling(monitor_id)
        self.cpu_monitor.start_monitoring(monitor_id)
        self.per_cpu_monitor.start_monitoring(monitor_id)

        logging.info(f"Running {phase}: {' '.join(bench_cmd)}")
        proc = self._run(
            bench_cmd, cwd=self.valkey_path, capture_output=True, timeout=None
        )

        self.profiler.stop_profiling(monitor_id)
        cpu_stats = self.cpu_monitor.stop_monitoring(monitor_id)
        per_cpu_stats = self.per_cpu_monitor.stop_monitoring(monitor_id)

        if proc:
            if proc.stderr:
                logging.warning(f"{phase.capitalize()} stderr: {proc.stderr}")

            metrics = self.metrics_processor.create_metrics(
                benchmark_csv_data=proc.stdout, **metric_params
            )
            if metrics:
                metrics["test_id"] = test_id
                metrics["test_phase"] = phase
                if config.get("dataset"):
                    metrics["dataset"] = config["dataset"]
                self._enrich_metrics_with_monitoring(metrics, cpu_stats, per_cpu_stats)
                self.all_metrics.append(metrics)

    def _get_field_size_for_dataset(self, dataset_path: Optional[str]) -> int:
        """Get field_size from dataset_generation config."""
        if not dataset_path:
            return 100

        dataset_name = Path(dataset_path).name
        dataset_gen = self.config.get("dataset_generation", {})

        if dataset_name in dataset_gen:
            return dataset_gen[dataset_name].get("field_size", 100)

        return 100

    def _enrich_metrics_with_monitoring(
        self, metrics: Dict[str, Any], cpu_stats: Dict, per_cpu_stats: Dict
    ) -> None:
        """Add monitoring data to metrics."""
        if cpu_stats and "threads" in cpu_stats:
            metrics["threads"] = {
                name: {
                    "cpu_percent": data["avg_percent"],
                    "core": data.get("primary_cpu"),
                }
                for name, data in cpu_stats["threads"].items()
            }

        if per_cpu_stats and "per_cpu" in per_cpu_stats:
            metrics["cores"] = {
                name: data for name, data in per_cpu_stats["per_cpu"].items()
            }

        if cpu_stats and "memory_max_mb" in cpu_stats:
            metrics["memory_mb"] = cpu_stats["memory_max_mb"]

    def _run_ingestion_phase(
        self, ingestion_config: Dict[str, Any], test_id: str
    ) -> None:
        """Run ingestion phase."""
        bench_cmd = self._build_fts_benchmark_command(
            dataset=ingestion_config.get("dataset"),
            xml_root_element=ingestion_config.get("xml_root_element", "page"),
            maxdocs=ingestion_config.get("maxdocs", 100000),
            command=ingestion_config["command"],
            test_phase="ingestion",
            concurrent_clients=ingestion_config.get("concurrent_clients", 1),
            batch_size=ingestion_config.get("batch_size", 1),
            sequential=ingestion_config.get("sequential", False),
        )

        field_size = self._get_field_size_for_dataset(ingestion_config.get("dataset"))

        self._run_monitored_benchmark(
            config=ingestion_config,
            test_id=test_id,
            bench_cmd=bench_cmd,
            phase="ingestion",
            metric_params={
                "command": ingestion_config["command"],
                "data_size": field_size,
                "pipeline": ingestion_config.get("batch_size", 1),
                "clients": ingestion_config.get("concurrent_clients", 1),
                "requests": ingestion_config.get("maxdocs", 100000),
            },
        )

    def _run_search_phase(self, search_config: Dict[str, Any], test_id: str) -> None:
        """Run search phase."""
        warmup_duration = search_config.get("warmup", 60)
        if warmup_duration > 0:
            logging.info(f"Starting warmup: {warmup_duration}s")
            warmup_cmd = self._build_fts_benchmark_command(
                dataset=search_config.get("dataset"),
                command=search_config["command"],
                test_phase="search",
                concurrent_clients=search_config.get("concurrent_clients", 1),
                duration=warmup_duration,
            )
            self._run(
                warmup_cmd, cwd=self.valkey_path, capture_output=True, timeout=None
            )
            logging.info("Warmup complete, starting actual test")

        bench_cmd = self._build_fts_benchmark_command(
            dataset=search_config.get("dataset"),
            command=search_config["command"],
            test_phase="search",
            concurrent_clients=search_config.get("concurrent_clients", 1),
            duration=search_config.get("duration"),
        )

        self._run_monitored_benchmark(
            config=search_config,
            test_id=test_id,
            bench_cmd=bench_cmd,
            phase="search",
            metric_params={
                "command": search_config["command"],
                "pipeline": 1,
                "clients": search_config.get("concurrent_clients", 1),
                "duration": search_config.get("duration", 200),
            },
        )

    def _build_fts_benchmark_command(
        self,
        dataset: str,
        command: str,
        test_phase: str,
        xml_root_element: str = "page",
        maxdocs: int = 100000,
        requests: int = None,
        concurrent_clients: int = 1,
        batch_size: int = 1,
        duration: int = None,
        sequential: bool = False,
    ) -> List[str]:
        """Build valkey-benchmark command with FTS dataset support."""
        cmd = []

        # Add CPU pinning if specified
        if self.cores:
            cmd += ["taskset", "-c", self.cores]

        cmd.append(self.valkey_benchmark_path)

        # Connection settings
        if self.tls_mode:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"]
            cmd += ["--cacert", "./tests/tls/ca.crt"]

        cmd += ["-h", self.target_ip]
        cmd += ["-p", str(self.config.get("port", 6379))]

        # Dataset configuration - for both ingestion and search phases
        if dataset:
            # Resolve relative paths
            if not Path(dataset).is_absolute():
                # If relative path, make it relative to framework directory
                dataset_path = Path.cwd() / dataset
                cmd += ["--dataset", str(dataset_path)]
            else:
                cmd += ["--dataset", dataset]
            if xml_root_element and test_phase == "ingestion":
                cmd += ["--xml-root-element", xml_root_element]
            if maxdocs and test_phase == "ingestion":
                cmd += ["--maxdocs", str(maxdocs)]

        # Test parameters
        if test_phase == "ingestion":
            # For ingestion: use exact insertion count based on maxdocs
            cmd += ["-n", str(maxdocs)]
        elif requests:
            # For search: use specific request count
            cmd += ["-n", str(requests)]
        else:
            # For search: use duration-based testing
            # Use per-scenario duration if provided, otherwise global duration
            test_duration = (
                duration if duration is not None else self.config.get("duration", 60)
            )
            cmd += ["--duration", str(test_duration)]

        cmd += ["-c", str(concurrent_clients)]
        cmd += ["-P", str(batch_size)]

        keyspacelen = self.config.get("keyspacelen", 1000000)
        cmd += ["-r", str(keyspacelen)]

        if sequential:
            cmd += ["--sequential"]

        cmd += ["--csv"]

        # Add the custom command
        cmd += ["--", command]

        return cmd
