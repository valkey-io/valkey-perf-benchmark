"""Client-side benchmark execution logic."""

import logging
import random
import subprocess
import time
from contextlib import contextmanager
from itertools import product
from pathlib import Path
from typing import Iterable, List, Optional

import valkey

from process_metrics import MetricsProcessor
from valkey_server import ServerLauncher

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

    def _create_client(self) -> valkey.Valkey:
        """Return a Valkey client configured for TLS or plain mode."""
        print(f"Connecting to {self.target_ip}")
        kwargs = {
            "host": self.target_ip,
            "port": DEFAULT_PORT,
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
        cmd_str = " ".join(cmd_list)
        logging.info(f"Running: {cmd_str}")

        try:
            result = subprocess.run(
                cmd_list,
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

    def _flush_database(self) -> None:
        """Flush all data from the database before benchmark runs."""
        logging.info("Flushing database before benchmark run")
        try:
            with self._client_context() as client:
                client.flushall(asynchronous=False)
                logging.info("Database flushed successfully")
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
        """Run benchmark for all config combinations."""
        commit_time = self.get_commit_time(self.commit_id)
        metrics_processor = MetricsProcessor(
            self.commit_id,
            self.cluster_mode,
            self.tls_mode,
            commit_time,
            self.io_threads,
            self.benchmark_threads,
            self.architecture,
        )
        metric_json = []

        logging.info(
            f"=== Starting benchmark: TLS={self.tls_mode}, Cluster={self.cluster_mode} ==="
        )

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

            if command not in READ_COMMANDS + WRITE_COMMANDS:
                logging.warning(f"Unsupported command: {command}, skipping.")
                continue

            if command in ["MSET", "MGET"] and self.cluster_mode:
                logging.warning(
                    f"Command {command} not supported in cluster mode, skipping."
                )
                continue

            # Show either requests or duration, not both
            if duration is not None:
                mode_info = f"duration={duration}s"
            else:
                mode_info = f"requests={requests}"

            # Run the benchmark multiple times based on self.runs
            for run_num in range(self.runs):
                if self.runs > 1:
                    logging.info(f"=== Run {run_num + 1}/{self.runs} ===")

                logging.info(
                    f"--> Running {command} | size={data_size} | pipeline={pipeline} | clients={clients} | {mode_info} | keyspacelen={keyspacelen} | warmup={warmup}"
                )

                seed_val = random.randint(0, 1000000)
                logging.info(f"Using seed value: {seed_val}")

                # Restart server before each test for clean state
                if self.server_launcher:
                    self._restart_server()
                else:
                    # Flush database if server restart is not available
                    self._flush_database()

                # Data injection for read commands
                if command in READ_COMMANDS:
                    # For duration mode, use keyspacelen as the number of keys to populate
                    populate_requests = (
                        requests if requests is not None else keyspacelen
                    )
                    self._populate_keyspace(
                        command,
                        populate_requests,
                        keyspacelen,
                        data_size,
                        pipeline,
                        clients,
                        seed_val,
                    )
                bench_cmd = self._build_benchmark_command(
                    self.tls_mode,
                    requests,
                    keyspacelen,
                    data_size,
                    pipeline,
                    clients,
                    command,
                    seed_val,
                    sequential=False,
                    duration=duration,
                    warmup=warmup,
                )

                # Run actual benchmark
                logging.info("Running main benchmark command")
                proc = self._run(
                    bench_cmd, cwd=self.valkey_path, capture_output=True, timeout=None
                )
                if proc is None:
                    logging.error("Benchmark command failed to return results")
                    continue

                logging.info(f"Benchmark output:\n{proc.stdout}")
                if proc.stderr:
                    logging.warning(f"Benchmark stderr:\n{proc.stderr}")

                metrics = metrics_processor.create_metrics(
                    proc.stdout,
                    command,
                    data_size,
                    pipeline,
                    clients,
                    requests,
                    warmup,
                    duration,
                )
                if metrics:
                    logging.info(f"Parsed metrics: {metrics}")
                    metric_json.append(metrics)

        if not metric_json:
            logging.warning("No metrics collected, skipping write.")
            return

        metrics_processor.write_metrics(self.results_dir, metric_json)

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
        tls: bool,
        requests: int,
        keyspacelen: int,
        data_size: int,
        pipeline: int,
        clients: int,
        command: str,
        seed_val: int,
        *,
        sequential: bool = True,
        duration: Optional[int] = None,
        warmup: Optional[int] = None,
    ) -> List[str]:
        cmd = []
        if self.cores:
            cmd += ["taskset", "-c", self.cores]
        cmd.append(self.valkey_benchmark_path)
        if tls:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"]
            cmd += ["--cacert", "./tests/tls/ca.crt"]
        cmd += ["-h", self.target_ip]
        cmd += ["-p", "6379"]
        # Use --duration if specified, otherwise use -n (requests)
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
        cmd += ["--seed", str(seed_val)]
        cmd += ["--csv"]
        return cmd

    def _restart_server(self) -> None:
        """Restart the Valkey server for a clean state."""
        if self.server_launcher is None:
            logging.error("No server launcher available for restart")
            return

        logging.info("Restarting Valkey server for clean state...")

        # Shutdown current server
        self.server_launcher.shutdown(self.tls_mode)

        # Start fresh server
        self.server_launcher.launch(
            cluster_mode=self.cluster_mode,
            tls_mode=self.tls_mode,
            io_threads=self.io_threads,
        )

        # Wait for server to be ready
        self.wait_for_server_ready()
        logging.info("Server restarted successfully")
