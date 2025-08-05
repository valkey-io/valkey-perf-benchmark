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

# Constants
VALKEY_BENCHMARK = "src/valkey-benchmark"
DEFAULT_PORT = 6379
DEFAULT_TIMEOUT = 30

# Supported Valkey benchmark commands
READ_COMMANDS = ["GET", "MGET", "LRANGE", "SPOP", "ZPOPMIN"]
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
    ) -> None:
        self.commit_id = commit_id
        self.config = config
        self.cluster_mode = cluster_mode
        self.tls_mode = tls_mode
        self.target_ip = target_ip
        self.results_dir = results_dir
        self.valkey_path = valkey_path
        self.cores = cores

    def _create_client(self) -> valkey.Valkey:
        """Return a Valkey client configured for TLS or plain mode."""
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
        timeout: int = 300,
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
            if result and result.stderr:
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
            commit_time = self._run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                cwd=self.valkey_path,
                capture_output=True,
            )
            return commit_time.stdout.strip()
        except Exception as e:
            logging.exception(f"Failed to get commit time for {commit_id}: {e}")
            raise

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

        self._run(command=bench_cmd, cwd=self.valkey_path)
        logging.info(f"Keyspace populated for {read_command} with {requests} keys")

    def run_benchmark_config(self) -> None:
        """Run benchmark for all config combinations."""
        commit_time = self.get_commit_time(self.commit_id)
        metrics_processor = MetricsProcessor(
            self.commit_id, self.cluster_mode, self.tls_mode, commit_time
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
        ) in self._generate_combinations():

            if command not in READ_COMMANDS + WRITE_COMMANDS:
                logging.warning(f"Unsupported command: {command}, skipping.")
                continue

            if command in ["MSET", "MGET"] and self.cluster_mode:
                logging.warning(
                    f"Command {command} not supported in cluster mode, skipping."
                )
                continue

            logging.info(
                f"--> Running {command} | size={data_size} | pipeline={pipeline} | clients={clients} | requests={requests} | keyspacelen={keyspacelen} | warmup={warmup}"
            )

            seed_val = random.randint(0, 1000000)
            logging.info(f"Using seed value: {seed_val}")

            # Data injection for read commands
            if command in READ_COMMANDS:
                self._populate_keyspace(
                    command,
                    requests,
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
            )

            if warmup:
                logging.info(f"Starting warmup for {warmup}s...")
                proc = subprocess.Popen(
                    bench_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=self.valkey_path,
                )
                time.sleep(warmup)
                proc.terminate()
                proc.wait(timeout=5)
                logging.info("Warmup phase complete.")

            # Run actual benchmark
            logging.info("Running main benchmark command")
            proc = self._run(bench_cmd, cwd=self.valkey_path, capture_output=True)
            logging.info(f"Benchmark output:\n{proc.stdout}")
            if proc.stderr:
                logging.warning(f"Benchmark stderr:\n{proc.stderr}")

            metrics = metrics_processor.parse_csv_output(
                proc.stdout,
                command,
                data_size,
                pipeline,
                clients,
                requests,
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
        return list(
            product(
                self.config["requests"],
                self.config["keyspacelen"],
                self.config["data_sizes"],
                self.config["pipelines"],
                self.config["clients"],
                self.config["commands"],
                [self.config["warmup"]],
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
    ) -> List[str]:
        cmd = []
        if self.cores:
            cmd += ["taskset", "-c", self.cores]
        cmd.append(VALKEY_BENCHMARK)
        if tls:
            cmd += ["--tls"]
            cmd += ["--cert", "./tests/tls/valkey.crt"]
            cmd += ["--key", "./tests/tls/valkey.key"]
            cmd += ["--cacert", "./tests/tls/ca.crt"]
        cmd += ["-h", self.target_ip]
        cmd += ["-p", "6379"]
        cmd += ["-n", str(requests)]
        cmd += ["-r", str(keyspacelen)]
        cmd += ["-d", str(data_size)]
        cmd += ["-P", str(pipeline)]
        cmd += ["-c", str(clients)]
        cmd += ["-t", command]
        if sequential:
            cmd += ["--sequential"]
        cmd += ["--seed", str(seed_val)]
        cmd += ["--csv"]
        return cmd
