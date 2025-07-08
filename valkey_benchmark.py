"""Client-side benchmark execution logic."""

import random
import subprocess
import time
from itertools import product
from pathlib import Path
from typing import Iterable, List, Optional

from process_metrics import MetricsProcessor
from logger import Logger

VALKEY_CLI = "src/valkey-cli"
VALKEY_BENCHMARK = "src/valkey-benchmark"

# Supported Valkey benchmark commands
READ_COMMANDS = ["GET", "LRANGE", "SPOP", "ZPOPMIN"]
WRITE_COMMANDS = [
    "SET", "MSET", "INCR", "LPUSH", "RPUSH", "LPOP",
    "RPOP", "SADD", "HSET", "ZADD", "XADD"
]

# Map for read commands to populate equivalents
READ_POPULATE_MAP = {
    "GET": "SET",
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
        cluster_mode: str,
        tls_mode: str,
        target_ip: str,
        results_dir: Path,
        valkey_path: str,
        cores: Optional[str] = None,
    ) -> None:
        self.commit_id = commit_id
        self.config = config
        self.cluster_mode = True if cluster_mode == "yes" else False
        self.tls_mode = True if tls_mode == "yes" else False
        self.target_ip = target_ip
        self.results_dir = results_dir
        self.valkey_path = valkey_path
        self.valkey_cli = f"{valkey_path}/{VALKEY_CLI}"
        self.valkey_benchmark = f"{valkey_path}/{VALKEY_BENCHMARK}"
        self.cores = cores

        self.tls_cli_args = [
            "--tls",
            "--cert", f"{valkey_path}/tests/tls/valkey.crt",
            "--key", f"{valkey_path}/tests/tls/valkey.key",
            "--cacert", f"{valkey_path}/tests/tls/ca.crt"
        ]

    def _run(self, cmd: Iterable[str]) -> None:
        Logger.info(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    def wait_for_server_ready(self, timeout: int = 30) -> None:
        """Poll until the Valkey server responds to PING or timeout expires."""
        Logger.info("Waiting for Valkey server to be ready...")
        cli_cmd = [self.valkey_cli]
        if self.tls_mode:
            cli_cmd += self.tls_cli_args
        cli_cmd += ["-h", self.target_ip, "-p", "6379", "PING"]

        start = time.time()
        while time.time() - start < timeout:
            try:
                subprocess.run(cli_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
                Logger.info("Valkey server is ready.")
                return
            except subprocess.CalledProcessError:
                time.sleep(1)

        Logger.error(f"Valkey server did not become ready within {timeout} seconds.")
        raise RuntimeError("Server failed to start in time.")

    def get_commit_time(self, commit_id: str) -> str:
        """Return ISO8601 timestamp for a commit."""
        try:
            commit_time = subprocess.run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                capture_output=True, text=True, check=True,
                cwd=self.valkey_path,
            )
            return commit_time.stdout.strip()
        except Exception as e:
            Logger.error(f"Failed to get commit time for {commit_id}: {e}")
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _populate_keyspace(self, read_command: str, requests: int, keyspacelen: int, data_size: int) -> None:
        """Populate keyspace for a read command using its write equivalent."""
        write_cmd = READ_POPULATE_MAP.get(read_command)
        if not write_cmd:
            Logger.info(f"No populate needed for {read_command}")
            return

        Logger.info(f"Populating keyspace for {read_command} using {write_cmd}")

        seed_val = random.randint(0, 1000000)
        bench_cmd = self._build_benchmark_command(
            tls=self.tls_mode,
            requests=requests,
            keyspacelen=keyspacelen,
            data_size=data_size,
            pipeline=1,
            clients=1,
            command=write_cmd,
            seed_val=seed_val,
        )

        self._run(bench_cmd)
        Logger.info(f"Keyspace populated for {read_command} with {requests} keys")

    def run_benchmark_config(self) -> None:
        """Run benchmark for all config combinations."""
        commit_time = self.get_commit_time(self.commit_id)
        metrics_processor = MetricsProcessor(
            self.commit_id, self.cluster_mode, self.tls_mode, commit_time
        )
        metric_json = []

        Logger.info(f"=== Starting benchmark: TLS={self.tls_mode}, Cluster={self.cluster_mode} ===")

        for (
            requests,
            keyspacelen,
            data_size,
            pipeline,
            clients,
            command,
            warmup,
        ) in self._generate_combinations():

            Logger.info(f"--> Running {command} | size={data_size} | pipeline={pipeline} | clients={clients}")
            Logger.info(f"requests={requests}, keyspacelen={keyspacelen}, warmup={warmup}")

            # Populate keyspace if read command
            if command in READ_COMMANDS:
                self._populate_keyspace(command, requests, keyspacelen, data_size)

            seed_val = random.randint(0, 1000000)
            Logger.info(f"Using seed value: {seed_val}")
            bench_cmd = self._build_benchmark_command(
                self.tls_mode,
                requests,
                keyspacelen,
                data_size,
                pipeline,
                clients,
                command,
                seed_val,
            )

            # Warmup for write commands if needed
            if command in WRITE_COMMANDS and warmup:
                Logger.info("Flushing keyspace before warmup...")
                flush_cmd = self._build_cli_command(self.tls_mode) + ["FLUSHALL", "SYNC"]
                self._run(flush_cmd)
                Logger.info(f"Starting warmup for {warmup}s...")
                proc = subprocess.Popen(
                            bench_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        )
                time.sleep(warmup)
                proc.terminate()
                proc.wait(timeout=5)
                Logger.info("Warmup phase complete.")

            # Run actual benchmark
            Logger.info("Running main benchmark...")
            proc = subprocess.run(bench_cmd, capture_output=True, text=True, check=True)
            Logger.info(f"Benchmark output:\n{proc.stdout}")
            if proc.stderr:
                Logger.warning(f"Benchmark stderr:\n{proc.stderr}")

            metrics = metrics_processor.parse_csv_output(proc.stdout, command, data_size, pipeline)
            if metrics:
                Logger.info(f"Parsed metrics: {metrics}")
                metric_json.append(metrics)

        if not metric_json:
            Logger.warning("No metrics collected, skipping write.")
            return

        metrics_processor.write_metrics(self.results_dir, metric_json)

    def _generate_combinations(self) -> List[tuple]:
        """Cartesian product of config dimensions."""
        return list(product(
            self.config["requests"],
            self.config["keyspacelen"],
            self.config["data_sizes"],
            self.config["pipelines"],
            self.config["clients"],
            self.config["commands"],
            [self.config["warmup"]],
        ))

    def _build_cli_command(self, tls: bool) -> List[str]:
        cmd = [self.valkey_cli, "-h", self.target_ip, "-p", "6379"]
        if tls:
            cmd += self.tls_cli_args
        return cmd

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
    ) -> List[str]:
        cmd = []
        if self.cores:
            cmd += ["taskset", "-c", self.cores]
        cmd.append(self.valkey_benchmark)
        if tls:
            cmd += self.tls_cli_args
        cmd += [
            "-h", self.target_ip, "-p", "6379",
            "-n", str(requests),
            "-r", str(keyspacelen),
            "-d", str(data_size),
            "-P", str(pipeline),
            "-c", str(clients),
            "-t", command,
            "--seed", str(seed_val),
            "--csv"
        ]
        return cmd

    def cleanup_terminate(self) -> None:
        Logger.info("Cleaning up...")
        flush_cmd = self._build_cli_command(self.tls_mode) + ["FLUSHALL", "SYNC"]
        self._run(flush_cmd)
        self._run(["pkill", "-f", "valkey-server"])
