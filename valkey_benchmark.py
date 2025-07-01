"""Client-side benchmark execution logic."""

import json
import os
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

    def _run(self, cmd: Iterable[str]) -> None:
        """Execute a command and log failures."""

        try:
            Logger.info(f"Running: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            Logger.error(f"Command failed with error: {e}")
        except Exception as e:
            Logger.error(f"An error occurred: {e}")

    def ping_server(self) -> None:
        """Verify the target server is reachable."""
        try:
            cmd = [self.valkey_cli]
            if self.tls_mode:
                cmd += [
                    "--tls",
                    "--cert",
                    f"{self.valkey_path}/tests/tls/valkey.crt",
                    "--key",
                    f"{self.valkey_path}/tests/tls/valkey.key",
                    "--cacert",
                    f"{self.valkey_path}/tests/tls/ca.crt",
                ]
            cmd += ["-h", self.target_ip, "-p", "6379", "ping"]
            Logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if "PONG" in result.stdout:
                Logger.info("Server is running")
            else:
                Logger.error("Server did not respond with PONG")
                exit(1)
        except subprocess.CalledProcessError as e:
            Logger.error(f"Command failed with error: {e}")
        except Exception as e:
            Logger.error(f"An error occurred: {e}")

    def get_commit_time(self, commit_id: str) -> str:
        """Return ISO8601 timestamp for a commit."""
        try:
            commit_time = subprocess.run(
                ["git", "show", "-s", "--format=%cI", commit_id],
                capture_output=True,
                text=True,
                check=True,
                cwd=self.valkey_path,
            )
            return commit_time.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            Logger.error(f"Failed to get commit time for {commit_id}: {e}")
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def run_benchmark_config(self) -> None:
        """Execute benchmarks for all configuration combinations."""
        commit_time = self.get_commit_time(self.commit_id)
        metrics_processor = MetricsProcessor(
            self.commit_id, self.cluster_mode, self.tls_mode, commit_time
        )
        metric_json = []

        Logger.info(
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
            Logger.info(
                f"--> Running {command} with data size {data_size}, pipeline {pipeline}, clients {clients}"
            )
            Logger.info(
                f"requests: {requests}, keyspacelen: {keyspacelen}, data_size: {data_size}, pipeline: {pipeline}, clients: {clients}, warmup: {warmup}"
            )

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

            # Optionally flush keyspace and warmup cache if needed
            if command in ["SET", "RPUSH", "LPUSH", "SADD"]:
                Logger.info("Flushing keyspace before benchmark...")
                flush_cmd = self._build_cli_command(self.tls_mode) + [
                    "FLUSHALL",
                    "SYNC",
                ]
                self._run(flush_cmd)
                time.sleep(2)

                # Warmup phase
                if warmup:
                    try:
                        Logger.info(f"Starting warmup for {warmup} seconds...")
                        proc = subprocess.Popen(
                            bench_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        )
                        time.sleep(warmup)
                        proc.terminate()
                        proc.wait(timeout=5)  # Wait for process to terminate
                        Logger.info(f"Warmup completed after {warmup} seconds")
                    except Exception as e:
                        Logger.error(f"Warmup failed: {e}")

            # Run benchmark
            Logger.info("Starting benchmark...")
            try:
                proc = subprocess.run(
                    bench_cmd, capture_output=True, text=True, check=True
                )
                # Log the benchmark output
                Logger.info(f"Benchmark output:\n{proc.stdout}")
                if proc.stderr:
                    Logger.warning(f"Benchmark stderr:\n{proc.stderr}")

                metrics = metrics_processor.parse_csv_output(
                    proc.stdout, command, data_size, pipeline
                )
                Logger.info(f"Benchmark completed: {metrics}")
                if metrics:
                    metric_json.append(metrics)
            except subprocess.CalledProcessError as e:
                Logger.error(f"Benchmark failed: {e}")
                if e.stdout:
                    Logger.info(f"Benchmark stdout:\n{e.stdout}")
                if e.stderr:
                    Logger.error(f"Benchmark stderr:\n{e.stderr}")

        # in case no benchmarks ran successfully)
        if not metric_json:
            Logger.warning("No metrics collected, skipping metrics write")
            return

        metrics_processor.write_metrics(self.results_dir, metric_json)

    def _generate_combinations(self) -> List[tuple]:
        """Return Cartesian product of configuration options."""

        return list(
            product(
                self.config["requests"],
                self.config["keyspacelen"],
                self.config["data_sizes"],
                self.config["pipelines"],
                self.config["clients"],
                self.config["commands"],
                self.config["warmup"],
            )
        )

    def _build_cli_command(self, tls: bool) -> List[str]:
        """Build a ``valkey-cli`` command."""

        cmd = [self.valkey_cli, "-h", self.target_ip, "-p", "6379"]
        if tls:
            cmd += [
                "--tls",
                "--cert",
                f"{self.valkey_path}/tests/tls/valkey.crt",
                "--key",
                f"{self.valkey_path}/tests/tls/valkey.key",
                "--cacert",
                f"{self.valkey_path}/tests/tls/ca.crt",
            ]
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
        """Construct the ``valkey-benchmark`` command line."""
        cmd: List[str] = []
        if self.cores:
            cmd += ["taskset", "-c", self.cores]
        cmd.append(self.valkey_benchmark)
        if tls:
            cmd += [
                "--tls",
                "--cert",
                f"{self.valkey_path}/tests/tls/valkey.crt",
                "--key",
                f"{self.valkey_path}/tests/tls/valkey.key",
                "--cacert",
                f"{self.valkey_path}/tests/tls/ca.crt",
            ]
        cmd += [
            "-h",
            self.target_ip,
            "-p",
            "6379",
            "-n",
            str(requests),
            "-r",
            str(keyspacelen),
            "-d",
            str(data_size),
            "-P",
            str(pipeline),
            "-c",
            str(clients),
            "-t",
            command,
            "--seed",
            str(seed_val),
            "--csv",
        ]
        return cmd

    def cleanup_terminate(self) -> None:
        """Cleanup any resources or processes."""
        Logger.info("Cleaning up resources...")
        cleanup_cmd = self._build_cli_command(self.tls_mode) + ["FLUSHALL", "SYNC"]
        self._run(cleanup_cmd)
        terminiate_cmd = ["pkill", "-f", "valkey-server"]
        self._run(terminiate_cmd)
