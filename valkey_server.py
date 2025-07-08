"""Launch local Valkey servers for benchmark runs."""

import subprocess
import time
from typing import Iterable, Optional

from logger import Logger

VALKEY_SERVER = "src/valkey-server"
VALKEY_CLI = "src/valkey-cli"


class ServerLauncher:
    """Manage Valkey server instances."""

    def __init__(
        self, commit_id: str, valkey_path: str = "../valkey", cores: Optional[str] = None
    ) -> None:
        self.commit_id = commit_id
        self.valkey_path = valkey_path
        self.valkey_cli = f"{valkey_path}/{VALKEY_CLI}"
        self.valkey_server = f"{valkey_path}/{VALKEY_SERVER}"
        self.cores = cores
        self.tls_cli_args = [
            "--tls",
            "--cert", f"{valkey_path}/tests/tls/valkey.crt",
            "--key", f"{valkey_path}/tests/tls/valkey.key",
            "--cacert", f"{valkey_path}/tests/tls/ca.crt",
        ]

    def launch(self, cluster_mode: str, tls_mode: str) -> None:
        """Launch Valkey server and setup cluster if needed."""
        self._launch_server(tls_mode=tls_mode, cluster_mode=cluster_mode)
        if cluster_mode == "yes":
            self._setup_cluster(tls_mode=tls_mode)

    def _run(self, command: Iterable[str], check: bool = True) -> None:
        """Execute a command with optional check and fail loudly if needed."""
        Logger.info(f"Running: {' '.join(command)}")
        try:
            subprocess.run(command, check=check)
        except subprocess.CalledProcessError as e:
            Logger.error(f"Command failed: {e}")
            raise
        except Exception as e:
            Logger.error(f"Unexpected error running command: {e}")
            raise
        
    def _wait_for_server_ready(self, tls_mode: str, timeout: int = 15) -> None:
        """Poll until the Valkey server responds to PING or timeout expires."""
        Logger.info("Waiting for Valkey server to be ready...")
        cli_cmd = [self.valkey_cli]
        if tls_mode == "yes":
            cli_cmd += self.tls_cli_args
        cli_cmd += ["PING"]

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


    def _launch_server(self, tls_mode: str, cluster_mode: str) -> None:
        """Start Valkey server."""
        log_file = f"results/{self.commit_id}/valkey_log_cluster_{'enabled' if (cluster_mode == 'yes') else 'disabled'}.log"

        base = []
        if self.cores:
            base += ["taskset", "-c", self.cores]

        base.append(self.valkey_server)

        args = []
        # Add TLS args or standard port
        if tls_mode == "yes":
            args += [
                "--tls-port", "6379",
                "--port", "0",
                "--tls-cert-file",
                f"{self.valkey_path}/tests/tls/valkey.crt",
                "--tls-key-file",
                f"{self.valkey_path}/tests/tls/valkey.key",
                "--tls-ca-cert-file",
                f"{self.valkey_path}/tests/tls/ca.crt",
            ]
        else:
            args += ["--port", "6379"]

        # Add common base server args
        args += [
            "--cluster-enabled",
            cluster_mode,
            "--daemonize",
            "yes",
            "--maxmemory-policy",
            "allkeys-lru",
            "--appendonly",
            "no",
            "--logfile",
            log_file,
            "--save",
            "''",
        ]

        self._run(base + args)
        Logger.info(f"Started Valkey Server | TLS: {tls_mode} | Cluster: {cluster_mode}")
        self._wait_for_server_ready(tls_mode=tls_mode)

    def _setup_cluster(self, tls_mode: str) -> None:
        """Setup cluster on single primary."""
        Logger.info("Setting up cluster configuration...")
        base = [self.valkey_cli]
        if tls_mode == "yes":
            base += self.tls_cli_args

        reset_cmd = ["CLUSTER", "RESET", "HARD"]
        add_slots_cmd = ["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]
        for cmd in [reset_cmd, add_slots_cmd]:
            self._run(base + cmd)
            time.sleep(2)
