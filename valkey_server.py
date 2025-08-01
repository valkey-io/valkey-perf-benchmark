"""Launch local Valkey servers for benchmark runs."""

import subprocess
import time
from pathlib import Path
from typing import Iterable, Optional

import valkey

import logging

VALKEY_SERVER = "src/valkey-server"


class ServerLauncher:
    """Manage Valkey server instances."""

    def __init__(
        self,
        results_dir: str,
        valkey_path: str = "../valkey",
        cores: Optional[str] = None,
    ) -> None:
        self.results_dir = results_dir
        self.valkey_path = valkey_path
        self.cores = cores

    def _create_client(self, tls_mode: bool):
        """Return a Valkey client for server management."""
        kwargs = {
            "host": "127.0.0.1",
            "port": 6379,
            "decode_responses": True,
        }
        if tls_mode:
            kwargs.update(
                {
                    "ssl": True,
                    "ssl_certfile": f"{self.valkey_path}/tests/tls/valkey.crt",
                    "ssl_keyfile": f"{self.valkey_path}/tests/tls/valkey.key",
                    "ssl_ca_certs": f"{self.valkey_path}/tests/tls/ca.crt",
                }
            )
        return valkey.Valkey(**kwargs)

    def _run(self, command: Iterable[str], cwd: Optional[Path] = None) -> None:
        """Execute a command with optional check and fail loudly if needed."""
        cmd_list = list(command)
        cmd_str = " ".join(command)
        logging.info(f"Running: {cmd_str}")
        try:
            subprocess.run(cmd_list, check=True, cwd=cwd)
        except subprocess.CalledProcessError:
            logging.exception(
                f"Command failed with CalledProcessError while running: {cmd_str}"
            )
        except Exception:
            logging.exception(f"Unexpected error while running: {cmd_str}")

    def _wait_for_server_ready(self, tls_mode: bool, timeout: int = 15) -> None:
        """Poll until the Valkey server responds to PING or timeout expires."""
        logging.info("Waiting for Valkey server to be ready...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                client = self._create_client(tls_mode)
                client.ping()
                client.close()
                logging.info("Valkey server is ready.")
                return
            except Exception:
                time.sleep(1)

        logging.error(f"Valkey server did not become ready within {timeout} seconds.")
        raise RuntimeError("Server failed to start in time.")

    def _launch_server(self, tls_mode: bool, cluster_mode: bool) -> None:
        """Start Valkey server."""
        log_file = f"{Path.cwd()}/{self.results_dir}/valkey_log_cluster_{'enabled' if cluster_mode else 'disabled'}_tls_{'enabled' if tls_mode else 'disabled'}.log"

        cmd = []
        if self.cores:
            cmd += ["taskset", "-c", self.cores]

        cmd.append(VALKEY_SERVER)
        # Add TLS args or standard port args
        if tls_mode:
            cmd += ["--tls-port", "6379"]
            cmd += ["--port", "0"]
            cmd += ["--tls-cert-file", f"{self.valkey_path}/tests/tls/valkey.crt"]
            cmd += ["--tls-key-file", f"{self.valkey_path}/tests/tls/valkey.key"]
            cmd += ["--tls-ca-cert-file", f"{self.valkey_path}/tests/tls/ca.crt"]
        else:
            cmd += ["--port", "6379"]

        # Add common base server args
        cmd += ["--cluster-enabled", "yes" if cluster_mode else "no"]
        cmd += ["--daemonize", "yes"]
        cmd += ["--maxmemory-policy", "allkeys-lru"]
        cmd += ["--appendonly", "no"]
        cmd += ["--logfile", log_file]
        cmd += ["--save", "''"]

        self._run(cmd, cwd=self.valkey_path)
        logging.info(
            f"Started Valkey Server | TLS: {tls_mode} | Cluster: {cluster_mode}"
        )
        self._wait_for_server_ready(tls_mode=tls_mode)

    def _setup_cluster(self, tls_mode: bool) -> None:
        """Setup cluster on single primary."""
        logging.info("Setting up cluster configuration...")
        client = self._create_client(tls_mode)
        cmd = ["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]
        client.execute_command(cmd)
        client.close()

    def launch(self, cluster_mode: bool, tls_mode: bool) -> None:
        """Launch Valkey server and setup cluster if needed."""
        self._launch_server(tls_mode=tls_mode, cluster_mode=cluster_mode)
        if cluster_mode:
            self._setup_cluster(tls_mode=tls_mode)
