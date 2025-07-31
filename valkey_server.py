"""Launch local Valkey servers for benchmark runs."""

import subprocess
import time
from typing import Iterable, Optional

import valkey

import logging

VALKEY_SERVER = "src/valkey-server"


class ServerLauncher:
    """Manage Valkey server instances."""

    def __init__(
        self,
        commit_id: str,
        valkey_path: str = "../valkey",
        cores: Optional[str] = None,
    ) -> None:
        self.commit_id = commit_id
        self.valkey_path = valkey_path
        self.valkey_server = f"{valkey_path}/{VALKEY_SERVER}"
        self.cores = cores

    def _create_client(self, tls_mode: str):
        """Return a Valkey client for server management."""
        kwargs = {
            "host": "127.0.0.1",
            "port": 6379,
            "decode_responses": True,
        }
        if tls_mode == "yes":
            kwargs.update(
                {
                    "ssl": True,
                    "ssl_certfile": f"{self.valkey_path}/tests/tls/valkey.crt",
                    "ssl_keyfile": f"{self.valkey_path}/tests/tls/valkey.key",
                    "ssl_ca_certs": f"{self.valkey_path}/tests/tls/ca.crt",
                }
            )
        return valkey.Valkey(**kwargs)

    def launch(self, cluster_mode: str, tls_mode: str) -> None:
        """Launch Valkey server and setup cluster if needed."""
        self._launch_server(tls_mode=tls_mode, cluster_mode=cluster_mode)
        if cluster_mode == "yes":
            self._setup_cluster(tls_mode=tls_mode)

    def _run(self, command: Iterable[str], check: bool = True) -> None:
        """Execute a command with optional check and fail loudly if needed."""
        logging.info(f"Running: {' '.join(command)}")
        try:
            subprocess.run(command, check=check)
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error running command: {e}")
            raise

    def _wait_for_server_ready(self, tls_mode: str, timeout: int = 15) -> None:
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
            args += ["--tls-port", "6379"]
            args += ["--port", "0"]
            args += ["--tls-cert-file", f"{self.valkey_path}/tests/tls/valkey.crt"]
            args += ["--tls-key-file", f"{self.valkey_path}/tests/tls/valkey.key"]
            args += ["--tls-ca-cert-file", f"{self.valkey_path}/tests/tls/ca.crt"]
        else:
            args += ["--port", "6379"]

        # Add common base server args
        args += ["--cluster-enabled", cluster_mode]
        args += ["--daemonize", "yes"]
        args += ["--maxmemory-policy", "allkeys-lru"]
        args += ["--appendonly", "no"]
        args += ["--logfile", log_file]
        args += ["--save", "''"]

        self._run(base + args)
        logging.info(
            f"Started Valkey Server | TLS: {tls_mode} | Cluster: {cluster_mode}"
        )
        self._wait_for_server_ready(tls_mode=tls_mode)

    def _setup_cluster(self, tls_mode: str) -> None:
        """Setup cluster on single primary."""
        logging.info("Setting up cluster configuration...")
        client = self._create_client(tls_mode)
        for cmd in (
            ["CLUSTER", "RESET", "HARD"],
            ["CLUSTER", "ADDSLOTSRANGE", "0", "16383"],
        ):
            client.execute_command(*cmd)
        client.close()
