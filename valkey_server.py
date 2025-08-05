"""Launch local Valkey servers for benchmark runs."""

import logging
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional

import valkey

# Constants
VALKEY_SERVER = "src/valkey-server"
DEFAULT_PORT = 6379
DEFAULT_TIMEOUT = 15
MAX_SHUTDOWN_WAIT = 10


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

    def _create_client(self, tls_mode: bool) -> valkey.Valkey:
        """Return a Valkey client for server management."""
        kwargs = {
            "host": "127.0.0.1",
            "port": DEFAULT_PORT,
            "decode_responses": True,
            "socket_timeout": 5,
            "socket_connect_timeout": 5,
        }
        if tls_mode:
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

    def _run(
        self, command: Iterable[str], cwd: Optional[Path] = None, timeout: int = 60
    ) -> subprocess.CompletedProcess:
        """Execute a command with proper error handling and timeout."""
        cmd_list = list(command)
        cmd_str = " ".join(cmd_list)
        logging.info(f"Running: {cmd_str}")

        try:
            result = subprocess.run(
                cmd_list,
                check=True,
                cwd=cwd,
                timeout=timeout,
                capture_output=True,
                text=True,
            )
            if result.stderr:
                logging.warning(f"Command stderr: {result.stderr}")
            return result
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

    def _wait_for_server_ready(
        self, tls_mode: bool, timeout: int = DEFAULT_TIMEOUT
    ) -> None:
        """Poll until the Valkey server responds to PING or timeout expires."""
        logging.info("Waiting for Valkey server to be ready...")
        start = time.time()
        last_error = None

        while time.time() - start < timeout:
            try:
                with self._client_context(tls_mode) as client:
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

    @contextmanager
    def _client_context(self, tls_mode: bool):
        """Context manager for Valkey client connections."""
        client = None
        try:
            client = self._create_client(tls_mode)
            yield client
        finally:
            if client:
                try:
                    client.close()
                except Exception as e:
                    logging.warning(f"Error closing client connection: {e}")

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
            cmd += ["--tls-cert-file", "./tests/tls/valkey.crt"]
            cmd += ["--tls-key-file", "./tests/tls/valkey.key"]
            cmd += ["--tls-ca-cert-file", "./tests/tls/ca.crt"]
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
        try:
            with self._client_context(tls_mode) as client:
                client.execute_command("CLUSTER", "ADDSLOTSRANGE", "0", "16383")
                logging.info("Cluster configuration completed successfully.")
        except Exception as e:
            logging.error(f"Failed to setup cluster: {e}")
            raise RuntimeError(f"Cluster setup failed: {e}") from e

    def launch(self, cluster_mode: bool, tls_mode: bool) -> None:
        """Launch Valkey server and setup cluster if needed."""
        try:
            self._launch_server(tls_mode=tls_mode, cluster_mode=cluster_mode)
            if cluster_mode:
                self._setup_cluster(tls_mode=tls_mode)
            logging.info("Valkey server launched successfully.")
        except Exception as e:
            logging.error(f"Failed to launch Valkey server: {e}")
            self.shutdown(tls_mode)
            raise

    def shutdown(self, tls_mode: bool) -> None:
        """Gracefully shutdown the Valkey server."""
        logging.info("Shutting down Valkey server...")
        try:
            with self._client_context(tls_mode) as client:
                client.shutdown(nosave=True)
                logging.info("Shutdown command sent to server.")
        except Exception as e:
            logging.warning(f"Could not send shutdown command: {e}")
            # Try to kill the process directly
            try:
                self._run(["pkill", "-f", VALKEY_SERVER], timeout=10)
                logging.info("Valkey server process killed.")
            except Exception as kill_error:
                logging.error(f"Failed to kill Valkey server process: {kill_error}")

        # Wait for process to actually stop
        time.sleep(2)
