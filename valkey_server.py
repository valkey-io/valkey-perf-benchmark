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
        self, command: Iterable[str], cwd: Optional[str] = None, timeout: int = 60
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

    def _launch_server(
        self, tls_mode: bool, cluster_mode: bool, io_threads: Optional[int] = None
    ) -> None:
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

        # Add io-threads if specified
        if io_threads is not None:
            cmd += ["--io-threads", str(io_threads)]

        # Add common base server args
        cmd += ["--cluster-enabled", "yes" if cluster_mode else "no"]
        cmd += ["--daemonize", "yes"]
        cmd += ["--maxmemory-policy", "allkeys-lru"]
        cmd += ["--appendonly", "no"]
        cmd += ["--protected-mode", "no"]
        cmd += ["--logfile", log_file]
        cmd += ["--save", "''"]

        self._run(cmd, cwd=self.valkey_path)
        logging.info(
            f"Started Valkey Server | TLS: {tls_mode} | Cluster: {cluster_mode} | IO Threads: {io_threads} '"
        )
        self._wait_for_server_ready(tls_mode=tls_mode)

    def _setup_cluster(self, tls_mode: bool) -> None:
        """Setup cluster on single primary."""
        logging.info("Setting up cluster configuration...")
        try:
            with self._client_context(tls_mode) as client:
                client.execute_command("CLUSTER", "RESET", "HARD")
                client.execute_command("CLUSTER", "ADDSLOTSRANGE", "0", "16383")

                # Wait for cluster to become ready
                self._wait_for_cluster_ready(client)
                logging.info("Cluster configuration completed successfully.")
        except Exception as e:
            logging.error(f"Failed to setup cluster: {e}")
            raise RuntimeError(f"Cluster setup failed: {e}") from e

    def _wait_for_cluster_ready(self, client: valkey.Valkey, timeout: int = 30) -> None:
        """Wait for cluster to become fully operational after slot assignment."""
        logging.info("Verifying cluster state after slot assignment...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                if self._check_cluster_state(client):
                    logging.info(
                        "Cluster is fully operational and ready for connections."
                    )
                    return
                time.sleep(1)
            except Exception as e:
                logging.warning(f"Error checking cluster state: {e}")
                time.sleep(1)

        elapsed = time.time() - start_time
        raise RuntimeError(f"Cluster failed to become ready within {elapsed:.1f}s")

    def _check_cluster_state(self, client: valkey.Valkey) -> bool:
        """Check if cluster is in a ready state."""
        cluster_info = client.execute_command("CLUSTER", "INFO")
        info_dict = self._parse_cluster_info(cluster_info)

        state_ok = info_dict.get("cluster_state") == "ok"
        slots_assigned = int(info_dict.get("cluster_slots_assigned", "0")) == 16384
        slots_ok = int(info_dict.get("cluster_slots_ok", "0")) == 16384
        nodes_ok = int(info_dict.get("cluster_known_nodes", "0")) >= 1

        self._log_cluster_state(info_dict)

        return state_ok and slots_assigned and slots_ok and nodes_ok

    def _parse_cluster_info(self, cluster_info: str) -> dict:
        """Parse cluster info response into a dictionary."""
        info_dict = {}
        for line in cluster_info.strip().split("\r\n"):
            if ":" in line:
                key, value = line.split(":", 1)
                info_dict[key] = value
        return info_dict

    def _log_cluster_state(self, info_dict: dict) -> None:
        """Log current cluster state information."""
        cluster_state = info_dict.get("cluster_state", "fail")
        cluster_slots_assigned = int(info_dict.get("cluster_slots_assigned", "0"))
        cluster_slots_ok = int(info_dict.get("cluster_slots_ok", "0"))
        cluster_known_nodes = int(info_dict.get("cluster_known_nodes", "0"))

        logging.info(
            f"Cluster state check: state={cluster_state}, slots_assigned={cluster_slots_assigned}, "
            f"slots_ok={cluster_slots_ok}, known_nodes={cluster_known_nodes}"
        )

    def launch(
        self, cluster_mode: bool, tls_mode: bool, io_threads: Optional[int] = None
    ) -> None:
        """Launch Valkey server and setup cluster if needed."""
        try:
            self._launch_server(
                tls_mode=tls_mode, cluster_mode=cluster_mode, io_threads=io_threads
            )
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
        self._wait_for_process_shutdown()

    def _wait_for_process_shutdown(self, timeout: int = 10) -> None:
        """Wait for Valkey server process to fully terminate."""
        logging.info("Waiting for Valkey server process to terminate...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Check if any valkey-server processes are still running
                result = subprocess.run(
                    ["ps", "aux"], capture_output=True, text=True, timeout=5
                )

                # Filter for valkey-server processes (excluding grep itself)
                valkey_processes = []
                for line in result.stdout.splitlines():
                    if "valkey-server" in line and "grep" not in line:
                        valkey_processes.append(line.strip())

                if not valkey_processes:
                    logging.info("Valkey server process has terminated successfully.")
                    return

                # Log found processes for debugging
                logging.info(
                    f"Found {len(valkey_processes)} valkey-server process(es) still running:"
                )
                for proc in valkey_processes:
                    logging.info(f"  {proc}")

                time.sleep(0.5)

            except subprocess.TimeoutExpired:
                logging.warning("Process check timed out, continuing to wait...")
                time.sleep(0.5)
            except Exception as e:
                logging.warning(f"Error checking process status: {e}")
                time.sleep(0.5)

        # Timeout reached - log warning but don't fail
        elapsed = time.time() - start_time
        logging.warning(f"Process shutdown verification timed out after {elapsed:.1f}s")

        # Final check to log any remaining processes
        try:
            result = subprocess.run(
                ["ps", "aux"], capture_output=True, text=True, timeout=5
            )
            remaining_processes = [
                line.strip()
                for line in result.stdout.splitlines()
                if "valkey-server" in line and "grep" not in line
            ]
            if remaining_processes:
                logging.warning("Remaining valkey-server processes:")
                for proc in remaining_processes:
                    logging.warning(f"  {proc}")
        except Exception as e:
            logging.warning(f"Could not perform final process check: {e}")
