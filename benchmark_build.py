"""Build valkey-benchmark from latest unstable for benchmarking."""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional


class BenchmarkBuilder:
    """Clone and compile latest Valkey unstable for valkey-benchmark binary."""

    def __init__(
        self,
        benchmark_dir: str = "../valkey-benchmark-latest",
        tls_enabled: bool = False,
    ) -> None:
        self.repo_url = "https://github.com/valkey-io/valkey.git"
        self.repo_branch = "unstable"
        self.benchmark_dir = Path(benchmark_dir)
        self.benchmark_binary = self.benchmark_dir / "src" / "valkey-benchmark"
        self.tls_enabled = tls_enabled

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
            raise
        except Exception:
            logging.exception(f"Unexpected error while running: {cmd_str}")
            raise

    def clone_latest_unstable(self) -> None:
        """Clone latest unstable branch if directory doesn't exist."""
        if self.benchmark_dir.exists():
            logging.info(
                f"Repository already exists at {self.benchmark_dir}, skipping clone"
            )
            return

        logging.info(f"Cloning latest Valkey unstable into {self.benchmark_dir}...")
        self._run(
            [
                "git",
                "clone",
                "--branch",
                str(self.repo_branch),
                "--depth",
                "1",
                self.repo_url,
                str(self.benchmark_dir),
            ]
        )

    def build_benchmark(self) -> str:
        """Build valkey-benchmark and return path to binary."""
        # Check if binary already exists
        if self.benchmark_binary.exists():
            logging.info(
                f"Using existing valkey-benchmark binary at {self.benchmark_binary}, no need to rebuild."
            )
            return str(self.benchmark_binary)

        logging.info("valkey-benchmark binary not found, building...")
        self.clone_latest_unstable()

        if self.tls_enabled:
            self._run(["make", "BUILD_TLS=yes", "-j"], cwd=self.benchmark_dir)
            tls_status = "with TLS"
        else:
            self._run(["make", "BUILD_TLS=no", "-j"], cwd=self.benchmark_dir)
            tls_status = "without TLS"

        if not self.benchmark_binary.exists():
            raise RuntimeError(
                f"Failed to build valkey-benchmark at {self.benchmark_binary}"
            )

        logging.info(
            f"Successfully built valkey-benchmark {tls_status} at {self.benchmark_binary}"
        )
        return str(self.benchmark_binary)

    def cleanup(self) -> None:
        """Remove the benchmark directory."""
        if self.benchmark_dir.exists():
            logging.info(f"Cleaning up benchmark directory: {self.benchmark_dir}")
            shutil.rmtree(self.benchmark_dir)
