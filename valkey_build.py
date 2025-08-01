"""Build Valkey from source for benchmarking."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional

import logging


class ServerBuilder:
    """Compile Valkey for a specific commit."""

    def __init__(self, commit_id: str, tls_mode: bool, valkey_path: str) -> None:
        self.commit_id = commit_id
        self.tls_mode = tls_mode
        self.repo_url = "https://github.com/valkey-io/valkey.git"
        self.valkey_dir = Path(valkey_path)

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

    def clone_and_checkout(self) -> None:
        if not self.valkey_dir.exists():
            logging.info(f"Cloning Valkey repo into {self.valkey_dir}...")
            self._run(["git", "clone", self.repo_url, str(self.valkey_dir)])

        if self.commit_id == "HEAD":
            return

        # Checkout specific commit
        logging.info(f"Checking out commit: {self.commit_id}")
        self._run(["git", "checkout", self.commit_id], cwd=self.valkey_dir)

    def build(self) -> None:
        self.clone_and_checkout()
        logging.info(f"Building with TLS {'enabled' if self.tls_mode else 'disabled'}")
        self._run(["make", "distclean"], cwd=self.valkey_dir)
        if self.tls_mode:
            self._run(["make", "BUILD_TLS=yes", "-j"], cwd=self.valkey_dir)
            self._run(["./utils/gen-test-certs.sh"], cwd=self.valkey_dir)
        else:
            self._run(["make", "-j"], cwd=self.valkey_dir)
