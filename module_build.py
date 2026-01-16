"""Build valkey modules (.so files)."""

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional


class ModuleBuilder:
    """Build valkey modules from source."""

    def __init__(self, module_path: str, tls_enabled: bool = False) -> None:
        """
        Initialize module builder.

        Args:
            module_path: Path to module source directory
            tls_enabled: Whether to build with TLS support
        """
        self.module_path = Path(module_path)
        self.tls_enabled = tls_enabled

        if not self.module_path.exists():
            raise FileNotFoundError(f"Module path does not exist: {module_path}")

    def build(self) -> str:
        """
        Build the module and return path to .so file.

        Returns:
            Absolute path to built .so file

        Raises:
            RuntimeError: If build fails or .so file not found
        """
        logging.info(f"Building module from {self.module_path}")

        try:
            # Clean previous builds
            logging.info("Cleaning previous builds...")
            subprocess.run(
                ["make", "distclean"],
                cwd=self.module_path,
                check=False,  # Don't fail if distclean fails
                capture_output=True,
                text=True,
            )

            # Build module
            build_cmd = ["make", f"-j{os.cpu_count()}"]
            if self.tls_enabled:
                build_cmd.append("BUILD_TLS=yes")

            logging.info(f"Running: {' '.join(build_cmd)}")
            result = subprocess.run(
                build_cmd,
                cwd=self.module_path,
                check=True,
                capture_output=True,
                text=True,
            )

            if result.stdout:
                logging.debug(f"Build stdout: {result.stdout}")
            if result.stderr:
                logging.warning(f"Build stderr: {result.stderr}")

            # Find the .so file
            so_file = self._find_so_file()
            logging.info(f"✓ Module built successfully: {so_file}")

            return str(so_file)

        except subprocess.CalledProcessError as e:
            logging.error(f"Module build failed with exit code {e.returncode}")
            if e.stdout:
                logging.error(f"stdout: {e.stdout}")
            if e.stderr:
                logging.error(f"stderr: {e.stderr}")
            raise RuntimeError(f"Failed to build module at {self.module_path}") from e
        except Exception as e:
            logging.error(f"Unexpected error during module build: {e}")
            raise

    def _find_so_file(self) -> Path:
        """
        Find the built .so file in the module directory.

        Returns:
            Path to .so file

        Raises:
            FileNotFoundError: If no .so file found
        """
        # Common locations for .so files in valkey modules
        search_patterns = [
            ".build-release/**/*.so",
            "build/**/*.so",
            "**/*.so",
        ]

        for pattern in search_patterns:
            so_files = list(self.module_path.glob(pattern))
            # Filter out symlinks and find actual .so files
            so_files = [f for f in so_files if f.is_file() and not f.is_symlink()]

            if so_files:
                # Return the first .so file found
                so_file = so_files[0]
                logging.info(f"Found .so file: {so_file}")
                return so_file.absolute()

        raise FileNotFoundError(
            f"No .so file found in {self.module_path}. "
            f"Build may have failed or module structure is unexpected."
        )
