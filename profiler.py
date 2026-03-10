"""Generic performance profiling module for valkey-perf-benchmark."""

import logging
import platform
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# FlameGraph version
FLAMEGRAPH_VERSION = "v1.0"


class PerformanceProfiler:
    """Generic performance profiler using perf and flamegraph tools."""

    def __init__(
        self,
        results_dir: Path,
        enabled: bool = True,
        config: Optional[dict] = None,
        commit_id: str = "HEAD",
    ):
        """Initialize profiler with config-driven parameters.

        Args:
            results_dir: Directory for profiling outputs
            enabled: Whether profiling is enabled
            config: Configuration dict containing profiling settings
            commit_id: Commit ID for directory organization
        """
        # Store timestamp for filenames
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = results_dir / commit_id
        self.results_dir = run_dir / "flamegraphs"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Profiler output directory: {self.results_dir}")
        self.enabled = enabled
        self.profiler_process = None
        self.profiling_thread = None
        self.config = config

        # Ensure flamegraph scripts are available upfront if profiling enabled
        if self.enabled:
            self._ensure_flamegraph_scripts()

        # Extract profiling configuration from config dict
        profiling_config = config.get("profiling", {}) if config else {}
        self.sampling_freq = profiling_config.get("sampling_freq", 999)
        self.profile_mode = profiling_config.get("mode", "cpu")

        # Store delays structure (new format)
        self.delays = profiling_config.get("delays", {})

        # Validate profile mode
        valid_modes = ["cpu", "wall-time"]
        if self.profile_mode not in valid_modes:
            logging.warning(
                f"Invalid profile_mode '{self.profile_mode}', defaulting to 'cpu'. "
                f"Valid modes: {valid_modes}"
            )
            self.profile_mode = "cpu"

        # Architecture-aware call graph selection
        arch = platform.machine()
        self.call_graph = "fp" if arch in ["aarch64", "arm64"] else "dwarf"

    def _ensure_flamegraph_scripts(self) -> Tuple[Path, Path]:
        """Download flamegraph scripts if not present. Called during init."""
        scripts_dir = Path(__file__).parent / "scripts"
        stackcollapse = scripts_dir / "stackcollapse-perf.pl"
        flamegraph = scripts_dir / "flamegraph.pl"

        if stackcollapse.exists() and flamegraph.exists():
            logging.info("Flamegraph scripts already cached")
            return stackcollapse, flamegraph

        logging.info("Downloading flamegraph scripts from GitHub...")

        base_url = f"https://raw.githubusercontent.com/brendangregg/FlameGraph/{FLAMEGRAPH_VERSION}"

        for script_name, path in [
            ("stackcollapse-perf.pl", stackcollapse),
            ("flamegraph.pl", flamegraph),
        ]:
            url = f"{base_url}/{script_name}"
            try:
                urllib.request.urlretrieve(url, path)
                path.chmod(0o755)  # Make executable
                logging.info(f"Downloaded and cached: {script_name}")
            except Exception as e:
                logging.error(f"Failed to download {script_name}: {e}")
                raise RuntimeError(
                    f"Cannot initialize profiling without flamegraph scripts"
                )

        return stackcollapse, flamegraph

    def start_profiling(
        self,
        test_id: str,
        target_process: str = "valkey-server",
        target_port: Optional[int] = None,
    ) -> None:
        """Start performance profiling for a test.

        Args:
            test_id: Identifier for this profiling session (e.g., "ingestion_1", "search_1a")
            target_process: Process name to profile
            target_port: Optional port number to target specific node in cluster (e.g., 6379 for node 0)
        """
        if not self.enabled:
            return

        test_id_lower = test_id.lower()
        if "write" in test_id_lower:
            phase_key = "write"
        elif "read" in test_id_lower:
            phase_key = "read"
        else:
            phase_key = None

        phase_delays = self.delays.get(phase_key, {}) if phase_key else {}
        delay = phase_delays.get("delay", 0)
        duration = phase_delays.get("duration", 10)

        self.profiling_thread = threading.Thread(
            target=self._profiling_worker,
            args=(test_id, target_process, delay, duration, target_port),
            daemon=True,
        )
        self.profiling_thread.start()

        port_info = f", port={target_port}" if target_port else ""
        logging.info(
            f"Profiling started: {test_id} (delay={delay}s, duration={duration}s{port_info})"
        )

    def _profiling_worker(
        self,
        test_id: str,
        target_process: str,
        delay: int,
        duration: int,
        target_port: Optional[int] = None,
    ) -> None:
        """Delayed profiling worker.

        Args:
            test_id: Profiling session identifier
            target_process: Process name to profile
            delay: Seconds to wait before profiling
            duration: Seconds to profile
            target_port: Optional port to target specific node (e.g., 6379 for node 0)
        """
        try:
            time.sleep(delay)

            # Build pgrep pattern (port-specific or generic)
            if target_port:
                # Target specific node by port (e.g., "valkey-server.*:6379")
                pattern = f"{target_process}.*:{target_port}"
                logging.info(f"Targeting process: {pattern}")
            else:
                # Generic pattern (backward compatible)
                pattern = target_process

            proc = subprocess.run(
                ["pgrep", "-f", pattern], capture_output=True, text=True
            )
            if proc.returncode != 0:
                logging.warning(f"Process matching '{pattern}' not found")
                return

            pids = proc.stdout.strip().split()
            if not pids:
                logging.warning(f"No PIDs found for pattern '{pattern}'")
                return

            server_pid = pids[0]
            logging.info(f"Profiling PID {server_pid} (pattern: {pattern})")
            perf_data = self.results_dir / f"{test_id}_{self.timestamp}.perf.data"

            perf_cmd = ["/usr/bin/sudo", "perf", "record"]
            if self.profile_mode == "cpu":
                perf_cmd += ["-e", "cycles"]
            elif self.profile_mode == "wall-time":
                perf_cmd += ["-e", "cpu-clock,sched:sched_switch"]
            perf_cmd += [
                "-F",
                str(self.sampling_freq),
                "--call-graph",
                self.call_graph,
                "-p",
                server_pid,
                "-o",
                str(perf_data),
            ]

            logging.info(
                f"Profiling: {self.profile_mode} mode, {self.sampling_freq}Hz, {duration}s"
            )

            self.profiler_process = subprocess.Popen(
                perf_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            time.sleep(0.5)
            if self.profiler_process.poll() is not None:
                logging.error(
                    f"Perf failed: {self.profiler_process.stderr.read().decode()}"
                )
                self.profiler_process = None
                return

            time.sleep(duration)
            self._stop_perf_process()

        except Exception as e:
            logging.warning(f"Profiling failed: {e}")

    def _stop_perf_process(self) -> None:
        """Stop perf process."""
        if not self.profiler_process:
            return

        try:
            perf_pid_search = subprocess.run(
                ["pgrep", "-P", str(self.profiler_process.pid)],
                capture_output=True,
                text=True,
            )
            if perf_pid_search.returncode == 0:
                actual_perf_pid = perf_pid_search.stdout.strip().split()[0]
                subprocess.run(
                    ["/usr/bin/sudo", "kill", "-INT", actual_perf_pid], check=False
                )
            else:
                subprocess.run(
                    ["/usr/bin/sudo", "kill", "-INT", str(self.profiler_process.pid)],
                    check=False,
                )
        except Exception as e:
            logging.warning(f"Stop failed: {e}")

    def stop_profiling(self, test_id: str) -> None:
        """Stop profiling and generate analysis."""
        if not self.enabled:
            return

        try:
            if self.profiling_thread and self.profiling_thread.is_alive():
                self.profiling_thread.join()

            if self.profiler_process:
                if self.profiler_process.poll() is None:
                    self._stop_perf_process()

                stdout, stderr = self.profiler_process.communicate()
                if stderr and b"Permission denied" in stderr:
                    logging.error(f"Perf permission error")
                    return

                self.profiler_process = None

            perf_data = self.results_dir / f"{test_id}_{self.timestamp}.perf.data"
            if perf_data.exists() and perf_data.stat().st_size > 0:
                self._generate_perf_report(perf_data, test_id)
                self._generate_flamegraph(perf_data, test_id)
            else:
                logging.warning(f"No perf data for {test_id}")

        except Exception as e:
            logging.warning(f"Stop profiling failed: {e}")

    def _generate_perf_report(self, perf_data: Path, test_id: str) -> None:
        """Generate perf report."""
        report_output = self.results_dir / f"{test_id}_{self.timestamp}_report.txt"

        perf_report = subprocess.run(
            ["/usr/bin/sudo", "perf", "report", "-i", str(perf_data), "--stdio"],
            capture_output=True,
            text=True,
        )

        if perf_report.returncode == 0:
            report_output.write_text(perf_report.stdout)
            logging.info(f"Report: {report_output}")

    def _generate_flamegraph(self, perf_data: Path, test_id: str) -> None:
        """Generate flamegraph."""
        flamegraph_output = self.results_dir / f"{test_id}_{self.timestamp}.svg"
        stackcollapse = Path(__file__).parent / "scripts" / "stackcollapse-perf.pl"
        flamegraph = Path(__file__).parent / "scripts" / "flamegraph.pl"

        if not stackcollapse.exists() or not flamegraph.exists():
            logging.warning(f"Flamegraph scripts not found")
            return

        perf_script = subprocess.run(
            ["/usr/bin/sudo", "perf", "script", "-i", str(perf_data)],
            capture_output=True,
            text=True,
        )
        if perf_script.returncode != 0:
            return

        stackcollapse_proc = subprocess.run(
            ["perl", str(stackcollapse)],
            input=perf_script.stdout,
            capture_output=True,
            text=True,
        )
        if stackcollapse_proc.returncode != 0:
            return

        flamegraph_proc = subprocess.run(
            ["perl", str(flamegraph)],
            input=stackcollapse_proc.stdout,
            capture_output=True,
            text=True,
        )
        if flamegraph_proc.returncode == 0:
            flamegraph_output.write_text(flamegraph_proc.stdout)
            logging.info(f"Flamegraph: {flamegraph_output}")
