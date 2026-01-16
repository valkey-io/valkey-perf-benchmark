"""Generic performance profiling module for valkey-perf-benchmark."""

import logging
import platform
import subprocess
import time
from pathlib import Path
from typing import Optional


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
        from datetime import datetime

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

    def _ensure_flamegraph_scripts(self) -> tuple[Path, Path]:
        """Download flamegraph scripts if not present. Called during init."""
        scripts_dir = Path(__file__).parent / "scripts"
        stackcollapse = scripts_dir / "stackcollapse-perf.pl"
        flamegraph = scripts_dir / "flamegraph.pl"

        if stackcollapse.exists() and flamegraph.exists():
            logging.info("Flamegraph scripts already cached")
            return stackcollapse, flamegraph

        logging.info("Downloading flamegraph scripts from GitHub...")

        import urllib.request

        base_url = "https://raw.githubusercontent.com/brendangregg/FlameGraph/master"

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
    ) -> None:
        """Start performance profiling for a test.

        Args:
            test_id: Identifier for this profiling session (e.g., "ingestion_1", "search_1a")
            target_process: Process name to profile
        """
        if not self.enabled:
            return

        # Determine phase (ingestion or search) from test_id
        phase_key = "ingestion" if "ingestion" in test_id.lower() else "search"

        # Get delay and duration from delays structure
        phase_delays = self.delays.get(phase_key, {})
        delay = phase_delays.get("delay", 0)
        duration = phase_delays.get("duration", 10)

        import threading

        self.profiling_thread = threading.Thread(
            target=self._profiling_worker,
            args=(test_id, target_process, delay, duration),
            daemon=True,
        )
        self.profiling_thread.start()
        logging.info(
            f"Profiling started: {test_id} (delay={delay}s, duration={duration}s)"
        )

    def _profiling_worker(
        self, test_id: str, target_process: str, delay: int, duration: int
    ) -> None:
        """Delayed profiling worker."""
        try:
            time.sleep(delay)

            proc = subprocess.run(
                ["pgrep", "-f", target_process], capture_output=True, text=True
            )
            if proc.returncode != 0:
                logging.warning(f"Process {target_process} not found")
                return

            server_pid = proc.stdout.strip().split()[0]
            perf_data = self.results_dir / f"{test_id}_{self.timestamp}.perf.data"

            perf_cmd = ["sudo", "perf", "record"]
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
                subprocess.run(["sudo", "kill", "-INT", actual_perf_pid], check=False)
            else:
                subprocess.run(
                    ["sudo", "kill", "-INT", str(self.profiler_process.pid)],
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
            ["sudo", "perf", "report", "-i", str(perf_data), "--stdio"],
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
            ["sudo", "perf", "script", "-i", str(perf_data)],
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
