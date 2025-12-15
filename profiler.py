"""Generic performance profiling module for valkey-perf-benchmark."""

import logging
import platform
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional


class PerformanceProfiler:
    """Generic performance profiler using perf and flamegraph tools."""

    def __init__(self, results_dir: Path, enabled: bool = True):
        self.results_dir = results_dir / "flamegraphs"
        self.results_dir.mkdir(exist_ok=True)
        self.enabled = enabled
        self.profiler_process = None

        # Architecture-aware call graph selection
        arch = platform.machine()
        self.call_graph = "fp" if arch in ["aarch64", "arm64"] else "dwarf"

    def start_profiling(
        self, test_id: str, target_process: str = "valkey-server"
    ) -> None:
        """Start performance profiling for a test."""
        if not self.enabled:
            return

        try:
            proc = subprocess.run(
                ["pgrep", "-f", target_process], capture_output=True, text=True
            )
            if proc.returncode != 0:
                logging.warning(f"No {target_process} process found")
                return

            server_pid = proc.stdout.strip().split()[0]

            perf_cmd = [
                "sudo",
                "perf",
                "record",
                "-e",
                "cycles",
                "-F",
                "99",
                "--call-graph",
                self.call_graph,
                "-p",
                server_pid,
                "-o",
                str(self.results_dir / f"{test_id}.perf.data"),
            ]

            self.profiler_process = subprocess.Popen(
                perf_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            logging.info(f"Using call-graph method: {self.call_graph}")

            time.sleep(0.5)
            if self.profiler_process.poll() is not None:
                stdout, stderr = self.profiler_process.communicate()
                logging.error(f"Perf failed to start: {stderr.decode()}")
                self.profiler_process = None
                return

            logging.info(f"Started profiling for {test_id} (PID: {server_pid})")
            time.sleep(0.5)

        except Exception as e:
            logging.warning(f"Failed to start profiling: {e}")

    def stop_profiling(self, test_id: str) -> None:
        """Stop profiling and generate comprehensive analysis."""
        if not self.enabled or not self.profiler_process:
            return

        try:
            # Stop perf recording gracefully with SIGINT (like Ctrl+C)
            # This allows perf to finalize the header with correct data_size field
            # Find the actual perf process PID (not the sudo wrapper)
            perf_pid_search = subprocess.run(
                ["pgrep", "-P", str(self.profiler_process.pid)],
                capture_output=True,
                text=True,
            )

            if perf_pid_search.returncode == 0:
                actual_perf_pid = perf_pid_search.stdout.strip().split()[0]
                logging.info(f"Sending SIGINT to perf process {actual_perf_pid}")
                subprocess.run(["sudo", "kill", "-INT", actual_perf_pid], check=False)
            else:
                # Fallback: signal the subprocess directly
                logging.warning(
                    "Could not find perf child process, signaling subprocess"
                )
                subprocess.run(
                    ["sudo", "kill", "-INT", str(self.profiler_process.pid)],
                    check=False,
                )

            # Wait indefinitely for perf to finish - large files can take several minutes
            # No timeout - let perf complete its finalization process
            logging.info("Waiting for perf to finalize data file...")
            stdout, stderr = self.profiler_process.communicate()

            # Check for errors
            if stderr:
                stderr_text = stderr.decode() if stderr else ""
                if stderr_text and "Permission denied" in stderr_text:
                    logging.error(f"Perf permission error: {stderr_text}")
                elif stderr_text:
                    logging.warning(f"Perf stderr: {stderr_text}")

            perf_data = self.results_dir / f"{test_id}.perf.data"

            if perf_data.exists() and perf_data.stat().st_size > 0:
                logging.info(f"Perf data size: {perf_data.stat().st_size} bytes")

                # Verify perf data header integrity before processing
                verify_header = subprocess.run(
                    ["sudo", "perf", "report", "-i", str(perf_data), "--header-only"],
                    capture_output=True,
                    text=True,
                )

                if verify_header.returncode != 0:
                    logging.error(f"Perf data header is corrupted or invalid")
                    logging.error(f"Perf verification error: {verify_header.stderr}")
                    logging.error("File will be preserved but cannot be processed")
                    return

                logging.info("Perf data header verified successfully")

                # Generate perf report
                self._generate_perf_report(perf_data, test_id)

                # Generate flamegraph if available
                self._generate_flamegraph(perf_data, test_id)

                logging.info(f"Preserved raw perf data: {perf_data}")
            elif perf_data.exists():
                logging.error(f"Perf data file is empty (0 bytes): {perf_data}")
                logging.error(
                    "This usually means perf couldn't collect samples - check permissions"
                )
            else:
                logging.error(f"Perf data file not created: {perf_data}")

            self.profiler_process = None
            logging.info(f"Completed profiling analysis for {test_id}")

        except Exception as e:
            logging.warning(f"Failed to stop profiling: {e}")

    def _generate_perf_report(self, perf_data: Path, test_id: str) -> None:
        """Generate perf report with function hotspots."""
        report_output = self.results_dir / f"{test_id}_report.txt"

        # Use sudo for perf report to read the data file
        perf_report = subprocess.run(
            ["sudo", "perf", "report", "-i", str(perf_data), "--stdio"],
            capture_output=True,
            text=True,
        )

        if perf_report.returncode == 0:
            with open(report_output, "w") as f:
                f.write(perf_report.stdout)
            logging.info(f"Generated perf report: {report_output}")

    def _generate_flamegraph(self, perf_data: Path, test_id: str) -> None:
        """Generate flamegraph visualization."""
        flamegraph_output = self.results_dir / f"{test_id}.svg"

        # Use local scripts from scripts directory
        stackcollapse_script = (
            Path(__file__).parent / "scripts" / "stackcollapse-perf.pl"
        )
        flamegraph_script = Path(__file__).parent / "scripts" / "flamegraph.pl"

        if not stackcollapse_script.exists():
            logging.warning(
                f"stackcollapse-perf.pl not found at {stackcollapse_script}"
            )
            return

        if not flamegraph_script.exists():
            logging.warning(f"flamegraph.pl not found at {flamegraph_script}")
            return

        # Pipeline: perf script -> stackcollapse-perf.pl -> flamegraph.pl
        # Step 1: Get raw perf script output
        perf_script = subprocess.run(
            ["sudo", "perf", "script", "-i", str(perf_data)],
            capture_output=True,
            text=True,
        )

        if perf_script.returncode != 0:
            logging.warning(f"perf script failed: {perf_script.stderr}")
            return

        # Step 2: Collapse stacks
        stackcollapse_proc = subprocess.run(
            ["perl", str(stackcollapse_script)],
            input=perf_script.stdout,
            capture_output=True,
            text=True,
        )

        if stackcollapse_proc.returncode != 0:
            logging.warning(f"stackcollapse failed: {stackcollapse_proc.stderr}")
            return

        # Step 3: Generate flamegraph
        flamegraph_proc = subprocess.run(
            ["perl", str(flamegraph_script)],
            input=stackcollapse_proc.stdout,
            capture_output=True,
            text=True,
        )

        if flamegraph_proc.returncode == 0:
            with open(flamegraph_output, "w") as f:
                f.write(flamegraph_proc.stdout)
            logging.info(f"Generated flamegraph: {flamegraph_output}")
        else:
            logging.warning(f"Failed to generate flamegraph: {flamegraph_proc.stderr}")
