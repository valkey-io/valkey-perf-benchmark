"""Generic performance profiling module for valkey-perf-benchmark."""

import logging
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
    
    def start_profiling(self, test_id: str, target_process: str = "valkey-server") -> None:
        """Start performance profiling for a test."""
        if not self.enabled:
            return
            
        try:
            # Find target process PID
            proc = subprocess.run(
                ["pgrep", "-f", target_process],
                capture_output=True,
                text=True
            )
            
            if proc.returncode != 0:
                logging.warning(f"No {target_process} process found for profiling")
                return
                
            server_pid = proc.stdout.strip().split()[0]
            
            # Start perf recording with sudo to bypass perf_event_paranoid
            perf_cmd = [
                "sudo", "perf", "record",
                "-e", "cycles:u",  # User-space only events
                "-F", "99",  # 99Hz sampling
                "--call-graph", "dwarf",  # User-space stack traces
                "-p", server_pid,
                "-o", str(self.results_dir / f"{test_id}.perf.data")
            ]
            
            # Start perf with error capture for debugging
            self.profiler_process = subprocess.Popen(
                perf_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Check if perf actually started
            time.sleep(0.5)
            if self.profiler_process.poll() is not None:
                # Process died immediately - capture error
                stdout, stderr = self.profiler_process.communicate()
                logging.error(f"Perf failed to start: {stderr.decode()}")
                self.profiler_process = None
                return
            
            logging.info(f"Started profiling for {test_id} (PID: {server_pid})")
            time.sleep(0.5)  # Give profiler time to start
            
        except Exception as e:
            logging.warning(f"Failed to start profiling: {e}")
    
    def stop_profiling(self, test_id: str) -> None:
        """Stop profiling and generate comprehensive analysis."""
        if not self.enabled or not self.profiler_process:
            return
            
        try:
            # Stop perf recording
            self.profiler_process.terminate()
            stdout, stderr = self.profiler_process.communicate(timeout=10)
            
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
                
                # Generate perf report
                self._generate_perf_report(perf_data, test_id)
                
                # Generate flamegraph if available
                self._generate_flamegraph(perf_data, test_id)
                
                logging.info(f"Preserved raw perf data: {perf_data}")
            elif perf_data.exists():
                logging.error(f"Perf data file is empty (0 bytes): {perf_data}")
                logging.error("This usually means perf couldn't collect samples - check permissions")
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
        perf_report = subprocess.run([
            "sudo", "perf", "report", "-i", str(perf_data), "--stdio"
        ], capture_output=True, text=True)
        
        if perf_report.returncode == 0:
            with open(report_output, 'w') as f:
                f.write(perf_report.stdout)
            logging.info(f"Generated perf report: {report_output}")
    
    def _generate_flamegraph(self, perf_data: Path, test_id: str) -> None:
        """Generate flamegraph visualization."""
        flamegraph_output = self.results_dir / f"{test_id}.svg"
        
        # Convert perf data to flamegraph using sudo
        perf_script = subprocess.run([
            "sudo", "perf", "script", "-i", str(perf_data)
        ], capture_output=True, text=True)
        
        if perf_script.returncode == 0:
            flamegraph_proc = subprocess.run([
                "flamegraph.pl"
            ], input=perf_script.stdout, capture_output=True, text=True)
            
            if flamegraph_proc.returncode == 0:
                with open(flamegraph_output, 'w') as f:
                    f.write(flamegraph_proc.stdout)
                logging.info(f"Generated flamegraph: {flamegraph_output}")
            else:
                logging.warning("flamegraph.pl not available - skipping flamegraph generation")
