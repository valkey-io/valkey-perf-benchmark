"""Per-CPU monitoring for detecting scheduler issues."""

import logging
import subprocess
import threading
import time
from typing import Dict, List
from pathlib import Path


class PerCPUMonitor:
    """Monitor per-physical-CPU utilization."""

    def __init__(self, cpu_cores: str = "0-7", enabled: bool = True):
        """
        Initialize per-CPU monitor.

        Args:
            cpu_cores: CPU cores to monitor (e.g., "0-7" for cores 0 through 7)
            enabled: Whether monitoring is enabled
        """
        self.enabled = enabled
        if not enabled:
            return

        self.cpu_cores = cpu_cores
        self.monitoring = False
        self.monitor_process = None
        self.monitor_thread = None
        self.cpu_samples = (
            {}
        )  # {cpu_id: {"usr": [samples], "sys": [samples], "idle": [samples]}}
        self.sample_count = 0

        # Parse core range to get individual CPU IDs
        self.cpu_list = self._parse_core_range(cpu_cores)

    def _parse_core_range(self, core_range: str) -> List[int]:
        """Parse core range string like '0-7' into list [0,1,2,3,4,5,6,7]."""
        if "-" in core_range:
            start, end = core_range.split("-")
            return list(range(int(start), int(end) + 1))
        else:
            return [int(core_range)]

    def start_monitoring(self, test_id: str) -> None:
        """Start monitoring per-CPU utilization."""
        if not self.enabled:
            return

        try:
            # Initialize sample storage for each CPU
            for cpu_id in self.cpu_list:
                self.cpu_samples[cpu_id] = {
                    "usr": [],
                    "sys": [],
                    "idle": [],
                    "iowait": [],
                }

            self.monitoring = True
            self.sample_count = 0

            # Start monitoring thread
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True
            )
            self.monitor_thread.start()

            logging.info(
                f"Started per-CPU monitoring for {test_id} (CPUs: {self.cpu_cores})"
            )

        except Exception as e:
            logging.warning(f"Failed to start per-CPU monitoring: {e}")

    def stop_monitoring(self, test_id: str) -> Dict:
        """Stop monitoring and return per-CPU statistics."""
        if not self.enabled:
            return {}

        logging.debug(
            f"Stopping per-CPU monitoring for {test_id}, current samples: {self.sample_count}"
        )

        self.monitoring = False

        # Terminate mpstat process first
        if self.monitor_process and self.monitor_process.poll() is None:
            try:
                self.monitor_process.terminate()
                self.monitor_process.wait(timeout=2)
                logging.debug("mpstat process terminated")
            except Exception as e:
                logging.debug(f"Error terminating mpstat: {e}")

        # Wait for monitor thread to finish
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            logging.debug("Monitor thread joined")

        if self.sample_count == 0:
            logging.warning(
                f"No per-CPU samples collected for {test_id} - mpstat may not have produced output in time"
            )
            return {}

        # Calculate busy % for each CPU
        per_cpu_stats = {}

        for cpu_id in self.cpu_list:
            if cpu_id not in self.cpu_samples or not self.cpu_samples[cpu_id]["idle"]:
                continue

            idle_samples = self.cpu_samples[cpu_id]["idle"]
            busy_avg = 100.0 - (sum(idle_samples) / len(idle_samples))
            per_cpu_stats[f"cpu{cpu_id}"] = round(busy_avg, 2)

        logging.info(f"Per-CPU utilization: {per_cpu_stats}")

        return {"per_cpu": per_cpu_stats}

    def _monitor_loop(self) -> None:
        """Background monitoring loop using mpstat."""
        try:
            # Launch mpstat to sample every second
            cmd = ["mpstat", "-P", "ALL", "1"]

            logging.debug(f"Starting mpstat with command: {' '.join(cmd)}")

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            self.monitor_process = proc

            # Check if process started successfully
            time.sleep(0.5)
            if proc.poll() is not None:
                # Process exited immediately
                stdout, stderr = proc.communicate()
                logging.error(f"mpstat failed to start:")
                logging.error(f"  stdout: {stdout}")
                logging.error(f"  stderr: {stderr}")
                return

            logging.debug("mpstat started successfully, parsing output...")
            lines_processed = 0

            # Parse mpstat output line by line
            for line in proc.stdout:
                if not self.monitoring:
                    logging.debug(
                        f"Monitoring stopped after {lines_processed} lines, {self.sample_count} samples"
                    )
                    break

                lines_processed += 1

                # Skip header lines and average lines
                if (
                    "CPU" in line
                    or "Average" in line
                    or "Linux" in line
                    or not line.strip()
                ):
                    continue

                # Parse data line: Time [AM/PM] CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle
                parts = line.split()

                # Need at least 12 fields, but may have 13 with AM/PM in time
                if len(parts) < 12:
                    logging.debug(
                        f"Skipping line with {len(parts)} parts (need 12+): {line[:50]}"
                    )
                    continue

                try:
                    # Determine if time includes AM/PM (12-hour format) or not (24-hour)
                    if parts[1] in ["AM", "PM"]:
                        # 12-hour format: parts[0]=time, parts[1]=AM/PM, parts[2]=CPU
                        cpu_id_str = parts[2]
                        usr_idx = 3
                        sys_idx = 5
                        iowait_idx = 6
                        idle_idx = 12
                    else:
                        # 24-hour format: parts[0]=time, parts[1]=CPU
                        cpu_id_str = parts[1]
                        usr_idx = 2
                        sys_idx = 4
                        iowait_idx = 5
                        idle_idx = 11

                    if cpu_id_str == "all":
                        continue  # Skip "all CPUs" aggregate line

                    cpu_id = int(cpu_id_str)

                    if cpu_id not in self.cpu_list:
                        continue  # Skip CPUs outside our monitored range

                    # Extract metrics using correct indices
                    usr = float(parts[usr_idx])
                    sys = float(parts[sys_idx])
                    iowait = float(parts[iowait_idx])
                    idle = float(parts[idle_idx])

                    # Store samples
                    self.cpu_samples[cpu_id]["usr"].append(usr)
                    self.cpu_samples[cpu_id]["sys"].append(sys)
                    self.cpu_samples[cpu_id]["idle"].append(idle)
                    self.cpu_samples[cpu_id]["iowait"].append(iowait)

                    self.sample_count += 1

                    if self.sample_count % 100 == 0:
                        logging.debug(f"Collected {self.sample_count} per-CPU samples")

                except (ValueError, IndexError) as e:
                    logging.debug(f"Parse error on line: {line[:50]} - {e}")
                    continue

            logging.debug(
                f"mpstat loop ended: processed {lines_processed} lines, collected {self.sample_count} samples"
            )

            # Clean up process
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)

        except Exception as e:
            logging.warning(f"Per-CPU monitoring loop failed: {e}")
