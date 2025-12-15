"""CPU monitoring for performance tests."""

import logging
import psutil
import threading
import time
from typing import List, Dict, Optional


class CPUMonitor:
    """Monitor CPU usage during performance tests."""

    def __init__(self, target_process: str = "redis-server"):
        self.target_process = target_process
        self.monitoring = False
        self.monitor_thread = None
        self.cpu_samples = []
        self.memory_samples = []
        self.process = None

    def start_monitoring(self, test_id: str) -> None:
        """Start CPU monitoring for a test."""
        try:
            # Find target process - search for both redis-server and valkey-server
            search_terms = [self.target_process, "valkey-server", "redis-server"]

            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    # Check if any of our search terms match
                    if any(term in cmdline for term in search_terms):
                        self.process = psutil.Process(proc.info["pid"])
                        logging.info(
                            f"Found server process: PID={proc.info['pid']}, cmdline contains '{[t for t in search_terms if t in cmdline][0]}'"
                        )
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if not self.process:
                logging.warning(
                    f"Could not find server process (searched for: {', '.join(search_terms)})"
                )
                return

            # Reset samples
            self.cpu_samples = []
            self.memory_samples = []
            self.monitoring = True

            # Start monitoring thread
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True
            )
            self.monitor_thread.start()

            logging.info(
                f"Started CPU monitoring for {test_id} (PID: {self.process.pid})"
            )

        except Exception as e:
            logging.warning(f"Failed to start CPU monitoring: {e}")

    def stop_monitoring(self, test_id: str) -> Dict[str, float]:
        """Stop CPU monitoring and return statistics."""
        self.monitoring = False

        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)

        if not self.cpu_samples:
            logging.warning(f"No CPU samples collected for {test_id}")
            return {}

        # Calculate statistics
        cpu_avg = sum(self.cpu_samples) / len(self.cpu_samples)
        cpu_max = max(self.cpu_samples)
        cpu_min = min(self.cpu_samples)

        # Memory stats (MB)
        mem_avg = sum(self.memory_samples) / len(self.memory_samples) / (1024 * 1024)
        mem_max = max(self.memory_samples) / (1024 * 1024)

        # Calculate p95 CPU
        sorted_cpu = sorted(self.cpu_samples)
        p95_idx = int(len(sorted_cpu) * 0.95)
        cpu_p95 = sorted_cpu[p95_idx] if sorted_cpu else 0

        stats = {
            "cpu_avg_percent": round(cpu_avg, 2),
            "cpu_max_percent": round(cpu_max, 2),
            "cpu_min_percent": round(cpu_min, 2),
            "cpu_p95_percent": round(cpu_p95, 2),
            "memory_avg_mb": round(mem_avg, 2),
            "memory_max_mb": round(mem_max, 2),
            "cpu_sample_count": len(self.cpu_samples),
        }

        logging.info(
            f"CPU stats for {test_id}: avg={stats['cpu_avg_percent']}% peak={stats['cpu_max_percent']}% mem={stats['memory_avg_mb']}MB"
        )

        return stats

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        try:
            # Initial CPU measurement (to establish baseline)
            self.process.cpu_percent(interval=None)
            time.sleep(0.1)

            while self.monitoring:
                try:
                    # Get CPU percentage (interval=None uses previous call as baseline)
                    cpu_percent = self.process.cpu_percent(interval=None)
                    mem_bytes = self.process.memory_info().rss

                    self.cpu_samples.append(cpu_percent)
                    self.memory_samples.append(mem_bytes)

                    time.sleep(1)  # Sample every second

                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    logging.warning(f"Process monitoring error: {e}")
                    break

        except Exception as e:
            logging.warning(f"CPU monitoring loop failed: {e}")
