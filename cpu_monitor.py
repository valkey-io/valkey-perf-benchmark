"""CPU monitoring for performance tests."""

import logging
import psutil
import threading
import time
from typing import List, Dict, Optional


class CPUMonitor:
    """Monitor CPU usage during performance tests with per-thread tracking and CPU affinity."""

    def __init__(self, server_type: str = "auto", enabled: bool = True):
        """Initialize CPU monitor with server type detection.

        Args:
            server_type: Type of server to monitor ("auto", "valkey", "redis", or custom process name)
            enabled: Whether monitoring is enabled
        """
        self.enabled = enabled
        if not enabled:
            return

        # Resolve server type to target process name(s)
        if server_type == "auto":
            # Auto-detect: search for both valkey and redis
            self.target_processes = ["valkey-server", "redis-server"]
        elif server_type == "valkey":
            self.target_processes = ["valkey-server"]
        elif server_type == "redis":
            self.target_processes = ["redis-server"]
        else:
            # Custom process name
            self.target_processes = [server_type]

        self.monitoring = False
        self.monitor_thread = None
        self.thread_cpu_samples = {}  # {tid: [cpu_samples]}
        self.thread_names = {}  # {tid: name}
        self.thread_cores = {}  # {tid: most_recent_core}
        self.thread_migrations = {}  # {tid: migration_count}
        self.peak_memory = 0
        self.process = None
        self.initial_thread_times = {}  # {tid: (user_time, system_time)}

    def start_monitoring(self, test_id: str) -> None:
        """Start CPU monitoring for a test."""
        if not self.enabled:
            return

        try:
            # Find target process
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    if any(term in cmdline for term in self.target_processes):
                        self.process = psutil.Process(proc.info["pid"])
                        matched_term = [
                            t for t in self.target_processes if t in cmdline
                        ][0]
                        logging.info(
                            f"Found server process: PID={proc.info['pid']}, matched '{matched_term}'"
                        )
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if not self.process:
                logging.warning(
                    f"Could not find server process (searched for: {', '.join(self.target_processes)})"
                )
                return

            # Initialize tracking
            self.thread_cpu_samples = {}
            self.thread_names = {}
            self.thread_cores = {}
            self.thread_migrations = {}
            self.peak_memory = 0
            self.initial_thread_times = {}

            self.monitoring = True
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True
            )
            self.monitor_thread.start()

            logging.info(
                f"Started CPU monitoring for {test_id} (PID: {self.process.pid})"
            )

        except Exception as e:
            logging.warning(f"Failed to start CPU monitoring: {e}")

    def stop_monitoring(self, test_id: str) -> Dict:
        """Stop CPU monitoring and return essential statistics."""
        if not self.enabled:
            return {}

        self.monitoring = False

        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)

        if not self.thread_cpu_samples:
            logging.warning(f"No CPU samples collected for {test_id}")
            return {}

        # Build simplified thread stats
        threads = {}
        for tid, samples in self.thread_cpu_samples.items():
            if samples:
                thread_name = self.thread_names.get(tid, f"thread_{tid}")
                threads[thread_name] = {
                    "avg_percent": round(sum(samples) / len(samples), 2),
                    "primary_cpu": self.thread_cores.get(tid),
                    "migrations": self.thread_migrations.get(tid, 0),
                }

        stats = {
            "threads": threads,
            "memory_max_mb": round(self.peak_memory / (1024 * 1024), 2),
        }

        logging.info(
            f"Monitored {len(threads)} threads, peak memory: {stats['memory_max_mb']}MB"
        )
        for name, data in threads.items():
            cpu_info = f"{data['avg_percent']}%"
            if data["primary_cpu"] is not None:
                cpu_info += f" (cpu{data['primary_cpu']})"
            if data.get("migrations", 0) > 0:
                cpu_info += f", {data['migrations']} migrations"
            logging.info(f"  {name}: {cpu_info}")

        return stats

    def _monitor_loop(self) -> None:
        """Monitoring loop - track CPU per thread and peak memory."""
        try:
            import os

            while self.monitoring:
                try:
                    # Track peak memory
                    mem_bytes = self.process.memory_info().rss
                    if mem_bytes > self.peak_memory:
                        self.peak_memory = mem_bytes

                    # Get per-thread CPU
                    threads = self.process.threads()
                    for thread in threads:
                        tid = thread.id

                        # Initialize new threads
                        if tid not in self.thread_cpu_samples:
                            self.thread_cpu_samples[tid] = []
                            self.thread_migrations[tid] = 0
                            self.initial_thread_times[tid] = (
                                thread.user_time,
                                thread.system_time,
                            )
                            try:
                                comm_path = f"/proc/{self.process.pid}/task/{tid}/comm"
                                if os.path.exists(comm_path):
                                    with open(comm_path, "r") as f:
                                        self.thread_names[tid] = f.read().strip()
                            except:
                                self.thread_names[tid] = f"thread_{tid}"

                        # Track core migrations
                        try:
                            stat_path = f"/proc/{self.process.pid}/task/{tid}/stat"
                            if os.path.exists(stat_path):
                                with open(stat_path, "r") as f:
                                    stat_data = f.read().split()
                                    if len(stat_data) > 38:
                                        current_core = int(stat_data[38])
                                        if (
                                            tid in self.thread_cores
                                            and self.thread_cores[tid] != current_core
                                        ):
                                            self.thread_migrations[tid] = (
                                                self.thread_migrations.get(tid, 0) + 1
                                            )
                                        self.thread_cores[tid] = current_core
                        except:
                            pass

                        # Calculate CPU % over 1 second interval
                        if tid in self.initial_thread_times:
                            prev_user, prev_sys = self.initial_thread_times[tid]
                            delta_user = thread.user_time - prev_user
                            delta_sys = thread.system_time - prev_sys
                            thread_cpu = (delta_user + delta_sys) * 100
                            self.thread_cpu_samples[tid].append(thread_cpu)
                            self.initial_thread_times[tid] = (
                                thread.user_time,
                                thread.system_time,
                            )

                    time.sleep(1)

                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    logging.warning(f"Process monitoring error: {e}")
                    break

        except Exception as e:
            logging.warning(f"CPU monitoring loop failed: {e}")
