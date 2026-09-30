"""Host and process CPU source for the per-second metrics sampler.

Reads /proc, so it describes the machine the sampler runs on and is collected
only against a loopback target. CPU percentages are tick deltas over the
measured interval, where 100.0 means one fully busy core, so a multi-threaded
server can exceed 100. The async IO worker threads are isolated by thread name
and reported separately from the rest of the server process.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .base import SampleSource, read_text

# Kernel clock ticks per second, used to convert /proc CPU times to seconds.
_CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100

_PROC_DIR = Path("/proc")
_PROC_STAT_PATH = "/proc/stat"


def read_system_cpu_ticks() -> Optional[Tuple[int, int, int]]:
    """Return (user_ticks, system_ticks, total_ticks) from /proc/stat.

    user includes nice, and total is the sum of every reported bucket, so the
    derived percentages account for iowait, irq, softirq and steal.
    """
    first_line = read_text(_PROC_STAT_PATH).split("\n", 1)[0]
    parts = first_line.split()
    if len(parts) < 5 or parts[0] != "cpu":
        return None
    try:
        values = [int(part) for part in parts[1:]]
    except ValueError:
        return None
    # /proc/stat cpu buckets: user nice system idle iowait irq softirq steal ...
    return values[0] + values[1], values[2], sum(values)


def _parse_proc_stat_ticks(stat_line: str) -> Optional[Tuple[int, int]]:
    """Return (utime, stime) ticks from a /proc/<pid>/stat or task stat line.

    The comm field can contain spaces and parentheses, so the line is split on
    the last ") " and indexed from the state field onwards.
    """
    _, _, tail = stat_line.partition(") ")
    parts = tail.split()
    # tail starts at field 3 (state), so utime (field 14) is index 11.
    if len(parts) < 13:
        return None
    try:
        return int(parts[11]), int(parts[12])
    except ValueError:
        return None


def read_process_cpu_ticks(pid: int) -> Optional[Tuple[int, int]]:
    """Return (utime, stime) ticks for a process, or None if unreadable."""
    stat_line = read_text(str(_PROC_DIR / str(pid) / "stat"))
    if not stat_line:
        return None
    return _parse_proc_stat_ticks(stat_line)


def read_thread_cpu_ticks(pid: int, thread_name: str) -> Optional[int]:
    """Return summed user+system ticks of a process's threads named thread_name.

    Returns 0 when the process has no such thread (a server with the async IO
    worker absent is a normal state), and None when the task directory itself
    cannot be listed.
    """
    try:
        task_dirs = list((_PROC_DIR / str(pid) / "task").iterdir())
    except OSError:
        return None

    total_ticks = 0
    for task_dir in task_dirs:
        if read_text(str(task_dir / "comm")).strip() != thread_name:
            continue
        ticks = _parse_proc_stat_ticks(read_text(str(task_dir / "stat")))
        if ticks:
            total_ticks += ticks[0] + ticks[1]
    return total_ticks


def ticks_to_percent(tick_delta: int, interval: float) -> float:
    """Convert a CPU tick delta over interval seconds to percent of one core."""
    if tick_delta <= 0 or interval <= 0:
        return 0.0
    return round(tick_delta / (interval * _CLK_TCK) * 100, 2)


class ProcessCpuSource(SampleSource):
    """Host CPU, valkey-server CPU and async IO worker thread CPU columns."""

    name = "process_cpu"
    local_only = True

    def __init__(self):
        """Initialize the tick baselines, empty until the second tick."""
        self._prev_system_cpu: Optional[Tuple[int, int, int]] = None
        self._prev_process_cpu: Optional[Tuple[int, int]] = None
        self._prev_asio_ticks: Optional[int] = None

    def sample(self, interval: Optional[float]) -> Dict[str, Any]:
        """Derive host, per-process and async IO thread CPU percentages."""
        metrics: Dict[str, Any] = {
            "valkey_cpu_user": 0.0,
            "valkey_cpu_sys": 0.0,
            "valkey_cpu_total": 0.0,
            "asio_cpu_pct": 0.0,
            "cpu_user": 0.0,
            "cpu_sys": 0.0,
        }

        system_cpu = read_system_cpu_ticks()
        if system_cpu is None:
            self.ctx.warn_once(
                "no_proc_stat", "Cannot read /proc/stat, host CPU will be 0"
            )
        else:
            if self._prev_system_cpu is not None:
                prev_user, prev_sys, prev_total = self._prev_system_cpu
                total_delta = system_cpu[2] - prev_total
                if total_delta > 0:
                    metrics["cpu_user"] = round(
                        (system_cpu[0] - prev_user) / total_delta * 100, 2
                    )
                    metrics["cpu_sys"] = round(
                        (system_cpu[1] - prev_sys) / total_delta * 100, 2
                    )
            self._prev_system_cpu = system_cpu

        if self.ctx.server_pid is None:
            self.ctx.warn_once(
                "no_pid", "No server pid given, process CPU columns will be 0"
            )
            return metrics

        process_cpu = read_process_cpu_ticks(self.ctx.server_pid)
        if process_cpu is None:
            self.ctx.warn_once(
                "no_proc_pid_stat",
                f"Cannot read /proc/{self.ctx.server_pid}/stat, process CPU will be 0",
            )
        else:
            if self._prev_process_cpu is not None and interval:
                metrics["valkey_cpu_user"] = ticks_to_percent(
                    process_cpu[0] - self._prev_process_cpu[0], interval
                )
                metrics["valkey_cpu_sys"] = ticks_to_percent(
                    process_cpu[1] - self._prev_process_cpu[1], interval
                )
                metrics["valkey_cpu_total"] = round(
                    metrics["valkey_cpu_user"] + metrics["valkey_cpu_sys"], 2
                )
            self._prev_process_cpu = process_cpu

        asio_ticks = read_thread_cpu_ticks(
            self.ctx.server_pid, self.ctx.asio_thread_name
        )
        if asio_ticks is None:
            self.ctx.warn_once(
                "no_task_dir",
                f"Cannot list /proc/{self.ctx.server_pid}/task, asio CPU will be 0",
            )
        else:
            if self._prev_asio_ticks is not None and interval:
                metrics["asio_cpu_pct"] = ticks_to_percent(
                    asio_ticks - self._prev_asio_ticks, interval
                )
            self._prev_asio_ticks = asio_ticks

        return metrics
