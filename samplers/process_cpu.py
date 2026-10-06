"""Host and server thread CPU source for the per-second metrics sampler.

Reads /proc, so it describes the machine the sampler runs on and is collected
only when the server runs there. Records the cumulative `cpu` lines of
/proc/stat and, for every thread of the server pid, its name and cumulative
utime and stime, all in raw clock ticks.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

from .base import SampleSource

_PROC_DIR = Path("/proc")


def parse_proc_stat(text: str) -> Dict[str, List[int]]:
    """Return the counters of every `cpu` line of /proc/stat, keyed by name."""
    lines = (line.split() for line in text.splitlines())
    return {
        parts[0]: [int(value) for value in parts[1:]]
        for parts in lines
        if parts and parts[0].startswith("cpu")
    }


def parse_task_stat(text: str) -> Tuple[str, int, int]:
    """Return (comm, utime, stime) from a /proc/<pid>/task/<tid>/stat line."""
    head, _, tail = text.rpartition(") ")
    fields = tail.split()
    # tail starts at field 3 (state), so utime (field 14) is index 11.
    return head.partition("(")[2], int(fields[11]), int(fields[12])


def read_threads(pid: int) -> Dict[str, Dict[str, Any]]:
    """Return {tid: {comm, utime, stime}} for every thread of pid."""
    threads: Dict[str, Dict[str, Any]] = {}
    for task_dir in (_PROC_DIR / str(pid) / "task").iterdir():
        try:
            stat = (task_dir / "stat").read_text()
        except OSError:
            continue
        comm, utime, stime = parse_task_stat(stat)
        threads[task_dir.name] = {"comm": comm, "utime": utime, "stime": stime}
    return threads


class ProcessCpuSource(SampleSource):
    """Raw /proc/stat cpu lines and per-thread server CPU ticks."""

    name = "process_cpu"
    local_only = True
    linux_only = True

    def start(self, ctx) -> None:
        """Warn once when there is no server pid to read threads for."""
        super().start(ctx)
        if ctx.server_pid is None:
            ctx.warn_once(
                "no_pid", "No server pid given, process_cpu records host CPU only"
            )

    def sample(self) -> Dict[str, Any]:
        """Return the host cpu counters and the server's per-thread ticks."""
        reading: Dict[str, Any] = {
            "proc_stat": parse_proc_stat((_PROC_DIR / "stat").read_text())
        }
        if self.ctx.server_pid is not None:
            reading["threads"] = read_threads(self.ctx.server_pid)
        return reading
