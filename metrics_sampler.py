"""Per-second metrics sampler for Valkey data tiering benchmarks.

Samples the server and the host once per interval (1 Hz by default) for the
duration of a benchmark's measured phase, and emits one row per sample. The
rows form a time series that a per-commit deep-dive dashboard can chart, with
`elapsed_sec` as the x-axis so several commits overlay on one chart.

Design notes
------------
Sampling runs on a background daemon thread and never raises into the caller:
every source is read defensively, and a missing or unparsable metric becomes 0
(or None where 0 would be misleading) plus a warning logged once per distinct
failure. A server built without data tiering, or with tiering disabled, still
samples cleanly: `genExternalStorageInfoString` returns early when
`ext_data_enabled` is false (valkey-data-tiering src/ext_storage.c:1549), so the
whole External Storage section is simply absent and the tiering columns are 0.

INFO is read by shelling out to valkey-cli, matching both the reference
collector and the way valkey_server.py already drives the CLI. At 1 Hz the
process spawn cost is negligible, and it keeps TLS and cluster invocation a
matter of extra CLI args later rather than client wiring now.

Tiering INFO fields
-------------------
Tiering INFO field names are emitted verbatim as column names, so the table
below is a source-location reference rather than a name mapping: each field
plus where it is registered in the valkey-data-tiering source.

| INFO field                               | Source                 |
| ---------------------------------------- | ---------------------- |
| total_num_items_spilled_to_ext_storage   | src/ext_storage.c:1551 |
| total_num_items_fetched_from_ext_storage | src/ext_storage.c:1552 |
| num_items_spilling_to_ext_storage        | src/ext_storage.c:1554 |
| completion_read_ok                       | src/ext_storage.c:1572 |
| dram_value_hits                          | src/ext_storage.c:1587 |

All five live in the `external_storage` INFO section (valkey-data-tiering
src/server.c:6803).

Hit ratios
----------
`completion_read_ok` and `dram_value_hits` are emitted as raw counters and are
also the two inputs to the hit ratios, so a later reader can tell a real
movement apart from a formula or divide-by-zero bug. The engine treats
`dram_value_hits` as the count of value-accessing commands (immediate DRAM hits
plus re-executions after a fetch completes).

Each ratio is emitted in two forms:

Cumulative (`disk_hit_pct`, `mem_hit_pct`), computed from the running totals:

    disk_hit_pct = completion_read_ok / dram_value_hits * 100
    mem_hit_pct  = (dram_value_hits - completion_read_ok) / dram_value_hits * 100

These names carry the cumulative meaning because they match the reference
dashboard CSV, whose published end-of-run figure is a cumulative value.

Per-interval (`disk_hit_pct_interval`, `mem_hit_pct_interval`), computed from
the deltas between consecutive samples, which is the form the engine itself
documents (valkey-data-tiering src/ext_storage.c:235):

    DRAM_hit% = (delta dram_value_hits - delta completion_read_ok)
                / delta dram_value_hits

A cumulative ratio late in a run is diluted by all prior history, so a mid-run
hit-rate collapse barely moves it. The per-interval form is what lets a
per-second chart show transients.

A zero denominator yields 0.0 for both forms (not null, not an error), matching
the reference CSV, which emits 0.0 for the first ten-plus samples of its run
while `dram_value_hits` is still 0.

Column names follow the reference dashboard CSV
(valkey-data-tiering/benchmark_dashboard/data/zipfian-80-20.csv) wherever this
slice covers the same metric, so the existing chart definitions port over. The
CSV header, not the collector README, is the authority: the README documents an
older column set.

Deltas
------
`ops_per_sec` and `total_commands_delta` are both derived from
`total_commands_processed` between consecutive samples, not from
`instantaneous_ops_per_sec`. `total_commands_delta` is the raw command count in
the interval; `ops_per_sec` is that count divided by the measured interval. The
first sample of a run has no predecessor, so every delta-derived field is 0
there. Disk IOPS and MB/s are likewise deltas over the measured interval, and
CPU percentages are tick deltas over the measured interval (100.0 means one
fully busy core, so a multi-threaded server can exceed 100).
"""

import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Kernel clock ticks per second, used to convert /proc CPU times to seconds.
_CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100

# /sys/block reports transfer sizes in 512-byte sectors regardless of the
# device's real logical block size.
_SECTOR_BYTES = 512
_BYTES_PER_MB = 1024 * 1024

# Thread name set by pthread_setname_np in the flash cache IO worker
# (valkey-data-tiering src/storage/storage_flashcache_real.c:162).
ASIO_THREAD_NAME = "fc_io_worker"

_PROC_DIR = Path("/proc")
_PROC_STAT_PATH = "/proc/stat"
_SYS_BLOCK_DIR = Path("/sys/block")
_INFO_TIMEOUT_SEC = 5
_JOIN_TIMEOUT_SEC = 5

# Block device name prefixes worth sampling, in preference order. Everything
# else in /sys/block (loop, ram, dm, zram) is not a benchmark data device.
_BLOCK_DEVICE_PREFIXES = ("nvme", "sd")

# Tiering INFO field names, emitted verbatim as column names. See the module
# docstring for the source locations these were resolved from.
TIERING_INFO_FIELDS = (
    "total_num_items_spilled_to_ext_storage",
    "total_num_items_fetched_from_ext_storage",
    "num_items_spilling_to_ext_storage",
    "completion_read_ok",
    "dram_value_hits",
)


def _read_text(path: str, default: str = "") -> str:
    """Read a procfs/sysfs file, returning default if it is unreadable."""
    try:
        return Path(path).read_text()
    except OSError:
        return default


def _to_int(value: Optional[str], default: int = 0) -> int:
    """Parse an INFO or /proc value as int, returning default on failure."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_float(value: Optional[str], default: float = 0.0) -> float:
    """Parse an INFO or /proc value as float, returning default on failure."""
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def parse_info(text: str) -> Dict[str, str]:
    """Parse INFO output into a flat field to value dict.

    Section headers ("# Memory") and blank lines are skipped. Sections are
    flattened because field names are unique across INFO sections.
    """
    fields: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip()
    return fields


def detect_block_device() -> Optional[str]:
    """Return the largest NVMe or SCSI whole-disk device name, or None.

    Prefers NVMe over SCSI because the tiering data device is an instance-store
    NVMe on the benchmark hosts, then picks the largest candidate within the
    preferred class so a small root volume is not chosen over the data device.
    """
    try:
        names = sorted(entry.name for entry in _SYS_BLOCK_DIR.iterdir())
    except OSError:
        return None

    for prefix in _BLOCK_DEVICE_PREFIXES:
        candidates = [name for name in names if name.startswith(prefix)]
        if not candidates:
            continue
        return max(
            candidates,
            key=lambda name: _to_int(_read_text(str(_SYS_BLOCK_DIR / name / "size"))),
        )
    return None


def read_system_cpu_ticks() -> Optional[Tuple[int, int, int]]:
    """Return (user_ticks, system_ticks, total_ticks) from /proc/stat.

    user includes nice, and total is the sum of every reported bucket, so the
    derived percentages account for iowait, irq, softirq and steal. The
    reference shell collector summed only user, nice, system and idle, so its
    percentages read slightly high on an IO-heavy host.
    """
    first_line = _read_text(_PROC_STAT_PATH).split("\n", 1)[0]
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
    stat_line = _read_text(str(_PROC_DIR / str(pid) / "stat"))
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
        if _read_text(str(task_dir / "comm")).strip() != thread_name:
            continue
        ticks = _parse_proc_stat_ticks(_read_text(str(task_dir / "stat")))
        if ticks:
            total_ticks += ticks[0] + ticks[1]
    return total_ticks


def read_disk_counters(device: str) -> Optional[Dict[str, int]]:
    """Return cumulative IO counters for a block device, or None if unreadable."""
    parts = _read_text(str(_SYS_BLOCK_DIR / device / "stat")).split()
    # /sys/block/<dev>/stat: rd_ios rd_merges rd_sectors rd_ticks
    #                        wr_ios wr_merges wr_sectors wr_ticks ...
    if len(parts) < 7:
        return None
    try:
        return {
            "read_ios": int(parts[0]),
            "read_sectors": int(parts[2]),
            "write_ios": int(parts[4]),
            "write_sectors": int(parts[6]),
        }
    except ValueError:
        return None


class MetricsSampler:
    """Sample Valkey INFO and host counters on a background thread at a fixed rate.

    Emits one row per sample. Every row carries `elapsed_sec` (integer seconds
    since start), an absolute `timestamp` for provenance, and every key of the
    injected `context` dict so rows are self-describing once loaded into a
    dashboard.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        cli_path: str = "valkey-cli",
        server_pid: Optional[int] = None,
        interval: float = 1.0,
        enabled: bool = True,
        context: Optional[Dict[str, Any]] = None,
        block_device: Optional[str] = None,
        asio_thread_name: str = ASIO_THREAD_NAME,
    ):
        """Initialize the sampler.

        Args:
            host: Valkey host to send INFO to
            port: Valkey port to send INFO to
            cli_path: valkey-cli executable used to issue INFO
            server_pid: valkey-server pid, for per-process and per-thread CPU
            interval: seconds between samples
            enabled: when False, start/stop are no-ops and no rows are produced
            context: run-identity fields merged into every emitted row
            block_device: block device name to sample, auto-detected when None
            asio_thread_name: thread name isolated as the async IO worker
        """
        self.enabled = enabled
        self.host = host
        self.port = port
        self.cli_path = cli_path
        self.server_pid = server_pid
        self.interval = interval
        self.context = dict(context or {})
        self.block_device = block_device
        self.asio_thread_name = asio_thread_name

        self._rows: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sampler_thread: Optional[threading.Thread] = None
        self._start_monotonic: Optional[float] = None
        self._warned: set = set()

        # Previous cumulative counters, for delta derivation.
        self._prev_monotonic: Optional[float] = None
        self._prev_total_commands: Optional[int] = None
        self._prev_dram_value_hits: Optional[int] = None
        self._prev_completion_read_ok: Optional[int] = None
        self._prev_system_cpu: Optional[Tuple[int, int, int]] = None
        self._prev_process_cpu: Optional[Tuple[int, int]] = None
        self._prev_asio_ticks: Optional[int] = None
        self._prev_disk: Optional[Dict[str, int]] = None

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Return a copy of the rows collected so far."""
        with self._lock:
            return list(self._rows)

    def start(self) -> None:
        """Start sampling on a background thread. Safe to call when disabled."""
        if not self.enabled:
            return
        if self._sampler_thread is not None:
            logging.warning("Metrics sampler already started, ignoring start()")
            return

        if self.block_device is None:
            self.block_device = detect_block_device()
            if self.block_device is None:
                self._warn_once(
                    "no_block_device",
                    "No NVMe or SCSI block device found, disk metrics will be 0",
                )

        self._start_monotonic = time.monotonic()
        self._stop_event.clear()
        self._sampler_thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._sampler_thread.start()
        logging.info(
            f"Started metrics sampler ({self.interval}s interval, "
            f"pid={self.server_pid}, device={self.block_device})"
        )

    def stop(self) -> None:
        """Stop sampling and join the background thread."""
        if not self.enabled:
            return

        self._stop_event.set()
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=_JOIN_TIMEOUT_SEC)
            if self._sampler_thread.is_alive():
                logging.warning("Metrics sampler thread did not stop within timeout")
            self._sampler_thread = None

        logging.info(f"Stopped metrics sampler after {len(self._rows)} samples")

    def write(self, path: Any) -> None:
        """Write the collected rows to path as a JSON array of row dicts."""
        rows = self.rows
        try:
            output_path = Path(path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as handle:
                json.dump(rows, handle, indent=2)
            logging.info(f"Wrote {len(rows)} metric samples to {output_path}")
        except OSError as e:
            logging.warning(f"Failed to write metric samples to {path}: {e}")

    def _warn_once(self, key: str, message: str) -> None:
        """Log a warning the first time key is seen, to avoid 1 Hz log spam."""
        if key in self._warned:
            return
        self._warned.add(key)
        logging.warning(message)

    def _sample_loop(self) -> None:
        """Sample until stopped, holding the configured interval between ticks."""
        while not self._stop_event.is_set():
            tick_started = time.monotonic()
            try:
                self._sample_once()
            except Exception as e:
                self._warn_once("sample_failed", f"Metrics sample failed: {e}")
            remaining = self.interval - (time.monotonic() - tick_started)
            self._stop_event.wait(max(0.0, remaining))

    def _sample_once(self) -> None:
        """Collect one row and append it to the series."""
        now = time.monotonic()
        start = self._start_monotonic if self._start_monotonic is not None else now
        # None on the first sample: nothing to take a delta against yet.
        interval = None
        if self._prev_monotonic is not None:
            elapsed_since_prev = now - self._prev_monotonic
            if elapsed_since_prev > 0:
                interval = elapsed_since_prev

        row: Dict[str, Any] = {
            "timestamp": int(time.time()),
            "elapsed_sec": int(round(now - start)),
        }
        row.update(self._info_metrics(self._read_info(), interval))
        row.update(self._cpu_metrics(interval))
        row.update(self._disk_metrics(interval))
        # Context last so run identity always survives a name collision.
        row.update(self.context)

        self._prev_monotonic = now
        with self._lock:
            self._rows.append(row)

    def _read_info(self) -> Dict[str, str]:
        """Return parsed INFO ALL fields, or an empty dict if INFO is unavailable."""
        cmd = [self.cli_path, "-h", self.host, "-p", str(self.port), "INFO", "ALL"]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=_INFO_TIMEOUT_SEC
            )
        except (OSError, subprocess.SubprocessError) as e:
            self._warn_once("info_failed", f"INFO command failed: {e}")
            return {}

        if result.returncode != 0:
            self._warn_once(
                "info_rc",
                f"INFO command exited {result.returncode}: {result.stderr.strip()}",
            )
            return {}
        return parse_info(result.stdout)

    def _info_metrics(
        self, info: Dict[str, str], interval: Optional[float]
    ) -> Dict[str, Any]:
        """Derive the INFO-sourced columns, including deltas and hit ratios."""
        if not info:
            self._warn_once("info_empty", "INFO returned no fields, emitting zeros")

        total_commands = _to_int(info.get("total_commands_processed"))
        commands_delta = 0
        ops_per_sec = 0.0
        if self._prev_total_commands is not None and interval:
            commands_delta = max(0, total_commands - self._prev_total_commands)
            ops_per_sec = round(commands_delta / interval, 2)
        self._prev_total_commands = total_commands

        # Tiering counters, emitted under their own INFO field names.
        tiering = {field: _to_int(info.get(field)) for field in TIERING_INFO_FIELDS}
        dram_value_hits = tiering["dram_value_hits"]
        completion_read_ok = tiering["completion_read_ok"]

        # Cumulative hit ratios, from the running totals. A tiering-disabled
        # server reports no dram_value_hits at all, which lands here as 0 and
        # yields 0.0 ratios.
        disk_hit_pct, mem_hit_pct = self._hit_ratios(
            dram_value_hits, completion_read_ok
        )

        # Per-interval hit ratios, from the deltas between consecutive samples.
        # The first sample has no predecessor, so both are 0.0 there.
        dram_delta = 0
        completion_delta = 0
        if self._prev_dram_value_hits is not None:
            dram_delta = max(0, dram_value_hits - self._prev_dram_value_hits)
        if self._prev_completion_read_ok is not None:
            completion_delta = max(
                0, completion_read_ok - self._prev_completion_read_ok
            )
        disk_hit_pct_interval, mem_hit_pct_interval = self._hit_ratios(
            dram_delta, completion_delta
        )
        self._prev_dram_value_hits = dram_value_hits
        self._prev_completion_read_ok = completion_read_ok

        metrics: Dict[str, Any] = {
            "used_memory": _to_int(info.get("used_memory")),
            "used_memory_rss": _to_int(info.get("used_memory_rss")),
            "maxmemory": _to_int(info.get("maxmemory")),
            "mem_frag_ratio": _to_float(info.get("mem_fragmentation_ratio")),
            "ops_per_sec": ops_per_sec,
            "total_commands_delta": commands_delta,
            "keyspace_hits": _to_int(info.get("keyspace_hits")),
            "keyspace_misses": _to_int(info.get("keyspace_misses")),
            "mem_hit_pct": mem_hit_pct,
            "disk_hit_pct": disk_hit_pct,
            "mem_hit_pct_interval": mem_hit_pct_interval,
            "disk_hit_pct_interval": disk_hit_pct_interval,
            "blocked_clients": _to_int(info.get("blocked_clients")),
        }
        metrics.update(tiering)
        return metrics

    @staticmethod
    def _hit_ratios(
        dram_value_hits: int, completion_read_ok: int
    ) -> Tuple[float, float]:
        """Return (disk_hit_pct, mem_hit_pct) for one pair of counter values.

        Takes either the running totals (cumulative form) or the deltas between
        consecutive samples (per-interval form). A zero denominator yields
        (0.0, 0.0), matching the reference dashboard CSV.
        """
        if dram_value_hits <= 0:
            return 0.0, 0.0
        disk_hit_pct = round(completion_read_ok / dram_value_hits * 100, 2)
        mem_hit_pct = round(
            (dram_value_hits - completion_read_ok) / dram_value_hits * 100, 2
        )
        return disk_hit_pct, mem_hit_pct

    def _cpu_metrics(self, interval: Optional[float]) -> Dict[str, Any]:
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
            self._warn_once(
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

        if self.server_pid is None:
            self._warn_once(
                "no_pid", "No server pid given, process CPU columns will be 0"
            )
            return metrics

        process_cpu = read_process_cpu_ticks(self.server_pid)
        if process_cpu is None:
            self._warn_once(
                "no_proc_pid_stat",
                f"Cannot read /proc/{self.server_pid}/stat, process CPU will be 0",
            )
        else:
            if self._prev_process_cpu is not None and interval:
                metrics["valkey_cpu_user"] = self._ticks_to_percent(
                    process_cpu[0] - self._prev_process_cpu[0], interval
                )
                metrics["valkey_cpu_sys"] = self._ticks_to_percent(
                    process_cpu[1] - self._prev_process_cpu[1], interval
                )
                metrics["valkey_cpu_total"] = round(
                    metrics["valkey_cpu_user"] + metrics["valkey_cpu_sys"], 2
                )
            self._prev_process_cpu = process_cpu

        asio_ticks = read_thread_cpu_ticks(self.server_pid, self.asio_thread_name)
        if asio_ticks is None:
            self._warn_once(
                "no_task_dir",
                f"Cannot list /proc/{self.server_pid}/task, asio CPU will be 0",
            )
        else:
            if self._prev_asio_ticks is not None and interval:
                metrics["asio_cpu_pct"] = self._ticks_to_percent(
                    asio_ticks - self._prev_asio_ticks, interval
                )
            self._prev_asio_ticks = asio_ticks

        return metrics

    @staticmethod
    def _ticks_to_percent(tick_delta: int, interval: float) -> float:
        """Convert a CPU tick delta over interval seconds to percent of one core."""
        if tick_delta <= 0 or interval <= 0:
            return 0.0
        return round(tick_delta / (interval * _CLK_TCK) * 100, 2)

    def _disk_metrics(self, interval: Optional[float]) -> Dict[str, Any]:
        """Derive disk IOPS and throughput as deltas over the interval."""
        metrics: Dict[str, Any] = {
            "disk_read_iops": 0.0,
            "disk_write_iops": 0.0,
            "disk_read_mb": 0.0,
            "disk_write_mb": 0.0,
        }
        if not self.block_device:
            return metrics

        counters = read_disk_counters(self.block_device)
        if counters is None:
            self._warn_once(
                "no_disk_stat",
                f"Cannot read /sys/block/{self.block_device}/stat, disk metrics will be 0",
            )
            return metrics

        if self._prev_disk is not None and interval:
            prev = self._prev_disk
            metrics["disk_read_iops"] = self._rate(
                counters["read_ios"] - prev["read_ios"], interval
            )
            metrics["disk_write_iops"] = self._rate(
                counters["write_ios"] - prev["write_ios"], interval
            )
            metrics["disk_read_mb"] = self._sector_rate_mb(
                counters["read_sectors"] - prev["read_sectors"], interval
            )
            metrics["disk_write_mb"] = self._sector_rate_mb(
                counters["write_sectors"] - prev["write_sectors"], interval
            )
        self._prev_disk = counters
        return metrics

    @staticmethod
    def _rate(delta: int, interval: float) -> float:
        """Return delta per second, clamped at 0 so counter resets do not go negative."""
        if delta <= 0 or interval <= 0:
            return 0.0
        return round(delta / interval, 2)

    @staticmethod
    def _sector_rate_mb(sector_delta: int, interval: float) -> float:
        """Convert a 512-byte sector delta over interval seconds to MB/s."""
        if sector_delta <= 0 or interval <= 0:
            return 0.0
        return round(sector_delta * _SECTOR_BYTES / _BYTES_PER_MB / interval, 2)
