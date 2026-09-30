"""Block device source for the per-second metrics sampler.

Reads /sys/block, so it describes the machine the sampler runs on and is
collected only when the server runs there. The device is the whole disk backing
a configured filesystem path, resolved through the path's st_dev, so no device
name is ever guessed. Every column except `disk_in_flight` is derived from
deltas of `/sys/block/<dev>/stat` over the gap since this source's last
successful read, and a zero denominator yields 0.0, which covers both the first
sample of a run and an interval in which the device completed no IO. When no
block device backs the path, the source emits no disk columns at all.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .base import SampleSource, read_text, run_cli

# /sys/block reports transfer sizes in 512-byte sectors regardless of the
# device's real logical block size.
_SECTOR_BYTES = 512
_BYTES_PER_MB = 1024 * 1024

_SYS_BLOCK_DIR = Path("/sys/block")
_SYS_DEV_BLOCK_DIR = Path("/sys/dev/block")

# /sys/block/<dev>/stat field positions, keyed by the name used internally.
# The kernel emits these eleven in this order for a whole disk, sectors in
# 512-byte units and all three tick fields in milliseconds.
_DISK_STAT_FIELDS = (
    ("read_ios", 0),
    ("read_merges", 1),
    ("read_sectors", 2),
    ("read_ticks", 3),
    ("write_ios", 4),
    ("write_merges", 5),
    ("write_sectors", 6),
    ("write_ticks", 7),
    ("in_flight", 8),
    ("io_ticks", 9),
    ("time_in_queue", 10),
)
_DISK_STAT_MIN_FIELDS = len(_DISK_STAT_FIELDS)


def resolve_block_device(path: str) -> Optional[str]:
    """Return the whole-disk device name backing path, or None.

    None means no block device backs the path, which is the case for tmpfs,
    overlay and network filesystems and on platforms without /sys.
    """
    try:
        st_dev = os.stat(path).st_dev
    except OSError:
        return None

    entry = _SYS_DEV_BLOCK_DIR / f"{os.major(st_dev)}:{os.minor(st_dev)}"
    device = Path(os.path.realpath(str(entry)))
    if not device.is_dir():
        return None
    if (device / "partition").exists():
        device = device.parent
    if not (device / "stat").exists():
        return None
    return device.name


def read_disk_counters(device: str) -> Optional[Dict[str, int]]:
    """Return the /sys/block/<device>/stat counters, or None when incomplete."""
    parts = read_text(str(_SYS_BLOCK_DIR / device / "stat")).split()
    if len(parts) < _DISK_STAT_MIN_FIELDS:
        return None
    try:
        return {name: int(parts[index]) for name, index in _DISK_STAT_FIELDS}
    except ValueError:
        return None


def rate(delta: int, interval: float) -> float:
    """Return delta per second, clamped at 0 so counter resets do not go negative."""
    if delta <= 0 or interval <= 0:
        return 0.0
    return round(delta / interval, 2)


def sector_rate_mb(sector_delta: int, interval: float) -> float:
    """Convert a 512-byte sector delta over interval seconds to MB/s."""
    if sector_delta <= 0 or interval <= 0:
        return 0.0
    return round(sector_delta * _SECTOR_BYTES / _BYTES_PER_MB / interval, 2)


def per_io_ms(tick_delta: int, io_delta: int) -> float:
    """Return mean milliseconds per IO, 0.0 when no IO completed."""
    if tick_delta <= 0 or io_delta <= 0:
        return 0.0
    return round(tick_delta / io_delta, 2)


def queue_length(queue_tick_delta: int, interval_ms: float) -> float:
    """Return mean queue depth: queued milliseconds over the interval."""
    if queue_tick_delta <= 0 or interval_ms <= 0:
        return 0.0
    return round(queue_tick_delta / interval_ms, 2)


def busy_percent(io_tick_delta: int, interval_ms: float) -> float:
    """Return device busy percent over the interval, capped at 100.0."""
    # io ticks is wall-clock busy time on a queue that can be served
    # concurrently, so the raw ratio can exceed 1 on an NVMe device.
    if io_tick_delta <= 0 or interval_ms <= 0:
        return 0.0
    return round(min(100.0, io_tick_delta / interval_ms * 100), 2)


def request_size_kb(sector_delta: int, io_delta: int) -> float:
    """Return mean IO size in KB, 0.0 when no IO completed."""
    if sector_delta <= 0 or io_delta <= 0:
        return 0.0
    return round(sector_delta * _SECTOR_BYTES / io_delta / 1024, 2)


class DiskSource(SampleSource):
    """Block device IOPS, throughput, latency and utilization columns."""

    name = "disk"
    local_only = True
    linux_only = True

    def __init__(self):
        """Initialize the counter baseline, empty until the second tick."""
        self.device: Optional[str] = None
        self._prev: Optional[Dict[str, int]] = None
        self._prev_time: Optional[float] = None

    def start(self, ctx) -> None:
        """Resolve the block device backing the configured path."""
        super().start(ctx)
        if ctx.block_device:
            self.device = ctx.block_device
            return

        path = ctx.disk_path or ctx.ext_storage_path or self._server_data_dir(ctx)
        self.device = resolve_block_device(path) if path else None
        if self.device is None:
            ctx.warn_once(
                "no_block_device",
                f"No block device backs {path!r}, disk columns are omitted",
            )
        else:
            logging.info(f"Sampling block device {self.device}, resolved from {path!r}")

    @staticmethod
    def _server_data_dir(ctx) -> Optional[str]:
        """Return the server's data directory from CONFIG GET dir, or None."""
        output = run_cli(ctx, "CONFIG", "GET", "dir")
        if output is None:
            return None
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        return lines[1] if len(lines) > 1 else None

    def sample(self, now: float) -> Dict[str, Any]:
        """Derive the block device columns from /sys/block stat deltas."""
        if not self.device:
            return {}

        metrics: Dict[str, Any] = {
            "disk_read_iops": 0.0,
            "disk_write_iops": 0.0,
            "disk_read_mb": 0.0,
            "disk_write_mb": 0.0,
            "disk_read_merges_ps": 0.0,
            "disk_write_merges_ps": 0.0,
            "disk_r_await_ms": 0.0,
            "disk_w_await_ms": 0.0,
            "disk_aqu_sz": 0.0,
            "disk_util_pct": 0.0,
            "disk_in_flight": 0,
            "disk_req_sz_kb": 0.0,
        }

        counters = read_disk_counters(self.device)
        if counters is None:
            self.ctx.warn_once(
                "no_disk_stat",
                f"Cannot read /sys/block/{self.device}/stat, disk metrics will be 0",
            )
            return metrics

        # A gauge rather than a counter, so it is read on every sample.
        metrics["disk_in_flight"] = counters["in_flight"]

        interval = None
        if self._prev_time is not None:
            elapsed = now - self._prev_time
            interval = elapsed if elapsed > 0 else None

        if self._prev is not None and interval:
            prev = self._prev
            delta = {name: counters[name] - prev[name] for name, _ in _DISK_STAT_FIELDS}
            interval_ms = interval * 1000
            total_ios = delta["read_ios"] + delta["write_ios"]
            total_sectors = delta["read_sectors"] + delta["write_sectors"]

            metrics["disk_read_iops"] = rate(delta["read_ios"], interval)
            metrics["disk_write_iops"] = rate(delta["write_ios"], interval)
            metrics["disk_read_mb"] = sector_rate_mb(delta["read_sectors"], interval)
            metrics["disk_write_mb"] = sector_rate_mb(delta["write_sectors"], interval)
            metrics["disk_read_merges_ps"] = rate(delta["read_merges"], interval)
            metrics["disk_write_merges_ps"] = rate(delta["write_merges"], interval)
            metrics["disk_r_await_ms"] = per_io_ms(
                delta["read_ticks"], delta["read_ios"]
            )
            metrics["disk_w_await_ms"] = per_io_ms(
                delta["write_ticks"], delta["write_ios"]
            )
            metrics["disk_aqu_sz"] = queue_length(delta["time_in_queue"], interval_ms)
            metrics["disk_util_pct"] = busy_percent(delta["io_ticks"], interval_ms)
            metrics["disk_req_sz_kb"] = request_size_kb(total_sectors, total_ios)
        self._prev = counters
        self._prev_time = now
        return metrics
