"""Per-second metrics sampler for Valkey data tiering benchmarks.

Samples the server and the host once per interval (1 Hz by default) for the
duration of a benchmark's measured phase, and emits one row per sample. The
rows form a time series that a per-commit deep-dive dashboard can chart, with
`elapsed_sec` as the x-axis so several commits overlay on one chart.

Design notes
------------
Sampling runs on a background daemon thread and never raises into the caller.
Each source is sampled inside its own guard, so one unreadable source leaves the
other columns intact and logs a warning once per distinct failure. Sources that
read /proc or /sys describe the machine the sampler runs on, so they are dropped
when the target is not a loopback address.

The thread can be pinned with `cpu_range`, which keeps the sampler and the
valkey-cli children it spawns off the cores under measurement.
"""

import ipaddress
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence

from samplers import DEFAULT_SOURCES, SOURCES, SamplerContext, SampleSource
from samplers.base import ASIO_THREAD_NAME
from utils.cpu_utils import parse_core_range

_JOIN_TIMEOUT_SEC = 5


def is_loopback_host(host: str) -> bool:
    """Return True when host names this machine's loopback interface.

    Accepts the literal name "localhost" in any case, and any address string
    that parses as a loopback IP. A hostname that is not an IP literal is not
    resolved, so it is treated as remote.
    """
    if str(host).strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class MetricsSampler:
    """Sample a set of sources on a background thread at a fixed rate.

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
        sources: Optional[Sequence[str]] = None,
        cpu_range: Optional[str] = None,
    ):
        """Initialize the sampler.

        Args:
            host: Valkey host to sample
            port: Valkey port to sample
            cli_path: valkey-cli executable used to issue commands
            server_pid: valkey-server pid, for per-process and per-thread CPU
            interval: seconds between samples
            enabled: when False, start/stop are no-ops and no rows are produced
            context: run-identity fields merged into every emitted row
            block_device: block device name to sample, auto-detected when None
            asio_thread_name: thread name isolated as the async IO worker
            sources: source names to sample, DEFAULT_SOURCES when None
            cpu_range: cores to pin the sampler thread to, unpinned when None
        """
        self.enabled = enabled
        self.host = host
        self.local_host = is_loopback_host(host)
        self.port = port
        self.cli_path = cli_path
        self.server_pid = server_pid
        self.interval = interval
        self.context = dict(context or {})
        self.block_device = block_device
        self.asio_thread_name = asio_thread_name
        self.source_names = tuple(sources) if sources else DEFAULT_SOURCES
        self.cpu_range = cpu_range

        self._sources: List[SampleSource] = []
        self._rows: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sampler_thread: Optional[threading.Thread] = None
        self._start_monotonic: Optional[float] = None
        self._prev_monotonic: Optional[float] = None
        self._warned: set = set()

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

        self._sources = self._build_sources()
        self._start_monotonic = time.monotonic()
        self._stop_event.clear()
        self._sampler_thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._sampler_thread.start()
        logging.info(
            f"Started metrics sampler ({self.interval}s interval, "
            f"pid={self.server_pid}, "
            f"sources={[source.name for source in self._sources]})"
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

    def _warn_once(self, key: str, message: str) -> None:
        """Log a warning the first time key is seen, to avoid 1 Hz log spam."""
        if key in self._warned:
            return
        self._warned.add(key)
        logging.warning(message)

    def _build_sources(self) -> List[SampleSource]:
        """Instantiate and start the selected sources, dropping those that fail."""
        selected = [SOURCES[name]() for name in self.source_names]

        if not self.local_host:
            host_only = [source.name for source in selected if source.local_only]
            if host_only:
                self._warn_once(
                    "remote_host",
                    f"Target {self.host} is not a loopback address, skipping "
                    f"{', '.join(host_only)}",
                )
            selected = [source for source in selected if not source.local_only]

        ctx = SamplerContext(
            host=self.host,
            port=self.port,
            cli_path=self.cli_path,
            server_pid=self.server_pid,
            block_device=self.block_device,
            asio_thread_name=self.asio_thread_name,
            warn_once=self._warn_once,
        )

        started: List[SampleSource] = []
        for source in selected:
            try:
                source.start(ctx)
                started.append(source)
            except Exception as e:
                self._warn_once(
                    f"start_failed_{source.name}",
                    f"Source {source.name} failed to start and is dropped: {e}",
                )
        return started

    def _pin_thread(self) -> None:
        """Pin the calling thread to `cpu_range`.

        pid 0 means the calling thread on Linux, and the valkey-cli children it
        spawns inherit the mask.
        """
        if self.cpu_range is None:
            return
        try:
            os.sched_setaffinity(0, set(parse_core_range(self.cpu_range)))
        except (ValueError, OSError) as e:
            self._warn_once(
                "pin_failed",
                f"Could not pin the sampler thread to {self.cpu_range}, "
                f"sampling unpinned: {e}",
            )

    def _sample_loop(self) -> None:
        """Sample until stopped, holding the configured interval between ticks."""
        self._pin_thread()
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
        for source in self._sources:
            try:
                row.update(source.sample(interval))
            except Exception as e:
                self._warn_once(
                    f"sample_failed_{source.name}",
                    f"Source {source.name} failed to sample: {e}",
                )
        # Context last so run identity always survives a name collision.
        row.update(self.context)

        self._prev_monotonic = now
        with self._lock:
            self._rows.append(row)
