"""Per-second metrics sampler for Valkey benchmarks.

Records one row per tick of a benchmark's measured phase: the run identity,
`sample_time`, `elapsed_sec` (the x-axis, so several commits overlay on one
chart) and one key per source holding that source's raw reading. A source that
fails a tick is absent from that row. Sampling runs on a background daemon
thread and never raises into the caller. Sources that read /proc or /sys are
dropped when the server does not run on this machine, and on platforms without
/proc and /sys.
"""

import ipaddress
import json
import logging
import math
import os
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import valkey

from samplers import DEFAULT_SOURCES, SOURCES, SamplerContext, SampleSource
from utils.cpu_utils import parse_core_range

TICK_FIELDS = ("sample_time", "elapsed_sec")

# Bounds every server read, so a tick can never hang and stop() can join.
SOCKET_TIMEOUT_SEC = 2


def create_client(host: str, port: int, tls_kwargs: Dict[str, Any]) -> valkey.Valkey:
    """Return a client whose INFO replies are the raw text the server sent."""
    client = valkey.Valkey(
        host=host,
        port=port,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_SEC,
        socket_connect_timeout=SOCKET_TIMEOUT_SEC,
        **tls_kwargs,
    )
    client.set_response_callback("INFO", lambda response, **options: response)
    return client


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Append rows to path as JSON Lines, one compact object per line."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _can_bind(family: int, address: str) -> bool:
    """Return True when address belongs to an interface on this machine."""
    sock = socket.socket(family, socket.SOCK_DGRAM)
    try:
        sock.bind((address, 0))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def is_local_address(host: str) -> bool:
    """Return True when host names an interface on this machine.

    A private IP that this machine answers on is local, which is what a
    self-hosted runner passes as its own target.
    """
    if str(host).strip().lower() == "localhost":
        return True
    try:
        resolved = socket.getaddrinfo(host, None)
    except (socket.gaierror, OSError):
        return False

    for family, _, _, _, sockaddr in resolved:
        address = sockaddr[0]
        try:
            if ipaddress.ip_address(address).is_loopback:
                return True
        except ValueError:
            continue
        if _can_bind(family, address):
            return True
    return False


class MetricsSampler:
    """Sample a set of sources on a background thread at a fixed rate."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        tls_kwargs: Optional[Dict[str, Any]] = None,
        interval: float = 1.0,
        context: Optional[Dict[str, Any]] = None,
        sources: Optional[Dict[str, Dict[str, Any]]] = None,
        cpu_range: Optional[str] = None,
        server_local: Optional[bool] = None,
        start_delay: float = 0.0,
    ):
        """Initialize the sampler.

        Args:
            host: Valkey host to sample
            port: Valkey port to sample
            tls_kwargs: valkey-py TLS arguments for the sampler's client
            interval: seconds between samples
            context: run identity fields leading every emitted row
            sources: source name to options, every DEFAULT_SOURCES entry with
                no options when None
            cpu_range: cores to pin the sampler thread to, unpinned when None
            server_local: whether the server runs on this machine, resolved
                from host when None
            start_delay: seconds to wait before the first sample, so an inline
                benchmark warmup is not sampled
        """
        self.host = host
        self.server_local = (
            server_local if server_local is not None else is_local_address(host)
        )
        self.port = port
        self.tls_kwargs = dict(tls_kwargs or {})
        self.interval = interval
        self.context = dict(context or {})
        self.source_options = (
            dict(sources) if sources else {name: {} for name in DEFAULT_SOURCES}
        )
        collisions = set(self.context) & {*TICK_FIELDS, *SOURCES}
        if collisions:
            raise ValueError(
                f"Row identity field(s) {sorted(collisions)} collide with sample keys"
            )
        self.cpu_range = cpu_range
        self.start_delay = start_delay

        self._client: Optional[valkey.Valkey] = None
        self._sources: List[SampleSource] = []
        self._rows: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sampler_thread: Optional[threading.Thread] = None
        self._warned: set = set()

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Return a copy of the rows collected so far."""
        with self._lock:
            return list(self._rows)

    def start(self) -> None:
        """Start sampling on a background thread."""
        self._client = create_client(self.host, self.port, self.tls_kwargs)
        self._sources = self._build_sources()
        self._stop_event.clear()
        self._sampler_thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._sampler_thread.start()
        logging.info(
            f"Started metrics sampler ({self.interval}s interval, "
            f"start delay {self.start_delay}s, "
            f"sources={[source.name for source in self._sources]})"
        )

    def stop(self) -> None:
        """Stop sampling, join the background thread and close the client."""
        self._stop_event.set()
        if self._sampler_thread is not None:
            self._sampler_thread.join()
            self._sampler_thread = None
        if self._client is not None:
            self._client.close()
            self._client = None

        logging.info(f"Stopped metrics sampler after {len(self._rows)} samples")

    def _warn_once(self, key: str, message: str) -> None:
        """Log a warning the first time key is seen, to avoid 1 Hz log spam."""
        if key in self._warned:
            return
        self._warned.add(key)
        logging.warning(message)

    def _build_sources(self) -> List[SampleSource]:
        """Instantiate and start the selected sources, dropping those that fail."""
        selected = [
            SOURCES[name](options) for name, options in self.source_options.items()
        ]

        if not sys.platform.startswith("linux"):
            platform_only = [source.name for source in selected if source.linux_only]
            if platform_only:
                self._warn_once(
                    "non_linux",
                    f"{sys.platform} has no /proc or /sys, skipping "
                    f"{', '.join(platform_only)}",
                )
            selected = [source for source in selected if not source.linux_only]

        if not self.server_local:
            host_only = [source.name for source in selected if source.local_only]
            if host_only:
                self._warn_once(
                    "remote_host",
                    f"Target {self.host} is not an address of this machine, "
                    f"skipping {', '.join(host_only)}",
                )
            selected = [source for source in selected if not source.local_only]

        ctx = SamplerContext(
            client=self._client,
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
        """Pin the sampler thread to cores."""
        if self.cpu_range is None:
            return
        if not hasattr(os, "sched_setaffinity"):
            self._warn_once(
                "pin_unsupported",
                "CPU pinning is not supported on this platform, sampling unpinned",
            )
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
        """Sample until stopped, one tick at each multiple of the interval.

        Tick k is due at the first tick's time plus k intervals. A tick that
        overruns skips the slots it missed, so ticks are never back to back.
        """
        self._pin_thread()
        if self._stop_event.wait(self.start_delay):
            return
        start = time.monotonic()
        tick = 0
        while not self._stop_event.is_set():
            try:
                self._sample_once(int(round(tick * self.interval)))
            except Exception as e:
                self._warn_once("sample_failed", f"Metrics sample failed: {e}")
            now = time.monotonic()
            tick = max(tick + 1, math.floor((now - start) / self.interval) + 1)
            self._stop_event.wait(max(0.0, start + tick * self.interval - now))

    def _sample_once(self, elapsed_sec: int) -> None:
        """Collect one row and append it to the series."""
        row: Dict[str, Any] = {
            **self.context,
            "sample_time": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "elapsed_sec": elapsed_sec,
        }
        for source in self._sources:
            try:
                reading = source.sample()
            except Exception as e:
                reading = None
                self._warn_once(
                    f"sample_failed_{source.name}",
                    f"Source {source.name} failed to sample: {e}",
                )
            if reading is not None:
                row[source.name] = reading

        with self._lock:
            self._rows.append(row)
