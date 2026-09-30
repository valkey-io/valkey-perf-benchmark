"""Shared plumbing for the pluggable per-second sample sources.

A source owns one group of columns and is asked for them once per tick. The
sampler loop never inspects a source beyond its `name`, its `local_only` flag
and the dict it returns, so adding a column group means adding a source and
naming it in `samplers.SOURCES`.
"""

import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

# Thread name set by pthread_setname_np in the flash cache IO worker
# (valkey-data-tiering src/storage/storage_flashcache_real.c:162).
ASIO_THREAD_NAME = "fc_io_worker"

CLI_TIMEOUT_SEC = 5


def log_warning(key: str, message: str) -> None:
    """Log message as a warning. key is the caller's deduplication key."""
    logging.warning(message)


@dataclass
class SamplerContext:
    """Target identity and shared helpers handed to every source at start."""

    host: str = "127.0.0.1"
    port: int = 6379
    cli_path: str = "valkey-cli"
    server_pid: Optional[int] = None
    block_device: Optional[str] = None
    asio_thread_name: str = ASIO_THREAD_NAME
    warn_once: Callable[[str, str], None] = field(default=log_warning)


class SampleSource:
    """One group of columns, sampled once per tick."""

    name: str = ""
    local_only: bool = False

    def start(self, ctx: SamplerContext) -> None:
        """Store the context. A source that resolves a target extends this."""
        self.ctx = ctx

    def sample(self, interval: Optional[float]) -> Dict[str, Any]:
        """Return this source's columns for one tick.

        interval is the measured seconds since the previous tick, and None on
        the first tick of a run, where nothing has a predecessor to delta
        against.
        """
        raise NotImplementedError


def read_text(path: str, default: str = "") -> str:
    """Read a procfs/sysfs file, returning default if it is unreadable."""
    try:
        return Path(path).read_text()
    except OSError:
        return default


def to_int(value: Optional[str], default: int = 0) -> int:
    """Parse an INFO or /proc value as int, returning default on failure."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def to_float(value: Optional[str], default: float = 0.0) -> float:
    """Parse an INFO or /proc value as float, returning default on failure."""
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _command_label(args: Tuple[str, ...]) -> str:
    """Return the first non-option argument, used as the warn-once key stem."""
    for arg in args:
        if not arg.startswith("-"):
            return arg.lower()
    return "cli"


def run_cli(ctx: SamplerContext, *args: str) -> Optional[str]:
    """Return valkey-cli stdout for args, or None when the call fails.

    At 1 Hz the process spawn cost is negligible, and it keeps TLS and cluster
    invocation a matter of extra CLI args rather than client wiring.
    """
    label = _command_label(args)
    cmd = [ctx.cli_path, "-h", ctx.host, "-p", str(ctx.port), *args]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=CLI_TIMEOUT_SEC
        )
    except (OSError, subprocess.SubprocessError) as e:
        ctx.warn_once(f"{label}_failed", f"{label.upper()} command failed: {e}")
        return None

    if result.returncode != 0:
        ctx.warn_once(
            f"{label}_rc",
            f"{label.upper()} command exited {result.returncode}: "
            f"{result.stderr.strip()}",
        )
        return None
    return result.stdout
