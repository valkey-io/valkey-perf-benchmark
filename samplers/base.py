"""Shared plumbing for the pluggable per-second sample sources.

A source reads one thing once per tick and returns what it read as a dict,
which the sampler stores in the row under the source's `name`. Each source
declares and validates its own options, so adding a source means adding a
class here and naming it in `samplers.SOURCES`.
"""

import logging
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple

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
    warn_once: Callable[[str, str], None] = field(default=log_warning)


class SampleSource:
    """One raw reading, taken once per tick."""

    name: str = ""
    local_only: bool = False
    linux_only: bool = False
    option_names: Tuple[str, ...] = ()

    def __init__(self, options: Optional[Dict[str, Any]] = None):
        """Store this source's options from the config."""
        self.options = dict(options or {})

    @classmethod
    def validate_options(cls, options: Any) -> None:
        """Raise ValueError when options is not an object of known keys."""
        label = f"'per_second_sampling.sources.{cls.name}'"
        if not isinstance(options, dict):
            raise ValueError(f"{label} must be an object")
        unknown = sorted(set(options) - set(cls.option_names))
        if unknown:
            raise ValueError(f"{label} does not support key(s): {unknown}")

    def start(self, ctx: SamplerContext) -> None:
        """Store the context. A source that resolves a target extends this."""
        self.ctx = ctx

    def sample(self) -> Optional[Dict[str, Any]]:
        """Return this tick's reading, or None when it could not be read."""
        raise NotImplementedError


def _command_label(args: Tuple[str, ...]) -> str:
    """Return the first non-option argument, used as the warn-once key stem."""
    for arg in args:
        if not arg.startswith("-"):
            return arg.lower()
    return "cli"


def run_cli(ctx: SamplerContext, *args: str) -> Optional[str]:
    """Return valkey-cli stdout for args, or None when the call fails."""
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
