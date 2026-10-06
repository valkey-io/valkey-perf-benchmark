"""Block device source for the per-second metrics sampler.

Reads /sys/block, so it describes the machine the sampler runs on and is
collected only when the server runs there. The device is the whole disk backing
a filesystem path, resolved through the path's st_dev, so no device name is
ever guessed. The path is the `path` option, else the server's data directory
from `CONFIG GET dir`. Each tick records the device name and the raw
`/sys/block/<dev>/stat` counters.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .base import SampleSource, run_cli

_SYS_BLOCK_DIR = Path("/sys/block")
_SYS_DEV_BLOCK_DIR = Path("/sys/dev/block")


def resolve_block_device(path: str) -> Optional[str]:
    """Return the whole-disk device name backing path, or None.

    None means no block device backs the path, which is the case for tmpfs,
    overlay and network filesystems.
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
    return device.name


class DiskSource(SampleSource):
    """Raw /sys/block/<dev>/stat counters of the disk backing a path."""

    name = "disk"
    local_only = True
    linux_only = True
    option_names = ("path",)

    @classmethod
    def validate_options(cls, options: Any) -> None:
        """Also require `path`, when given, to be a non-empty string."""
        super().validate_options(options)
        path = options.get("path")
        if "path" in options and (not isinstance(path, str) or not path.strip()):
            raise ValueError(
                "'per_second_sampling.sources.disk.path' must be a non-empty string"
            )

    def start(self, ctx) -> None:
        """Resolve the block device, raising when none can be found."""
        super().start(ctx)
        path = self.options.get("path") or self._server_data_dir(ctx)
        if path is None:
            raise RuntimeError("CONFIG GET dir failed, no path to resolve a disk from")
        self.device = resolve_block_device(path)
        if self.device is None:
            raise RuntimeError(f"No block device backs {path!r}")
        logging.info(f"Sampling block device {self.device}, resolved from {path!r}")

    @staticmethod
    def _server_data_dir(ctx) -> Optional[str]:
        """Return the server's data directory from CONFIG GET dir, or None."""
        output = run_cli(ctx, "CONFIG", "GET", "dir")
        if output is None:
            return None
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        return lines[1] if len(lines) > 1 else None

    def sample(self) -> Dict[str, Any]:
        """Return the device name and its raw stat counters."""
        stat = (_SYS_BLOCK_DIR / self.device / "stat").read_text()
        return {"device": self.device, "stat": [int(value) for value in stat.split()]}
