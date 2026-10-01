"""Valkey INFO source for the per-second metrics sampler.

Reads `INFO ALL` once per tick and emits the derived columns, the external
storage counters under their own INFO field names, and the whole reply as a
nested `info` dict. A missing field becomes 0, so a server without tiering
samples cleanly with its tiering columns at 0. `valkey_cpu_user`,
`valkey_cpu_sys` and `valkey_cpu_total` come from the main thread CPU seconds
INFO reports, and are omitted when the server does not report them.
"""

import re
from typing import Any, Dict, Optional, Tuple

from .base import SampleSource, run_cli, to_float, to_int

# Tiering INFO field names, emitted verbatim as column names.
TIERING_INFO_FIELDS = (
    "ext_storage_enabled",
    "ext_storage_capacity_bytes",
    "ext_storage_total_num_items",
    "ext_storage_total_num_bytes",
    "ext_storage_total_num_items_spilled_to_storage",
    "ext_storage_total_num_items_fetched_from_storage",
    "ext_storage_total_num_items_deleted_from_storage",
)

_INT_VALUE = re.compile(r"^-?\d+$")
_FLOAT_VALUE = re.compile(r"^-?\d+\.\d+$")


def parse_info(text: str) -> Dict[str, str]:
    """Parse INFO output into a flat field to value dict, skipping headers.

    Sections are flattened because field names are unique across INFO sections.
    """
    fields: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip()
    return fields


def _coerce(value: str) -> Any:
    """Return value as an int or a float when it is wholly numeric, else as is."""
    if _INT_VALUE.match(value):
        return int(value)
    if _FLOAT_VALUE.match(value):
        return float(value)
    return value


def _is_pair_list(value: str) -> bool:
    """Return True when every comma-separated part of value holds a "=" pair."""
    return all("=" in part for part in value.split(","))


def info_snapshot(fields: Dict[str, str]) -> Dict[str, Any]:
    """Return every INFO field with numeric values and k=v groups converted."""
    snapshot: Dict[str, Any] = {}
    for key, value in fields.items():
        if _is_pair_list(value):
            nested: Dict[str, Any] = {}
            for part in value.split(","):
                sub_key, _, sub_value = part.partition("=")
                nested[sub_key] = _coerce(sub_value)
            snapshot[key] = nested
        else:
            snapshot[key] = _coerce(value)
    return snapshot


class ValkeyInfoSource(SampleSource):
    """Memory, throughput, keyspace, tiering and CPU columns."""

    name = "valkey_info"

    def __init__(self):
        """Initialize the delta state, which is empty until the second tick."""
        self._last_success_time: Optional[float] = None
        self._prev_total_commands: Optional[int] = None
        self._prev_main_thread_cpu: Optional[Tuple[float, float]] = None
        self._main_thread_cpu_available = False

    def sample(self, now: float) -> Dict[str, Any]:
        """Return the INFO-derived columns plus the full snapshot."""
        info = self.read_info()
        metrics = self._info_metrics(info, now)
        metrics["info"] = info_snapshot(info)
        return metrics

    def read_info(self) -> Dict[str, str]:
        """Return parsed INFO ALL fields, or an empty dict when unavailable."""
        output = run_cli(self.ctx, "INFO", "ALL")
        if output is None:
            return {}
        return parse_info(output)

    def _interval(self, now: float) -> Optional[float]:
        """Return seconds since this source's last successful read."""
        if self._last_success_time is None:
            return None
        elapsed = now - self._last_success_time
        return elapsed if elapsed > 0 else None

    def _main_thread_cpu(
        self, info: Dict[str, str], interval: Optional[float]
    ) -> Dict[str, Any]:
        """Return the main thread CPU percentages, empty when INFO omits them.

        The server reports cumulative main thread seconds, which excludes the
        IO worker threads reported separately as `asio_cpu_pct`.
        """
        user_seconds = info.get("used_cpu_user_main_thread")
        sys_seconds = info.get("used_cpu_sys_main_thread")
        if user_seconds is not None and sys_seconds is not None:
            self._main_thread_cpu_available = True
            self.ctx.main_thread_cpu_from_info = True
        if not self._main_thread_cpu_available:
            return {}

        user_pct = 0.0
        sys_pct = 0.0
        current = None
        if user_seconds is not None and sys_seconds is not None:
            current = (to_float(user_seconds), to_float(sys_seconds))
            if self._prev_main_thread_cpu is not None and interval:
                user_pct = round(
                    max(0.0, current[0] - self._prev_main_thread_cpu[0])
                    / interval
                    * 100,
                    2,
                )
                sys_pct = round(
                    max(0.0, current[1] - self._prev_main_thread_cpu[1])
                    / interval
                    * 100,
                    2,
                )
            self._prev_main_thread_cpu = current

        return {
            "valkey_cpu_user": user_pct,
            "valkey_cpu_sys": sys_pct,
            "valkey_cpu_total": round(user_pct + sys_pct, 2),
        }

    def _info_metrics(self, info: Dict[str, str], now: float) -> Dict[str, Any]:
        """Derive the INFO-sourced columns, including deltas."""
        if not info:
            self.ctx.warn_once("info_empty", "INFO returned no fields, emitting zeros")

        interval = self._interval(now)
        total_commands = to_int(info.get("total_commands_processed"))
        commands_delta = 0
        ops_per_sec = 0.0
        if info and self._prev_total_commands is not None and interval:
            commands_delta = max(0, total_commands - self._prev_total_commands)
            ops_per_sec = round(commands_delta / interval, 2)

        tiering = {field: to_int(info.get(field)) for field in TIERING_INFO_FIELDS}

        metrics: Dict[str, Any] = {
            "used_memory": to_int(info.get("used_memory")),
            "used_memory_rss": to_int(info.get("used_memory_rss")),
            "maxmemory": to_int(info.get("maxmemory")),
            "mem_frag_ratio": to_float(info.get("mem_fragmentation_ratio")),
            "ops_per_sec": ops_per_sec,
            "total_commands_delta": commands_delta,
            "keyspace_hits": to_int(info.get("keyspace_hits")),
            "keyspace_misses": to_int(info.get("keyspace_misses")),
            "blocked_clients": to_int(info.get("blocked_clients")),
        }
        metrics.update(tiering)
        metrics.update(self._main_thread_cpu(info, interval))

        if info:
            self._prev_total_commands = total_commands
            self._last_success_time = now
        return metrics
