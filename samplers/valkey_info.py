"""Valkey INFO source for the per-second metrics sampler.

Reads `INFO ALL` once per tick and emits the derived columns, the tiering
counters under their own INFO field names, and the whole reply as a nested
`info` dict. A missing field becomes 0, so a server without tiering samples
cleanly with its tiering columns at 0. The two throttle rate fields are floats,
and `spill_submitted_count` is emitted rather than the distinct counter
`spill_submitted`, because it is the one that pairs with
`spill_serialized_count`.
"""

import re
from typing import Any, Dict, Optional, Tuple

from .base import SampleSource, run_cli, to_float, to_int

# Tiering INFO field names, emitted verbatim as column names.
TIERING_INFO_FIELDS = (
    "total_num_items_spilled_to_ext_storage",
    "total_num_items_fetched_from_ext_storage",
    "num_items_spilling_to_ext_storage",
    "kbc_fetching_block",
    "completion_read_ok",
    "dram_value_hits",
    "throttle_total_throttled",
    "throttle_queued_clients",
    "spill_attempts",
    "spill_submitted_count",
    "spill_serialized_count",
    "mean_spill_ram",
    "inflight_spill_ram_bytes",
    "oom_reject_write_count",
)

# Tiering INFO fields the engine formats as floating point rather than as
# integer counters (src/ext_storage.c:1590-1591).
TIERING_INFO_FLOAT_FIELDS = (
    "throttle_current_rate",
    "throttle_allowed_tps",
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
    """Memory, throughput, keyspace, tiering and hit ratio columns from INFO."""

    name = "valkey_info"

    def __init__(self):
        """Initialize the delta state, which is empty until the second tick."""
        self._prev_total_commands: Optional[int] = None
        self._prev_dram_value_hits: Optional[int] = None
        self._prev_completion_read_ok: Optional[int] = None

    def sample(self, interval: Optional[float]) -> Dict[str, Any]:
        """Return the INFO-derived columns plus the full snapshot."""
        info = self.read_info()
        metrics = self._info_metrics(info, interval)
        metrics["info"] = info_snapshot(info)
        return metrics

    def read_info(self) -> Dict[str, str]:
        """Return parsed INFO ALL fields, or an empty dict when unavailable."""
        output = run_cli(self.ctx, "INFO", "ALL")
        if output is None:
            return {}
        return parse_info(output)

    def _info_metrics(
        self, info: Dict[str, str], interval: Optional[float]
    ) -> Dict[str, Any]:
        """Derive the INFO-sourced columns, including deltas and hit ratios."""
        if not info:
            self.ctx.warn_once("info_empty", "INFO returned no fields, emitting zeros")

        total_commands = to_int(info.get("total_commands_processed"))
        commands_delta = 0
        ops_per_sec = 0.0
        if self._prev_total_commands is not None and interval:
            commands_delta = max(0, total_commands - self._prev_total_commands)
            ops_per_sec = round(commands_delta / interval, 2)
        self._prev_total_commands = total_commands

        # The throttle rate fields are float-formatted by the engine, so they
        # are parsed as float and the rest as int.
        tiering = {field: to_int(info.get(field)) for field in TIERING_INFO_FIELDS}
        tiering.update(
            {field: to_float(info.get(field)) for field in TIERING_INFO_FLOAT_FIELDS}
        )
        dram_value_hits = tiering["dram_value_hits"]
        completion_read_ok = tiering["completion_read_ok"]

        # Cumulative hit ratios, from the running totals.
        disk_hit_pct, mem_hit_pct = hit_ratios(dram_value_hits, completion_read_ok)

        # Per-interval hit ratios, from the deltas between consecutive samples.
        dram_delta = 0
        completion_delta = 0
        if self._prev_dram_value_hits is not None:
            dram_delta = max(0, dram_value_hits - self._prev_dram_value_hits)
        if self._prev_completion_read_ok is not None:
            completion_delta = max(
                0, completion_read_ok - self._prev_completion_read_ok
            )
        disk_hit_pct_interval, mem_hit_pct_interval = hit_ratios(
            dram_delta, completion_delta
        )
        self._prev_dram_value_hits = dram_value_hits
        self._prev_completion_read_ok = completion_read_ok

        metrics: Dict[str, Any] = {
            "used_memory": to_int(info.get("used_memory")),
            "used_memory_rss": to_int(info.get("used_memory_rss")),
            "maxmemory": to_int(info.get("maxmemory")),
            "mem_frag_ratio": to_float(info.get("mem_fragmentation_ratio")),
            "ops_per_sec": ops_per_sec,
            "total_commands_delta": commands_delta,
            "keyspace_hits": to_int(info.get("keyspace_hits")),
            "keyspace_misses": to_int(info.get("keyspace_misses")),
            "mem_hit_pct": mem_hit_pct,
            "disk_hit_pct": disk_hit_pct,
            "mem_hit_pct_interval": mem_hit_pct_interval,
            "disk_hit_pct_interval": disk_hit_pct_interval,
            "blocked_clients": to_int(info.get("blocked_clients")),
        }
        metrics.update(tiering)
        return metrics


def hit_ratios(dram_value_hits: int, completion_read_ok: int) -> Tuple[float, float]:
    """Return (disk_hit_pct, mem_hit_pct) for one pair of counter values.

    Takes either the running totals or the deltas between consecutive samples.
    A zero denominator yields (0.0, 0.0).
    """
    if dram_value_hits <= 0:
        return 0.0, 0.0
    disk_hit_pct = round(completion_read_ok / dram_value_hits * 100, 2)
    mem_hit_pct = round(
        (dram_value_hits - completion_read_ok) / dram_value_hits * 100, 2
    )
    return disk_hit_pct, mem_hit_pct
