"""Valkey INFO source for the per-second metrics sampler.

Reads `INFO ALL` through valkey-cli once per tick and emits the derived
columns, the tiering counters under their own INFO field names, and the whole
reply as a nested `info` dict. A missing or unparsable field becomes 0, so a
server built without data tiering, or with tiering disabled, still samples
cleanly: `genExternalStorageInfoString` returns early when `ext_data_enabled`
is false (valkey-data-tiering src/ext_storage.c:1549), so the whole External
Storage section is simply absent and the tiering columns are 0.

Tiering INFO fields
-------------------
Tiering INFO field names are emitted verbatim as column names, so the table
below is a source-location reference rather than a name mapping: each field
plus where it is registered in the valkey-data-tiering source. Every line
number is a `sdscatprintf` format string in `genExternalStorageInfoString`.

| INFO field                               | Source                 | Type  |
| ---------------------------------------- | ---------------------- | ----- |
| total_num_items_spilled_to_ext_storage   | src/ext_storage.c:1551 | int   |
| total_num_items_fetched_from_ext_storage | src/ext_storage.c:1552 | int   |
| num_items_spilling_to_ext_storage        | src/ext_storage.c:1554 | int   |
| kbc_fetching_block                       | src/ext_storage.c:1560 | int   |
| completion_read_ok                       | src/ext_storage.c:1572 | int   |
| oom_reject_write_count                   | src/ext_storage.c:1579 | int   |
| spill_attempts                           | src/ext_storage.c:1583 | int   |
| dram_value_hits                          | src/ext_storage.c:1587 | int   |
| throttle_total_throttled                 | src/ext_storage.c:1588 | int   |
| throttle_queued_clients                  | src/ext_storage.c:1589 | int   |
| throttle_current_rate                    | src/ext_storage.c:1590 | float |
| throttle_allowed_tps                     | src/ext_storage.c:1591 | float |
| spill_submitted_count                    | src/ext_storage.c:1635 | int   |
| spill_serialized_count                   | src/ext_storage.c:1636 | int   |
| mean_spill_ram                           | src/ext_storage.c:1637 | int   |
| inflight_spill_ram_bytes                 | src/ext_storage.c:1638 | int   |

All of them live in the `external_storage` INFO section (valkey-data-tiering
src/server.c:6803).

Two of the resolutions are worth stating outright.

`throttle_current_rate` is formatted `%.4f` and `throttle_allowed_tps` `%.1f`
(src/ext_storage.c:1590-1591), so both are parsed as float. The other two
throttle fields are `%lld` counters and are parsed as int.

`spill_submitted_count` (src/ext_storage.c:1635) and `spill_submitted`
(src/ext_storage.c:1584) are both emitted and are two different counters.
`spill_submitted` is a plain `long long` incremented by the V1 spilling loop
when a spill reaches the IO thread (src/ext_storage.c:149). `spill_submitted_count`
is an atomic incremented on the main thread at submit (src/ext_storage.c:42) and
is one of the two inputs to the spill dead-time predictor, whose submit depth is
`spill_submitted_count - spill_serialized_count` (src/ext_storage.c:82-83). This
source emits `spill_submitted_count`, which is both the name the reference
dashboard CSV uses and the counter that pairs with `spill_serialized_count`,
`mean_spill_ram` and `inflight_spill_ram_bytes` to describe one pipeline.

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

Column names match the reference dashboard CSV
(valkey-data-tiering/benchmark_dashboard/data/zipfian-80-20.csv) wherever this
source covers the same metric, so the existing chart definitions port over.

Deltas
------
`ops_per_sec` and `total_commands_delta` are both derived from
`total_commands_processed` between consecutive samples, not from
`instantaneous_ops_per_sec`. `total_commands_delta` is the raw command count in
the interval, and `ops_per_sec` is that count divided by the measured interval.
The first sample of a run has no predecessor, so every delta-derived field is 0
there.

Full snapshot
-------------
`info` carries the whole reply so a column that is not derived here is still
recoverable from a stored row. Keys are verbatim, a wholly numeric value becomes
an int or a float, and a `k=v,k=v` value (a Commandstats or Keyspace line)
becomes a nested dict with the same numeric conversion per part.
"""

import re
from typing import Any, Dict, Optional, Tuple

from .base import SampleSource, run_cli, to_float, to_int

# Tiering INFO field names, emitted verbatim as column names. See the module
# docstring for the source locations these were resolved from.
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
    """Return every INFO field with numeric values and k=v groups converted.

    A duplicated sub-key inside one k=v value keeps the last occurrence.
    """
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

        # Cumulative hit ratios, from the running totals. A tiering-disabled
        # server reports no dram_value_hits at all, which lands here as 0 and
        # yields 0.0 ratios.
        disk_hit_pct, mem_hit_pct = hit_ratios(dram_value_hits, completion_read_ok)

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
