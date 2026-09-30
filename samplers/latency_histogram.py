"""Per-interval command latency source for the per-second metrics sampler.

Runs `valkey-cli --json LATENCY HISTOGRAM` once per tick and emits the
percentiles of the calls served since the previous tick under the key `latency`.

Reply shape
-----------
`--json` sets the output mode to JSON and requests RESP3 implicitly, keeping an
explicit `-2` or `-3` (valkey src/valkey-cli.c:2637). Under RESP3 a map reply
renders as a JSON object with its keys coerced to strings
(`cliFormatReplyJson`, valkey src/valkey-cli.c:2094). Because the implicit
request tolerates a failed `HELLO 3` (valkey src/valkey-cli.c:1614), a
RESP2-only server answers with arrays instead, which render as flat
`[key, value, key, value]` JSON lists. Both forms are read.

`LATENCY HISTOGRAM` with no command names replies with a map of command name to
`{calls, histogram_usec}`, where `histogram_usec` maps a bucket's upper bound in
microseconds to the cumulative call count at that bound. A bucket is emitted
only when the cumulative count increased, so the map is sparse and its largest
bucket carries the command's total (`fillCommandCDF`, valkey src/latency.c:507).

Per-interval percentiles
------------------------
Both CDFs are step functions, so the previous cumulative count at a current
bucket b is the previous value at the largest previous bucket <= b, and 0 when
there is none. Subtracting that from the current cumulative count at b gives the
interval's own cumulative count at b, which is non-decreasing in b because the
current histogram contains every sample the previous one did. The interval total
is that value at the largest current bucket, and percentile p is the smallest
bucket whose interval cumulative count reaches p of the total.

A command is reported only when its `calls` increased since the previous tick.
A command that was not in the previous reply is measured against a zero
baseline, so its first appearance is reported in full. A command whose counter
went down after a server restart or a `CONFIG RESETSTAT` is omitted, and the
tick's own reply becomes its new baseline. A tick with no reply at all leaves no
baseline, so the tick after it is reported empty rather than as one large
interval. The sampler's own commands are dropped so the series describes the
benchmark load rather than the measurement of it.
"""

import json
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .base import SampleSource, run_cli

# Commands issued by the sampler itself: its INFO call, and the HELLO and
# CONFIG GET handshake every valkey-cli invocation sends. "latency" also covers
# the subcommand fullname "latency|histogram", which is what the server reports.
_EXCLUDED_COMMANDS = frozenset({"info", "hello", "config|get"})
_EXCLUDED_PREFIX = "latency"


@dataclass
class CommandHistogram:
    """One command's total calls and its sparse cumulative latency buckets."""

    calls: int = 0
    buckets: Dict[int, int] = field(default_factory=dict)


def _as_mapping(value: Any) -> Optional[Dict[Any, Any]]:
    """Return value as a mapping, pairing up a flat [k, v, k, v] list."""
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and len(value) % 2 == 0:
        return dict(zip(value[::2], value[1::2]))
    return None


def parse_histogram(text: Optional[str]) -> Dict[str, CommandHistogram]:
    """Return one CommandHistogram per command in a LATENCY HISTOGRAM reply."""
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except ValueError:
        return {}

    commands = _as_mapping(payload)
    if commands is None:
        return {}

    parsed: Dict[str, CommandHistogram] = {}
    for name, body in commands.items():
        entry = _as_mapping(body)
        if entry is None:
            continue
        buckets = _as_mapping(entry.get("histogram_usec"))
        if buckets is None:
            continue
        parsed[str(name)] = CommandHistogram(
            calls=int(entry.get("calls", 0)),
            buckets={int(bucket): int(count) for bucket, count in buckets.items()},
        )
    return parsed


def _previous_cumulative(
    previous: Dict[int, int], previous_buckets: List[int], bucket: int
) -> int:
    """Return the previous CDF's cumulative count at bucket."""
    index = bisect_right(previous_buckets, bucket)
    if index == 0:
        return 0
    return previous[previous_buckets[index - 1]]


def _percentile(deltas: List[Tuple[int, int]], total: int, fraction: float) -> int:
    """Return the smallest bucket whose interval count reaches fraction of total."""
    threshold = fraction * total
    for bucket, cumulative in deltas[:-1]:
        if cumulative >= threshold:
            return bucket
    return deltas[-1][0]


def interval_percentiles(
    previous: Dict[int, int], current: Dict[int, int]
) -> Dict[str, int]:
    """Return p50, p99 and p999 microseconds of the calls added since previous."""
    previous_buckets = sorted(previous)
    deltas = [
        (
            bucket,
            current[bucket] - _previous_cumulative(previous, previous_buckets, bucket),
        )
        for bucket in sorted(current)
    ]
    total = deltas[-1][1]
    return {
        "p50_usec": _percentile(deltas, total, 0.5),
        "p99_usec": _percentile(deltas, total, 0.99),
        "p999_usec": _percentile(deltas, total, 0.999),
    }


class LatencyHistogramSource(SampleSource):
    """Per-interval command latency percentiles under the key `latency`."""

    name = "latency_histogram"

    def __init__(self):
        """Initialize the baseline, which is empty until the second tick."""
        self._previous: Dict[str, CommandHistogram] = {}

    def sample(self, interval: Optional[float]) -> Dict[str, Any]:
        """Return the per-command percentiles of this interval's calls."""
        current = parse_histogram(run_cli(self.ctx, "--json", "LATENCY", "HISTOGRAM"))
        latency = self._interval_latency(current) if self._previous else {}
        self._previous = current
        return {"latency": latency}

    def _interval_latency(
        self, current: Dict[str, CommandHistogram]
    ) -> Dict[str, Dict[str, int]]:
        """Return the percentiles of every command whose calls increased."""
        latency: Dict[str, Dict[str, int]] = {}
        for name, histogram in current.items():
            if name in _EXCLUDED_COMMANDS or name.startswith(_EXCLUDED_PREFIX):
                continue
            previous = self._previous.get(name, CommandHistogram())
            calls = histogram.calls - previous.calls
            if calls <= 0:
                continue
            latency[name] = {
                "calls": calls,
                **interval_percentiles(previous.buckets, histogram.buckets),
            }
        return latency
