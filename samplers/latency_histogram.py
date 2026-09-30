"""Per-interval command latency source for the per-second metrics sampler.

Runs `valkey-cli --json LATENCY HISTOGRAM` once per tick and emits the
percentiles of the calls served since the previous tick under the key
`latency`. Both replies are cumulative sparse CDFs, so an interval count at a
bucket is the current count there minus the previous count at the largest
previous bucket at or below it, and percentile p is the smallest bucket whose
interval count reaches p of the interval total. The commands the sampler issues
itself are excluded.
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


def parse_histogram(text: Optional[str]) -> Dict[str, CommandHistogram]:
    """Return one CommandHistogram per command in a LATENCY HISTOGRAM reply."""
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except ValueError:
        return {}

    if not isinstance(payload, dict):
        return {}

    parsed: Dict[str, CommandHistogram] = {}
    for name, body in payload.items():
        if not isinstance(body, dict):
            continue
        buckets = body.get("histogram_usec")
        if not isinstance(buckets, dict):
            continue
        parsed[str(name)] = CommandHistogram(
            calls=int(body.get("calls", 0)),
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
