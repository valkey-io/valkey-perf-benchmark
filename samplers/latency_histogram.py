"""Command latency histogram source for the per-second metrics sampler.

Sends `LATENCY HISTOGRAM` once per tick and records the reply as reported: per
command, its cumulative `calls` and `histogram_usec`, the cumulative count of
calls whose server-side execution time fell in each power-of-two microsecond
bucket. Interval percentiles are left to ingest, which can diff consecutive
replies. Commands the sampler issues itself appear in the reply like any other.
"""

from typing import Any, Dict, List, Tuple

from .base import SampleSource


def _pairs(flat: List[Any]) -> List[Tuple[Any, Any]]:
    """Return the (key, value) pairs of a flat RESP key value array."""
    return list(zip(flat[::2], flat[1::2]))


def parse_latency_histogram(reply: List[Any]) -> Dict[str, Any]:
    """Return {command: {calls, histogram_usec: {bucket: count}}} from the reply."""
    histogram: Dict[str, Any] = {}
    for command, details in _pairs(reply):
        fields = dict(_pairs(details))
        histogram[command] = {
            "calls": fields["calls"],
            "histogram_usec": {
                str(bucket): count for bucket, count in _pairs(fields["histogram_usec"])
            },
        }
    return histogram


class LatencyHistogramSource(SampleSource):
    """The raw cumulative LATENCY HISTOGRAM reply."""

    name = "latency_histogram"

    def sample(self) -> Dict[str, Any]:
        """Return the reply as {command: {calls, histogram_usec}}."""
        return parse_latency_histogram(
            self.ctx.client.execute_command("LATENCY HISTOGRAM")
        )
