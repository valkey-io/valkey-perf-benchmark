"""Command latency histogram source for the per-second metrics sampler.

Runs `valkey-cli --json LATENCY HISTOGRAM` once per tick and records the reply
as reported: per command, its cumulative `calls` and `histogram_usec`, the
cumulative count of calls whose server-side execution time fell in each
power-of-two microsecond bucket. Interval percentiles are left to ingest, which
can diff consecutive replies. Commands the sampler issues itself appear in the
reply like any other.
"""

import json
from typing import Any, Dict, Optional

from .base import SampleSource, run_cli


class LatencyHistogramSource(SampleSource):
    """The raw cumulative LATENCY HISTOGRAM reply."""

    name = "latency_histogram"

    def sample(self) -> Optional[Dict[str, Any]]:
        """Return the parsed reply, or None when the call failed."""
        output = run_cli(self.ctx, "--json", "LATENCY", "HISTOGRAM")
        if output is None:
            return None
        return json.loads(output)
