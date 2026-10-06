"""Unit tests for the LATENCY HISTOGRAM sample source."""

from unittest.mock import MagicMock

from samplers import SamplerContext
from samplers.latency_histogram import LatencyHistogramSource

REPLY = [
    "set",
    ["calls", 1200, "histogram_usec", [1, 900, 2, 1180, 4, 1200]],
    "config|get",
    ["calls", 2, "histogram_usec", [4, 2]],
]


def test_records_the_reply_per_command():
    client = MagicMock()
    client.execute_command.return_value = REPLY
    source = LatencyHistogramSource()
    source.start(SamplerContext(client=client))

    assert source.sample() == {
        "set": {"calls": 1200, "histogram_usec": {"1": 900, "2": 1180, "4": 1200}},
        "config|get": {"calls": 2, "histogram_usec": {"4": 2}},
    }
    client.execute_command.assert_called_once_with("LATENCY HISTOGRAM")
