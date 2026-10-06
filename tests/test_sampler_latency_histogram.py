"""Unit tests for the LATENCY HISTOGRAM sample source."""

import json
from unittest.mock import patch

from samplers import SamplerContext
from samplers.latency_histogram import LatencyHistogramSource

REPLY = {
    "set": {"calls": 1200, "histogram_usec": {"1": 900, "2": 1180, "4": 1200}},
    "config|get": {"calls": 2, "histogram_usec": {"4": 2}},
}


def started_source():
    source = LatencyHistogramSource()
    source.start(SamplerContext())
    return source


def test_records_the_reply_as_reported():
    with patch(
        "samplers.latency_histogram.run_cli", return_value=json.dumps(REPLY)
    ) as run_cli:
        assert started_source().sample() == REPLY
    assert run_cli.call_args.args[1:] == ("--json", "LATENCY", "HISTOGRAM")


def test_failed_call_reads_as_none():
    with patch("samplers.latency_histogram.run_cli", return_value=None):
        assert started_source().sample() is None
