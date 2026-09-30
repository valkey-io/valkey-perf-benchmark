"""Unit tests for the per-interval latency histogram sample source.

LATENCY HISTOGRAM output is canned in the JSON object form, and the expected
percentiles are hand-computed.
"""

import json
from unittest.mock import patch

from samplers.base import SamplerContext
from samplers.latency_histogram import (
    LatencyHistogramSource,
    interval_percentiles,
    parse_histogram,
)

# One command sampled twice. The previous CDF holds 50 calls in buckets 1, 2 and
# 4, and the current one adds 1000 calls spread so that p50, p99 and p999 each
# land on a different bucket.
#
# Interval cumulative counts, current minus the previous step value at or below
# each bucket: 1 -> 0, 2 -> 0, 4 -> 10, 8 -> 900, 16 -> 940, 32 -> 980,
# 64 -> 995, 128 -> 1000. The interval total is 1000, so p50 crosses 500 at
# bucket 8, p99 crosses 990 at bucket 64, and p999 crosses 999 at bucket 128.
PREVIOUS_BUCKETS = {1: 10, 2: 30, 4: 50}
CURRENT_BUCKETS = {
    1: 10,
    2: 30,
    4: 60,
    8: 950,
    16: 990,
    32: 1030,
    64: 1045,
    128: 1050,
}
EXPECTED_P50 = 8
EXPECTED_P99 = 64
EXPECTED_P999 = 128


def histogram_json(commands):
    """Render {command: (calls, buckets)} the way --json does over RESP3."""
    return json.dumps(
        {
            name: {
                "calls": calls,
                "histogram_usec": {
                    str(bucket): count for bucket, count in buckets.items()
                },
            }
            for name, (calls, buckets) in commands.items()
        }
    )


def make_source():
    """Build a started LatencyHistogramSource with warnings discarded."""
    source = LatencyHistogramSource()
    source.start(SamplerContext(warn_once=lambda key, message: None))
    return source


def latency_rows(outputs):
    """Sample once per canned CLI output, returning the `latency` value of each."""
    source = make_source()
    rows = []
    with patch("samplers.latency_histogram.run_cli", side_effect=outputs):
        for _ in outputs:
            rows.append(source.sample(1.0)["latency"])
    return rows


class TestParseHistogram:
    def test_parses_the_json_object_form(self):
        text = histogram_json({"get": (1050, CURRENT_BUCKETS)})
        parsed = parse_histogram(text)
        assert parsed["get"].calls == 1050
        assert parsed["get"].buckets == CURRENT_BUCKETS

    def test_unavailable_output_parses_to_nothing(self):
        assert parse_histogram(None) == {}
        assert parse_histogram("") == {}

    def test_non_json_output_parses_to_nothing(self):
        assert parse_histogram("ERR unknown subcommand") == {}


class TestIntervalPercentiles:
    def test_each_percentile_lands_on_its_own_bucket(self):
        percentiles = interval_percentiles(PREVIOUS_BUCKETS, CURRENT_BUCKETS)
        assert percentiles == {
            "p50_usec": EXPECTED_P50,
            "p99_usec": EXPECTED_P99,
            "p999_usec": EXPECTED_P999,
        }

    def test_bucket_below_every_previous_bucket_starts_from_zero(self):
        # The interval added 50 calls at bucket 2, which the previous CDF has no
        # entry at or below, plus 150 at bucket 4. p50 of 150 crosses at 4.
        percentiles = interval_percentiles({4: 100}, {2: 50, 4: 250})
        assert percentiles["p50_usec"] == 4
        assert percentiles["p999_usec"] == 4

    def test_previous_bucket_missing_from_current_is_not_counted(self):
        # Bucket 4 exists only in the previous CDF, so bucket 8's interval count
        # is 300 minus the previous step value 100.
        percentiles = interval_percentiles({4: 100}, {8: 300})
        assert percentiles["p50_usec"] == 8


class TestIntervalLatency:
    def test_first_tick_is_empty(self):
        rows = latency_rows([histogram_json({"get": (1050, CURRENT_BUCKETS)})])
        assert rows[0] == {}

    def test_second_tick_reports_the_interval(self):
        rows = latency_rows(
            [
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
                histogram_json({"get": (1050, CURRENT_BUCKETS)}),
            ]
        )
        assert rows[1] == {
            "get": {
                "calls": 1000,
                "p50_usec": EXPECTED_P50,
                "p99_usec": EXPECTED_P99,
                "p999_usec": EXPECTED_P999,
            }
        }

    def test_sampler_own_commands_are_excluded(self):
        commands_before = {
            "get": (50, PREVIOUS_BUCKETS),
            "info": (50, PREVIOUS_BUCKETS),
            "latency|histogram": (50, PREVIOUS_BUCKETS),
            "hello": (50, PREVIOUS_BUCKETS),
            "config|get": (50, PREVIOUS_BUCKETS),
        }
        commands_after = {
            "get": (1050, CURRENT_BUCKETS),
            "info": (1050, CURRENT_BUCKETS),
            "latency|histogram": (1050, CURRENT_BUCKETS),
            "hello": (1050, CURRENT_BUCKETS),
            "config|get": (1050, CURRENT_BUCKETS),
        }
        rows = latency_rows(
            [histogram_json(commands_before), histogram_json(commands_after)]
        )
        assert set(rows[1]) == {"get"}

    def test_unchanged_calls_are_omitted(self):
        rows = latency_rows(
            [
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
            ]
        )
        assert rows[1] == {}

    def test_calls_decrease_resets_the_baseline(self):
        # A server restart or CONFIG RESETSTAT drops the counter. That tick is
        # omitted, and the next one measures against the reset baseline.
        rows = latency_rows(
            [
                histogram_json({"get": (1050, CURRENT_BUCKETS)}),
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
                histogram_json({"get": (1050, CURRENT_BUCKETS)}),
            ]
        )
        assert rows[1] == {}
        assert rows[2]["get"]["calls"] == 1000

    def test_unavailable_output_yields_an_empty_dict(self):
        rows = latency_rows([histogram_json({"get": (50, PREVIOUS_BUCKETS)}), None])
        assert rows[1] == {}

    def test_tick_after_an_unavailable_one_is_empty(self):
        # An unavailable reply leaves no baseline, so the next tick must not
        # report a command's whole running total as one interval.
        rows = latency_rows(
            [
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
                None,
                histogram_json({"get": (1050, CURRENT_BUCKETS)}),
            ]
        )
        assert rows[1] == {}
        assert rows[2] == {}

    def test_command_new_since_the_previous_tick_is_reported_in_full(self):
        rows = latency_rows(
            [
                histogram_json({"get": (50, PREVIOUS_BUCKETS)}),
                histogram_json(
                    {
                        "get": (50, PREVIOUS_BUCKETS),
                        "set": (1050, CURRENT_BUCKETS),
                    }
                ),
            ]
        )
        assert set(rows[1]) == {"set"}
        assert rows[1]["set"]["calls"] == 1050
        assert rows[1]["set"]["p999_usec"] == EXPECTED_P999

    def test_latency_key_is_always_present(self):
        rows = latency_rows([None, None])
        assert rows == [{}, {}]
