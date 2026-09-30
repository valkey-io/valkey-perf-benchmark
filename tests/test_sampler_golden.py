"""Golden row test for the per-second sampler output columns.

Replays the stub inputs in tests/sampler_golden_inputs.py through the sampler
and asserts every column in tests/data/sampler_golden_rows.json is present with
the same value, so a change to a column name, a rounding rule or a delta formula
fails here rather than in a dashboard. `info` and `latency` are the only keys the
replayed rows are allowed to add.
"""

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import sampler_golden_inputs as inputs
from metrics_sampler import MetricsSampler

GOLDEN_PATH = Path(__file__).resolve().parent / "data" / "sampler_golden_rows.json"
ADDED_KEYS = {"info", "latency"}


def golden_rows():
    """Return the stored golden rows."""
    return json.loads(GOLDEN_PATH.read_text())


def replayed_rows():
    """Sample three ticks from the golden stub inputs."""
    sampler = MetricsSampler(
        host=inputs.GOLDEN_HOST,
        port=inputs.GOLDEN_PORT,
        cli_path=inputs.GOLDEN_CLI_PATH,
        server_pid=inputs.GOLDEN_SERVER_PID,
        interval=inputs.GOLDEN_INTERVAL,
        context=inputs.GOLDEN_CONTEXT,
        block_device=inputs.GOLDEN_BLOCK_DEVICE,
    )
    sampler._start_monotonic = inputs.GOLDEN_START_MONOTONIC
    with patch(
        "samplers.disk.detect_block_device", return_value=inputs.GOLDEN_BLOCK_DEVICE
    ):
        sampler._sources = sampler._build_sources()

    with ExitStack() as stack:
        stack.enter_context(
            patch("samplers.process_cpu._CLK_TCK", inputs.GOLDEN_CLK_TCK)
        )
        stack.enter_context(
            patch("metrics_sampler.time.monotonic", side_effect=inputs.GOLDEN_MONOTONIC)
        )
        stack.enter_context(
            patch("metrics_sampler.time.time", return_value=inputs.GOLDEN_WALL_CLOCK)
        )
        stack.enter_context(
            patch("samplers.valkey_info.run_cli", side_effect=inputs.GOLDEN_INFO_TEXTS)
        )
        stack.enter_context(
            patch("samplers.latency_histogram.run_cli", return_value=None)
        )
        stack.enter_context(
            patch(
                "samplers.process_cpu.read_system_cpu_ticks",
                side_effect=inputs.GOLDEN_SYSTEM_CPU,
            )
        )
        stack.enter_context(
            patch(
                "samplers.process_cpu.read_process_cpu_ticks",
                side_effect=inputs.GOLDEN_PROCESS_CPU,
            )
        )
        stack.enter_context(
            patch(
                "samplers.process_cpu.read_thread_cpu_ticks",
                side_effect=inputs.GOLDEN_ASIO_TICKS,
            )
        )
        stack.enter_context(
            patch(
                "samplers.disk.read_disk_counters",
                side_effect=inputs.GOLDEN_DISK_COUNTERS,
            )
        )
        for _ in inputs.GOLDEN_MONOTONIC:
            sampler._sample_once()
    return sampler.rows


class TestGoldenRows:
    def test_row_count_matches(self):
        assert len(replayed_rows()) == len(golden_rows())

    def test_every_golden_column_is_present(self):
        for index, (golden, replayed) in enumerate(zip(golden_rows(), replayed_rows())):
            missing = set(golden) - set(replayed)
            assert not missing, f"row {index} lost column(s) {sorted(missing)}"

    def test_every_golden_value_is_unchanged(self):
        for index, (golden, replayed) in enumerate(zip(golden_rows(), replayed_rows())):
            for column, value in golden.items():
                assert replayed[column] == value, (
                    f"row {index} column {column} is {replayed[column]!r}, "
                    f"golden is {value!r}"
                )

    def test_info_and_latency_are_the_only_added_keys(self):
        for index, (golden, replayed) in enumerate(zip(golden_rows(), replayed_rows())):
            added = set(replayed) - set(golden)
            assert added == ADDED_KEYS, f"row {index} added {sorted(added)}"

    def test_info_snapshot_agrees_with_the_derived_column(self):
        for row in replayed_rows():
            assert row["info"]["used_memory"] == row["used_memory"]
            assert row["info"]["db0"] == {
                "keys": row["info"]["db0"]["keys"],
                "expires": 0,
                "avg_ttl": 0,
            }

    def test_latency_is_empty_without_a_histogram_reply(self):
        for row in replayed_rows():
            assert row["latency"] == {}
