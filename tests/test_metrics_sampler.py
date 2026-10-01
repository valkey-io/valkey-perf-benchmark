"""Unit tests for the metrics sampler loop.

Covers what the loop owns: elapsed time, context denormalization, source
selection, locality and platform filtering, per-source failure isolation, the
inline warmup start delay, thread pinning and the background thread lifecycle.
Each source's own columns are covered by tests/test_sampler_*.py.
"""

import socket
from contextlib import ExitStack
from itertools import combinations
from unittest.mock import patch

import pytest

from metrics_sampler import MetricsSampler, is_local_address
from samplers import DEFAULT_SOURCES, SOURCES, SamplerContext
from samplers.disk import DiskSource
from test_sampler_disk import DISK_DERIVED_COLUMNS, DISK_SAMPLE_1
from test_sampler_process_cpu import CPU_COLUMNS
from test_sampler_valkey_info import INFO_WITH_TIERING, info_with_main_thread_cpu

WALL_CLOCK = 1782868117


def make_sampler(**overrides):
    """Build a sampler with its sources started against a named block device."""
    kwargs = {
        "context": {},
        "server_pid": None,
        "block_device": "nvme0n1",
        "interval": 1.0,
    }
    kwargs.update(overrides)
    sampler = MetricsSampler(**kwargs)
    # start() is bypassed in most tests so time.monotonic can be controlled.
    sampler._start_monotonic = 100.0
    sampler._sources = sampler._build_sources()
    return sampler


def stub_sources(stack, count, info_texts=None, disk_counters=None):
    """Patch every source's reads for `count` ticks."""
    stack.enter_context(
        patch(
            "samplers.valkey_info.run_cli",
            side_effect=info_texts or [INFO_WITH_TIERING] * count,
        )
    )
    stack.enter_context(patch("samplers.latency_histogram.run_cli", return_value=None))
    stack.enter_context(
        patch(
            "samplers.disk.read_disk_counters",
            side_effect=disk_counters or [None] * count,
        )
    )


def sample_at(sampler, monotonic_values, info_texts=None, disk_counters=None):
    """Drive _sample_once once per monotonic value with every source stubbed."""
    count = len(monotonic_values)
    with ExitStack() as stack:
        stack.enter_context(
            patch("metrics_sampler.time.monotonic", side_effect=monotonic_values)
        )
        stack.enter_context(patch("metrics_sampler.time.time", return_value=WALL_CLOCK))
        stub_sources(stack, count, info_texts, disk_counters)
        for _ in range(count):
            sampler._sample_once()
    return sampler.rows


class TestElapsedSec:
    def test_starts_at_zero_and_increments(self):
        rows = sample_at(make_sampler(), [100.0, 101.0, 102.0])
        assert [row["elapsed_sec"] for row in rows] == [0, 1, 2]
        assert all(isinstance(row["elapsed_sec"], int) for row in rows)

    def test_absolute_timestamp_present_for_provenance(self):
        rows = sample_at(make_sampler(), [100.0])
        assert rows[0]["timestamp"] == WALL_CLOCK

    def test_rounds_to_nearest_second(self):
        rows = sample_at(make_sampler(), [100.0, 101.4, 102.6])
        assert [row["elapsed_sec"] for row in rows] == [0, 1, 3]


class TestContextDenormalization:
    def test_context_lands_on_every_row(self):
        context = {
            "commit": "abc123",
            "scenario": "zipfian-80-20",
            "run": 1,
        }
        rows = sample_at(make_sampler(context=context), [100.0, 101.0, 102.0])
        assert len(rows) == 3
        for row in rows:
            for key, value in context.items():
                assert row[key] == value

    def test_context_is_copied_not_aliased(self):
        context = {"commit": "abc123"}
        sampler = make_sampler(context=context)
        context["commit"] = "mutated"
        rows = sample_at(sampler, [100.0])
        assert rows[0]["commit"] == "abc123"

    def test_context_wins_on_key_collision(self):
        sampler = make_sampler(context={"used_memory": "pinned"})
        rows = sample_at(sampler, [100.0])
        assert rows[0]["used_memory"] == "pinned"


class TestRowShape:
    def test_every_slice_field_present(self):
        expected = {
            "used_memory",
            "used_memory_rss",
            "maxmemory",
            "mem_frag_ratio",
            "ops_per_sec",
            "total_commands_delta",
            "keyspace_hits",
            "keyspace_misses",
            "blocked_clients",
            "ext_storage_enabled",
            "ext_storage_capacity_bytes",
            "ext_storage_total_num_items",
            "ext_storage_total_num_bytes",
            "ext_storage_total_num_items_spilled_to_storage",
            "ext_storage_total_num_items_fetched_from_storage",
            "ext_storage_total_num_items_deleted_from_storage",
            "info",
            "latency",
            "valkey_cpu_user",
            "valkey_cpu_sys",
            "valkey_cpu_total",
            "asio_cpu_pct",
            "cpu_user",
            "cpu_sys",
            "disk_read_iops",
            "disk_write_iops",
            "disk_read_mb",
            "disk_write_mb",
            "disk_read_merges_ps",
            "disk_write_merges_ps",
            "disk_r_await_ms",
            "disk_w_await_ms",
            "disk_aqu_sz",
            "disk_util_pct",
            "disk_in_flight",
            "disk_req_sz_kb",
            "elapsed_sec",
            "timestamp",
        }
        row = sample_at(make_sampler(), [100.0])[0]
        assert expected <= set(row)


class TestSourceColumnOwnership:
    @pytest.mark.parametrize(
        "info_text", [INFO_WITH_TIERING, info_with_main_thread_cpu(10.0, 4.0)]
    )
    def test_default_source_columns_are_pairwise_disjoint(self, info_text):
        # valkey_cpu_* belong to valkey_info when INFO reports the main thread
        # CPU seconds and to process_cpu when it does not, never to both.
        ctx = SamplerContext(
            block_device="nvme0n1", warn_once=lambda key, message: None
        )
        columns = {}
        for name in DEFAULT_SOURCES:
            source = SOURCES[name]()
            source.start(ctx)
            with ExitStack() as stack:
                stub_sources(stack, 1, info_texts=[info_text])
                columns[name] = set(source.sample(100.0))

        for first, second in combinations(DEFAULT_SOURCES, 2):
            shared = columns[first] & columns[second]
            assert not shared, f"{first} and {second} both emit {sorted(shared)}"

    def test_main_thread_cpu_columns_are_emitted_exactly_once(self):
        row = sample_at(
            make_sampler(server_pid=1234),
            [100.0],
            info_texts=[info_with_main_thread_cpu(10.0, 4.0)],
        )[0]
        for column in ("valkey_cpu_user", "valkey_cpu_sys", "valkey_cpu_total"):
            assert column in row


class TestSourceSelection:
    def test_default_sources_are_used_when_none_are_named(self):
        sampler = make_sampler()
        assert [source.name for source in sampler._sources] == list(DEFAULT_SOURCES)

    def test_named_subset_emits_only_its_columns(self):
        sampler = make_sampler(sources=["valkey_info"])
        assert [source.name for source in sampler._sources] == ["valkey_info"]
        row = sample_at(sampler, [100.0])[0]
        assert row["used_memory"] == 1799288
        assert "latency" not in row
        assert "disk_in_flight" not in row
        assert "cpu_user" not in row

    def test_source_that_fails_to_start_is_dropped(self):
        with patch.object(DiskSource, "start", side_effect=RuntimeError("no device")):
            sampler = make_sampler()
        names = [source.name for source in sampler._sources]
        assert "disk" not in names
        assert len(names) == len(DEFAULT_SOURCES) - 1
        row = sample_at(sampler, [100.0])[0]
        assert "disk_in_flight" not in row
        assert row["used_memory"] == 1799288


class TestPerSourceFailureIsolation:
    def test_one_failing_source_does_not_blank_the_others(self):
        sampler = make_sampler(block_device="nvme0n1")
        info_source = sampler._sources[0]
        assert info_source.name == "valkey_info"
        with patch.object(info_source, "sample", side_effect=RuntimeError("boom")):
            rows = sample_at(sampler, [100.0], disk_counters=[DISK_SAMPLE_1])
        assert "used_memory" not in rows[0]
        assert rows[0]["disk_in_flight"] == 3
        assert rows[0]["cpu_user"] == 0.0
        assert rows[0]["elapsed_sec"] == 0

    def test_a_row_is_still_appended_when_every_source_fails(self):
        sampler = make_sampler(context={"commit": "abc123"})
        with ExitStack() as stack:
            for source in sampler._sources:
                stack.enter_context(
                    patch.object(source, "sample", side_effect=RuntimeError("boom"))
                )
            rows = sample_at(sampler, [100.0])
        assert rows[0]["commit"] == "abc123"
        assert rows[0]["elapsed_sec"] == 0


class TestThreadPinning:
    def test_pins_to_the_parsed_core_set(self):
        sampler = make_sampler(cpu_range="56-58,1")
        with patch("metrics_sampler.os.sched_setaffinity", create=True) as set_affinity:
            sampler._pin_thread()
        set_affinity.assert_called_once_with(0, {1, 56, 57, 58})

    def test_no_cpu_range_leaves_the_thread_unpinned(self):
        sampler = make_sampler()
        with patch("metrics_sampler.os.sched_setaffinity", create=True) as set_affinity:
            sampler._pin_thread()
        set_affinity.assert_not_called()

    def test_pin_failure_does_not_raise(self):
        sampler = make_sampler(cpu_range="0-1")
        with patch(
            "metrics_sampler.os.sched_setaffinity",
            create=True,
            side_effect=OSError("denied"),
        ):
            sampler._pin_thread()

    def test_unparsable_range_does_not_raise(self):
        sampler = make_sampler(cpu_range="not-a-range")
        with patch("metrics_sampler.os.sched_setaffinity", create=True) as set_affinity:
            sampler._pin_thread()
        set_affinity.assert_not_called()

    def test_background_thread_pins_itself(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1", cpu_range="0")
        with ExitStack() as stack:
            stack.enter_context(
                patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING)
            )
            stack.enter_context(
                patch("samplers.latency_histogram.run_cli", return_value=None)
            )
            stack.enter_context(
                patch("samplers.disk.read_disk_counters", return_value=None)
            )
            set_affinity = stack.enter_context(
                patch("metrics_sampler.os.sched_setaffinity", create=True)
            )
            sampler.start()
            wait_for_rows(sampler, 1)
            sampler.stop()
        set_affinity.assert_called_once_with(0, {0})

    def test_platform_without_the_syscall_warns_once_and_still_samples(
        self, monkeypatch
    ):
        monkeypatch.delattr("os.sched_setaffinity", raising=False)
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1", cpu_range="0")
        warnings = []
        with ExitStack() as stack:
            stack.enter_context(
                patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING)
            )
            stack.enter_context(
                patch("samplers.latency_histogram.run_cli", return_value=None)
            )
            stack.enter_context(
                patch("samplers.disk.read_disk_counters", return_value=None)
            )
            stack.enter_context(
                patch("logging.warning", side_effect=lambda msg: warnings.append(msg))
            )
            sampler.start()
            wait_for_rows(sampler, 2)
            sampler.stop()

        assert len(sampler.rows) >= 2
        pin_warnings = [
            message for message in warnings if "CPU pinning is not supported" in message
        ]
        assert len(pin_warnings) == 1


class TestPlatformFiltering:
    def test_non_linux_drops_the_proc_and_sys_sources(self):
        warnings = []
        with patch("metrics_sampler.sys.platform", "darwin"):
            sampler = MetricsSampler(host="127.0.0.1", block_device="nvme0n1")
            sampler._warn_once = lambda key, message: warnings.append(message)
            sampler._start_monotonic = 100.0
            sampler._sources = sampler._build_sources()

        assert [source.name for source in sampler._sources] == [
            "valkey_info",
            "latency_histogram",
        ]
        assert len(warnings) == 1
        assert "process_cpu" in warnings[0]
        assert "disk" in warnings[0]

    def test_non_linux_still_samples_the_cli_sources(self):
        with patch("metrics_sampler.sys.platform", "darwin"):
            sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0])
        for row in rows:
            assert row["used_memory"] == 1799288
            assert "latency" in row
            for column in CPU_COLUMNS + DISK_DERIVED_COLUMNS:
                assert column not in row


class TestStartDelay:
    def test_rows_start_after_the_delay_at_elapsed_zero(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1", start_delay=0.3)
        with ExitStack() as stack:
            stack.enter_context(
                patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING)
            )
            stack.enter_context(
                patch("samplers.latency_histogram.run_cli", return_value=None)
            )
            stack.enter_context(
                patch("samplers.disk.read_disk_counters", return_value=None)
            )
            sampler.start()
            assert sampler.rows == []
            wait_for_rows(sampler, 2)
            sampler.stop()

        rows = sampler.rows
        assert len(rows) >= 2
        assert rows[0]["elapsed_sec"] == 0

    def test_stop_during_the_delay_yields_no_rows(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1", start_delay=30)
        with patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING):
            sampler.start()
            sampler.stop()
        assert sampler.rows == []
        assert sampler._sampler_thread is None


class TestLocality:
    """Host CPU and disk columns are collected only when the server is local."""

    @pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost", "LOCALHOST"])
    def test_loopback_and_localhost_are_local(self, host):
        assert is_local_address(host) is True

    def test_this_machine_s_own_address_is_local(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect(("192.0.2.1", 9))
            address = sock.getsockname()[0]
        except OSError:
            pytest.skip("no routable interface to discover a local address from")
        finally:
            sock.close()
        assert is_local_address(address) is True

    @pytest.mark.parametrize("host", ["192.0.2.1", "no-such-host.invalid", ""])
    def test_addresses_this_machine_does_not_own_are_not_local(self, host):
        assert is_local_address(host) is False

    def test_remote_host_omits_cpu_and_disk_columns(self):
        sampler = make_sampler(host="192.0.2.1", server_pid=1234)
        assert [source.name for source in sampler._sources] == [
            "valkey_info",
            "latency_histogram",
        ]
        with patch("samplers.process_cpu.read_system_cpu_ticks") as system_cpu:
            with patch("samplers.process_cpu.read_thread_cpu_ticks") as thread_cpu:
                with patch("samplers.disk.read_disk_counters") as disk:
                    rows = sample_at(sampler, [100.0, 101.0])

        system_cpu.assert_not_called()
        thread_cpu.assert_not_called()
        disk.assert_not_called()
        for row in rows:
            for column in CPU_COLUMNS + DISK_DERIVED_COLUMNS:
                assert column not in row
            assert "disk_in_flight" not in row
            # INFO-derived columns and the context still land on every row.
            assert row["used_memory"] == 1799288

    def test_local_host_keeps_cpu_and_disk_columns(self):
        sampler = make_sampler(host="127.0.0.1", server_pid=1234)
        rows = sample_at(
            sampler, [100.0, 101.0], disk_counters=[DISK_SAMPLE_1, DISK_SAMPLE_1]
        )
        for row in rows:
            for column in CPU_COLUMNS + DISK_DERIVED_COLUMNS:
                assert column in row
            assert "disk_in_flight" in row

    def test_server_local_true_keeps_host_sources_for_any_host(self):
        sampler = make_sampler(host="10.255.255.1", server_local=True)
        assert [source.name for source in sampler._sources] == list(DEFAULT_SOURCES)

    def test_server_local_false_drops_host_sources_for_loopback(self):
        sampler = make_sampler(host="127.0.0.1", server_local=False)
        assert [source.name for source in sampler._sources] == [
            "valkey_info",
            "latency_histogram",
        ]


def wait_for_rows(sampler, count, deadline=5.0):
    """Wait until the sampler has `count` rows, without asserting on timing."""
    import time as _time

    waited = 0.0
    while len(sampler.rows) < count and waited < deadline:
        _time.sleep(0.01)
        waited += 0.01


class TestBackgroundThread:
    def test_start_stop_collects_on_background_thread(self):
        sampler = MetricsSampler(
            interval=0.01,
            context={"commit": "abc123"},
            server_pid=None,
            block_device="nvme0n1",
        )
        with patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING):
            with patch("samplers.disk.read_disk_counters", return_value=None):
                with patch("samplers.latency_histogram.run_cli", return_value=None):
                    sampler.start()
                    assert sampler._sampler_thread is not None
                    wait_for_rows(sampler, 2)
                    sampler.stop()

        rows = sampler.rows
        assert len(rows) >= 2
        assert sampler._sampler_thread is None
        assert all(row["commit"] == "abc123" for row in rows)
        assert rows[0]["elapsed_sec"] == 0

    def test_source_failure_does_not_kill_the_loop(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1")
        calls = {"count": 0}

        def flaky_info(*args):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("INFO exploded")
            return INFO_WITH_TIERING

        with patch("samplers.valkey_info.run_cli", side_effect=flaky_info):
            with patch("samplers.disk.read_disk_counters", return_value=None):
                with patch("samplers.latency_histogram.run_cli", return_value=None):
                    sampler.start()
                    wait_for_rows(sampler, 2)
                    sampler.stop()

        assert calls["count"] >= 2
        assert len(sampler.rows) >= 2

    def test_sample_failure_does_not_kill_the_loop(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1")
        calls = {"count": 0}
        original = sampler._sample_once

        def flaky_sample():
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("tick exploded")
            original()

        with patch("samplers.valkey_info.run_cli", return_value=INFO_WITH_TIERING):
            with patch("samplers.disk.read_disk_counters", return_value=None):
                with patch("samplers.latency_histogram.run_cli", return_value=None):
                    with patch.object(
                        sampler, "_sample_once", side_effect=flaky_sample
                    ):
                        sampler.start()
                        wait_for_rows(sampler, 1)
                        sampler.stop()

        assert calls["count"] >= 2
        assert len(sampler.rows) >= 1

    def test_double_start_is_ignored(self):
        sampler = MetricsSampler(interval=0.01, block_device="nvme0n1")
        with patch("samplers.valkey_info.run_cli", return_value=None):
            with patch("samplers.latency_histogram.run_cli", return_value=None):
                with patch("samplers.disk.read_disk_counters", return_value=None):
                    sampler.start()
                    first_thread = sampler._sampler_thread
                    sampler.start()
                    assert sampler._sampler_thread is first_thread
                    sampler.stop()

    def test_stop_without_start_does_not_raise(self):
        sampler = MetricsSampler(block_device="nvme0n1")
        sampler.stop()
        assert sampler.rows == []
