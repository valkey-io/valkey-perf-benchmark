"""Unit tests for metrics_sampler module.

No live server, no real Postgres: INFO output is mocked and every /proc and
/sys read is either patched or pointed at a tmp_path tree.
"""

import json
from unittest.mock import patch

import pytest

from metrics_sampler import (
    ASIO_THREAD_NAME,
    MetricsSampler,
    TIERING_INFO_FIELDS,
    TIERING_INFO_FLOAT_FIELDS,
    detect_block_device,
    parse_info,
    read_disk_counters,
    read_process_cpu_ticks,
    read_system_cpu_ticks,
    read_thread_cpu_ticks,
)

# A tiering-enabled INFO snapshot, trimmed to the fields the sampler reads.
INFO_WITH_TIERING = """# Memory
used_memory:1799288
used_memory_rss:6959104
maxmemory:1073741824
mem_fragmentation_ratio:3.96

# Clients
blocked_clients:2

# Stats
total_commands_processed:1000
instantaneous_ops_per_sec:12345
keyspace_hits:800
keyspace_misses:200

# External Storage
total_num_items_spilled_to_ext_storage:5000
total_num_items_fetched_from_ext_storage:1200
num_items_spilling_to_ext_storage:7
kbc_fetching_block:4
completion_read_ok:250
oom_reject_write_count:11
spill_attempts:6100
spill_submitted:6000
dram_value_hits:1000
throttle_total_throttled:900
throttle_queued_clients:13
throttle_current_rate:0.8125
throttle_allowed_tps:45000.5
spill_submitted_count:5900
spill_serialized_count:5850
mean_spill_ram:2048
inflight_spill_ram_bytes:102400
"""

# The same server with tiering disabled: genExternalStorageInfoString returns
# early, so the whole External Storage section is absent.
INFO_WITHOUT_TIERING = """# Memory
used_memory:1799288
used_memory_rss:6959104
maxmemory:0
mem_fragmentation_ratio:1.21

# Clients
blocked_clients:0

# Stats
total_commands_processed:1000
keyspace_hits:800
keyspace_misses:200
"""


def info_with_counters(dram_value_hits, completion_read_ok):
    """Return the tiering INFO snapshot with the two ratio inputs overridden."""
    return INFO_WITH_TIERING.replace(
        "completion_read_ok:250", f"completion_read_ok:{completion_read_ok}"
    ).replace("dram_value_hits:1000", f"dram_value_hits:{dram_value_hits}")


def make_sampler(**overrides):
    """Build a sampler with all host sources neutralized unless overridden."""
    kwargs = {
        "context": {},
        "server_pid": None,
        "block_device": None,
        "interval": 1.0,
    }
    kwargs.update(overrides)
    sampler = MetricsSampler(**kwargs)
    # start() is bypassed in most tests so time.monotonic can be controlled.
    sampler._start_monotonic = 100.0
    return sampler


def sample_at(sampler, monotonic_values, info_texts):
    """Drive _sample_once once per monotonic value, serving canned INFO text."""
    with patch("metrics_sampler.time.monotonic", side_effect=monotonic_values):
        with patch("metrics_sampler.time.time", return_value=1782868117):
            for info_text in info_texts:
                with patch.object(
                    sampler, "_read_info", return_value=parse_info(info_text)
                ):
                    sampler._sample_once()
    return sampler.rows


class TestParseInfo:
    def test_skips_headers_and_blank_lines(self):
        fields = parse_info(INFO_WITH_TIERING)
        assert fields["used_memory"] == "1799288"
        assert fields["num_items_spilling_to_ext_storage"] == "7"
        assert not any(key.startswith("#") for key in fields)

    def test_flattens_all_sections(self):
        fields = parse_info(INFO_WITH_TIERING)
        # memory, clients, stats and external_storage fields coexist flat
        assert {
            "used_memory",
            "blocked_clients",
            "keyspace_hits",
            "dram_value_hits",
        } <= (set(fields))

    def test_keeps_values_containing_colons(self):
        fields = parse_info("db0:keys=10,expires=0\r\nrole:primary\r\n")
        assert fields["db0"] == "keys=10,expires=0"
        assert fields["role"] == "primary"

    def test_empty_input(self):
        assert parse_info("") == {}


class TestElapsedSec:
    def test_starts_at_zero_and_increments(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0, 102.0], [INFO_WITH_TIERING] * 3)
        assert [row["elapsed_sec"] for row in rows] == [0, 1, 2]
        assert all(isinstance(row["elapsed_sec"], int) for row in rows)

    def test_absolute_timestamp_present_for_provenance(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0], [INFO_WITH_TIERING])
        assert rows[0]["timestamp"] == 1782868117

    def test_rounds_to_nearest_second(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.4, 102.6], [INFO_WITH_TIERING] * 3)
        assert [row["elapsed_sec"] for row in rows] == [0, 1, 3]


class TestContextDenormalization:
    def test_context_lands_on_every_row(self):
        context = {
            "commit": "abc123",
            "scenario": "zipfian-80-20",
            "run": 1,
        }
        sampler = make_sampler(context=context)
        rows = sample_at(sampler, [100.0, 101.0, 102.0], [INFO_WITH_TIERING] * 3)
        assert len(rows) == 3
        for row in rows:
            for key, value in context.items():
                assert row[key] == value

    def test_context_is_copied_not_aliased(self):
        context = {"commit": "abc123"}
        sampler = make_sampler(context=context)
        context["commit"] = "mutated"
        rows = sample_at(sampler, [100.0], [INFO_WITH_TIERING])
        assert rows[0]["commit"] == "abc123"

    def test_context_wins_on_key_collision(self):
        sampler = make_sampler(context={"used_memory": "pinned"})
        rows = sample_at(sampler, [100.0], [INFO_WITH_TIERING])
        assert rows[0]["used_memory"] == "pinned"


class TestGaugeFields:
    def test_info_gauges_land_on_row(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["used_memory"] == 1799288
        assert row["used_memory_rss"] == 6959104
        assert row["maxmemory"] == 1073741824
        assert row["mem_frag_ratio"] == 3.96
        assert row["keyspace_hits"] == 800
        assert row["keyspace_misses"] == 200
        assert row["blocked_clients"] == 2

    def test_tiering_fields_use_info_field_names(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["total_num_items_spilled_to_ext_storage"] == 5000
        assert row["total_num_items_fetched_from_ext_storage"] == 1200
        assert row["num_items_spilling_to_ext_storage"] == 7

    def test_blocked_on_fetch_field_emitted(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["kbc_fetching_block"] == 4

    def test_throttle_counters_parsed_as_int(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["throttle_total_throttled"] == 900
        assert row["throttle_queued_clients"] == 13

    def test_throttle_rates_parsed_as_float(self):
        # The engine formats current_rate %.4f and allowed_tps %.1f, so an int
        # parse would floor both to 0 and 45000.
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["throttle_current_rate"] == 0.8125
        assert row["throttle_allowed_tps"] == 45000.5
        assert isinstance(row["throttle_current_rate"], float)
        assert isinstance(row["throttle_allowed_tps"], float)

    def test_spill_pipeline_fields_emitted(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["spill_attempts"] == 6100
        assert row["spill_serialized_count"] == 5850
        assert row["mean_spill_ram"] == 2048
        assert row["inflight_spill_ram_bytes"] == 102400
        assert row["oom_reject_write_count"] == 11

    def test_spill_submitted_count_not_spill_submitted(self):
        # The engine emits both names as separate counters. The column is the
        # atomic that pairs with spill_serialized_count, so the fixture gives
        # the two different values and this pins which one is read.
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["spill_submitted_count"] == 5900
        assert "spill_submitted" not in row

    def test_raw_ratio_inputs_emitted(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        # Emitted alongside the ratios so a later reader can tell a real
        # movement apart from a formula or divide-by-zero bug.
        assert row["completion_read_ok"] == 250
        assert row["dram_value_hits"] == 1000

    def test_derived_hit_ratios(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        # completion_read_ok 250 of dram_value_hits 1000
        assert row["disk_hit_pct"] == 25.0
        assert row["mem_hit_pct"] == 75.0

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
            "mem_hit_pct",
            "disk_hit_pct",
            "mem_hit_pct_interval",
            "disk_hit_pct_interval",
            "blocked_clients",
            "total_num_items_spilled_to_ext_storage",
            "total_num_items_fetched_from_ext_storage",
            "num_items_spilling_to_ext_storage",
            "kbc_fetching_block",
            "completion_read_ok",
            "dram_value_hits",
            "throttle_total_throttled",
            "throttle_queued_clients",
            "throttle_current_rate",
            "throttle_allowed_tps",
            "spill_attempts",
            "spill_submitted_count",
            "spill_serialized_count",
            "mean_spill_ram",
            "inflight_spill_ram_bytes",
            "oom_reject_write_count",
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
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert expected <= set(row)


class TestHitRatioForms:
    # Both forms are emitted: the cumulative one matches the reference dashboard
    # CSV and its published end-of-run figure, the per-interval one matches the
    # engine formula at valkey-data-tiering src/ext_storage.c:235 and is what
    # makes a per-second chart able to show transients.
    #
    # The three samples below are chosen so the two forms diverge. Cumulative
    # totals run 250/1000, 1250/2000, 1250/3000, so the per-interval deltas run
    # (first sample, no predecessor), 1000/1000 (every access a disk hit), then
    # 0/1000 (every access a DRAM hit). A per-interval swing from 100% disk to
    # 0% disk moves the cumulative figure only from 62.5 to 41.67.
    SERIES = [
        info_with_counters(1000, 250),
        info_with_counters(2000, 1250),
        info_with_counters(3000, 1250),
    ]

    def test_both_forms_present_on_every_row(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0, 102.0], self.SERIES)
        for row in rows:
            assert {
                "disk_hit_pct",
                "mem_hit_pct",
                "disk_hit_pct_interval",
                "mem_hit_pct_interval",
            } <= set(row)

    def test_cumulative_form_matches_running_totals(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0, 102.0], self.SERIES)
        assert [row["disk_hit_pct"] for row in rows] == [25.0, 62.5, 41.67]
        assert [row["mem_hit_pct"] for row in rows] == [75.0, 37.5, 58.33]

    def test_interval_form_matches_consecutive_deltas(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0, 102.0], self.SERIES)
        # First sample has no predecessor, then 1000/1000 and 0/1000
        assert [row["disk_hit_pct_interval"] for row in rows] == [0.0, 100.0, 0.0]
        assert [row["mem_hit_pct_interval"] for row in rows] == [0.0, 0.0, 100.0]

    def test_interval_form_differs_from_cumulative_when_rate_changes(self):
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0, 102.0], self.SERIES)
        for row in rows[1:]:
            assert row["disk_hit_pct_interval"] != row["disk_hit_pct"]
            assert row["mem_hit_pct_interval"] != row["mem_hit_pct"]

    def test_interval_form_is_zero_on_first_sample(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["disk_hit_pct_interval"] == 0.0
        assert row["mem_hit_pct_interval"] == 0.0

    def test_zero_denominator_yields_zero_for_both_forms(self):
        # The reference CSV emits 0.0, not null, while dram_value_hits is 0.
        sampler = make_sampler()
        rows = sample_at(
            sampler,
            [100.0, 101.0],
            [info_with_counters(0, 0), info_with_counters(0, 0)],
        )
        for row in rows:
            assert row["disk_hit_pct"] == 0.0
            assert row["mem_hit_pct"] == 0.0
            assert row["disk_hit_pct_interval"] == 0.0
            assert row["mem_hit_pct_interval"] == 0.0

    def test_flat_counters_yield_zero_interval_ratios(self):
        # Cumulative stays put while the interval denominator is 0.
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[1]["disk_hit_pct"] == 25.0
        assert rows[1]["disk_hit_pct_interval"] == 0.0
        assert rows[1]["mem_hit_pct_interval"] == 0.0


class TestCommandDeltas:
    def test_first_sample_has_no_delta(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITH_TIERING])[0]
        assert row["total_commands_delta"] == 0
        assert row["ops_per_sec"] == 0.0

    def test_delta_across_two_samples(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING, second])
        assert rows[1]["total_commands_delta"] == 2000
        assert rows[1]["ops_per_sec"] == 2000.0

    def test_ops_per_sec_normalized_by_measured_interval(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        sampler = make_sampler()
        # 2s of wall clock between samples, so the same delta halves the rate
        rows = sample_at(sampler, [100.0, 102.0], [INFO_WITH_TIERING, second])
        assert rows[1]["total_commands_delta"] == 2000
        assert rows[1]["ops_per_sec"] == 1000.0

    def test_counter_reset_clamps_to_zero(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:5"
        )
        sampler = make_sampler()
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING, second])
        assert rows[1]["total_commands_delta"] == 0
        assert rows[1]["ops_per_sec"] == 0.0


class TestMissingTieringSection:
    def test_tiering_fields_are_zero_not_missing(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITHOUT_TIERING])[0]
        for column in TIERING_INFO_FIELDS:
            assert row[column] == 0

    def test_tiering_float_fields_are_zero_not_missing(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITHOUT_TIERING])[0]
        for column in TIERING_INFO_FLOAT_FIELDS:
            assert row[column] == 0.0

    def test_hit_ratios_are_zero_without_dram_value_hits(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITHOUT_TIERING])[0]
        assert row["disk_hit_pct"] == 0.0
        assert row["mem_hit_pct"] == 0.0
        assert row["disk_hit_pct_interval"] == 0.0
        assert row["mem_hit_pct_interval"] == 0.0

    def test_non_tiering_fields_still_collected(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [INFO_WITHOUT_TIERING])[0]
        assert row["used_memory"] == 1799288
        assert row["mem_frag_ratio"] == 1.21

    def test_empty_info_yields_zeros_without_raising(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], [""])[0]
        assert row["used_memory"] == 0
        assert row["mem_frag_ratio"] == 0.0
        assert row["total_num_items_spilled_to_ext_storage"] == 0

    def test_unparsable_values_fall_back_to_zero(self):
        sampler = make_sampler()
        row = sample_at(sampler, [100.0], ["used_memory:not_a_number\n"])[0]
        assert row["used_memory"] == 0


class TestInfoCommandFailure:
    def test_cli_missing_yields_empty_info_not_exception(self):
        sampler = make_sampler(cli_path="/nonexistent/valkey-cli")
        assert sampler._read_info() == {}

    @patch("metrics_sampler.subprocess.run")
    def test_nonzero_exit_yields_empty_info(self, mock_run):
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "Could not connect"
        sampler = make_sampler()
        assert sampler._read_info() == {}

    @patch("metrics_sampler.subprocess.run")
    def test_successful_call_parses_stdout(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = INFO_WITH_TIERING
        mock_run.return_value.stderr = ""
        sampler = make_sampler()
        assert sampler._read_info()["used_memory"] == "1799288"


class TestSystemCpu:
    def test_parses_proc_stat_first_line(self):
        proc_stat = "cpu  100 10 50 900 5 0 0 0 0 0\ncpu0 1 2 3 4 5 0 0 0 0 0\n"
        with patch("metrics_sampler._read_text", return_value=proc_stat):
            # user+nice, system, sum of every bucket
            assert read_system_cpu_ticks() == (110, 50, 1065)

    def test_returns_none_when_unreadable(self):
        with patch("metrics_sampler._read_text", return_value=""):
            assert read_system_cpu_ticks() is None

    def test_returns_none_on_garbage(self):
        with patch("metrics_sampler._read_text", return_value="cpu  a b c d e\n"):
            assert read_system_cpu_ticks() is None

    def test_percentages_derived_across_two_samples(self):
        sampler = make_sampler()
        with patch(
            "metrics_sampler.read_system_cpu_ticks",
            side_effect=[(100, 50, 1000), (200, 100, 2000)],
        ):
            rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[0]["cpu_user"] == 0.0
        # 100 user ticks of 1000 total ticks, 50 system ticks of 1000
        assert rows[1]["cpu_user"] == 10.0
        assert rows[1]["cpu_sys"] == 5.0

    def test_missing_proc_stat_does_not_raise(self):
        sampler = make_sampler()
        with patch("metrics_sampler.read_system_cpu_ticks", return_value=None):
            rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[1]["cpu_user"] == 0.0
        assert rows[1]["cpu_sys"] == 0.0


class TestProcessAndThreadCpu:
    # utime is field 14 and stime field 15 of /proc/<pid>/stat, counted after
    # the parenthesized comm field.

    def _stat_line(self, comm, utime, stime):
        fields = list(range(3, 30))
        fields[11] = utime  # field 14
        fields[12] = stime  # field 15
        return f"1234 ({comm}) " + " ".join(str(n) for n in fields)

    def _make_task_tree(self, proc_root, threads, pid=1234):
        """Build a fake /proc/<pid>/task tree of (tid, comm, utime, stime)."""
        for tid, comm, utime, stime in threads:
            thread_dir = proc_root / str(pid) / "task" / str(tid)
            thread_dir.mkdir(parents=True)
            (thread_dir / "comm").write_text(f"{comm}\n")
            (thread_dir / "stat").write_text(self._stat_line(comm, utime, stime))

    def test_parses_utime_and_stime(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text(self._stat_line("valkey-server", 500, 250))
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) == (500, 250)

    def test_comm_with_spaces_and_parens_does_not_break_parsing(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text(self._stat_line("valkey server (x)", 700, 300))
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) == (700, 300)

    def test_missing_pid_returns_none(self, tmp_path):
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(9999) is None

    def test_truncated_stat_line_returns_none(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text("1234 (valkey-server) S 1 2 3")
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) is None

    def test_process_cpu_percent_across_two_samples(self):
        sampler = make_sampler(server_pid=1234)
        with patch("metrics_sampler._CLK_TCK", 100):
            with patch(
                "metrics_sampler.read_process_cpu_ticks",
                side_effect=[(100, 50), (180, 70)],
            ):
                with patch("metrics_sampler.read_thread_cpu_ticks", return_value=0):
                    rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        # 80 user ticks in 1s at 100Hz is 80% of one core, 20 sys ticks is 20%
        assert rows[1]["valkey_cpu_user"] == 80.0
        assert rows[1]["valkey_cpu_sys"] == 20.0
        assert rows[1]["valkey_cpu_total"] == 100.0

    def test_asio_thread_ticks_summed_by_name(self, tmp_path):
        self._make_task_tree(
            tmp_path,
            [
                (10, "valkey-server", 1000, 1000),
                (11, ASIO_THREAD_NAME, 40, 10),
                (12, ASIO_THREAD_NAME, 20, 30),
            ],
        )
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            # Only the two fc_io_worker threads count: 40+10 plus 20+30
            assert read_thread_cpu_ticks(1234, ASIO_THREAD_NAME) == 100

    def test_no_matching_thread_returns_zero(self, tmp_path):
        self._make_task_tree(tmp_path, [(10, "valkey-server", 1000, 1000)])
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_thread_cpu_ticks(1234, ASIO_THREAD_NAME) == 0

    def test_missing_task_dir_returns_none(self, tmp_path):
        with patch("metrics_sampler._PROC_DIR", tmp_path):
            assert read_thread_cpu_ticks(9999, ASIO_THREAD_NAME) is None

    def test_asio_percent_across_two_samples(self):
        sampler = make_sampler(server_pid=1234)
        with patch("metrics_sampler._CLK_TCK", 100):
            with patch("metrics_sampler.read_process_cpu_ticks", return_value=(0, 0)):
                with patch(
                    "metrics_sampler.read_thread_cpu_ticks", side_effect=[100, 150]
                ):
                    rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        # 50 ticks in 1s at 100Hz is half a core
        assert rows[1]["asio_cpu_pct"] == 50.0

    def test_no_pid_yields_zero_process_cpu(self):
        sampler = make_sampler(server_pid=None)
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[1]["valkey_cpu_total"] == 0.0
        assert rows[1]["asio_cpu_pct"] == 0.0

    def test_unreadable_task_dir_does_not_raise(self):
        sampler = make_sampler(server_pid=1234)
        with patch("metrics_sampler.read_process_cpu_ticks", return_value=None):
            with patch("metrics_sampler.read_thread_cpu_ticks", return_value=None):
                rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[1]["valkey_cpu_total"] == 0.0
        assert rows[1]["asio_cpu_pct"] == 0.0


class TestDiskDetection:
    def test_prefers_nvme_over_scsi(self, tmp_path):
        for name, size in [("nvme0n1", "1000"), ("sda", "9999999")]:
            device = tmp_path / name
            device.mkdir()
            (device / "size").write_text(size)
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert detect_block_device() == "nvme0n1"

    def test_picks_largest_within_class(self, tmp_path):
        for name, size in [("nvme0n1", "1000"), ("nvme1n1", "5000")]:
            device = tmp_path / name
            device.mkdir()
            (device / "size").write_text(size)
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert detect_block_device() == "nvme1n1"

    def test_ignores_loop_and_ram_devices(self, tmp_path):
        for name in ["loop0", "ram0", "dm-0"]:
            (tmp_path / name).mkdir()
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert detect_block_device() is None

    def test_missing_sys_block_returns_none(self, tmp_path):
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path / "missing"):
            assert detect_block_device() is None


# Two consecutive stat snapshots, one second apart. The deltas are chosen so
# every derived value comes out different from every other, which is what makes
# a swapped formula fail rather than coincidentally pass.
#
# Deltas: read ios 200, read merges 40, read sectors 8192, read ticks 600,
#         write ios 50, write merges 15, write sectors 3200, write ticks 400,
#         io ticks 250, time in queue 1500.
DISK_SAMPLE_1 = {
    "read_ios": 100,
    "read_merges": 10,
    "read_sectors": 2048,
    "read_ticks": 50,
    "write_ios": 200,
    "write_merges": 20,
    "write_sectors": 4096,
    "write_ticks": 90,
    "in_flight": 3,
    "io_ticks": 1000,
    "time_in_queue": 5000,
}
DISK_SAMPLE_2 = {
    "read_ios": 300,
    "read_merges": 50,
    "read_sectors": 10240,
    "read_ticks": 650,
    "write_ios": 250,
    "write_merges": 35,
    "write_sectors": 7296,
    "write_ticks": 490,
    "in_flight": 7,
    "io_ticks": 1250,
    "time_in_queue": 6500,
}

# Every disk column except disk_in_flight, which is a gauge.
DISK_DERIVED_COLUMNS = (
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
    "disk_req_sz_kb",
)


def disk_rows(counters, monotonic_values=None):
    """Sample once per entry in `counters`, serving each as the stat snapshot."""
    monotonic_values = monotonic_values or [100.0 + n for n in range(len(counters))]
    sampler = make_sampler(block_device="nvme0n1")
    with patch("metrics_sampler.read_disk_counters", side_effect=counters):
        return sample_at(sampler, monotonic_values, [INFO_WITH_TIERING] * len(counters))


class TestDiskCounters:
    # /sys/block/<dev>/stat: read ios, read merges, read sectors, read ticks,
    # write ios, write merges, write sectors, write ticks, in flight, io ticks,
    # time in queue. Sectors are 512-byte units, ticks are milliseconds.
    STAT = "100 10 2048 50 200 20 4096 90 3 1000 5000"
    COUNTERS = {
        "read_ios": 100,
        "read_merges": 10,
        "read_sectors": 2048,
        "read_ticks": 50,
        "write_ios": 200,
        "write_merges": 20,
        "write_sectors": 4096,
        "write_ticks": 90,
        "in_flight": 3,
        "io_ticks": 1000,
        "time_in_queue": 5000,
    }

    def test_parses_counters(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text(self.STAT)
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") == self.COUNTERS

    def test_trailing_fields_are_ignored(self, tmp_path):
        # Current kernels append discard and flush counters after the eleventh.
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text(self.STAT + " 7 8 9 10 11 12")
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") == self.COUNTERS

    def test_missing_device_returns_none(self, tmp_path):
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme9n9") is None

    def test_short_stat_line_returns_none(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("1 2 3")
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_ten_field_stat_line_returns_none(self, tmp_path):
        # One field short of the eleven the derivations need.
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("100 10 2048 50 200 20 4096 90 3 1000")
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_non_numeric_stat_line_returns_none(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("a b c d e f g h i j k")
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_missing_block_device_does_not_raise(self):
        sampler = make_sampler(block_device=None)
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        for row in rows:
            for column in DISK_DERIVED_COLUMNS:
                assert row[column] == 0.0
            assert row["disk_in_flight"] == 0

    def test_unreadable_device_stat_does_not_raise(self):
        sampler = make_sampler(block_device="nvme0n1")
        with patch("metrics_sampler.read_disk_counters", return_value=None):
            rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        for column in DISK_DERIVED_COLUMNS:
            assert rows[1][column] == 0.0
        assert rows[1]["disk_in_flight"] == 0


class TestDiskDerivedStats:
    """Each derived disk column, against hand-computed expected values."""

    def test_iops_and_throughput(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 200 read ios and 50 write ios in 1s
        assert row["disk_read_iops"] == 200.0
        assert row["disk_write_iops"] == 50.0
        # 8192 sectors of 512 bytes is exactly 4 MiB, 3200 sectors is 1.5625
        assert row["disk_read_mb"] == 4.0
        assert row["disk_write_mb"] == 1.56

    def test_merge_rates(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 40 read merges and 15 write merges in 1s
        assert row["disk_read_merges_ps"] == 40.0
        assert row["disk_write_merges_ps"] == 15.0

    def test_await_is_ticks_per_io_not_per_second(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 600 read ticks over 200 read ios, 400 write ticks over 50 write ios
        assert row["disk_r_await_ms"] == 3.0
        assert row["disk_w_await_ms"] == 8.0

    def test_queue_depth_is_queued_ms_over_interval_ms(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 1500ms queued over a 1000ms interval
        assert row["disk_aqu_sz"] == 1.5

    def test_util_is_io_ticks_over_interval_ms(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 250ms busy over a 1000ms interval
        assert row["disk_util_pct"] == 25.0

    def test_in_flight_is_a_gauge(self):
        rows = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])
        # Read directly, so it is populated on the first sample and is not the
        # difference between the two snapshots (which would be 4).
        assert rows[0]["disk_in_flight"] == 3
        assert rows[1]["disk_in_flight"] == 7

    def test_request_size_combines_reads_and_writes(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # (8192 + 3200) sectors of 512 bytes over (200 + 50) ios, in KB
        assert row["disk_req_sz_kb"] == 22.78

    def test_every_derived_value_is_distinct(self):
        # A formula swap between any two of these would change a value.
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        values = [row[column] for column in DISK_DERIVED_COLUMNS]
        assert len(set(values)) == len(values)

    def test_interval_normalizes_the_rates(self):
        # The same deltas over 2s halve every rate, while the two awaits are
        # per-io and do not move.
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2], [100.0, 102.0])[1]
        assert row["disk_read_iops"] == 100.0
        assert row["disk_read_mb"] == 2.0
        assert row["disk_read_merges_ps"] == 20.0
        assert row["disk_aqu_sz"] == 0.75
        assert row["disk_util_pct"] == 12.5
        assert row["disk_r_await_ms"] == 3.0
        assert row["disk_w_await_ms"] == 8.0
        assert row["disk_req_sz_kb"] == 22.78

    def test_first_sample_yields_zero_for_every_derived_stat(self):
        row = disk_rows([DISK_SAMPLE_1])[0]
        for column in DISK_DERIVED_COLUMNS:
            assert row[column] == 0.0, f"{column} is {row[column]} on first sample"

    def test_zero_io_interval_zeroes_await_and_request_size(self):
        # Ticks advance while no io completes: work started in an earlier
        # interval is still in service.
        idle = dict(DISK_SAMPLE_2)
        idle.update(
            {
                "read_ticks": 750,
                "write_ticks": 890,
                "io_ticks": 1400,
                "time_in_queue": 7000,
            }
        )
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2, idle])[2]
        assert row["disk_r_await_ms"] == 0.0
        assert row["disk_w_await_ms"] == 0.0
        assert row["disk_req_sz_kb"] == 0.0
        assert row["disk_read_iops"] == 0.0
        assert row["disk_write_iops"] == 0.0
        # The device was still busy, so these are unaffected by the io count.
        assert row["disk_aqu_sz"] == 0.5
        assert row["disk_util_pct"] == 15.0

    def test_util_caps_at_one_hundred(self):
        # io ticks is wall-clock busy time on a concurrent queue, so the raw
        # ratio can exceed 1: 2500ms busy over a 1000ms interval is 250%.
        saturated = dict(DISK_SAMPLE_2)
        saturated["io_ticks"] = DISK_SAMPLE_1["io_ticks"] + 2500
        row = disk_rows([DISK_SAMPLE_1, saturated])[1]
        assert row["disk_util_pct"] == 100.0

    def test_counter_reset_clamps_to_zero(self):
        reset = {name: 0 for name in DISK_SAMPLE_1}
        row = disk_rows([DISK_SAMPLE_2, reset])[1]
        for column in DISK_DERIVED_COLUMNS:
            assert row[column] == 0.0
        assert row["disk_in_flight"] == 0


class TestWrite:
    def test_writes_json_array_of_rows(self, tmp_path):
        sampler = make_sampler(context={"commit": "abc123"})
        sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        output = tmp_path / "nested" / "timeseries.json"
        sampler.write(output)

        rows = json.loads(output.read_text())
        assert isinstance(rows, list)
        assert len(rows) == 2
        assert rows[0]["elapsed_sec"] == 0
        assert rows[1]["commit"] == "abc123"

    def test_writes_empty_array_when_no_samples(self, tmp_path):
        sampler = make_sampler()
        output = tmp_path / "timeseries.json"
        sampler.write(output)
        assert json.loads(output.read_text()) == []

    def test_unwritable_path_does_not_raise(self, tmp_path):
        sampler = make_sampler()
        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory")
        sampler.write(blocker / "timeseries.json")


class TestDisabled:
    def test_start_and_stop_are_noops(self):
        sampler = MetricsSampler(enabled=False, context={"commit": "abc123"})
        sampler.start()
        sampler.stop()
        assert sampler.rows == []
        assert sampler._sampler_thread is None

    def test_write_still_emits_empty_array(self, tmp_path):
        sampler = MetricsSampler(enabled=False)
        output = tmp_path / "timeseries.json"
        sampler.write(output)
        assert json.loads(output.read_text()) == []


class TestBackgroundThread:
    def test_start_stop_collects_on_background_thread(self):
        sampler = MetricsSampler(
            interval=0.01,
            context={"commit": "abc123"},
            server_pid=None,
            block_device="nvme0n1",
        )
        with patch.object(
            sampler, "_read_info", return_value=parse_info(INFO_WITH_TIERING)
        ):
            with patch("metrics_sampler.read_disk_counters", return_value=None):
                sampler.start()
                assert sampler._sampler_thread is not None
                # Wait for at least two ticks without asserting on wall-clock timing
                deadline = 5.0
                waited = 0.0
                while len(sampler.rows) < 2 and waited < deadline:
                    import time as _time

                    _time.sleep(0.01)
                    waited += 0.01
                sampler.stop()

        rows = sampler.rows
        assert len(rows) >= 2
        assert sampler._sampler_thread is None
        assert all(row["commit"] == "abc123" for row in rows)
        assert rows[0]["elapsed_sec"] == 0

    def test_sample_failure_does_not_kill_the_loop(self):
        sampler = MetricsSampler(interval=0.01, block_device=None)
        calls = {"count": 0}

        def flaky_info():
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("INFO exploded")
            return parse_info(INFO_WITH_TIERING)

        with patch.object(sampler, "_read_info", side_effect=flaky_info):
            sampler.start()
            deadline = 5.0
            waited = 0.0
            while len(sampler.rows) < 1 and waited < deadline:
                import time as _time

                _time.sleep(0.01)
                waited += 0.01
            sampler.stop()

        assert calls["count"] >= 2
        assert len(sampler.rows) >= 1

    def test_double_start_is_ignored(self):
        sampler = MetricsSampler(interval=0.01, block_device=None)
        with patch.object(sampler, "_read_info", return_value={}):
            sampler.start()
            first_thread = sampler._sampler_thread
            sampler.start()
            assert sampler._sampler_thread is first_thread
            sampler.stop()

    def test_stop_without_start_does_not_raise(self):
        sampler = MetricsSampler(block_device=None)
        sampler.stop()
        assert sampler.rows == []
