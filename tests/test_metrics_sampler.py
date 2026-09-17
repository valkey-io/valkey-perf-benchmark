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
completion_read_ok:250
dram_value_hits:1000
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
            "completion_read_ok",
            "dram_value_hits",
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


class TestDiskCounters:
    # /sys/block/<dev>/stat: rd_ios rd_merges rd_sectors rd_ticks
    #                        wr_ios wr_merges wr_sectors wr_ticks in_flight ...
    STAT = "100 0 2048 5 200 0 4096 9 0 12 34"

    def test_parses_counters(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text(self.STAT)
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") == {
                "read_ios": 100,
                "read_sectors": 2048,
                "write_ios": 200,
                "write_sectors": 4096,
            }

    def test_missing_device_returns_none(self, tmp_path):
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme9n9") is None

    def test_short_stat_line_returns_none(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("1 2 3")
        with patch("metrics_sampler._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_iops_and_throughput_deltas(self):
        sampler = make_sampler(block_device="nvme0n1")
        counters = [
            {
                "read_ios": 100,
                "read_sectors": 2048,
                "write_ios": 200,
                "write_sectors": 4096,
            },
            {
                "read_ios": 400,
                "read_sectors": 4096,
                "write_ios": 700,
                "write_sectors": 8192,
            },
        ]
        with patch("metrics_sampler.read_disk_counters", side_effect=counters):
            rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[0]["disk_read_iops"] == 0.0
        assert rows[1]["disk_read_iops"] == 300.0
        assert rows[1]["disk_write_iops"] == 500.0
        # 2048 sectors of 512 bytes is exactly 1 MiB in 1s
        assert rows[1]["disk_read_mb"] == 1.0
        assert rows[1]["disk_write_mb"] == 2.0

    def test_missing_block_device_does_not_raise(self):
        sampler = make_sampler(block_device=None)
        rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        for row in rows:
            assert row["disk_read_iops"] == 0.0
            assert row["disk_write_iops"] == 0.0
            assert row["disk_read_mb"] == 0.0
            assert row["disk_write_mb"] == 0.0

    def test_unreadable_device_stat_does_not_raise(self):
        sampler = make_sampler(block_device="nvme0n1")
        with patch("metrics_sampler.read_disk_counters", return_value=None):
            rows = sample_at(sampler, [100.0, 101.0], [INFO_WITH_TIERING] * 2)
        assert rows[1]["disk_read_iops"] == 0.0


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
