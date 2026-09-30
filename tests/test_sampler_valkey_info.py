"""Unit tests for the Valkey INFO sample source.

No live server: INFO output is canned and the CLI call is patched.
"""

from unittest.mock import patch

from samplers.base import SamplerContext
from samplers.valkey_info import (
    TIERING_INFO_FIELDS,
    TIERING_INFO_FLOAT_FIELDS,
    ValkeyInfoSource,
    info_snapshot,
    parse_info,
)

# A tiering-enabled INFO snapshot, trimmed to the fields the source reads.
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

INFO_COLUMNS = (
    (
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
        "info",
    )
    + TIERING_INFO_FIELDS
    + TIERING_INFO_FLOAT_FIELDS
)

MAIN_THREAD_CPU_COLUMNS = ("valkey_cpu_user", "valkey_cpu_sys", "valkey_cpu_total")


def info_with_main_thread_cpu(user_seconds, sys_seconds):
    """Return the tiering INFO snapshot plus the main thread CPU seconds."""
    return (
        INFO_WITH_TIERING
        + f"\n# CPU\nused_cpu_user_main_thread:{user_seconds}\n"
        + f"used_cpu_sys_main_thread:{sys_seconds}\n"
    )


def info_with_counters(dram_value_hits, completion_read_ok):
    """Return the tiering INFO snapshot with the two ratio inputs overridden."""
    return INFO_WITH_TIERING.replace(
        "completion_read_ok:250", f"completion_read_ok:{completion_read_ok}"
    ).replace("dram_value_hits:1000", f"dram_value_hits:{dram_value_hits}")


def make_source(**ctx_overrides):
    """Build a started ValkeyInfoSource with warnings discarded."""
    kwargs = {"warn_once": lambda key, message: None}
    kwargs.update(ctx_overrides)
    source = ValkeyInfoSource()
    source.start(SamplerContext(**kwargs))
    return source


def source_rows(info_texts, times=None):
    """Sample once per INFO text, at `times` seconds on the monotonic clock."""
    if times is None:
        times = [100.0 + index for index in range(len(info_texts))]
    source = make_source()
    rows = []
    for now, info_text in zip(times, info_texts):
        with patch.object(source, "read_info", return_value=parse_info(info_text)):
            rows.append(source.sample(now))
    return rows


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


class TestInfoSnapshot:
    """The full INFO reply, with numeric and k=v values converted."""

    def test_integer_value_becomes_int(self):
        snapshot = info_snapshot({"used_memory": "1799288"})
        assert snapshot["used_memory"] == 1799288
        assert isinstance(snapshot["used_memory"], int)

    def test_float_value_becomes_float(self):
        snapshot = info_snapshot({"mem_fragmentation_ratio": "3.96"})
        assert snapshot["mem_fragmentation_ratio"] == 3.96
        assert isinstance(snapshot["mem_fragmentation_ratio"], float)

    def test_negative_values_convert(self):
        snapshot = info_snapshot({"maxmemory": "-1", "drift": "-0.5"})
        assert snapshot["maxmemory"] == -1
        assert snapshot["drift"] == -0.5

    def test_version_string_stays_a_string(self):
        snapshot = info_snapshot({"valkey_version": "8.0.1"})
        assert snapshot["valkey_version"] == "8.0.1"

    def test_non_numeric_string_stays_a_string(self):
        snapshot = info_snapshot({"role": "primary"})
        assert snapshot["role"] == "primary"

    def test_keyspace_line_becomes_nested_dict(self):
        snapshot = info_snapshot({"db0": "keys=5,expires=0,avg_ttl=0"})
        assert snapshot["db0"] == {"keys": 5, "expires": 0, "avg_ttl": 0}

    def test_cmdstat_line_nests_with_float_per_call(self):
        snapshot = info_snapshot(
            {
                "cmdstat_get": (
                    "calls=10,usec=25,usec_per_call=2.50,"
                    "rejected_calls=0,failed_calls=0"
                )
            }
        )
        assert snapshot["cmdstat_get"] == {
            "calls": 10,
            "usec": 25,
            "usec_per_call": 2.5,
            "rejected_calls": 0,
            "failed_calls": 0,
        }

    def test_duplicate_sub_key_keeps_the_last_value(self):
        snapshot = info_snapshot({"db0": "keys=5,keys=9"})
        assert snapshot["db0"] == {"keys": 9}

    def test_keys_are_verbatim(self):
        snapshot = info_snapshot(parse_info(INFO_WITH_TIERING))
        assert set(snapshot) == set(parse_info(INFO_WITH_TIERING))

    def test_emitted_on_every_row(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["info"]["used_memory"] == 1799288
        assert row["info"]["throttle_current_rate"] == 0.8125

    def test_empty_when_info_is_unavailable(self):
        row = source_rows([""])[0]
        assert row["info"] == {}


class TestGaugeFields:
    def test_info_gauges_land_on_row(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["used_memory"] == 1799288
        assert row["used_memory_rss"] == 6959104
        assert row["maxmemory"] == 1073741824
        assert row["mem_frag_ratio"] == 3.96
        assert row["keyspace_hits"] == 800
        assert row["keyspace_misses"] == 200
        assert row["blocked_clients"] == 2

    def test_tiering_fields_use_info_field_names(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["total_num_items_spilled_to_ext_storage"] == 5000
        assert row["total_num_items_fetched_from_ext_storage"] == 1200
        assert row["num_items_spilling_to_ext_storage"] == 7

    def test_blocked_on_fetch_field_emitted(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["kbc_fetching_block"] == 4

    def test_throttle_counters_parsed_as_int(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["throttle_total_throttled"] == 900
        assert row["throttle_queued_clients"] == 13

    def test_throttle_rates_parsed_as_float(self):
        # The engine formats current_rate %.4f and allowed_tps %.1f, so an int
        # parse would floor both to 0 and 45000.
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["throttle_current_rate"] == 0.8125
        assert row["throttle_allowed_tps"] == 45000.5
        assert isinstance(row["throttle_current_rate"], float)
        assert isinstance(row["throttle_allowed_tps"], float)

    def test_spill_pipeline_fields_emitted(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["spill_attempts"] == 6100
        assert row["spill_serialized_count"] == 5850
        assert row["mean_spill_ram"] == 2048
        assert row["inflight_spill_ram_bytes"] == 102400
        assert row["oom_reject_write_count"] == 11

    def test_spill_submitted_count_not_spill_submitted(self):
        # The engine emits both names as separate counters. The column is the
        # atomic that pairs with spill_serialized_count, so the fixture gives
        # the two different values and this pins which one is read.
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["spill_submitted_count"] == 5900
        assert "spill_submitted" not in row

    def test_raw_ratio_inputs_emitted(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        # Emitted alongside the ratios so a later reader can tell a real
        # movement apart from a formula or divide-by-zero bug.
        assert row["completion_read_ok"] == 250
        assert row["dram_value_hits"] == 1000

    def test_derived_hit_ratios(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        # completion_read_ok 250 of dram_value_hits 1000
        assert row["disk_hit_pct"] == 25.0
        assert row["mem_hit_pct"] == 75.0

    def test_every_info_column_present(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert set(INFO_COLUMNS) <= set(row)


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
        for row in source_rows(self.SERIES):
            assert {
                "disk_hit_pct",
                "mem_hit_pct",
                "disk_hit_pct_interval",
                "mem_hit_pct_interval",
            } <= set(row)

    def test_cumulative_form_matches_running_totals(self):
        rows = source_rows(self.SERIES)
        assert [row["disk_hit_pct"] for row in rows] == [25.0, 62.5, 41.67]
        assert [row["mem_hit_pct"] for row in rows] == [75.0, 37.5, 58.33]

    def test_interval_form_matches_consecutive_deltas(self):
        rows = source_rows(self.SERIES)
        # First sample has no predecessor, then 1000/1000 and 0/1000
        assert [row["disk_hit_pct_interval"] for row in rows] == [0.0, 100.0, 0.0]
        assert [row["mem_hit_pct_interval"] for row in rows] == [0.0, 0.0, 100.0]

    def test_interval_form_differs_from_cumulative_when_rate_changes(self):
        for row in source_rows(self.SERIES)[1:]:
            assert row["disk_hit_pct_interval"] != row["disk_hit_pct"]
            assert row["mem_hit_pct_interval"] != row["mem_hit_pct"]

    def test_interval_form_is_zero_on_first_sample(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["disk_hit_pct_interval"] == 0.0
        assert row["mem_hit_pct_interval"] == 0.0

    def test_zero_denominator_yields_zero_for_both_forms(self):
        # The reference CSV emits 0.0, not null, while dram_value_hits is 0.
        rows = source_rows([info_with_counters(0, 0), info_with_counters(0, 0)])
        for row in rows:
            assert row["disk_hit_pct"] == 0.0
            assert row["mem_hit_pct"] == 0.0
            assert row["disk_hit_pct_interval"] == 0.0
            assert row["mem_hit_pct_interval"] == 0.0

    def test_flat_counters_yield_zero_interval_ratios(self):
        # Cumulative stays put while the interval denominator is 0.
        rows = source_rows([INFO_WITH_TIERING] * 2)
        assert rows[1]["disk_hit_pct"] == 25.0
        assert rows[1]["disk_hit_pct_interval"] == 0.0
        assert rows[1]["mem_hit_pct_interval"] == 0.0


class TestCommandDeltas:
    def test_first_sample_has_no_delta(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert row["total_commands_delta"] == 0
        assert row["ops_per_sec"] == 0.0

    def test_delta_across_two_samples(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        rows = source_rows([INFO_WITH_TIERING, second])
        assert rows[1]["total_commands_delta"] == 2000
        assert rows[1]["ops_per_sec"] == 2000.0

    def test_ops_per_sec_normalized_by_measured_interval(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        # 2s of wall clock between samples, so the same delta halves the rate
        rows = source_rows([INFO_WITH_TIERING, second], [100.0, 102.0])
        assert rows[1]["total_commands_delta"] == 2000
        assert rows[1]["ops_per_sec"] == 1000.0

    def test_failed_read_widens_the_next_interval(self):
        third = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        # INFO is unavailable at 101.0, so the 2000-command delta measured at
        # 102.0 spans the full 2s gap since the last successful read.
        rows = source_rows([INFO_WITH_TIERING, "", third], [100.0, 101.0, 102.0])
        assert rows[1]["total_commands_delta"] == 0
        assert rows[1]["ops_per_sec"] == 0.0
        assert rows[2]["total_commands_delta"] == 2000
        assert rows[2]["ops_per_sec"] == 1000.0

    def test_failed_read_does_not_reset_the_baseline(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:3000"
        )
        rows = source_rows([INFO_WITH_TIERING, "", second], [100.0, 101.0, 102.0])
        # A cleared baseline would report the whole running total of 3000.
        assert rows[2]["total_commands_delta"] == 2000

    def test_counter_reset_clamps_to_zero(self):
        second = INFO_WITH_TIERING.replace(
            "total_commands_processed:1000", "total_commands_processed:5"
        )
        rows = source_rows([INFO_WITH_TIERING, second])
        assert rows[1]["total_commands_delta"] == 0
        assert rows[1]["ops_per_sec"] == 0.0


class TestMissingTieringSection:
    def test_tiering_fields_are_zero_not_missing(self):
        row = source_rows([INFO_WITHOUT_TIERING])[0]
        for column in TIERING_INFO_FIELDS:
            assert row[column] == 0

    def test_tiering_float_fields_are_zero_not_missing(self):
        row = source_rows([INFO_WITHOUT_TIERING])[0]
        for column in TIERING_INFO_FLOAT_FIELDS:
            assert row[column] == 0.0

    def test_hit_ratios_are_zero_without_dram_value_hits(self):
        row = source_rows([INFO_WITHOUT_TIERING])[0]
        assert row["disk_hit_pct"] == 0.0
        assert row["mem_hit_pct"] == 0.0
        assert row["disk_hit_pct_interval"] == 0.0
        assert row["mem_hit_pct_interval"] == 0.0

    def test_non_tiering_fields_still_collected(self):
        row = source_rows([INFO_WITHOUT_TIERING])[0]
        assert row["used_memory"] == 1799288
        assert row["mem_frag_ratio"] == 1.21

    def test_empty_info_yields_zeros_without_raising(self):
        row = source_rows([""])[0]
        assert row["used_memory"] == 0
        assert row["mem_frag_ratio"] == 0.0
        assert row["total_num_items_spilled_to_ext_storage"] == 0

    def test_unparsable_values_fall_back_to_zero(self):
        row = source_rows(["used_memory:not_a_number\n"])[0]
        assert row["used_memory"] == 0


class TestInfoCommandFailure:
    def test_cli_missing_yields_empty_info_not_exception(self):
        source = make_source(cli_path="/nonexistent/valkey-cli")
        assert source.read_info() == {}

    @patch("samplers.base.subprocess.run")
    def test_nonzero_exit_yields_empty_info(self, mock_run):
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "Could not connect"
        assert make_source().read_info() == {}

    @patch("samplers.base.subprocess.run")
    def test_successful_call_parses_stdout(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = INFO_WITH_TIERING
        mock_run.return_value.stderr = ""
        assert make_source().read_info()["used_memory"] == "1799288"


class TestMainThreadCpu:
    """`valkey_cpu_*` derived from the main thread CPU seconds INFO reports."""

    def test_columns_absent_when_info_omits_the_fields(self):
        for row in source_rows([INFO_WITH_TIERING, INFO_WITH_TIERING]):
            for column in MAIN_THREAD_CPU_COLUMNS:
                assert column not in row

    def test_first_sample_is_zero(self):
        row = source_rows([info_with_main_thread_cpu(10.0, 4.0)])[0]
        assert row["valkey_cpu_user"] == 0.0
        assert row["valkey_cpu_sys"] == 0.0
        assert row["valkey_cpu_total"] == 0.0

    def test_percent_across_two_samples(self):
        rows = source_rows(
            [info_with_main_thread_cpu(10.0, 4.0), info_with_main_thread_cpu(10.8, 4.2)]
        )
        # 0.8s of user and 0.2s of system time over a 1s interval
        assert rows[1]["valkey_cpu_user"] == 80.0
        assert rows[1]["valkey_cpu_sys"] == 20.0
        assert rows[1]["valkey_cpu_total"] == 100.0

    def test_percent_normalized_by_the_interval(self):
        rows = source_rows(
            [
                info_with_main_thread_cpu(10.0, 4.0),
                info_with_main_thread_cpu(10.8, 4.2),
            ],
            [100.0, 102.0],
        )
        assert rows[1]["valkey_cpu_user"] == 40.0
        assert rows[1]["valkey_cpu_sys"] == 10.0

    def test_can_exceed_one_hundred_percent_in_total(self):
        rows = source_rows(
            [info_with_main_thread_cpu(10.0, 4.0), info_with_main_thread_cpu(11.5, 4.5)]
        )
        assert rows[1]["valkey_cpu_total"] == 200.0

    def test_failed_read_widens_the_next_interval(self):
        rows = source_rows(
            [
                info_with_main_thread_cpu(10.0, 4.0),
                "",
                info_with_main_thread_cpu(11.6, 4.4),
            ],
            [100.0, 101.0, 102.0],
        )
        # 1.6s of user time over the full 2s gap since the last good read
        assert rows[2]["valkey_cpu_user"] == 80.0
        assert rows[2]["valkey_cpu_sys"] == 20.0

    def test_failed_read_keeps_the_columns_at_zero(self):
        rows = source_rows([info_with_main_thread_cpu(10.0, 4.0), ""], [100.0, 101.0])
        for column in MAIN_THREAD_CPU_COLUMNS:
            assert rows[1][column] == 0.0

    def test_flags_the_context_so_proc_cpu_stands_down(self):
        source = make_source()
        assert source.ctx.main_thread_cpu_from_info is False
        with patch.object(
            source,
            "read_info",
            return_value=parse_info(info_with_main_thread_cpu(10.0, 4.0)),
        ):
            source.sample(100.0)
        assert source.ctx.main_thread_cpu_from_info is True

    def test_context_flag_stays_unset_without_the_fields(self):
        source = make_source()
        with patch.object(
            source, "read_info", return_value=parse_info(INFO_WITH_TIERING)
        ):
            source.sample(100.0)
        assert source.ctx.main_thread_cpu_from_info is False
