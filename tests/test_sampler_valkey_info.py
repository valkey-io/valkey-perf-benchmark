"""Unit tests for the Valkey INFO sample source.

No live server: INFO output is canned and the CLI call is patched.
"""

from unittest.mock import patch

from samplers.base import SamplerContext
from samplers.valkey_info import (
    TIERING_INFO_FIELDS,
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

# Ext_storage
ext_storage_enabled:1
ext_storage_engine:rocksdb
ext_storage_api_version:1
ext_storage_capacity_bytes:1099511627776
ext_storage_total_num_items:40000
ext_storage_total_num_bytes:81920000
ext_storage_total_num_items_spilled_to_storage:5000
ext_storage_total_num_items_fetched_from_storage:1200
ext_storage_total_num_items_deleted_from_storage:300
"""

# A tiering-disabled server, which reports the enabled flag as the only line
# of the section.
INFO_TIERING_DISABLED = """# Memory
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

# Ext_storage
ext_storage_enabled:0
"""

# A server built without tiering, which omits the section entirely.
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
    "used_memory",
    "used_memory_rss",
    "maxmemory",
    "mem_frag_ratio",
    "ops_per_sec",
    "total_commands_delta",
    "keyspace_hits",
    "keyspace_misses",
    "blocked_clients",
    "info",
) + TIERING_INFO_FIELDS

MAIN_THREAD_CPU_COLUMNS = ("valkey_cpu_user", "valkey_cpu_sys", "valkey_cpu_total")


def info_with_main_thread_cpu(user_seconds, sys_seconds):
    """Return the tiering INFO snapshot plus the main thread CPU seconds."""
    return (
        INFO_WITH_TIERING
        + f"\n# CPU\nused_cpu_user_main_thread:{user_seconds}\n"
        + f"used_cpu_sys_main_thread:{sys_seconds}\n"
    )


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
        assert fields["ext_storage_total_num_items"] == "40000"
        assert not any(key.startswith("#") for key in fields)

    def test_flattens_all_sections(self):
        fields = parse_info(INFO_WITH_TIERING)
        # memory, clients, stats and ext_storage fields coexist flat
        assert {
            "used_memory",
            "blocked_clients",
            "keyspace_hits",
            "ext_storage_enabled",
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
        assert row["info"]["ext_storage_engine"] == "rocksdb"

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
        assert row["ext_storage_enabled"] == 1
        assert row["ext_storage_capacity_bytes"] == 1099511627776
        assert row["ext_storage_total_num_items"] == 40000
        assert row["ext_storage_total_num_bytes"] == 81920000
        assert row["ext_storage_total_num_items_spilled_to_storage"] == 5000
        assert row["ext_storage_total_num_items_fetched_from_storage"] == 1200
        assert row["ext_storage_total_num_items_deleted_from_storage"] == 300

    def test_string_and_version_fields_stay_in_the_snapshot(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert "ext_storage_engine" not in row
        assert "ext_storage_api_version" not in row
        assert row["info"]["ext_storage_engine"] == "rocksdb"
        assert row["info"]["ext_storage_api_version"] == 1

    def test_every_info_column_present(self):
        row = source_rows([INFO_WITH_TIERING])[0]
        assert set(INFO_COLUMNS) <= set(row)


class TestTieringDisabled:
    """The enabled flag is the only line of the section when tiering is off."""

    def test_enabled_flag_is_zero(self):
        row = source_rows([INFO_TIERING_DISABLED])[0]
        assert row["ext_storage_enabled"] == 0

    def test_remaining_columns_are_zero(self):
        row = source_rows([INFO_TIERING_DISABLED])[0]
        for column in TIERING_INFO_FIELDS:
            if column == "ext_storage_enabled":
                continue
            assert row[column] == 0

    def test_non_tiering_fields_still_collected(self):
        row = source_rows([INFO_TIERING_DISABLED])[0]
        assert row["used_memory"] == 1799288
        assert row["mem_frag_ratio"] == 1.21


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

    def test_non_tiering_fields_still_collected(self):
        row = source_rows([INFO_WITHOUT_TIERING])[0]
        assert row["used_memory"] == 1799288
        assert row["mem_frag_ratio"] == 1.21

    def test_empty_info_yields_zeros_without_raising(self):
        row = source_rows([""])[0]
        assert row["used_memory"] == 0
        assert row["mem_frag_ratio"] == 0.0
        assert row["ext_storage_total_num_items_spilled_to_storage"] == 0

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
