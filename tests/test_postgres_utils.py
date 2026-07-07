"""Unit tests for postgres utility pure logic functions.

Tests cover:
- _is_list_subset, _is_config_subset, _is_config_array_subset from utils/postgres_track_commits.py
- detect_field_type, analyze_metrics_schema, convert_metrics_to_rows, resolve_table_name from utils/push_to_postgres.py
"""

from datetime import datetime

from psycopg2.extras import Json

import pytest

from utils.postgres_track_commits import (
    _is_list_subset,
    _is_config_subset,
    _is_config_array_subset,
    _resolve_module_table_name,
    _extract_config_name,
    _load_config,
    CORE_TABLE_NAME,
)
from utils.push_to_postgres import (
    CONFIG_NAME_MAX_LENGTH,
    DESCRIPTION_MAX_LENGTH,
    analyze_metrics_schema,
    convert_metrics_to_rows,
    detect_field_type,
    resolve_table_name,
)

# ---------------------------------------------------------------------------
# _is_list_subset
# ---------------------------------------------------------------------------


class TestIsListSubset:
    def test_empty_is_subset_of_any(self):
        assert _is_list_subset([], [1, 2, 3]) is True

    def test_identical_lists(self):
        assert _is_list_subset([1, 2, 3], [1, 2, 3]) is True

    def test_proper_subset(self):
        assert _is_list_subset([1, 3], [1, 2, 3]) is True

    def test_not_a_subset(self):
        assert _is_list_subset([1, 4], [1, 2, 3]) is False

    def test_superset_is_not_subset(self):
        assert _is_list_subset([1, 2, 3, 4], [1, 2, 3]) is False

    def test_both_empty(self):
        assert _is_list_subset([], []) is True

    def test_non_list_subset_returns_false(self):
        assert _is_list_subset("not a list", [1, 2]) is False

    def test_non_list_superset_returns_false(self):
        assert _is_list_subset([1, 2], "not a list") is False

    def test_string_elements(self):
        assert _is_list_subset(["a", "b"], ["a", "b", "c"]) is True

    def test_string_elements_not_subset(self):
        assert _is_list_subset(["a", "d"], ["a", "b", "c"]) is False


# ---------------------------------------------------------------------------
# _is_config_subset
# ---------------------------------------------------------------------------


class TestIsConfigSubset:
    def test_identical_configs(self):
        cfg = {"key": "value", "num": 42}
        assert _is_config_subset(cfg, cfg) is True

    def test_subset_of_larger_config(self):
        subset = {"key": "value"}
        superset = {"key": "value", "extra": "data"}
        assert _is_config_subset(subset, superset) is True

    def test_missing_key_not_subset(self):
        subset = {"key": "value", "missing": "field"}
        superset = {"key": "value"}
        assert _is_config_subset(subset, superset) is False

    def test_different_value_not_subset(self):
        subset = {"key": "different"}
        superset = {"key": "value"}
        assert _is_config_subset(subset, superset) is False

    def test_list_field_subset(self):
        subset = {"sizes": [64]}
        superset = {"sizes": [64, 128, 256]}
        assert _is_config_subset(subset, superset) is True

    def test_list_field_not_subset(self):
        subset = {"sizes": [64, 512]}
        superset = {"sizes": [64, 128, 256]}
        assert _is_config_subset(subset, superset) is False

    def test_empty_subset(self):
        assert _is_config_subset({}, {"key": "value"}) is True

    def test_non_dict_subset_returns_false(self):
        assert _is_config_subset("not a dict", {"key": "value"}) is False

    def test_non_dict_superset_returns_false(self):
        assert _is_config_subset({"key": "value"}, "not a dict") is False

    def test_mixed_list_and_scalar_fields(self):
        subset = {"mode": "cluster", "sizes": [64], "threads": 4}
        superset = {"mode": "cluster", "sizes": [64, 128], "threads": 4, "tls": True}
        assert _is_config_subset(subset, superset) is True


# ---------------------------------------------------------------------------
# _is_config_array_subset
# ---------------------------------------------------------------------------


class TestIsConfigArraySubset:
    def test_matching_single_element(self):
        subset = [{"key": "value"}]
        superset = [{"key": "value", "extra": "data"}]
        assert _is_config_array_subset(subset, superset) is True

    def test_no_matching_element(self):
        subset = [{"key": "missing"}]
        superset = [{"key": "value"}]
        assert _is_config_array_subset(subset, superset) is False

    def test_multiple_elements_all_match(self):
        subset = [{"a": 1}, {"b": 2}]
        superset = [{"a": 1, "x": 10}, {"b": 2, "y": 20}]
        assert _is_config_array_subset(subset, superset) is True

    def test_one_element_no_match(self):
        subset = [{"a": 1}, {"c": 3}]
        superset = [{"a": 1, "x": 10}, {"b": 2, "y": 20}]
        assert _is_config_array_subset(subset, superset) is False

    def test_empty_subset_array(self):
        assert _is_config_array_subset([], [{"key": "value"}]) is True

    def test_non_list_subset_returns_false(self):
        assert _is_config_array_subset("not a list", [{"key": "value"}]) is False

    def test_non_list_superset_returns_false(self):
        assert _is_config_array_subset([{"key": "value"}], "not a list") is False


# ---------------------------------------------------------------------------
# detect_field_type
# ---------------------------------------------------------------------------


class TestDetectFieldType:
    def test_none_returns_text(self):
        assert detect_field_type(None) == "TEXT"

    def test_bool_returns_boolean(self):
        assert detect_field_type(True) == "BOOLEAN"
        assert detect_field_type(False) == "BOOLEAN"

    def test_int_returns_integer(self):
        assert detect_field_type(42) == "INTEGER"

    def test_float_returns_decimal(self):
        assert detect_field_type(3.14) == "DECIMAL(15,6)"

    def test_short_string_returns_varchar50(self):
        assert detect_field_type("GET") == "VARCHAR(50)"

    def test_medium_string_returns_varchar255(self):
        value = "a" * 100
        assert detect_field_type(value) == "VARCHAR(255)"

    def test_long_string_returns_text(self):
        value = "a" * 300
        assert detect_field_type(value) == "TEXT"


# ---------------------------------------------------------------------------
# analyze_metrics_schema
# ---------------------------------------------------------------------------


class TestAnalyzeMetricsSchema:
    def test_includes_core_fields(self):
        metrics = [
            {"rps": 150000.0, "timestamp": "2024-01-01T00:00:00", "commit": "abc123"}
        ]
        schema = analyze_metrics_schema(metrics)
        assert "id" in schema
        assert "created_at" in schema

    def test_includes_all_data_fields(self):
        metrics = [
            {
                "rps": 150000.0,
                "avg_latency_ms": 0.5,
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc123",
                "command": "GET",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert "rps" in schema
        assert "avg_latency_ms" in schema
        assert "timestamp" in schema
        assert "commit" in schema
        assert "command" in schema

    def test_timestamp_type(self):
        metrics = [{"timestamp": "2024-01-01T00:00:00", "commit": "abc"}]
        schema = analyze_metrics_schema(metrics)
        assert schema["timestamp"] == "TIMESTAMPTZ NOT NULL"

    def test_commit_and_command_types(self):
        metrics = [
            {"timestamp": "2024-01-01T00:00:00", "commit": "abc", "command": "GET"}
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["commit"] == "VARCHAR(255) NOT NULL"
        assert schema["command"] == "VARCHAR(255) NOT NULL"

    def test_numeric_field_types(self):
        metrics = [{"rps": 150000.0, "pipeline": 1, "timestamp": "t", "commit": "c"}]
        schema = analyze_metrics_schema(metrics)
        assert schema["rps"] == "DECIMAL(15,6)"
        assert schema["pipeline"] == "INTEGER"

    def test_group_description_uses_max_length(self):

        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "group_description": "small payload latency suite",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["group_description"] == f"VARCHAR({DESCRIPTION_MAX_LENGTH})"

    def test_scenario_description_uses_max_length(self):

        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "scenario_description": "GET, 64B payload, pipeline=1, 50 clients",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["scenario_description"] == f"VARCHAR({DESCRIPTION_MAX_LENGTH})"

    def test_long_description_still_uses_max_length(self):

        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "group_description": "x" * 400,
                "scenario_description": "y" * 10,
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["group_description"] == f"VARCHAR({DESCRIPTION_MAX_LENGTH})"
        assert schema["scenario_description"] == f"VARCHAR({DESCRIPTION_MAX_LENGTH})"


# ---------------------------------------------------------------------------
# convert_metrics_to_rows
# ---------------------------------------------------------------------------


class TestConvertMetricsToRows:
    def test_basic_conversion(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc123",
                "command": "GET",
                "rps": 150000.0,
            }
        ]
        columns = ["id", "created_at", "timestamp", "commit", "command", "rps"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == 1
        assert skipped == 0
        assert len(rows[0]) == 4
        assert rows[0][-1] == 150000.0

    def test_skips_missing_timestamp(self):
        metrics = [{"commit": "abc123", "command": "GET"}]
        columns = ["timestamp", "commit", "command"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == 0
        assert skipped == 1

    def test_skips_missing_commit(self):
        metrics = [{"timestamp": "2024-01-01T00:00:00", "command": "GET"}]
        columns = ["timestamp", "commit", "command"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == 0
        assert skipped == 1

    def test_skips_none_entry(self):
        metrics = [None, {"timestamp": "2024-01-01T00:00:00", "commit": "abc"}]
        columns = ["timestamp", "commit"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == 1
        assert skipped == 1

    def test_column_order_respected(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "command": "SET",
                "rps": 100.0,
            }
        ]
        columns = ["command", "rps", "timestamp", "commit"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert rows[0][0] == "SET"
        assert rows[0][1] == 100.0

    def test_timestamp_parsed_to_datetime(self):
        metrics = [{"timestamp": "2024-01-01T00:00:00", "commit": "abc"}]
        columns = ["timestamp", "commit"]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert isinstance(rows[0][0], datetime)

    def test_descriptions_over_max_length_truncated(self):

        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "group_description": "g" * (DESCRIPTION_MAX_LENGTH + 250),
                "scenario_description": "s" * (DESCRIPTION_MAX_LENGTH + 100),
            }
        ]
        columns = [
            "timestamp",
            "commit",
            "group_description",
            "scenario_description",
        ]
        rows, _ = convert_metrics_to_rows(metrics, columns)
        assert len(rows[0][2]) == DESCRIPTION_MAX_LENGTH
        assert rows[0][2] == "g" * DESCRIPTION_MAX_LENGTH
        assert len(rows[0][3]) == DESCRIPTION_MAX_LENGTH
        assert rows[0][3] == "s" * DESCRIPTION_MAX_LENGTH


# ---------------------------------------------------------------------------
# module_commit and config_name schema handling
# ---------------------------------------------------------------------------


class TestModuleCommitSchema:
    def test_module_commit_gets_varchar255(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc123",
                "module_commit": "def456",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["module_commit"] == "VARCHAR(255)"

    def test_module_commit_varchar255_regardless_of_length(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "module_commit": "ab",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["module_commit"] == "VARCHAR(255)"

    def test_module_commit_not_in_schema_when_absent(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "rps": 100000.0,
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert "module_commit" not in schema

    def test_config_name_gets_varchar_max_length(self):

        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "config_name": "fts-benchmarks-arm.json",
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert schema["config_name"] == f"VARCHAR({CONFIG_NAME_MAX_LENGTH})"

    def test_config_name_not_in_schema_when_absent(self):
        metrics = [
            {
                "timestamp": "2024-01-01T00:00:00",
                "commit": "abc",
                "rps": 100000.0,
            }
        ]
        schema = analyze_metrics_schema(metrics)
        assert "config_name" not in schema


# ---------------------------------------------------------------------------
# resolve_table_name
# ---------------------------------------------------------------------------


class TestResolveTableName:

    def test_explicit_table_name_takes_precedence(self):
        assert resolve_table_name("custom_table", "search") == "custom_table"

    def test_module_generates_table_name(self):
        assert resolve_table_name(None, "search") == "benchmark_metrics_search"

    def test_neither_provided_returns_none(self):
        assert resolve_table_name(None, None) is None


# ---------------------------------------------------------------------------
# _resolve_module_table_name (postgres_track_commits)
# ---------------------------------------------------------------------------


class TestResolveModuleTableName:
    def test_module_name_generates_table(self):
        assert _resolve_module_table_name("search") == "benchmark_module_commits_search"

    def test_none_returns_core_table_name(self):
        assert _resolve_module_table_name(None) == CORE_TABLE_NAME

    def test_whitespace_only_raises_value_error(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            _resolve_module_table_name("   ")


# ---------------------------------------------------------------------------
# _extract_config_name (postgres_track_commits)
# ---------------------------------------------------------------------------


class TestExtractConfigName:
    def test_extracts_name_from_path(self):
        assert (
            _extract_config_name("configs/fts-benchmarks-arm.json")
            == "fts-benchmarks-arm.json"
        )

    def test_extracts_name_from_filename_only(self):
        assert _extract_config_name("my-config.json") == "my-config.json"

    def test_none_returns_none(self):
        assert _extract_config_name(None) is None


# ---------------------------------------------------------------------------
# _load_config (postgres_track_commits)
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_no_config_file_returns_none(self):
        assert _load_config(None) is None

    def test_no_config_file_with_module_returns_none(self):
        assert _load_config(None, module_name="search") is None

    def test_loads_list_config_without_module(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text(
            '[{"test_name": "FTS", "test_groups": [1, 2], "port": 6379}]'
        )
        result = _load_config(str(config_file), None)
        assert isinstance(result, list)
        assert len(result) == 1
        assert "test_groups" in result[0]
        assert "config_name" not in result[0]

    def test_loads_dict_config_without_module(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text('{"test_name": "FTS", "test_groups": [1, 2]}')
        result = _load_config(str(config_file), None)
        assert isinstance(result, dict)
        assert "test_groups" in result
        assert "config_name" not in result

    def test_module_list_strips_keys_and_adds_config_name(self, tmp_path):
        config_file = tmp_path / "fts-benchmarks-arm.json"
        config_file.write_text(
            '[{"test_name": "FTS", "test_groups": [1], '
            '"dataset_generation": {"x": 1}, "query_generation": {"y": 2}, '
            '"port": 6379}]'
        )
        result = _load_config(str(config_file), module_name="search")
        assert isinstance(result, list)
        # config_name dict prepended
        assert result[0] == {"config_name": "fts-benchmarks-arm.json"}
        # original dict stripped of large keys
        assert "test_groups" not in result[1]
        assert "dataset_generation" not in result[1]
        assert "query_generation" not in result[1]
        # keeps other keys
        assert result[1]["test_name"] == "FTS"
        assert result[1]["port"] == 6379

    def test_module_dict_strips_keys_and_adds_config_name(self, tmp_path):
        config_file = tmp_path / "my-config.json"
        config_file.write_text(
            '{"test_name": "FTS", "test_groups": [1], '
            '"dataset_generation": {"x": 1}, "query_generation": {"y": 2}, '
            '"port": 6379}'
        )
        result = _load_config(str(config_file), module_name="search")
        assert isinstance(result, dict)
        assert result["config_name"] == "my-config.json"
        assert "test_groups" not in result
        assert "dataset_generation" not in result
        assert "query_generation" not in result
        assert result["test_name"] == "FTS"
        assert result["port"] == 6379

    def test_module_list_multiple_dicts_all_stripped(self, tmp_path):
        config_file = tmp_path / "multi.json"
        config_file.write_text(
            '[{"test_name": "A", "test_groups": [1]}, '
            '{"test_name": "B", "dataset_generation": {"x": 1}}]'
        )
        result = _load_config(str(config_file), module_name="search")
        # config_name prepended + 2 stripped dicts
        assert len(result) == 3
        assert result[0] == {"config_name": "multi.json"}
        assert "test_groups" not in result[1]
        assert "dataset_generation" not in result[2]
        assert result[1]["test_name"] == "A"
        assert result[2]["test_name"] == "B"

    def test_cluster_mode_override_true(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text('[{"cluster_mode": [false, true], "port": 6379}]')
        result = _load_config(str(config_file), cluster_mode="true")
        assert result[0]["cluster_mode"] is True
        assert result[0]["port"] == 6379

    def test_cluster_mode_filter_none_keeps_original(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text('[{"cluster_mode": [false, true], "port": 6379}]')
        result = _load_config(str(config_file), cluster_mode=None)
        assert result[0]["cluster_mode"] == [False, True]

    def test_skip_config_set_overwrites_with_default(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text('[{"config_sets": [{"threads": 1}], "port": 6379}]')
        result = _load_config(str(config_file), skip_config_set=True)
        assert result[0]["config_sets"] == [{}]
        assert result[0]["port"] == 6379

    def test_skip_config_set_false_keeps_field(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text('[{"config_sets": [{"threads": 1}], "port": 6379}]')
        result = _load_config(str(config_file), skip_config_set=False)
        assert "config_sets" in result[0]

    def test_skip_profiling_overwrites_with_default(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text(
            '[{"profiling_sets": [{"enabled": false}, '
            '{"enabled": true, "mode": "wall-time", "sampling_freq": 999}], '
            '"port": 6379}]'
        )
        result = _load_config(str(config_file), skip_profiling=True)
        assert result[0]["profiling_sets"] == [{"enabled": False}]
        assert result[0]["port"] == 6379

    def test_skip_profiling_false_keeps_original(self, tmp_path):
        config_file = tmp_path / "test.json"
        config_file.write_text(
            '[{"profiling_sets": [{"enabled": false}, '
            '{"enabled": true, "mode": "wall-time"}], "port": 6379}]'
        )
        result = _load_config(str(config_file), skip_profiling=False)
        assert len(result[0]["profiling_sets"]) == 2
        assert result[0]["profiling_sets"][1]["enabled"] is True
