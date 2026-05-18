"""Unit tests for postgres utility pure logic functions.

Tests cover:
- _is_list_subset, _is_config_subset, _is_config_array_subset from utils/postgres_track_commits.py
- detect_field_type, analyze_metrics_schema, convert_metrics_to_rows from utils/push_to_postgres.py
"""

from datetime import datetime


from utils.postgres_track_commits import (
    _is_list_subset,
    _is_config_subset,
    _is_config_array_subset,
)
from utils.push_to_postgres import (
    detect_field_type,
    analyze_metrics_schema,
    convert_metrics_to_rows,
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
