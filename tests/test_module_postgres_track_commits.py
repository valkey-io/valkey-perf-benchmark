"""Unit tests for utils/module_postgres_track_commits.py — pure logic only.

Tests cover:
- _parse_timestamp
- get_config_name
- _module_table_name
- _is_config_sets_subset
- CommitPair
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from psycopg2.extras import Json

from utils.module_postgres_track_commits import (
    CommitPair,
    get_config_name,
    _module_table_name,
    _is_config_sets_subset,
    _parse_timestamp,
)

# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------


class TestParseTimestamp:
    def test_iso_with_utc_offset(self):
        result = _parse_timestamp("2026-06-01T10:00:00+00:00")
        assert result == datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc)

    def test_iso_with_z_suffix(self):
        result = _parse_timestamp("2026-06-01T10:00:00Z")
        assert result == datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc)

    def test_passthrough_datetime_object(self):
        dt = datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        result = _parse_timestamp(dt)
        assert result is dt

    def test_naive_timestamp_no_timezone(self):
        with pytest.raises(ValueError, match="missing timezone"):
            _parse_timestamp("2026-06-01T10:00:00")

    def test_naive_datetime_object_raises(self):
        dt = datetime(2026, 6, 1, 10, 0, 0)
        with pytest.raises(ValueError, match="missing timezone"):
            _parse_timestamp(dt)

    def test_non_utc_offset(self):
        result = _parse_timestamp("2026-06-01T10:00:00-05:00")
        assert result.utcoffset().total_seconds() == -5 * 3600

    def test_cross_timezone_comparison(self):
        """Timestamps with different offsets compare correctly by absolute time."""
        # 10:00 UTC = 15:00 +05:00 — same instant, different representation
        utc = _parse_timestamp("2026-06-01T10:00:00+00:00")
        plus5 = _parse_timestamp("2026-06-01T15:00:00+05:00")
        assert utc == plus5

        # 11:00 UTC > 10:00 UTC regardless of offset representation
        later_utc = _parse_timestamp("2026-06-01T11:00:00+00:00")
        earlier_minus5 = _parse_timestamp("2026-06-01T05:00:00-05:00")  # = 10:00 UTC
        assert later_utc > earlier_minus5
        assert max(later_utc, earlier_minus5) == later_utc

    def test_malformed_string_raises(self):
        with pytest.raises(ValueError):
            _parse_timestamp("not-a-timestamp")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _parse_timestamp("")

    def test_none_raises(self):
        with pytest.raises(ValueError):
            _parse_timestamp(None)


# ---------------------------------------------------------------------------
# get_config_name
# ---------------------------------------------------------------------------


class TestGetConfigName:
    def test_extracts_filename_from_relative_path(self):
        path = "../valkey-search/.github/benchmark_configs/fts-benchmarks-arm.json"
        assert get_config_name(path) == "fts-benchmarks-arm.json"


# ---------------------------------------------------------------------------
# _module_table_name
# ---------------------------------------------------------------------------


class TestModuleTableName:

    def test_arbitrary_name(self):
        assert _module_table_name("my_module") == "benchmark_module_commits_my_module"

    def test_sql_injection_drop_table(self):
        with pytest.raises(ValueError):
            _module_table_name("search; DROP TABLE benchmark_commits;")

    def test_sql_injection_quotes(self):
        with pytest.raises(ValueError):
            _module_table_name("'; DROP TABLE x; --")

    def test_rejects_uppercase(self):
        with pytest.raises(ValueError):
            _module_table_name("Search")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError):
            _module_table_name("")


# ---------------------------------------------------------------------------
# _is_config_sets_subset
# ---------------------------------------------------------------------------


class TestIsConfigSetsSubset:
    """Test subset detection for config_sets arrays (exact element match)."""

    def test_single_element_subset(self):
        subset = [{"reader-threads": 1}]
        superset = [{"reader-threads": 1}, {"reader-threads": 8}]
        assert _is_config_sets_subset(subset, superset) is True

    def test_exact_match_is_subset(self):
        a = [{"reader-threads": 1}, {"reader-threads": 8}]
        b = [{"reader-threads": 1}, {"reader-threads": 8}]
        assert _is_config_sets_subset(a, b) is True

    def test_superset_is_not_subset(self):
        subset = [{"reader-threads": 1}, {"reader-threads": 8}]
        superset = [{"reader-threads": 1}]
        assert _is_config_sets_subset(subset, superset) is False

    def test_different_values_not_subset(self):
        subset = [{"reader-threads": 4}]
        superset = [{"reader-threads": 1}, {"reader-threads": 8}]
        assert _is_config_sets_subset(subset, superset) is False

    def test_partial_key_match_not_subset(self):
        subset = [{"reader-threads": 1}]
        superset = [{"reader-threads": 1, "writer-threads": 8}]
        assert _is_config_sets_subset(subset, superset) is False

    def test_empty_subset_of_anything(self):
        assert _is_config_sets_subset([], [{"reader-threads": 1}]) is True

    def test_empty_superset_fails(self):
        assert _is_config_sets_subset([{"reader-threads": 1}], []) is False

    def test_multi_key_multi_element_subset(self):
        subset = [
            {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1},
            {"io-threads": 8, "search.reader-threads": 8, "search.writer-threads": 8},
        ]
        superset = [
            {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1},
            {"io-threads": 8, "search.reader-threads": 4, "search.writer-threads": 4},
            {"io-threads": 8, "search.reader-threads": 8, "search.writer-threads": 8},
        ]
        assert _is_config_sets_subset(subset, superset) is True

    def test_multi_key_multi_element_not_subset(self):
        subset = [
            {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1},
            {"io-threads": 8, "search.reader-threads": 4, "search.writer-threads": 4},
        ]
        superset = [
            {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1},
            {"io-threads": 8, "search.reader-threads": 8, "search.writer-threads": 8},
            {"io-threads": 4, "search.reader-threads": 4, "search.writer-threads": 4},
        ]
        assert _is_config_sets_subset(subset, superset) is False


# ---------------------------------------------------------------------------
# CommitPair dataclass
# ---------------------------------------------------------------------------


class TestCommitPair:

    def _make_pair(self, **overrides):
        """Helper to create a valid CommitPair with optional overrides."""

        defaults = {
            "core_sha": "abc123",
            "module_sha": "xyz789",
            "core_timestamp": _parse_timestamp("2026-06-01T10:00:00+00:00"),
            "module_timestamp": _parse_timestamp("2026-06-02T10:00:00+00:00"),
            "max_commit_timestamp": _parse_timestamp("2026-06-02T10:00:00+00:00"),
            "min_commit_timestamp": _parse_timestamp("2026-06-01T10:00:00+00:00"),
            "config_name": "fts-benchmarks-arm.json",
            "config_sets": [{"io-threads": 8}],
            "architecture": "aarch64",
        }
        defaults.update(overrides)
        return CommitPair(**defaults)

    def test_creates_valid_pair(self):
        pair = self._make_pair()
        assert pair.core_sha == "abc123"
        assert pair.module_sha == "xyz789"
        assert pair.status == "pending"
        assert pair.priority is None

    def test_raises_on_missing_core_sha(self):
        with pytest.raises(ValueError):
            self._make_pair(core_sha="")

    def test_raises_on_missing_module_sha(self):
        with pytest.raises(ValueError):
            self._make_pair(module_sha=None)

    def test_raises_on_missing_config_name(self):
        with pytest.raises(ValueError):
            self._make_pair(config_name="")

    def test_is_ready_to_insert_false_without_priority(self):
        pair = self._make_pair()
        assert pair.is_ready_to_insert() is False

    def test_is_ready_to_insert_true_with_priority(self):
        pair = self._make_pair()
        pair.priority = 1
        assert pair.is_ready_to_insert() is True

    def test_is_ready_to_insert_true_with_subset_priority(self):
        pair = self._make_pair()
        pair.status = "completed_as_subset"
        pair.priority = 99
        assert pair.is_ready_to_insert() is True

    def test_to_insert_tuple_correct_order(self):
        pair = self._make_pair()
        pair.priority = 1
        t = pair.to_insert_tuple()
        assert t[0] == "abc123"  # core_sha
        assert t[1] == "xyz789"  # module_sha
        assert t[6] == "pending"  # status
        assert t[7] == 1  # priority
        assert t[8] == "fts-benchmarks-arm.json"  # config_name
        assert t[10] == "aarch64"  # architecture

    def test_to_insert_tuple_wraps_config_sets_as_json(self):

        pair = self._make_pair()
        pair.priority = 2
        t = pair.to_insert_tuple()
        assert isinstance(t[9], Json)
        assert t[9].adapted == [{"io-threads": 8}]
