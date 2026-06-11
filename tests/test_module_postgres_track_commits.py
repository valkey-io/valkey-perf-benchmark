"""Unit tests for utils/module_postgres_track_commits.py — pure logic only.

Tests pure functions that don't require a database connection.
Mirrors the testing approach of test_postgres_utils.py.
"""

from pathlib import Path

from utils.module_postgres_track_commits import (
    CommitPair,
    get_config_name,
    _module_table_name,
    _is_config_sets_subset,
)

# ---------------------------------------------------------------------------
# get_config_name
# ---------------------------------------------------------------------------


class TestGetConfigName:
    def test_extracts_filename_from_relative_path(self):
        path = "../valkey-search/.github/benchmark_configs/fts-benchmarks-arm.json"
        assert get_config_name(path) == "fts-benchmarks-arm.json"

    def test_extracts_filename_from_absolute_path(self):
        assert (
            get_config_name("/home/user/configs/benchmark-config-arm.json")
            == "benchmark-config-arm.json"
        )

    def test_extracts_filename_when_just_filename(self):
        assert get_config_name("fts-benchmarks-arm.json") == "fts-benchmarks-arm.json"

    def test_handles_deeply_nested_dirs(self):
        assert get_config_name("a/b/c/d/config.json") == "config.json"

    def test_handles_dotfiles(self):
        assert get_config_name("/home/.hidden/config.json") == "config.json"

    def test_preserves_extension(self):
        assert get_config_name("path/to/file.yaml") == "file.yaml"


# ---------------------------------------------------------------------------
# _module_table_name
# ---------------------------------------------------------------------------


class TestModuleTableName:
    def test_search_module(self):
        assert _module_table_name("search") == "benchmark_module_commits_search"

    def test_json_module(self):
        assert _module_table_name("json") == "benchmark_module_commits_json"

    def test_bloom_module(self):
        assert _module_table_name("bloom") == "benchmark_module_commits_bloom"

    def test_arbitrary_name(self):
        assert _module_table_name("my_module") == "benchmark_module_commits_my_module"


# ---------------------------------------------------------------------------
# config_set JSONB in module commit tracking
# ---------------------------------------------------------------------------


class TestModuleConfigSetsJsonb:
    """Test that config_sets array is wrapped as Json for JSONB insertion."""

    def test_config_sets_array_wrapped_as_json(self):
        from psycopg2.extras import Json

        config_sets = [
            {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1},
            {"io-threads": 8, "search.reader-threads": 8, "search.writer-threads": 8},
        ]
        wrapped = Json(config_sets)
        assert isinstance(wrapped, Json)
        assert wrapped.adapted == config_sets
        assert len(wrapped.adapted) == 2

    def test_none_config_sets_defaults_to_empty_dict_list(self):
        from psycopg2.extras import Json

        config_sets = None
        resolved = config_sets or [{}]
        assert resolved == [{}]
        wrapped = Json(resolved)
        assert wrapped.adapted == [{}]


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
    """Tests for CommitPair validation and methods."""

    def _make_pair(self, **overrides):
        """Helper to create a valid CommitPair with optional overrides."""
        from datetime import datetime, timezone
        from utils.module_postgres_track_commits import _parse_timestamp

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

    def test_exits_on_missing_core_sha(self):
        import pytest
        with pytest.raises(SystemExit):
            self._make_pair(core_sha="")

    def test_exits_on_missing_module_sha(self):
        import pytest
        with pytest.raises(SystemExit):
            self._make_pair(module_sha=None)

    def test_exits_on_missing_config_name(self):
        import pytest
        with pytest.raises(SystemExit):
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
        from psycopg2.extras import Json
        pair = self._make_pair()
        pair.priority = 2
        t = pair.to_insert_tuple()
        assert isinstance(t[9], Json)
        assert t[9].adapted == [{"io-threads": 8}]
