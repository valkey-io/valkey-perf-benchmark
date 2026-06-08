"""Unit tests for utils/module_postgres_track_commits.py — pure logic only.

Tests pure functions that don't require a database connection.
Mirrors the testing approach of test_postgres_utils.py.
"""

from pathlib import Path

from utils.module_postgres_track_commits import (
    get_config_name,
    _module_table_name,
)


# ---------------------------------------------------------------------------
# get_config_name
# ---------------------------------------------------------------------------


class TestGetConfigName:
    def test_extracts_filename_from_relative_path(self):
        path = "../valkey-search/.github/benchmark_configs/fts-benchmarks-arm.json"
        assert get_config_name(path) == "fts-benchmarks-arm.json"

    def test_extracts_filename_from_absolute_path(self):
        assert get_config_name("/home/user/configs/benchmark-config-arm.json") == "benchmark-config-arm.json"

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
