"""Unit tests for valkey_server.py — ServerLauncher._parse_cluster_info."""

import pytest

from valkey_server import ServerLauncher


@pytest.fixture
def server_launcher():
    """Create a minimal ServerLauncher instance for testing _parse_cluster_info."""
    return ServerLauncher(
        results_dir="/tmp/test_results",
        valkey_path="/tmp/valkey",
    )


# ---------------------------------------------------------------------------
# _parse_cluster_info — valid inputs
# ---------------------------------------------------------------------------


class TestParseClusterInfoValid:
    def test_single_key_value(self, server_launcher):
        info = "cluster_enabled:1"
        result = server_launcher._parse_cluster_info(info)
        assert result == {"cluster_enabled": "1"}

    def test_multiple_key_values(self, server_launcher):
        info = (
            "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384"
        )
        result = server_launcher._parse_cluster_info(info)
        assert result == {
            "cluster_state": "ok",
            "cluster_slots_assigned": "16384",
            "cluster_slots_ok": "16384",
        }

    def test_value_containing_colon(self, server_launcher):
        info = "some_key:value:with:colons"
        result = server_launcher._parse_cluster_info(info)
        assert result == {"some_key": "value:with:colons"}


# ---------------------------------------------------------------------------
# _parse_cluster_info — empty / edge cases
# ---------------------------------------------------------------------------


class TestParseClusterInfoEmpty:
    def test_empty_string(self, server_launcher):
        result = server_launcher._parse_cluster_info("")
        assert result == {}

    def test_whitespace_only(self, server_launcher):
        result = server_launcher._parse_cluster_info("  \r\n  ")
        assert result == {}
