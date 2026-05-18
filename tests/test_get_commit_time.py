"""Unit tests for ClientRunner.get_commit_time."""

from pathlib import Path
from unittest.mock import patch

import pytest

from valkey_benchmark import ClientRunner


@pytest.fixture
def runner(minimal_valid_config):
    """ClientRunner with a dummy valkey path."""
    return ClientRunner(
        commit_id="abc123",
        config=minimal_valid_config,
        cluster_mode=False,
        tls_mode=False,
        target_ip="127.0.0.1",
        results_dir=Path("/tmp/results"),
        valkey_path="/tmp/valkey",
        valkey_benchmark_path="/tmp/valkey-benchmark",
    )


class TestGetCommitTime:
    """Tests for get_commit_time."""

    @patch("valkey_benchmark.get_commit_timestamp")
    @patch("valkey_benchmark.resolve_ref")
    def test_resolves_ref_and_returns_timestamp(
        self, mock_resolve, mock_timestamp, runner
    ):
        """Resolves the ref to a SHA, then returns its timestamp."""
        mock_resolve.return_value = "abc123full"
        mock_timestamp.return_value = "2026-03-18T01:00:00-07:00"

        result = runner.get_commit_time("abc123")

        assert result == "2026-03-18T01:00:00-07:00"
        mock_resolve.assert_called_once_with("abc123", runner.valkey_path)
        mock_timestamp.assert_called_once_with("abc123full", runner.valkey_path)

    @patch("valkey_benchmark.get_commit_timestamp")
    @patch("valkey_benchmark.resolve_ref")
    def test_resolve_failure_raises(self, mock_resolve, mock_timestamp, runner):
        """When resolve_ref fails, the error is re-raised."""
        mock_resolve.side_effect = RuntimeError("unknown revision")

        with pytest.raises(RuntimeError, match="unknown revision"):
            runner.get_commit_time("bad-ref")

        mock_timestamp.assert_not_called()

    @patch("valkey_benchmark.get_commit_timestamp")
    @patch("valkey_benchmark.resolve_ref")
    def test_timestamp_failure_raises(self, mock_resolve, mock_timestamp, runner):
        """When get_commit_timestamp fails, the error is re-raised."""
        mock_resolve.return_value = "abc123full"
        mock_timestamp.side_effect = RuntimeError("git broken")

        with pytest.raises(RuntimeError, match="git broken"):
            runner.get_commit_time("abc123")
