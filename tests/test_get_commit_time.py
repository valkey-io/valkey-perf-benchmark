"""Unit tests for ClientRunner.get_commit_time."""

import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

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
    """Tests for get_commit_time with HEAD fallback."""

    def test_direct_ref_works(self, runner):
        """When the ref resolves directly, return its timestamp."""
        with patch.object(runner, "_run") as mock_run:
            mock_run.return_value = MagicMock(stdout="2026-03-18T01:00:00-07:00")
            result = runner.get_commit_time("abc123")
            assert result == "2026-03-18T01:00:00-07:00"
            mock_run.assert_called_once()

    def test_falls_back_to_head(self, runner):
        """When the ref fails, fall back to HEAD."""
        with patch.object(runner, "_run") as mock_run:
            mock_run.side_effect = [
                RuntimeError("unknown revision"),  # first call fails
                MagicMock(stdout="2026-03-17T22:00:00-07:00"),  # HEAD succeeds
            ]
            result = runner.get_commit_time("unstable")
            assert result == "2026-03-17T22:00:00-07:00"
            assert mock_run.call_count == 2
            # Second call should use HEAD
            assert mock_run.call_args_list[1][0][0] == [
                "git",
                "show",
                "-s",
                "--format=%cI",
                "HEAD",
            ]

    def test_both_fail_raises(self, runner):
        """When both ref and HEAD fail, raise."""
        with patch.object(runner, "_run") as mock_run:
            mock_run.side_effect = RuntimeError("git broken")
            with pytest.raises(RuntimeError):
                runner.get_commit_time("unstable")
