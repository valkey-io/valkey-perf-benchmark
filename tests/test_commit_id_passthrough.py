"""Verify commit_id is passed through to ClientRunner, not defaulted to HEAD.

Regression test for f42f933 which changed ClientRunner instantiation to read
commit_id from exec_config["cfg"] (which never has it) instead of using the
commit_id parameter, causing all metrics to be recorded as "HEAD".
"""

import argparse
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from benchmark import _execute_benchmark_run


@pytest.fixture
def exec_config(minimal_valid_config):
    """Minimal exec_config as produced by _iterate_execution_configs."""
    return {
        "cfg": minimal_valid_config,
        "cluster_mode": False,
        "profiling_set": {"enabled": False},
        "config_set": {},
        "config_suffix": "default",
        "io_threads": None,
    }


@pytest.fixture
def mock_args(tmp_path):
    """Minimal args namespace for _execute_benchmark_run."""
    return argparse.Namespace(
        use_running_server=True,
        mode="client",
        valkey_benchmark_path="/usr/bin/valkey-benchmark",
        valkey_path=str(tmp_path),
        skip_config_set=True,
        runs=1,
        target_ip="127.0.0.1",
        repository=None,
    )


class TestCommitIdPassthrough:
    """commit_id passed to _execute_benchmark_run must reach ClientRunner."""

    @patch("benchmark.ClientRunner")
    def test_sha_commit_id_reaches_client_runner(
        self, mock_runner_cls, exec_config, mock_args, tmp_path
    ):
        mock_instance = MagicMock()
        mock_runner_cls.return_value = mock_instance

        _execute_benchmark_run(
            exec_config=exec_config,
            args=mock_args,
            results_dir=tmp_path,
            valkey_dir=tmp_path,
            commit_id="abc123def456",
            module_path=None,
            uses_test_groups=False,
            architecture="x86_64",
            client_cpu_ranges=None,
        )

        mock_runner_cls.assert_called_once()
        assert mock_runner_cls.call_args.kwargs["commit_id"] == "abc123def456"

    @patch("benchmark.ClientRunner")
    def test_commit_id_is_not_head_when_sha_provided(
        self, mock_runner_cls, exec_config, mock_args, tmp_path
    ):
        """Ensure we never silently fall back to 'HEAD'."""
        mock_instance = MagicMock()
        mock_runner_cls.return_value = mock_instance

        _execute_benchmark_run(
            exec_config=exec_config,
            args=mock_args,
            results_dir=tmp_path,
            valkey_dir=tmp_path,
            commit_id="9a29b97a8e80e07e6db1758f8c74bfd5db2becc6",
            module_path=None,
            uses_test_groups=False,
            architecture="x86_64",
            client_cpu_ranges=None,
        )

        actual = mock_runner_cls.call_args.kwargs["commit_id"]
        assert actual != "HEAD"
        assert actual == "9a29b97a8e80e07e6db1758f8c74bfd5db2becc6"
