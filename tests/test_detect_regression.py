"""Unit tests for utils/detect_regression.py"""

import pytest
from unittest.mock import patch, MagicMock
from utils.detect_regression import (
    fetch_last_two_commits,
    fetch_metrics_for_commit,
    detect,
)


class TestFetchLastTwoCommits:
    def test_returns_two_commits_ordered_by_timestamp(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [
            ("commit_new", "2025-11-19 09:00:00"),
            ("commit_old", "2025-11-18 09:00:00"),
        ]

        result = fetch_last_two_commits(conn, "benchmark_metrics", "core")
        assert result == ["commit_new", "commit_old"]

    def test_exits_when_fewer_than_two_commits(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [("only_one",)]

        with pytest.raises(SystemExit) as exc_info:
            fetch_last_two_commits(conn, "benchmark_metrics", "core")
        assert exc_info.value.code == 0


class TestFetchMetricsForCommit:
    def test_converts_decimal_to_float_and_skips_id_created_at(self):
        from decimal import Decimal

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.description = [
            ("id",),
            ("commit",),
            ("rps",),
            ("created_at",),
            ("command",),
        ]
        cursor.fetchall.return_value = [
            (1, "abc123", Decimal("500000.00"), "2025-01-01", "SET"),
        ]

        result = fetch_metrics_for_commit(conn, "benchmark_metrics", "abc123")
        assert len(result) == 1
        assert "id" not in result[0]
        assert "created_at" not in result[0]
        assert result[0]["rps"] == 500000.00
        assert isinstance(result[0]["rps"], float)
        assert result[0]["commit"] == "abc123"
        assert result[0]["command"] == "SET"


class TestDetect:
    @patch("utils.detect_regression.fetch_metrics_for_commit")
    @patch("utils.detect_regression.fetch_last_two_commits")
    def test_no_regression_when_rps_is_same(
        self, mock_fetch_commits, mock_fetch_metrics
    ):
        mock_fetch_commits.return_value = ["new_commit", "baseline_commit"]

        # Both commits have same RPS across 5 runs
        def make_metrics(commit, rps):
            rows = []
            for i in range(5):
                rows.append(
                    {
                        "timestamp": f"2025-01-0{i+1} 00:00:00",
                        "commit": commit,
                        "command": "GET",
                        "pipeline": 1,
                        "io_threads": 1,
                        "clients": 1600,
                        "data_size": 16,
                        "rps": rps + (i * 100),  # slight noise
                        "avg_latency_ms": 0.5,
                        "p50_latency_ms": 0.4,
                        "p95_latency_ms": 1.2,
                        "p99_latency_ms": 2.0,
                        "test_type": "core",
                    }
                )
            return rows

        mock_fetch_metrics.side_effect = [
            make_metrics("baseline_commit", 500000),
            make_metrics("new_commit", 500000),
        ]

        conn = MagicMock()
        result = detect(conn, "benchmark_metrics", 5.0, "core")
        assert result["has_regression"] is False
        assert result["regressions"] == []

    @patch("utils.detect_regression.fetch_metrics_for_commit")
    @patch("utils.detect_regression.fetch_last_two_commits")
    def test_regression_detected_when_rps_drops(
        self, mock_fetch_commits, mock_fetch_metrics
    ):
        mock_fetch_commits.return_value = ["new_commit", "baseline_commit"]

        def make_metrics(commit, rps):
            rows = []
            for i in range(5):
                rows.append(
                    {
                        "timestamp": f"2025-01-0{i+1} 00:00:00",
                        "commit": commit,
                        "command": "GET",
                        "pipeline": 1,
                        "io_threads": 1,
                        "clients": 1600,
                        "data_size": 16,
                        "rps": rps + (i * 10),
                        "avg_latency_ms": 0.5,
                        "p50_latency_ms": 0.4,
                        "p95_latency_ms": 1.2,
                        "p99_latency_ms": 2.0,
                        "test_type": "core",
                    }
                )
            return rows

        mock_fetch_metrics.side_effect = [
            make_metrics("baseline_commit", 500000),
            make_metrics("new_commit", 425000),  # 15% drop
        ]

        conn = MagicMock()
        result = detect(conn, "benchmark_metrics", 5.0, "core")
        assert result["has_regression"] is True
        assert len(result["regressions"]) > 0
        assert result["new_commit"] == "new_commit"
        assert result["baseline_commit"] == "baseline_commit"

    @patch("utils.detect_regression.fetch_metrics_for_commit")
    @patch("utils.detect_regression.fetch_last_two_commits")
    def test_small_regression_below_threshold_not_reported(
        self, mock_fetch_commits, mock_fetch_metrics
    ):
        mock_fetch_commits.return_value = ["new_commit", "baseline_commit"]

        def make_metrics(commit, rps):
            rows = []
            for i in range(5):
                rows.append(
                    {
                        "timestamp": f"2025-01-0{i+1} 00:00:00",
                        "commit": commit,
                        "command": "GET",
                        "pipeline": 1,
                        "io_threads": 1,
                        "clients": 1600,
                        "data_size": 16,
                        "rps": rps + (i * 10),
                        "avg_latency_ms": 0.5,
                        "p50_latency_ms": 0.4,
                        "p95_latency_ms": 1.2,
                        "p99_latency_ms": 2.0,
                        "test_type": "core",
                    }
                )
            return rows

        mock_fetch_metrics.side_effect = [
            make_metrics("baseline_commit", 500000),
            make_metrics("new_commit", 490000),  # 2% drop — below 5% threshold
        ]

        conn = MagicMock()
        result = detect(conn, "benchmark_metrics", 5.0, "core")
        assert result["has_regression"] is False
