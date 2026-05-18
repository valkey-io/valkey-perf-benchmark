"""Integration tests for benchmark comparison workflow."""

import subprocess
import sys

import pytest

from .conftest import (
    PROJECT_ROOT,
    create_sample_metrics,
    write_metrics_file,
    read_metrics_file,
    run_comparison,
    run_comparison_with_metrics,
)


class TestComparisonWorkflow:
    """Test the complete comparison workflow used in benchmarking."""

    def test_compare_two_metrics_files(self, tmp_path):
        """Test basic comparison between two metrics files."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("baseline123", "GET", rps=100000.0),
                create_sample_metrics("baseline123", "SET", rps=80000.0),
            ],
            new_metrics=[
                create_sample_metrics("newcommit456", "GET", rps=110000.0),
                create_sample_metrics("newcommit456", "SET", rps=88000.0),
            ],
        )

        assert "baseline12" in content  # Truncated commit
        assert "newcommi" in content
        assert "GET" in content
        assert "SET" in content

    def test_compare_with_rps_filter(self, tmp_path):
        """Test comparison with RPS-only filter (used in PR workflow)."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[create_sample_metrics("base", "GET", rps=100000.0)],
            new_metrics=[create_sample_metrics("new", "GET", rps=105000.0)],
            extra_args=["--metrics", "rps"],
        )

        assert "rps" in content.lower()

    def test_compare_multiple_runs_averaging(self, tmp_path):
        """Test that multiple runs are properly averaged."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("base", "GET", rps=r, pipeline=1)
                for r in [100000.0, 102000.0, 98000.0]
            ],
            new_metrics=[
                create_sample_metrics("new", "GET", rps=r, pipeline=1)
                for r in [110000.0, 112000.0, 108000.0]
            ],
        )

        assert "n=3" in content or "3 runs" in content.lower()

    def test_compare_different_configurations(self, tmp_path):
        """Test comparison handles different pipeline configurations."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("base", "GET", rps=100000.0, pipeline=1),
                create_sample_metrics("base", "GET", rps=500000.0, pipeline=10),
            ],
            new_metrics=[
                create_sample_metrics("new", "GET", rps=105000.0, pipeline=1),
                create_sample_metrics("new", "GET", rps=520000.0, pipeline=10),
            ],
        )

        assert "P1" in content
        assert "P10" in content

    def test_compare_empty_baseline_fails_gracefully(self, tmp_path):
        """Test comparison handles missing baseline gracefully."""
        new_path = tmp_path / "new" / "metrics.json"
        write_metrics_file(new_path, [create_sample_metrics("new", "GET")])

        result = run_comparison(
            tmp_path / "nonexistent.json",
            new_path,
            tmp_path / "comparison.md",
        )

        assert result.returncode != 0
        assert "not found" in result.stderr.lower() or "error" in result.stderr.lower()


class TestMetricsFileFormat:
    """Test metrics file format compatibility."""

    def test_metrics_file_structure(self, tmp_path):
        """Verify metrics file has expected structure."""
        path = tmp_path / "metrics.json"
        write_metrics_file(path, [create_sample_metrics("abc123", "GET")])

        m = read_metrics_file(path)[0]
        for key in (
            "commit",
            "command",
            "rps",
            "avg_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
        ):
            assert key in m

    def test_metrics_with_io_threads(self, tmp_path):
        """Test metrics with io_threads field."""
        metrics = [create_sample_metrics("abc123", "GET")]
        metrics[0]["io_threads"] = 4
        path = tmp_path / "metrics.json"
        write_metrics_file(path, metrics)

        assert read_metrics_file(path)[0]["io_threads"] == 4

    def test_metrics_with_cluster_mode(self, tmp_path):
        """Test metrics with cluster mode enabled."""
        metrics = [create_sample_metrics("abc123", "GET")]
        metrics[0]["cluster_mode"] = True
        path = tmp_path / "metrics.json"
        write_metrics_file(path, metrics)

        assert read_metrics_file(path)[0]["cluster_mode"] is True


class TestPRCommentGeneration:
    """Test generation of PR comment content."""

    def test_comparison_output_is_valid_markdown(self, tmp_path):
        """Verify comparison output is valid markdown for PR comments."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("baseline", "GET", rps=100000.0),
                create_sample_metrics("baseline", "SET", rps=80000.0),
            ],
            new_metrics=[
                create_sample_metrics("newcommit", "GET", rps=110000.0),
                create_sample_metrics("newcommit", "SET", rps=88000.0),
            ],
        )

        assert content.startswith("#")
        assert "|" in content
        assert "---" in content

    def test_percentage_change_shown(self, tmp_path):
        """Verify percentage change is shown in comparison."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[create_sample_metrics("base", "GET", rps=100000.0)],
            new_metrics=[create_sample_metrics("new", "GET", rps=110000.0)],
        )

        assert "%" in content
        assert "+" in content or "10" in content
