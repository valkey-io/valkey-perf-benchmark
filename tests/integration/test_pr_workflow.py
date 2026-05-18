"""Integration tests simulating the complete PR benchmark workflow."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import (
    GitRepoFixture,
    PROJECT_ROOT,
    create_sample_metrics,
    write_metrics_file,
    run_comparison,
    run_comparison_with_metrics,
)


class TestPRWorkflowSimulation:
    """Simulate the PR benchmark workflow end-to-end."""

    def test_simulate_pr_workflow_comparison_phase(self, tmp_path):
        """Simulate the comparison phase of PR workflow.

        Tests the workflow from having benchmark results to generating
        the PR comment, without actually running benchmarks.
        """
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("baseline_xyz", "GET", rps=100000.0, pipeline=1),
                create_sample_metrics("baseline_xyz", "GET", rps=500000.0, pipeline=10),
                create_sample_metrics("baseline_xyz", "SET", rps=90000.0, pipeline=1),
                create_sample_metrics("baseline_xyz", "SET", rps=450000.0, pipeline=10),
            ],
            new_metrics=[
                create_sample_metrics("pr_commit_abc", "GET", rps=110000.0, pipeline=1),
                create_sample_metrics(
                    "pr_commit_abc", "GET", rps=550000.0, pipeline=10
                ),
                create_sample_metrics("pr_commit_abc", "SET", rps=95000.0, pipeline=1),
                create_sample_metrics(
                    "pr_commit_abc", "SET", rps=480000.0, pipeline=10
                ),
            ],
            extra_args=["--metrics", "rps"],
        )

        assert content.startswith("#")
        assert "pr_commi" in content or "pr_commit" in content
        assert "baseline" in content
        assert "+" in content  # Positive change indicator
        assert "GET" in content
        assert "SET" in content

    def test_simulate_pr_workflow_with_multiple_runs(self, tmp_path):
        """Simulate workflow with multiple benchmark runs for statistical analysis."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("base_xyz", "GET", rps=100000.0 + v)
                for v in [0, 1500, -1000]
            ],
            new_metrics=[
                create_sample_metrics("pr_abc", "GET", rps=110000.0 + v)
                for v in [0, 2000, -1500]
            ],
        )

        assert "n=3" in content
        assert "σ=" in content or "stdev" in content.lower()

    def test_simulate_regression_detection(self, tmp_path):
        """Simulate detecting a performance regression in PR."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[create_sample_metrics("base_xyz", "GET", rps=100000.0)],
            new_metrics=[create_sample_metrics("pr_abc", "GET", rps=85000.0)],
        )

        assert "-" in content  # Negative percentage
        assert "15" in content or "14" in content or "16" in content


class TestWorkflowArtifacts:
    """Test workflow artifact generation."""

    def test_results_directory_structure(self, tmp_path):
        """Verify expected results directory structure."""
        results_dir = tmp_path / "results" / "abc123"
        results_dir.mkdir(parents=True)
        (results_dir / "logs.txt").write_text("benchmark logs here")
        (results_dir / "metrics.json").write_text("[]")

        assert (results_dir / "logs.txt").exists()
        assert (results_dir / "metrics.json").exists()

    def test_comparison_output_format_for_github(self, tmp_path):
        """Verify comparison output is GitHub-compatible markdown."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[create_sample_metrics("base", "GET", rps=95000.0)],
            new_metrics=[create_sample_metrics("pr", "GET", rps=100000.0)],
        )

        # Tables must have header separator
        assert "| ---" in content or "|---" in content
        # No raw HTML that might be stripped
        assert "<script" not in content.lower()
        assert "<style" not in content.lower()
        # Reasonable length (GitHub comment limit)
        assert len(content) < 65000


class TestGitIntegrationForPR:
    """Test git operations specific to PR workflow."""

    def test_checkout_pr_and_baseline_branches(self, git_repo: GitRepoFixture):
        """Simulate checking out PR and baseline branches."""
        baseline_sha = git_repo.create_commit(
            "Baseline version",
            files={"version.txt": "1.0.0"},
        )

        git_repo.create_branch("feature/improvement")
        pr_sha = git_repo.create_commit(
            "Performance improvement",
            files={"version.txt": "1.0.1", "optimization.c": "// faster code"},
        )

        git_repo.checkout(baseline_sha)
        assert git_repo.get_current_commit() == baseline_sha
        assert (git_repo.path / "version.txt").read_text() == "1.0.0"

        git_repo.checkout(pr_sha)
        assert git_repo.get_current_commit() == pr_sha
        assert (git_repo.path / "version.txt").read_text() == "1.0.1"
        assert (git_repo.path / "optimization.c").exists()

    def test_get_merge_base(self, git_repo: GitRepoFixture):
        """Test finding merge base between PR and baseline."""
        base_sha = git_repo.create_commit("Base")

        git_repo.create_branch("main-branch")
        main_sha = git_repo.create_commit("Main progress")

        git_repo.checkout(base_sha)
        git_repo.create_branch("pr-branch")
        pr_sha = git_repo.create_commit("PR changes")

        result = subprocess.run(
            ["git", "merge-base", main_sha, pr_sha],
            cwd=git_repo.path,
            capture_output=True,
            text=True,
        )

        assert result.stdout.strip() == base_sha


class TestModuleBenchmarkWorkflow:
    """Test module-specific benchmark workflow."""

    def test_module_results_directory_structure(self, tmp_path):
        """Verify module benchmark results use correct directory."""
        module_results = tmp_path / "results" / "search_tests"
        module_results.mkdir(parents=True)

        write_metrics_file(
            module_results / "metrics.json",
            [create_sample_metrics("abc123", "FT.SEARCH", rps=50000.0)],
        )

        loaded = json.loads((module_results / "metrics.json").read_text())
        assert loaded[0]["command"] == "FT.SEARCH"

    def test_compare_module_results(self, tmp_path):
        """Test comparing module benchmark results."""
        content = run_comparison_with_metrics(
            tmp_path,
            baseline_metrics=[
                create_sample_metrics("base", "FT.SEARCH idx query", rps=50000.0),
                create_sample_metrics("base", "FT.AGGREGATE idx query", rps=42000.0),
            ],
            new_metrics=[
                create_sample_metrics("pr", "FT.SEARCH idx query", rps=55000.0),
                create_sample_metrics("pr", "FT.AGGREGATE idx query", rps=45000.0),
            ],
            extra_args=["--metrics", "rps"],
        )

        assert "FT.SEARCH" in content or "SEARCH" in content
