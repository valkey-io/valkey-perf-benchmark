"""Integration tests for git operations used in benchmarking workflows."""

import pytest
import subprocess

from .conftest import GitRepoFixture


class TestGitRepoFixture:
    """Tests for the GitRepoFixture helper itself."""

    def test_creates_valid_git_repo(self, git_repo: GitRepoFixture):
        """Verify fixture creates a valid git repository."""
        assert (git_repo.path / ".git").is_dir()

    def test_has_initial_commit(self, git_repo: GitRepoFixture):
        """Verify repo has at least one commit."""
        result = subprocess.run(
            ["git", "rev-list", "--count", "HEAD"],
            cwd=git_repo.path,
            capture_output=True,
            text=True,
        )
        assert int(result.stdout.strip()) >= 1

    def test_create_commit_returns_sha(self, git_repo: GitRepoFixture):
        """Verify create_commit returns valid SHA."""
        sha = git_repo.create_commit("Test commit")
        assert len(sha) == 40
        assert all(c in "0123456789abcdef" for c in sha)

    def test_create_commit_with_files(self, git_repo: GitRepoFixture):
        """Verify create_commit can add files."""
        sha = git_repo.create_commit(
            "Add test file",
            files={"test.txt": "hello world"},
        )
        assert (git_repo.path / "test.txt").read_text() == "hello world"
        assert len(sha) == 40

    def test_create_branch(self, git_repo: GitRepoFixture):
        """Verify branch creation works."""
        git_repo.create_branch("feature-branch")
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_repo.path,
            capture_output=True,
            text=True,
        )
        assert result.stdout.strip() == "feature-branch"

    def test_checkout(self, git_repo: GitRepoFixture):
        """Verify checkout works."""
        git_repo.create_branch("test-branch")
        git_repo.checkout("master")
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_repo.path,
            capture_output=True,
            text=True,
        )
        # Could be master or main depending on git version
        assert result.stdout.strip() in ("master", "main")


class TestPRWorkflowGitOperations:
    """Test git operations that mirror the PR benchmark workflow."""

    def test_simulate_pr_branch_structure(self, git_repo: GitRepoFixture):
        """Simulate the branch structure of a PR."""
        # Create baseline (like 'unstable' branch)
        baseline_sha = git_repo.create_commit(
            "Baseline commit",
            files={"src/main.c": "// baseline code"},
        )

        # Create PR branch with changes
        git_repo.create_branch("pr-branch")
        pr_sha = git_repo.create_commit(
            "PR changes",
            files={"src/main.c": "// improved code"},
        )

        # Verify we can access both commits
        assert baseline_sha != pr_sha
        assert len(baseline_sha) == 40
        assert len(pr_sha) == 40

        # Verify we can checkout baseline
        git_repo.checkout(baseline_sha)
        assert git_repo.get_current_commit() == baseline_sha

        # Verify we can checkout PR
        git_repo.checkout(pr_sha)
        assert git_repo.get_current_commit() == pr_sha

    def test_multiple_commits_between_baseline_and_pr(self, git_repo: GitRepoFixture):
        """Test scenario with multiple commits between baseline and PR."""
        baseline_sha = git_repo.create_commit("Baseline")

        # Simulate multiple commits on main branch
        git_repo.create_commit("Intermediate 1")
        git_repo.create_commit("Intermediate 2")

        # Create PR from latest
        git_repo.create_branch("pr-branch")
        pr_sha = git_repo.create_commit("PR changes")

        # Both should be accessible
        git_repo.checkout(baseline_sha)
        assert (git_repo.path / ".marker").read_text() == "Baseline"

        git_repo.checkout(pr_sha)
        assert (git_repo.path / ".marker").read_text() == "PR changes"

    def test_checkout_by_short_sha(self, git_repo: GitRepoFixture):
        """Verify checkout works with short SHA (like workflow uses)."""
        full_sha = git_repo.create_commit("Test commit")
        short_sha = full_sha[:7]

        git_repo.create_commit("Another commit")

        # Checkout using short SHA
        git_repo.checkout(short_sha)
        current = git_repo.get_current_commit()
        assert current == full_sha


class TestMockValkeyRepo:
    """Test the mock Valkey repository structure."""

    def test_has_valkey_server(self, mock_valkey_repo: GitRepoFixture):
        """Verify mock repo has valkey-server."""
        server = mock_valkey_repo.path / "src" / "valkey-server"
        assert server.exists()
        assert server.stat().st_mode & 0o111  # Executable

    def test_has_valkey_benchmark(self, mock_valkey_repo: GitRepoFixture):
        """Verify mock repo has valkey-benchmark."""
        benchmark = mock_valkey_repo.path / "src" / "valkey-benchmark"
        assert benchmark.exists()
        assert benchmark.stat().st_mode & 0o111  # Executable

    def test_has_tls_certs(self, mock_valkey_repo: GitRepoFixture):
        """Verify mock repo has TLS certificate structure."""
        tls_dir = mock_valkey_repo.path / "tests" / "tls"
        assert (tls_dir / "valkey.crt").exists()
        assert (tls_dir / "valkey.key").exists()
        assert (tls_dir / "ca.crt").exists()

    def test_mock_benchmark_produces_csv(self, mock_valkey_repo: GitRepoFixture):
        """Verify mock benchmark produces valid CSV output."""
        benchmark = mock_valkey_repo.path / "src" / "valkey-benchmark"
        result = subprocess.run(
            ["python3", str(benchmark), "-t", "GET,SET", "--csv"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        lines = result.stdout.strip().split("\n")

        # Should have header + 2 data lines (GET, SET)
        assert len(lines) == 3

        # Verify header
        assert '"test"' in lines[0]
        assert '"rps"' in lines[0]

        # Verify data lines have expected commands
        assert '"GET"' in lines[1]
        assert '"SET"' in lines[2]

    def test_mock_benchmark_respects_seed(self, mock_valkey_repo: GitRepoFixture):
        """Verify mock benchmark produces reproducible results with seed."""
        benchmark = mock_valkey_repo.path / "src" / "valkey-benchmark"

        result1 = subprocess.run(
            ["python3", str(benchmark), "-t", "GET", "--csv", "--seed", "12345"],
            capture_output=True,
            text=True,
        )
        result2 = subprocess.run(
            ["python3", str(benchmark), "-t", "GET", "--csv", "--seed", "12345"],
            capture_output=True,
            text=True,
        )

        assert result1.stdout == result2.stdout
