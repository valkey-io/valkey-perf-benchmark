"""Integration tests for utils/git_utils.py ref resolution and timestamp retrieval.

These tests exercise resolve_ref's multi-step fallback logic using real git
repos (with a local bare remote to simulate fetch without network access).
"""

import subprocess

import pytest

from .conftest import GitRepoFixture

from utils.git_utils import resolve_ref, get_commit_timestamp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def repo_with_remote(tmp_path):
    """Create a local repo + bare remote to test fetch fallback paths.

    Layout:
        bare_remote/  – bare repo acting as 'origin'
        clone/        – shallow-ish clone of bare_remote
    """
    bare_path = tmp_path / "bare_remote"
    clone_path = tmp_path / "clone"

    # Create the "upstream" bare repo with some history
    upstream = GitRepoFixture(tmp_path / "upstream_work")
    base_sha = upstream.create_commit("base", files={"a.txt": "base"})
    upstream.create_branch("feature-x")
    feature_sha = upstream.create_commit("feature", files={"b.txt": "feature"})
    upstream.checkout("master")
    main_sha = upstream.create_commit("main-next", files={"c.txt": "next"})

    # Clone to bare
    subprocess.run(
        ["git", "clone", "--bare", str(upstream.path), str(bare_path)],
        capture_output=True,
        text=True,
        check=True,
    )

    # Make a shallow clone (depth=1) from the bare remote
    subprocess.run(
        ["git", "clone", "--depth=1", str(bare_path), str(clone_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    # Configure user for any commits we might need
    for key, val in [("user.email", "test@example.com"), ("user.name", "Test")]:
        subprocess.run(
            ["git", "config", key, val],
            cwd=clone_path,
            capture_output=True,
            text=True,
        )

    return {
        "clone_path": clone_path,
        "bare_path": bare_path,
        "base_sha": base_sha,
        "feature_sha": feature_sha,
        "main_sha": main_sha,
    }


# ---------------------------------------------------------------------------
# Local resolution (no fetch needed)
# ---------------------------------------------------------------------------


class TestResolveRefLocal:
    """Test resolve_ref when the ref is already available locally."""

    def test_resolve_full_sha(self, git_repo: GitRepoFixture):
        """Full 40-char SHA resolves directly."""
        sha = git_repo.create_commit("test", files={"f.txt": "x"})
        assert resolve_ref(sha, git_repo.path) == sha

    def test_resolve_short_sha(self, git_repo: GitRepoFixture):
        """Short SHA prefix resolves to full SHA."""
        sha = git_repo.create_commit("test", files={"f.txt": "x"})
        resolved = resolve_ref(sha[:7], git_repo.path)
        assert resolved == sha

    def test_resolve_branch_name(self, git_repo: GitRepoFixture):
        """Branch name resolves to its HEAD commit."""
        git_repo.create_branch("my-branch")
        sha = git_repo.create_commit("on branch", files={"f.txt": "y"})
        git_repo.checkout("master")

        assert resolve_ref("my-branch", git_repo.path) == sha

    def test_resolve_tag(self, git_repo: GitRepoFixture):
        """Lightweight tag resolves to the tagged commit."""
        sha = git_repo.create_commit("tagged", files={"f.txt": "z"})
        git_repo._run_git("tag", "v1.0.0")

        assert resolve_ref("v1.0.0", git_repo.path) == sha

    def test_resolve_head(self, git_repo: GitRepoFixture):
        """HEAD resolves to current commit."""
        sha = git_repo.create_commit("latest", files={"f.txt": "h"})
        assert resolve_ref("HEAD", git_repo.path) == sha


# ---------------------------------------------------------------------------
# Fetch fallback (ref not available locally)
# ---------------------------------------------------------------------------


class TestResolveRefFetchFallback:
    """Test resolve_ref's fetch-from-origin fallback using a local bare remote."""

    def test_resolve_remote_branch_not_local(self, repo_with_remote):
        """Branch that exists on origin but not locally is fetched and resolved."""
        clone_path = repo_with_remote["clone_path"]
        feature_sha = repo_with_remote["feature_sha"]

        resolved = resolve_ref("feature-x", clone_path)
        assert resolved == feature_sha

    def test_resolve_sha_not_in_shallow_clone(self, repo_with_remote):
        """SHA outside shallow clone depth is fetched and resolved."""
        clone_path = repo_with_remote["clone_path"]
        base_sha = repo_with_remote["base_sha"]

        # In a depth=1 clone, the base commit may not be present.
        # resolve_ref should fetch it.
        resolved = resolve_ref(base_sha, clone_path)
        assert resolved == base_sha


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestResolveRefErrors:
    """Test resolve_ref failure modes."""

    def test_unresolvable_ref_raises(self, git_repo: GitRepoFixture):
        """Completely bogus ref raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Could not resolve ref"):
            resolve_ref("nonexistent-branch-xyz-999", git_repo.path)

    def test_invalid_repo_path_raises(self, tmp_path):
        """Non-repo path raises (subprocess error wrapped in RuntimeError)."""
        not_a_repo = tmp_path / "empty"
        not_a_repo.mkdir()
        with pytest.raises(RuntimeError, match="Could not resolve ref"):
            resolve_ref("HEAD", not_a_repo)


# ---------------------------------------------------------------------------
# get_commit_timestamp
# ---------------------------------------------------------------------------


class TestGetCommitTimestamp:
    """Test get_commit_timestamp returns valid ISO8601 timestamps."""

    def test_returns_iso8601(self, git_repo: GitRepoFixture):
        """Timestamp is a valid ISO8601 string."""
        sha = git_repo.create_commit("ts test", files={"f.txt": "t"})
        ts = get_commit_timestamp(sha, git_repo.path)

        # ISO8601 with timezone, e.g. 2024-01-15T10:30:00+00:00
        assert "T" in ts
        assert len(ts) >= 19  # At minimum YYYY-MM-DDTHH:MM:SS

    def test_different_commits_can_differ(self, git_repo: GitRepoFixture):
        """Two commits made at different times can have different timestamps."""
        sha1 = git_repo.create_commit("first", files={"a.txt": "1"})
        sha2 = git_repo.create_commit("second", files={"b.txt": "2"})

        ts1 = get_commit_timestamp(sha1, git_repo.path)
        ts2 = get_commit_timestamp(sha2, git_repo.path)

        # Both should be valid; they may or may not differ (fast machine),
        # but at minimum both should parse without error.
        assert ts1 and ts2

    def test_invalid_sha_raises(self, git_repo: GitRepoFixture):
        """Non-existent SHA raises RuntimeError."""
        fake_sha = "a" * 40
        with pytest.raises(RuntimeError, match="Failed to get timestamp"):
            get_commit_timestamp(fake_sha, git_repo.path)
