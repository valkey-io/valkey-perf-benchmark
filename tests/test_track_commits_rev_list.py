"""Unit tests for the git commit listing used by postgres_track_commits determine."""

import subprocess

from utils.postgres_track_commits import _git_rev_list


def _git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(repo, message):
    _git(repo, "commit", "--allow-empty", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _repo_with_merge(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    base = _commit(repo, "base")
    _git(repo, "checkout", "-q", "-b", "side")
    side = _commit(repo, "side")
    _git(repo, "checkout", "-q", "main")
    main = _commit(repo, "main")
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge side", "side")
    merge = _git(repo, "rev-parse", "HEAD")
    return repo, base, side, main, merge


def test_lists_merged_in_commits_by_default(tmp_path):
    repo, base, side, main, merge = _repo_with_merge(tmp_path)
    shas = _git_rev_list(repo, "main")
    assert set(shas) == {base, side, main, merge}
    assert shas[0] == merge


def test_first_parent_skips_merged_in_commits(tmp_path):
    repo, base, side, main, merge = _repo_with_merge(tmp_path)
    assert _git_rev_list(repo, "main", first_parent=True) == [merge, main, base]


def test_first_parent_with_a_range(tmp_path):
    repo, base, side, main, merge = _repo_with_merge(tmp_path)
    assert _git_rev_list(repo, f"{base}..main", first_parent=True) == [merge, main]
