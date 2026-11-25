"""Unit tests for valkey_build.py ServerBuilder.build()."""

from unittest.mock import patch

import pytest

from valkey_build import ServerBuilder


def _make_commands(tls_mode, build_args):
    builder = ServerBuilder(
        commit_id="HEAD",
        tls_mode=tls_mode,
        valkey_path="/tmp/valkey",
        build_args=build_args,
    )
    with (
        patch.object(builder, "clone_and_checkout"),
        patch("valkey_build.subprocess.run") as run,
    ):
        builder.build()
    return [c.args[0] for c in run.call_args_list if c.args[0][0] == "make"]


@pytest.mark.parametrize(
    "tls_mode, expected",
    [
        (False, ["make", "BUILD_EXT_STORAGE=yes", "-j"]),
        (True, ["make", "BUILD_TLS=yes", "BUILD_EXT_STORAGE=yes", "-j"]),
    ],
)
def test_build_args_passed_to_make(tls_mode, expected):
    commands = _make_commands(tls_mode, ["BUILD_EXT_STORAGE=yes"])
    assert commands == [["make", "distclean"], expected]


@pytest.mark.parametrize(
    "tls_mode, expected",
    [(False, ["make", "-j"]), (True, ["make", "BUILD_TLS=yes", "-j"])],
)
def test_no_build_args(tls_mode, expected):
    assert _make_commands(tls_mode, None) == [["make", "distclean"], expected]


def _git_tree(path):
    """A one-commit git repo holding a stand-in server binary."""
    import subprocess

    git = lambda *a: subprocess.run(
        ["git", *a], cwd=path, check=True, capture_output=True
    )
    git("init", "-q")
    (path / "README").write_text("x\n")
    git("add", "README")
    git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", "c")
    (path / "src").mkdir()
    (path / "src" / "valkey-server").write_text("binary")
    return git


def _build_count(path, builds):
    """Run builds() builders against path; return how many ran make distclean."""
    made = []
    for build_args in builds:
        builder = ServerBuilder(
            commit_id="HEAD",
            tls_mode=False,
            valkey_path=str(path),
            build_args=build_args,
        )
        with (
            patch.object(builder, "clone_and_checkout"),
            patch.object(
                builder, "_run", side_effect=lambda cmd, cwd=None: made.append(cmd)
            ),
        ):
            builder.build()
    return sum(1 for cmd in made if cmd == ["make", "distclean"])


class TestBuildSkip:
    def test_same_commit_and_options_is_built_once(self, tmp_path):
        _git_tree(tmp_path)
        assert _build_count(tmp_path, [None, None, None]) == 1

    def test_changed_build_args_rebuild(self, tmp_path):
        _git_tree(tmp_path)
        assert _build_count(tmp_path, [None, ["BUILD_EXT_STORAGE=yes"], None]) == 3

    def test_uncommitted_changes_always_rebuild(self, tmp_path):
        _git_tree(tmp_path)
        (tmp_path / "README").write_text("changed\n")
        assert _build_count(tmp_path, [None, None]) == 2

    def test_new_commit_rebuilds(self, tmp_path):
        git = _git_tree(tmp_path)
        assert _build_count(tmp_path, [None]) == 1
        git(
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "commit",
            "-qm",
            "c2",
            "--allow-empty",
        )
        assert _build_count(tmp_path, [None]) == 1

    def test_no_binary_after_make_leaves_no_stamp(self, tmp_path):
        _git_tree(tmp_path)
        (tmp_path / "src" / "valkey-server").unlink()
        assert _build_count(tmp_path, [None, None]) == 2
