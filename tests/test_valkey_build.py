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
