"""Unit tests for custom-server-configs in ServerLauncher._build_server_command."""

import pytest

from valkey_server import ServerLauncher


@pytest.fixture
def launcher():
    """Create a minimal ServerLauncher without triggering side effects."""
    obj = ServerLauncher.__new__(ServerLauncher)
    obj.cores = None
    obj.target_ip = "127.0.0.1"
    obj.config = {}
    return obj


def _call_build(launcher, cluster_mode=False):
    """Helper to call _build_server_command with sensible defaults."""
    return launcher._build_server_command(
        port=6379,
        bind_ip=None,
        cpu_range=None,
        tls_mode=False,
        cluster_mode=cluster_mode,
        io_threads=None,
        module_path=None,
        log_file="/tmp/test.log",
    )


# ---------------------------------------------------------------------------
# _build_server_command — custom-server-configs
# ---------------------------------------------------------------------------


class TestBuildServerCommandNoCustomConfigs:
    """WHEN no custom-server-configs are set, cmd has no extra flags."""

    def test_empty_config(self, launcher):
        cmd = _call_build(launcher)
        # The last config pair should be --save ''
        assert cmd[-2:] == ["--save", "''"]

    def test_config_without_custom_key(self, launcher):
        launcher.config = {"some_other_key": "value"}
        cmd = _call_build(launcher)
        assert cmd[-2:] == ["--save", "''"]


class TestBuildServerCommandCustomConfigsAppended:
    """WHEN custom-server-configs are set, they appear in cmd before defaults."""

    def test_configs_appear_in_cmd(self, launcher):
        launcher.config = {
            "custom-server-configs": {"maxmemory": "4gb", "timeout": 300}
        }
        cmd = _call_build(launcher)
        assert "--maxmemory" in cmd
        assert "4gb" in cmd
        assert "--timeout" in cmd
        assert "300" in cmd

    def test_configs_appear_BEFORE_benchmark_defaults(self, launcher):
        """Custom configs must come before the defaults block so last-wins
        semantics give defaults precedence."""
        launcher.config = {"custom-server-configs": {"maxmemory": "4gb"}}
        cmd = _call_build(launcher)
        save_idx = cmd.index("--save")
        maxmemory_idx = cmd.index("--maxmemory")
        assert maxmemory_idx < save_idx, (
            f"custom config should come before defaults; got "
            f"--maxmemory at {maxmemory_idx}, --save at {save_idx}"
        )


class TestBuildServerCommandClusterMode:
    """Custom configs also apply in cluster mode."""

    def test_cluster_mode_applies_custom_configs(self, launcher):
        launcher.config = {"custom-server-configs": {"hz": "100"}}
        cmd = _call_build(launcher, cluster_mode=True)
        assert "--hz" in cmd
        assert "100" in cmd


class TestBuildServerCommandNumericStringification:
    """Numeric values are stringified in the command."""

    def test_int_value_stringified(self, launcher):
        launcher.config = {"custom-server-configs": {"timeout": 300}}
        cmd = _call_build(launcher)
        idx = cmd.index("--timeout")
        assert cmd[idx + 1] == "300"

    def test_float_value_stringified(self, launcher):
        launcher.config = {"custom-server-configs": {"hz": 10.5}}
        cmd = _call_build(launcher)
        idx = cmd.index("--hz")
        assert cmd[idx + 1] == "10.5"


class TestBuildServerCommandDefenseInDepth:
    """Even if a reserved key bypassed validation, harness defaults still win
    via valkey CLI last-wins semantics (defaults come after custom configs)."""

    def test_benchmark_defaults_win_on_collision(self, launcher):
        # Bypass validation by setting config directly on launcher.
        launcher.config = {"custom-server-configs": {"maxmemory-policy": "noeviction"}}
        cmd = _call_build(launcher)
        # Both should be present; the LAST occurrence is what valkey honors.
        assert cmd.count("--maxmemory-policy") == 2
        last_idx = len(cmd) - 1 - cmd[::-1].index("--maxmemory-policy")
        assert (
            cmd[last_idx + 1] == "allkeys-lru"
        ), "benchmark default must come last (and therefore win)"


# ---------------------------------------------------------------------------
# _build_server_command — custom-server-config-file
# ---------------------------------------------------------------------------


class TestBuildServerCommandCustomConfigFile:
    """Optional positional config file passed right after the binary."""

    def test_no_conf_file_means_no_positional(self, launcher):
        """Without conf file, no positional arg between binary and first --flag."""
        cmd = _call_build(launcher)
        binary_idx = next(i for i, x in enumerate(cmd) if x.endswith("valkey-server"))
        assert cmd[binary_idx + 1].startswith("--")

    def test_conf_file_appears_right_after_binary(self, launcher):
        """Conf file must be the first positional arg after valkey-server."""
        launcher.config = {"custom-server-config-file": "/etc/valkey/extra.conf"}
        cmd = _call_build(launcher)
        binary_idx = next(i for i, x in enumerate(cmd) if x.endswith("valkey-server"))
        assert cmd[binary_idx + 1] == "/etc/valkey/extra.conf"
        # The next token must start with "--" (flags come after)
        assert cmd[binary_idx + 2].startswith("--")

    def test_conf_file_precedes_custom_configs_and_defaults(self, launcher):
        """Order: conf file (lowest) → custom configs → benchmark defaults."""
        launcher.config = {
            "custom-server-config-file": "/etc/valkey/base.conf",
            "custom-server-configs": {"maxmemory": "8gb"},
        }
        cmd = _call_build(launcher)
        conf_idx = cmd.index("/etc/valkey/base.conf")
        maxmem_idx = cmd.index("--maxmemory")
        save_idx = cmd.index("--save")
        assert conf_idx < maxmem_idx < save_idx

    def test_only_conf_file_no_inline(self, launcher):
        """Conf file alone works; cmd has no extra --flags from inline."""
        launcher.config = {"custom-server-config-file": "/path/to/x.conf"}
        cmd = _call_build(launcher)
        assert "/path/to/x.conf" in cmd
        # Last config pair should still be --save '' (no inline appended)
        assert cmd[-2:] == ["--save", "''"]
