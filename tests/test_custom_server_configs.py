"""Unit tests for custom-server-configs in ServerLauncher._build_server_command."""

import copy

import pytest

from benchmark import validate_config
from valkey_server import FRAMEWORK_SERVER_FLAGS, ServerLauncher


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
        """Custom configs come before the defaults block. A default that
        collides with a custom key is skipped rather than reordered."""
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


class TestBuildServerCommandPrecedenceOnCollision:
    """A user-supplied key wins over the matching benchmark default: the default
    is skipped so the flag appears exactly once."""

    def test_user_maxmemory_policy_wins(self, launcher):
        launcher.config = {"custom-server-configs": {"maxmemory-policy": "noeviction"}}
        cmd = _call_build(launcher)
        # The default is skipped entirely, so the flag appears exactly once.
        assert cmd.count("--maxmemory-policy") == 1
        idx = cmd.index("--maxmemory-policy")
        assert cmd[idx + 1] == "noeviction"
        assert "allkeys-lru" not in cmd

    def test_no_collision_command_unchanged(self, launcher):
        """Non-colliding customs leave the defaults region exactly as before."""
        launcher.config = {
            "custom-server-configs": {
                "maxmemory": "16gb",
                "timeout": 0,
                "maxclients": 10000,
            }
        }
        cmd = _call_build(launcher)
        save_idx = cmd.index("--save")
        assert cmd[save_idx - 12 :] == [
            "--cluster-enabled",
            "no",
            "--daemonize",
            "yes",
            "--maxmemory-policy",
            "allkeys-lru",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
            "--logfile",
            "/tmp/test.log",
            "--save",
            "''",
        ]


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


# ---------------------------------------------------------------------------
# validate_config: custom-server-configs key rejection
# ---------------------------------------------------------------------------


class TestCustomServerConfigsValidation:
    """Keys the framework manages are rejected, everything else passes."""

    @pytest.mark.parametrize(
        "key",
        [
            "port",
            "io-threads",
            "tls-port",
            "loadmodule",
            "cluster-config-file",
            "daemonize",
            "logfile",
            "save",
        ],
    )
    def test_framework_managed_key_rejected(self, minimal_valid_config, key):
        cfg = copy.deepcopy(minimal_valid_config)
        cfg["custom-server-configs"] = {key: "x"}
        with pytest.raises(ValueError) as exc:
            validate_config(cfg)
        msg = str(exc.value)
        assert repr(key) in msg
        assert "is managed by the framework" in msg
        assert FRAMEWORK_SERVER_FLAGS[key] in msg

    def test_unmanaged_keys_accepted(self, minimal_valid_config):
        cfg = copy.deepcopy(minimal_valid_config)
        cfg["custom-server-configs"] = {
            "maxmemory-policy": "noeviction",
            "maxmemory": "1gb",
            "hz": 20,
        }
        validate_config(cfg)


class TestFrameworkServerFlagsCoverEmitter:
    """Every flag _build_server_command emits is either a framework flag or a
    benchmark default, so FRAMEWORK_SERVER_FLAGS cannot drift from the emitter."""

    BENCHMARK_DEFAULTS = {
        "cluster-enabled",
        "daemonize",
        "maxmemory-policy",
        "appendonly",
        "protected-mode",
        "logfile",
        "save",
    }

    def test_all_emitted_flags_are_known(self, launcher):
        launcher.valkey_path = "/tmp/valkey"
        launcher.config = {"cluster_config_dir": "."}
        launcher.modules = [
            {"path": "/x.so", "startup_args": ["--a"]},
            {"path": "/y.so"},
        ]
        cmd = launcher._build_server_command(
            port=6379,
            bind_ip="10.0.0.1",
            cpu_range=None,
            tls_mode=True,
            cluster_mode=True,
            io_threads=4,
            module_path=None,
            log_file="/tmp/t.log",
        )
        emitted = {tok.split()[0][2:] for tok in cmd if tok.split()[0].startswith("--")}
        known = set(FRAMEWORK_SERVER_FLAGS) | self.BENCHMARK_DEFAULTS
        assert emitted <= known, f"unlisted flags: {sorted(emitted - known)}"
        for flag in (
            "port",
            "tls-port",
            "io-threads",
            "loadmodule",
            "cluster-config-file",
            "bind",
        ):
            assert flag in emitted
