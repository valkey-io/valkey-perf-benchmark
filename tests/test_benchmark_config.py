"""Unit tests for benchmark.py: validate_config, parse_bool, and validation helpers."""

import pytest

from benchmark import (
    validate_config,
    parse_bool,
    _validate_positive_int_list,
    _validate_positive_int,
    _validate_non_negative_int,
    _validate_positive_int_or_list,
    validate_cpu_allocation,
    validate_test_groups,
    _get_active_ports,
    _resolve_io_threads_list,
)

# ---------------------------------------------------------------------------
# validate_config — missing required keys
# ---------------------------------------------------------------------------


class TestValidateConfigMissingKeys:
    """WHEN validate_config is called with a config missing required keys,
    it SHALL raise a ValueError."""

    def test_missing_keyspacelen(self, minimal_valid_config):
        del minimal_valid_config["keyspacelen"]
        with pytest.raises(ValueError, match="Missing required key"):
            validate_config(minimal_valid_config)

    def test_missing_commands(self, minimal_valid_config):
        del minimal_valid_config["commands"]
        # Without commands AND without test_groups → "must have either"
        with pytest.raises(ValueError):
            validate_config(minimal_valid_config)

    def test_missing_warmup(self, minimal_valid_config):
        del minimal_valid_config["warmup"]
        with pytest.raises(ValueError, match="Missing required key"):
            validate_config(minimal_valid_config)

    def test_missing_cluster_mode(self, minimal_valid_config):
        del minimal_valid_config["cluster_mode"]
        with pytest.raises(ValueError, match="Missing required key"):
            validate_config(minimal_valid_config)


# ---------------------------------------------------------------------------
# validate_config — both requests and duration
# ---------------------------------------------------------------------------


class TestValidateConfigBothRequestsAndDuration:
    """WHEN validate_config is called with both 'requests' and 'duration',
    it SHALL raise a ValueError."""

    def test_both_requests_and_duration(self, minimal_valid_config):
        minimal_valid_config["duration"] = 10
        with pytest.raises(ValueError, match="Cannot specify both"):
            validate_config(minimal_valid_config)


# ---------------------------------------------------------------------------
# validate_config — neither requests nor duration
# ---------------------------------------------------------------------------


class TestValidateConfigNeitherRequestsNorDuration:
    """WHEN validate_config is called with neither 'requests' nor 'duration',
    it SHALL raise a ValueError."""

    def test_neither_requests_nor_duration(self, minimal_valid_config):
        del minimal_valid_config["requests"]
        with pytest.raises(ValueError, match="Either 'requests' or 'duration'"):
            validate_config(minimal_valid_config)

    def test_requests_none_and_no_duration(self, minimal_valid_config):
        minimal_valid_config["requests"] = None
        with pytest.raises(ValueError, match="Either 'requests' or 'duration'"):
            validate_config(minimal_valid_config)


# ---------------------------------------------------------------------------
# validate_config — valid commands-based config
# ---------------------------------------------------------------------------


class TestValidateConfigCommandsFormat:
    """WHEN validate_config is called with a valid commands-based config,
    it SHALL complete without error."""

    def test_valid_commands_config(self, minimal_valid_config):
        validate_config(minimal_valid_config)  # should not raise

    def test_valid_commands_config_with_duration(self, minimal_valid_config):
        del minimal_valid_config["requests"]
        minimal_valid_config["duration"] = 30
        validate_config(minimal_valid_config)  # should not raise


# ---------------------------------------------------------------------------
# validate_config — valid test_groups-based config
# ---------------------------------------------------------------------------


class TestValidateConfigTestGroupsFormat:
    """WHEN validate_config is called with a valid test_groups-based config,
    it SHALL complete without error."""

    def test_valid_test_groups_config(self, minimal_test_groups_config):
        validate_config(minimal_test_groups_config)  # should not raise


# ---------------------------------------------------------------------------
# validate_config — mutation of cluster_mode / tls_mode
# ---------------------------------------------------------------------------


class TestValidateConfigMutation:
    """validate_config SHALL convert cluster_mode and tls_mode to bool."""

    def test_cluster_mode_string_converted(self, minimal_valid_config):
        minimal_valid_config["cluster_mode"] = "yes"
        validate_config(minimal_valid_config)
        assert minimal_valid_config["cluster_mode"] is True

    def test_tls_mode_string_converted(self, minimal_valid_config):
        minimal_valid_config["tls_mode"] = "false"
        validate_config(minimal_valid_config)
        assert minimal_valid_config["tls_mode"] is False


# ---------------------------------------------------------------------------
# validate_config — post_commands
# ---------------------------------------------------------------------------


class TestValidateConfigPostCommands:
    """validate_config SHALL accept a list of command strings and reject
    malformed entries."""

    def test_command_strings_accepted(self, minimal_valid_config):
        minimal_valid_config["post_commands"] = ["INFO memory", "MEMORY STATS"]
        validate_config(minimal_valid_config)

    def test_empty_list_accepted(self, minimal_valid_config):
        minimal_valid_config["post_commands"] = []
        validate_config(minimal_valid_config)

    def test_not_a_list(self, minimal_valid_config):
        minimal_valid_config["post_commands"] = "INFO memory"
        with pytest.raises(ValueError, match="'post_commands' must be a list"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize("entry", ["", "  ", 123, None, {"cmd": "INFO"}])
    def test_invalid_entry_rejected(self, minimal_valid_config, entry):
        minimal_valid_config["post_commands"] = [entry]
        with pytest.raises(
            ValueError, match=r"post_commands\[0\]' must be a non-empty string"
        ):
            validate_config(minimal_valid_config)


# ---------------------------------------------------------------------------
# parse_bool
# ---------------------------------------------------------------------------


class TestParseBool:
    """Tests for parse_bool with booleans, truthy/falsy strings, and other types."""

    def test_true(self):
        assert parse_bool(True) is True

    def test_false(self):
        assert parse_bool(False) is False

    @pytest.mark.parametrize("val", ["yes", "true", "1", "YES", "True", "TRUE"])
    def test_truthy_strings(self, val):
        assert parse_bool(val) is True

    @pytest.mark.parametrize("val", ["no", "false", "0", "NO", "False", "FALSE"])
    def test_falsy_strings(self, val):
        assert parse_bool(val) is False

    def test_unrecognized_string_returns_false(self):
        assert parse_bool("maybe") is False

    def test_non_string_non_bool_uses_builtin(self):
        assert parse_bool(42) is True
        assert parse_bool(0) is False


# ---------------------------------------------------------------------------
# _validate_positive_int_list
# ---------------------------------------------------------------------------


class TestValidatePositiveIntList:
    """Tests for _validate_positive_int_list helper."""

    def test_valid_list(self):
        _validate_positive_int_list([1, 2, 3], "test")  # should not raise

    def test_empty_list_accepted(self):
        # all() on empty iterable is True, so no raise per implementation
        _validate_positive_int_list([], "test")  # should not raise

    def test_not_a_list_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int_list("not a list", "test")

    def test_contains_zero_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int_list([1, 0, 3], "test")

    def test_contains_negative_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int_list([1, -1], "test")

    def test_contains_non_int_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int_list([1, 2.5], "test")


# ---------------------------------------------------------------------------
# _validate_positive_int
# ---------------------------------------------------------------------------


class TestValidatePositiveInt:
    """Tests for _validate_positive_int helper."""

    def test_valid(self):
        _validate_positive_int(5, "test")  # should not raise

    def test_zero_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int(0, "test")

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int(-1, "test")

    def test_non_int_raises(self):
        with pytest.raises(ValueError):
            _validate_positive_int(3.14, "test")


# ---------------------------------------------------------------------------
# _validate_non_negative_int
# ---------------------------------------------------------------------------


class TestValidateNonNegativeInt:
    """Tests for _validate_non_negative_int helper."""

    def test_zero_valid(self):
        _validate_non_negative_int(0, "test")  # should not raise

    def test_positive_valid(self):
        _validate_non_negative_int(10, "test")  # should not raise

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            _validate_non_negative_int(-1, "test")

    def test_non_int_raises(self):
        with pytest.raises(ValueError):
            _validate_non_negative_int(1.5, "test")


# ---------------------------------------------------------------------------
# validate_cpu_allocation
# ---------------------------------------------------------------------------


class TestValidateCpuAllocation:
    """Tests for validate_cpu_allocation."""

    def test_no_cpu_fields_passes(self):
        validate_cpu_allocation({})  # should not raise

    def test_mutually_exclusive_raises(self):
        cfg = {
            "cpu_allocation": {"cores_per_server": 2, "cores_per_client": 2},
            "server_cpu_range": "0-3",
        }
        with pytest.raises(ValueError, match="Cannot use both"):
            validate_cpu_allocation(cfg)

    def test_mutually_exclusive_with_client_range_raises(self):
        cfg = {
            "cpu_allocation": {"cores_per_server": 2, "cores_per_client": 2},
            "client_cpu_range": "4-7",
        }
        with pytest.raises(ValueError, match="Cannot use both"):
            validate_cpu_allocation(cfg)

    def test_missing_cores_per_client_raises(self):
        cfg = {"cpu_allocation": {"cores_per_server": 4}}
        with pytest.raises(ValueError, match="requires both"):
            validate_cpu_allocation(cfg)

    def test_missing_cores_per_server_raises(self):
        cfg = {"cpu_allocation": {"cores_per_client": 4}}
        with pytest.raises(ValueError, match="requires both"):
            validate_cpu_allocation(cfg)

    def test_zero_cores_per_server_raises(self):
        cfg = {"cpu_allocation": {"cores_per_server": 0, "cores_per_client": 2}}
        with pytest.raises(ValueError, match="must be positive"):
            validate_cpu_allocation(cfg)

    def test_negative_cores_per_client_raises(self):
        cfg = {"cpu_allocation": {"cores_per_server": 2, "cores_per_client": -1}}
        with pytest.raises(ValueError, match="must be positive"):
            validate_cpu_allocation(cfg)

    def test_valid_cpu_allocation_passes(self):
        cfg = {"cpu_allocation": {"cores_per_server": 4, "cores_per_client": 4}}
        validate_cpu_allocation(cfg)  # should not raise

    def test_old_style_with_both_ranges_calls_validation(self):
        cfg = {"server_cpu_range": "0", "client_cpu_range": "1"}
        validate_cpu_allocation(cfg)  # should not raise

    def test_old_style_with_only_server_range(self):
        cfg = {"server_cpu_range": "0-3"}
        validate_cpu_allocation(cfg)  # should not raise

    def test_explicit_arrays_overlap_raises(self):
        cfg = {
            "cpu_allocation": {
                "cores_per_server": 4,
                "cores_per_client": 4,
                "servers": ["0-7"],
                "clients": ["6-13"],
            }
        }
        with pytest.raises(
            ValueError, match=r"'servers' and 'clients' overlap on cores: \[6, 7\]"
        ):
            validate_cpu_allocation(cfg)

    def test_explicit_arrays_overlap_across_ranges_raises(self):
        cfg = {
            "cpu_allocation": {
                "cores_per_server": 2,
                "cores_per_client": 2,
                "servers": ["0-1", "4-5"],
                "clients": ["2-3", "5,9"],
            }
        }
        with pytest.raises(ValueError, match=r"overlap on cores: \[5\]"):
            validate_cpu_allocation(cfg)

    def test_explicit_arrays_disjoint_passes(self):
        cfg = {
            "cpu_allocation": {
                "cores_per_server": 8,
                "cores_per_client": 24,
                "servers": ["0-7"],
                "clients": ["8-31", "32-55"],
            }
        }
        validate_cpu_allocation(cfg)  # should not raise

    def test_explicit_arrays_may_exceed_system_cores(self):
        cfg = {
            "cpu_allocation": {
                "cores_per_server": 8,
                "cores_per_client": 8,
                "servers": ["0-7"],
                "clients": ["8-8191"],
            }
        }
        validate_cpu_allocation(cfg)  # should not raise

    def test_clients_only_skips_overlap_check(self):
        cfg = {
            "cpu_allocation": {
                "cores_per_server": 4,
                "cores_per_client": 4,
                "clients": ["0-7"],
            }
        }
        validate_cpu_allocation(cfg)  # should not raise


# ---------------------------------------------------------------------------
# _validate_positive_int_or_list
# ---------------------------------------------------------------------------


class TestValidatePositiveIntOrList:
    """Tests for _validate_positive_int_or_list helper."""

    def test_valid_int(self):
        _validate_positive_int_or_list(5, "test")  # should not raise

    def test_zero_int_raises(self):
        with pytest.raises(ValueError, match="must be positive"):
            _validate_positive_int_or_list(0, "test")

    def test_negative_int_raises(self):
        with pytest.raises(ValueError, match="must be positive"):
            _validate_positive_int_or_list(-3, "test")

    def test_valid_list(self):
        _validate_positive_int_or_list([1, 2, 3], "test")  # should not raise

    def test_list_with_zero_raises(self):
        with pytest.raises(ValueError, match="must be list of positive integers"):
            _validate_positive_int_or_list([1, 0], "test")

    def test_list_with_negative_raises(self):
        with pytest.raises(ValueError, match="must be list of positive integers"):
            _validate_positive_int_or_list([1, -2], "test")

    def test_list_with_non_int_raises(self):
        with pytest.raises(ValueError, match="must be list of positive integers"):
            _validate_positive_int_or_list([1, 2.5], "test")

    def test_string_raises(self):
        with pytest.raises(ValueError, match="must be int or list"):
            _validate_positive_int_or_list("hello", "test")

    def test_float_raises(self):
        with pytest.raises(ValueError, match="must be int or list"):
            _validate_positive_int_or_list(3.14, "test")


# ---------------------------------------------------------------------------
# validate_test_groups
# ---------------------------------------------------------------------------


def _tg(scenario):
    """Wrap a single scenario dict in the minimal test_groups config shape."""
    return {"test_groups": [{"scenarios": [scenario]}]}


class TestValidateTestGroups:
    """Tests for validate_test_groups."""

    def test_no_test_groups_key_passes(self):
        validate_test_groups({})  # should not raise

    @pytest.mark.parametrize(
        "cfg, match",
        [
            ({"test_groups": "not a list"}, "must be a non-empty list"),
            ({"test_groups": []}, "must be a non-empty list"),
            ({"test_groups": ["not a dict"]}, "must be a dict"),
            ({"test_groups": [{"group": 1}]}, "missing 'scenarios' field"),
            (
                {"test_groups": [{"scenarios": []}]},
                "scenarios must be a non-empty list",
            ),
            (
                {"test_groups": [{"scenarios": "bad"}]},
                "scenarios must be a non-empty list",
            ),
        ],
    )
    def test_invalid_container_structure_raises(self, cfg, match):
        with pytest.raises(ValueError, match=match):
            validate_test_groups(cfg)

    @pytest.mark.parametrize(
        "scenario, match",
        [
            # exactly-one-of test/command
            (
                {"id": "s1", "test": "GET", "command": "GET key"},
                "exactly one of 'test' or 'command'",
            ),
            ({"id": "s1", "clients": 1}, "exactly one of 'test' or 'command'"),
            # options only valid with command
            (
                {"id": "s1", "test": "GET", "options": {"--foo": "_foo"}},
                "only valid with 'command', not 'test'",
            ),
            # populate_with value validation (guards mutation: drop this check)
            (
                {"id": "s1", "test": "GET", "populate_with": 123},
                "must be a non-empty string",
            ),
            (
                {"id": "s1", "test": "GET", "populate_with": ""},
                "must be a non-empty string",
            ),
            (
                {"id": "s1", "test": "GET", "populate_with": "GET"},
                "not a supported write command",
            ),
            # a single-word mixed populate must name a predefined write
            (
                {
                    "id": "s1",
                    "type": "mixed",
                    "writes": [{"id": "w", "test": "SET"}],
                    "reads": [{"id": "r", "test": "GET"}],
                    "populate_with": "GET",
                },
                "not a supported write command",
            ),
            # the populate tuning keys require populate_with
            (
                {"id": "s1", "test": "GET", "populate_clients": 50},
                "sets 'populate_clients' with no 'populate_with'",
            ),
            (
                {
                    "id": "s1",
                    "test": "GET",
                    "populate_benchmark_args": ["--keysize 100"],
                },
                "sets 'populate_benchmark_args' with no 'populate_with'",
            ),
            (
                {"id": "s1", "test": "GET", "populate_retries": 3},
                "sets 'populate_retries' with no 'populate_with'",
            ),
            # populate tuning key types
            (
                {
                    "id": "s1",
                    "test": "GET",
                    "populate_with": "SET",
                    "populate_clients": 0,
                },
                "'test_groups\\[0\\].scenarios\\[0\\].populate_clients' must be a positive integer",
            ),
            (
                {
                    "id": "s1",
                    "test": "GET",
                    "populate_with": "SET",
                    "populate_retries": -1,
                },
                "'test_groups\\[0\\].scenarios\\[0\\].populate_retries' must be a non-negative integer",
            ),
            (
                {
                    "id": "s1",
                    "test": "GET",
                    "populate_with": "SET",
                    "populate_benchmark_args": "--keysize 100",
                },
                "'populate_benchmark_args' must be a list of strings",
            ),
            (
                {
                    "id": "s1",
                    "test": "GET",
                    "populate_with": "SET",
                    "populate_benchmark_args": ["-n 100"],
                },
                "'populate_benchmark_args' sets '-n', which the framework emits itself",
            ),
            # benchmark_args must be a list of strings
            (
                {"id": "s1", "test": "GET", "benchmark_args": "--zipfian 1.0"},
                "'benchmark_args' must be a list of strings",
            ),
            (
                {"id": "s1", "test": "GET", "benchmark_args": ["--keysize", 100]},
                "'benchmark_args' must be a list of strings",
            ),
            # checked on a mixed parent too, before the mixed early-return
            (
                {
                    "id": "m1",
                    "type": "mixed",
                    "writes": [{"id": "w", "command": "SET foo bar"}],
                    "reads": [{"id": "r", "command": "GET foo"}],
                    "benchmark_args": {"--zipfian": "1.0"},
                },
                "'benchmark_args' must be a list of strings",
            ),
            # mixed children are validated at their own location
            (
                {
                    "id": "m1",
                    "type": "mixed",
                    "writes": [
                        {"id": "w", "test": "SET", "benchmark_args": "--zipfian 1.0"}
                    ],
                    "reads": [{"id": "r", "test": "GET"}],
                },
                r"scenarios\[0\]\.writes\[0\] 'benchmark_args' must be a list of strings",
            ),
            (
                {
                    "id": "m1",
                    "type": "mixed",
                    "writes": [{"id": "w", "test": "SET"}],
                    "reads": [
                        {"id": "r1", "test": "GET"},
                        {
                            "id": "r2",
                            "test": "GET",
                            "benchmark_args": ["--keysize", 100],
                        },
                    ],
                },
                r"scenarios\[0\]\.reads\[1\] 'benchmark_args' must be a list of strings",
            ),
            # flags the framework emits itself are rejected
            (
                {"id": "s1", "test": "GET", "benchmark_args": ["-c 50"]},
                "'benchmark_args' sets '-c', which the framework emits itself",
            ),
            (
                {"id": "s1", "test": "GET", "benchmark_args": ["--duration=30"]},
                "'benchmark_args' sets '--duration', which the framework emits itself",
            ),
            (
                {"id": "s1", "test": "GET", "benchmark_args": ["-- SET foo bar"]},
                "'benchmark_args' sets '--', which the framework emits itself",
            ),
            (
                {
                    "id": "m1",
                    "type": "mixed",
                    "writes": [
                        {"id": "w", "test": "SET", "benchmark_args": ["--seed 7"]}
                    ],
                    "reads": [{"id": "r", "test": "GET"}],
                },
                r"scenarios\[0\]\.writes\[0\] 'benchmark_args' sets '--seed'",
            ),
            # scenario-level post_commands get the same shape checks
            (
                {"id": "s1", "test": "GET", "post_commands": "INFO memory"},
                r"scenarios\[0\]\.post_commands' must be a list",
            ),
            (
                {"id": "s1", "test": "GET", "post_commands": [{"cmd": "INFO"}]},
                r"scenarios\[0\]\.post_commands\[0\]' must be a non-empty string",
            ),
            # mixed scenarios are validated before the early continue
            (
                {
                    "id": "s1",
                    "type": "mixed",
                    "writes": [{"id": "w", "command": "SET foo bar"}],
                    "reads": [{"id": "r", "command": "GET foo"}],
                    "post_commands": [""],
                },
                r"scenarios\[0\]\.post_commands\[0\]' must be a non-empty",
            ),
        ],
    )
    def test_invalid_scenario_raises(self, scenario, match):
        with pytest.raises(ValueError, match=match):
            validate_test_groups(_tg(scenario))

    @pytest.mark.parametrize(
        "scenario",
        [
            # plain command scenario
            {"id": "s1", "command": "GET key"},
            # test scenario seeded by a supported predefined write
            {"id": "s1", "test": "GET", "populate_with": "SET"},
            # options are valid on a command scenario
            {
                "id": "s1",
                "command": "FT.SEARCH idx q",
                "options": {"--nocontent": "_nocontent"},
            },
            # command scenario treats populate_with as an arbitrary write string
            {
                "id": "s1",
                "command": "GET key:__rand_int__",
                "populate_with": "SET key:__rand_int__ __data__",
            },
            # mixed scenario with no test/command and no populate_with
            {
                "id": "m1",
                "type": "mixed",
                "writes": [{"id": "w1", "command": "HSET k f v"}],
                "reads": [{"id": "r1", "command": "FT.SEARCH idx q"}],
            },
            # mixed scenario seeded by an arbitrary write command
            {
                "id": "m1",
                "type": "mixed",
                "writes": [{"id": "w1", "command": "SET k:__rand_int__ __data__ EX 5"}],
                "reads": [{"id": "r1", "command": "GET k:__rand_int__"}],
                "populate_with": "SET k:__rand_int__ __data__ EX 5",
            },
            # mixed scenario seeded by a predefined write, with its tuning keys
            {
                "id": "m1",
                "type": "mixed",
                "writes": [{"id": "w1", "test": "SET"}],
                "reads": [{"id": "r1", "test": "GET"}],
                "populate_with": "SET",
                "populate_clients": 50,
                "populate_benchmark_args": ["--keysize 100"],
                "populate_retries": 20,
            },
            # benchmark_args as a list of strings, on a test scenario
            {
                "id": "s1",
                "test": "GET",
                "benchmark_args": ["--zipfian 1.0", "--keysize 100"],
            },
            # an empty benchmark_args list is accepted
            {"id": "s1", "command": "GET key", "benchmark_args": []},
            # benchmark_args on a mixed parent, for its children to inherit
            {
                "id": "m1",
                "type": "mixed",
                "writes": [{"id": "w1", "test": "SET"}],
                "reads": [{"id": "r1", "test": "GET"}],
                "benchmark_args": ["--zipfian 1.0"],
            },
            # benchmark_args on a mixed child
            {
                "id": "m1",
                "type": "mixed",
                "writes": [
                    {
                        "id": "w1",
                        "test": "SET",
                        "benchmark_args": ["--zipfian 1.0", "--keysize 100"],
                    }
                ],
                "reads": [{"id": "r1", "test": "GET"}],
            },
            # a negative value token is not a protected flag
            {"id": "s1", "test": "GET", "benchmark_args": ["--zipfian -1.0"]},
            # scenario-level post_commands, mirroring setup_commands
            {
                "id": "s1",
                "test": "GET",
                "setup_commands": ["FT.CREATE idx ON HASH SCHEMA t TEXT"],
                "post_commands": ["INFO memory", "FT.INFO idx"],
            },
            # an empty list is valid and simply runs nothing
            {"id": "s1", "test": "GET", "post_commands": []},
        ],
    )
    def test_valid_scenario_passes(self, scenario):
        validate_test_groups(_tg(scenario))  # should not raise


# ---------------------------------------------------------------------------
# _get_active_ports
# ---------------------------------------------------------------------------


class TestGetActivePorts:
    """Tests for _get_active_ports."""

    def test_cluster_mode_with_cluster_ports(self):
        cfg = {"cluster_mode": True, "cluster_ports": [7000, 7001, 7002]}
        assert _get_active_ports(cfg) == [7000, 7001, 7002]

    def test_non_cluster_mode_with_port(self):
        cfg = {"cluster_mode": False, "port": 6380}
        assert _get_active_ports(cfg) == [6380]

    def test_non_cluster_mode_default_port(self):
        cfg = {"cluster_mode": False}
        assert _get_active_ports(cfg) == [6379]

    def test_no_port_key_defaults_to_6379(self):
        cfg = {}
        assert _get_active_ports(cfg) == [6379]

    def test_cluster_mode_without_cluster_ports_falls_back(self):
        cfg = {"cluster_mode": True, "port": 6380}
        assert _get_active_ports(cfg) == [6380]


# ---------------------------------------------------------------------------
# validate_config — custom-server-configs
# ---------------------------------------------------------------------------


class TestCustomServerConfigsValidation:
    """Tests for custom-server-configs validation in validate_config."""

    def test_valid_dict_with_str_int_float(self, minimal_valid_config):
        minimal_valid_config["custom-server-configs"] = {
            "maxmemory": "4gb",
            "timeout": 300,
            "tcp-keepalive": 60.0,
        }
        validate_config(minimal_valid_config)  # should not raise

    def test_missing_key_is_fine(self, minimal_valid_config):
        assert "custom-server-configs" not in minimal_valid_config
        validate_config(minimal_valid_config)  # should not raise

    def test_empty_dict_accepted(self, minimal_valid_config):
        minimal_valid_config["custom-server-configs"] = {}
        validate_config(minimal_valid_config)  # should not raise

    @pytest.mark.parametrize("bad_value", ["a string", ["a", "list"], 42, None])
    def test_reject_non_dict(self, minimal_valid_config, bad_value):
        minimal_valid_config["custom-server-configs"] = bad_value
        with pytest.raises(ValueError, match="must be a dictionary"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize("bad_key", [1, None])
    def test_reject_non_string_key(self, minimal_valid_config, bad_key):
        minimal_valid_config["custom-server-configs"] = {bad_key: "v"}
        with pytest.raises(ValueError, match="keys must be strings"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize("bad_value", [True, False])
    def test_reject_bool_value(self, minimal_valid_config, bad_value):
        minimal_valid_config["custom-server-configs"] = {"k": bad_value}
        with pytest.raises(ValueError, match="values must be strings or numbers"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize("bad_value", [[1, 2], {"nested": 1}, None])
    def test_reject_non_scalar_value(self, minimal_valid_config, bad_value):
        minimal_valid_config["custom-server-configs"] = {"k": bad_value}
        with pytest.raises(ValueError, match="values must be strings or numbers"):
            validate_config(minimal_valid_config)


class TestResolveIoThreadsList:
    """The sweep list comes from the top-level field, falling back to an
    io-threads value in custom-server-configs."""

    def test_absent_field_and_no_custom(self):
        assert _resolve_io_threads_list({}) == [None]

    def test_list_field_passes_through(self):
        assert _resolve_io_threads_list({"io-threads": [1, 4]}) == [1, 4]

    def test_scalar_field_wrapped(self):
        assert _resolve_io_threads_list({"io-threads": 8}) == [8]

    def test_custom_value_used_when_field_absent(self):
        cfg = {"custom-server-configs": {"io-threads": "9"}}
        assert _resolve_io_threads_list(cfg) == [9]


class TestCustomServerConfigFileValidation:
    """Tests for custom-server-config-file validation in validate_config."""

    def test_valid_string_path(self, minimal_valid_config):
        minimal_valid_config["custom-server-config-file"] = "/etc/valkey/extra.conf"
        validate_config(minimal_valid_config)  # should not raise

    def test_missing_key_is_fine(self, minimal_valid_config):
        assert "custom-server-config-file" not in minimal_valid_config
        validate_config(minimal_valid_config)  # should not raise

    @pytest.mark.parametrize("bad_value", [42, ["/path"], {"path": "x"}, None, True])
    def test_reject_non_string(self, minimal_valid_config, bad_value):
        minimal_valid_config["custom-server-config-file"] = bad_value
        with pytest.raises(ValueError, match="must be a string path"):
            validate_config(minimal_valid_config)


class TestPerSecondSamplingValidation:
    """Tests for per_second_sampling validation in validate_config."""

    @pytest.mark.parametrize(
        "good_value",
        [
            True,
            False,
            {},
            {"cpu_range": "56-63"},
            {"sources": {"valkey_info": {}}},
            {"sources": {"valkey_info": {}, "disk": {"path": "/mnt/nvme"}}},
            {"sources": {"latency_histogram": {}}, "cpu_range": "56-63,1"},
        ],
    )
    def test_accepts(self, minimal_valid_config, good_value):
        minimal_valid_config["per_second_sampling"] = good_value
        validate_config(minimal_valid_config)

    @pytest.mark.parametrize(
        "bad_value, match",
        [
            ("yes", "must be a boolean or an object"),
            (None, "must be a boolean or an object"),
            ({"disk_path": "/mnt"}, r"does not support key\(s\): \['disk_path'\]"),
            ({"sources": ["valkey_info"]}, "must be a non-empty object"),
            ({"sources": {}}, "must be a non-empty object"),
            ({"sources": {"network": {}}}, "unknown source 'network'"),
            ({"sources": {"valkey_info": True}}, "valkey_info' must be an object"),
            (
                {"sources": {"valkey_info": {"path": "/mnt"}}},
                r"valkey_info' does not support key\(s\): \['path'\]",
            ),
            ({"sources": {"disk": {"path": ""}}}, "disk.path' must be a non-empty"),
            ({"cpu_range": "56-"}, "per_second_sampling.cpu_range"),
        ],
    )
    def test_rejects(self, minimal_valid_config, bad_value, match):
        minimal_valid_config["per_second_sampling"] = bad_value
        with pytest.raises(ValueError, match=match):
            validate_config(minimal_valid_config)


class TestBuildArgsValidation:
    """Tests for build_args validation in validate_config."""

    @pytest.mark.parametrize(
        "value", [["BUILD_EXT_STORAGE=yes"], ["MALLOC=libc", "OPT=-O2"], []]
    )
    def test_valid_lists_accepted(self, minimal_valid_config, value):
        minimal_valid_config["build_args"] = value
        validate_config(minimal_valid_config)

    def test_missing_key_is_fine(self, minimal_valid_config):
        validate_config(minimal_valid_config)

    @pytest.mark.parametrize("bad_value", ["BUILD_EXT_STORAGE=yes", None, {"A": "b"}])
    def test_reject_non_list(self, minimal_valid_config, bad_value):
        minimal_valid_config["build_args"] = bad_value
        with pytest.raises(ValueError, match="must be a list of strings"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize("bad_value", [[1], ["A=b", None]])
    def test_reject_non_string_entry(self, minimal_valid_config, bad_value):
        minimal_valid_config["build_args"] = bad_value
        with pytest.raises(ValueError, match="must be a list of strings"):
            validate_config(minimal_valid_config)

    @pytest.mark.parametrize(
        "bad_arg", ["build_ext=yes", "=yes", "FOO", "1FOO=yes", "FOO-BAR=yes"]
    )
    def test_reject_bad_name(self, minimal_valid_config, bad_arg):
        minimal_valid_config["build_args"] = [bad_arg]
        with pytest.raises(ValueError, match="must have the form NAME=value"):
            validate_config(minimal_valid_config)

    def test_reject_build_tls(self, minimal_valid_config):
        minimal_valid_config["build_args"] = ["BUILD_TLS=yes"]
        with pytest.raises(ValueError, match="set from 'tls_mode'"):
            validate_config(minimal_valid_config)

    def test_reject_duplicate_name(self, minimal_valid_config):
        minimal_valid_config["build_args"] = ["OPT=-O2", "OPT=-O3"]
        with pytest.raises(ValueError, match="more than once"):
            validate_config(minimal_valid_config)


class TestHasSelectedScenarios:
    CFG = {
        "test_groups": [
            {"group": 1, "scenarios": [{"id": "a"}, {"id": "b"}]},
            {"group": 2, "scenarios": [{"id": "c"}]},
        ]
    }

    @pytest.mark.parametrize(
        "groups, scenarios, expected",
        [
            (None, None, True),
            ({2}, None, True),
            ({3}, None, False),
            (None, {"c"}, True),
            (None, {"z"}, False),
            ({1}, {"c"}, False),
            ({1, 2}, {"c"}, True),
        ],
    )
    def test_filters(self, groups, scenarios, expected):
        from benchmark import has_selected_scenarios

        cfg = dict(self.CFG)
        if groups:
            cfg["groups_to_run"] = groups
        if scenarios:
            cfg["scenario_filter"] = scenarios
        assert has_selected_scenarios(cfg) is expected

    def test_config_without_test_groups_always_runs(self):
        from benchmark import has_selected_scenarios

        assert has_selected_scenarios({"commands": ["SET"]}) is True
