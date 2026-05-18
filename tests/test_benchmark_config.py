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


class TestValidateTestGroups:
    """Tests for validate_test_groups."""

    def test_no_test_groups_key_passes(self):
        validate_test_groups({})  # should not raise

    def test_not_a_list_raises(self):
        with pytest.raises(ValueError, match="must be a non-empty list"):
            validate_test_groups({"test_groups": "not a list"})

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="must be a non-empty list"):
            validate_test_groups({"test_groups": []})

    def test_element_not_dict_raises(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_test_groups({"test_groups": ["not a dict"]})

    def test_element_missing_scenarios_raises(self):
        with pytest.raises(ValueError, match="missing 'scenarios' field"):
            validate_test_groups({"test_groups": [{"group": 1}]})

    def test_empty_scenarios_raises(self):
        with pytest.raises(ValueError, match="scenarios must be a non-empty list"):
            validate_test_groups({"test_groups": [{"scenarios": []}]})

    def test_scenarios_not_list_raises(self):
        with pytest.raises(ValueError, match="scenarios must be a non-empty list"):
            validate_test_groups({"test_groups": [{"scenarios": "bad"}]})

    def test_valid_test_groups_passes(self):
        cfg = {"test_groups": [{"scenarios": [{"id": "s1", "command": "GET key"}]}]}
        validate_test_groups(cfg)  # should not raise


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
