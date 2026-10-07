"""Unit tests for utils/cpu_utils.py — parse_core_range, calculate_cpu_ranges, validate_explicit_cpu_ranges."""

import pytest

from utils.cpu_utils import (
    calculate_client_cpu_ranges,
    calculate_cpu_ranges,
    calculate_server_cpu_ranges,
    max_mixed_processes,
    parse_core_range,
    validate_explicit_cpu_ranges,
)

# ---------------------------------------------------------------------------
# parse_core_range — valid inputs
# ---------------------------------------------------------------------------


class TestParseCoreRangeValid:
    def test_simple_range(self):
        assert parse_core_range("0-3") == [0, 1, 2, 3]

    def test_comma_separated(self):
        assert parse_core_range("0,2,4") == [0, 2, 4]

    def test_mixed_ranges(self):
        assert parse_core_range("0-3,8-11") == [0, 1, 2, 3, 8, 9, 10, 11]

    def test_single_core(self):
        assert parse_core_range("5") == [5]

    def test_single_core_range(self):
        assert parse_core_range("3-3") == [3]

    def test_large_range(self):
        result = parse_core_range("144-191")
        assert len(result) == 48
        assert result[0] == 144
        assert result[-1] == 191


# ---------------------------------------------------------------------------
# parse_core_range — invalid inputs
# ---------------------------------------------------------------------------


class TestParseCoreRangeInvalid:
    def test_empty_string(self):
        with pytest.raises(ValueError):
            parse_core_range("")

    def test_reversed_range(self):
        with pytest.raises(ValueError):
            parse_core_range("5-2")

    def test_negative_value(self):
        with pytest.raises(ValueError):
            parse_core_range("-1")

    def test_malformed_string(self):
        with pytest.raises(ValueError):
            parse_core_range("abc")

    def test_leading_comma(self):
        with pytest.raises(ValueError):
            parse_core_range(",0-3")

    def test_trailing_comma(self):
        with pytest.raises(ValueError):
            parse_core_range("0-3,")

    def test_consecutive_commas(self):
        with pytest.raises(ValueError):
            parse_core_range("0,,3")

    def test_none_input(self):
        with pytest.raises(ValueError):
            parse_core_range(None)


# ---------------------------------------------------------------------------
# calculate_cpu_ranges
# ---------------------------------------------------------------------------


class TestCalculateCpuRanges:
    def test_single_node(self):
        result = calculate_cpu_ranges(cluster_nodes=1, cores_per_unit=4)
        assert result == ["0-3"]

    def test_multiple_nodes(self):
        result = calculate_cpu_ranges(cluster_nodes=3, cores_per_unit=4)
        assert result == ["0-3", "4-7", "8-11"]

    def test_with_offset(self):
        result = calculate_cpu_ranges(cluster_nodes=2, cores_per_unit=4, offset=8)
        assert result == ["8-11", "12-15"]

    def test_single_core_per_unit(self):
        result = calculate_cpu_ranges(cluster_nodes=3, cores_per_unit=1)
        assert result == ["0-0", "1-1", "2-2"]

    def test_returns_correct_count(self):
        result = calculate_cpu_ranges(cluster_nodes=5, cores_per_unit=2, offset=10)
        assert len(result) == 5


# ---------------------------------------------------------------------------
# validate_explicit_cpu_ranges
# ---------------------------------------------------------------------------


class TestValidateExplicitCpuRanges:
    def test_non_overlapping_passes(self):
        validate_explicit_cpu_ranges("0", "1")

    def test_overlapping_raises(self):
        with pytest.raises(ValueError, match="overlap"):
            validate_explicit_cpu_ranges("0-1", "1-2")

    def test_identical_ranges_raises(self):
        with pytest.raises(ValueError, match="overlap"):
            validate_explicit_cpu_ranges("0", "0")

    def test_non_overlapping_non_contiguous(self):
        validate_explicit_cpu_ranges("0", "1")


# ---------------------------------------------------------------------------
# max_mixed_processes
# ---------------------------------------------------------------------------


def _mixed(writes, reads):
    return {"id": "m", "type": "mixed", "writes": writes, "reads": reads}


class TestMaxMixedProcesses:
    def test_no_test_groups(self):
        assert max_mixed_processes({}) == 0

    def test_no_mixed_scenario(self):
        cfg = {"test_groups": [{"scenarios": [{"id": "s", "test": "GET"}]}]}
        assert max_mixed_processes(cfg) == 0

    def test_counts_writes_plus_reads(self):
        cfg = {"test_groups": [{"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]}]}
        assert max_mixed_processes(cfg) == 2

    def test_widest_across_groups(self):
        cfg = {
            "test_groups": [
                {"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]},
                {
                    "scenarios": [
                        {"id": "s", "test": "GET"},
                        _mixed([{"id": "w"}], [{"id": f"r{i}"} for i in range(8)]),
                    ]
                },
            ]
        }
        assert max_mixed_processes(cfg) == 9


# ---------------------------------------------------------------------------
# calculate_client_cpu_ranges: automatic pool sizing
# ---------------------------------------------------------------------------


def _auto_cfg(cores_per_server, cores_per_client, **extra):
    cfg = {
        "cpu_allocation": {
            "cores_per_server": cores_per_server,
            "cores_per_client": cores_per_client,
        }
    }
    cfg.update(extra)
    return cfg


class TestAutomaticClientPoolSizing:
    def test_no_cpu_allocation_returns_none(self):
        assert calculate_client_cpu_ranges({}) is None

    def test_single_node_no_mixed_scenario(self):
        assert calculate_client_cpu_ranges(_auto_cfg(8, 24)) == ["8-31"]

    def test_single_node_two_mixed_processes(self):
        cfg = _auto_cfg(
            8,
            24,
            cluster_mode=False,
            test_groups=[{"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]}],
        )
        assert calculate_client_cpu_ranges(cfg) == ["8-31", "32-55"]

    def test_single_node_three_mixed_processes(self):
        cfg = _auto_cfg(
            4,
            2,
            test_groups=[
                {"scenarios": [_mixed([{"id": "w"}], [{"id": "r1"}, {"id": "r2"}])]}
            ],
        )
        assert calculate_client_cpu_ranges(cfg) == ["4-5", "6-7", "8-9"]

    def test_server_ranges_unaffected_by_mixed_width(self):
        cfg = _auto_cfg(
            8,
            24,
            test_groups=[{"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]}],
        )
        assert calculate_server_cpu_ranges(cfg) == ["0-7"]

    def test_cluster_nodes_win_when_wider_than_mixed(self):
        cfg = _auto_cfg(
            8,
            8,
            cluster_mode=True,
            cluster_nodes=5,
            test_groups=[{"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]}],
        )
        assert calculate_client_cpu_ranges(cfg) == [
            "40-47",
            "48-55",
            "56-63",
            "64-71",
            "72-79",
        ]

    def test_mixed_width_extends_cluster_pool_keeping_node_prefix(self):
        cfg = _auto_cfg(
            8,
            8,
            cluster_mode=True,
            cluster_nodes=2,
            test_groups=[
                {"scenarios": [_mixed([{"id": "w"}], [{"id": "r1"}, {"id": "r2"}])]}
            ],
        )
        ranges = calculate_client_cpu_ranges(cfg)
        assert ranges == ["16-23", "24-31", "32-39"]
        assert ranges[:2] == ["16-23", "24-31"]

    def test_explicit_clients_array_is_untouched(self):
        cfg = _auto_cfg(
            8,
            24,
            test_groups=[{"scenarios": [_mixed([{"id": "w"}], [{"id": "r"}])]}],
        )
        cfg["cpu_allocation"]["clients"] = ["8-31"]
        assert calculate_client_cpu_ranges(cfg) == ["8-31"]
