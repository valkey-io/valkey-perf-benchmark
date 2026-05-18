"""Unit tests for utils/cpu_utils.py — parse_core_range, calculate_cpu_ranges, validate_explicit_cpu_ranges."""

import pytest

from utils.cpu_utils import (
    calculate_cpu_ranges,
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
