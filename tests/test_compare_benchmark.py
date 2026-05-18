"""Unit tests for utils/compare_benchmark_results.py statistical functions."""

import pytest

from utils.compare_benchmark_results import (
    calculate_mean,
    calculate_stdev,
    calculate_confidence_interval,
    calculate_prediction_interval,
    calculate_percentage_change,
    average_multiple_runs,
    discover_config_keys,
    group_by_command,
    calculate_prediction_interval_percentage,
    calculate_confidence_interval_percentage,
    calculate_percent_change_with_ci,
    extract_version_identifier,
    create_config_signature,
    create_config_sort_key,
    summarize_benchmark_results,
    _format_with_sig_figs,
    _format_stats_only,
    _format_percent_change,
    _extract_common_and_unique_config,
    CONFIDENCE_PERCENT,
)

# --- calculate_mean ---


class TestCalculateMean:
    def test_normal_list(self):
        assert calculate_mean([1.0, 2.0, 3.0]) == 2.0

    def test_list_with_none_values(self):
        result = calculate_mean([1.0, None, 3.0])
        assert result == 2.0

    def test_empty_list(self):
        assert calculate_mean([]) == 0.0

    def test_single_value(self):
        assert calculate_mean([5.0]) == 5.0

    def test_all_none(self):
        assert calculate_mean([None, None]) == 0.0


# --- calculate_stdev ---


class TestCalculateStdev:
    def test_single_value(self):
        assert calculate_stdev([5.0]) == 0.0

    def test_empty_list(self):
        assert calculate_stdev([]) == 0.0

    def test_normal_list(self):
        result = calculate_stdev([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        assert result == pytest.approx(2.138, abs=0.001)

    def test_identical_values(self):
        assert calculate_stdev([3.0, 3.0, 3.0]) == 0.0

    def test_filters_none(self):
        # With None filtered out, only one value remains -> 0.0
        assert calculate_stdev([5.0, None]) == 0.0

    def test_filters_none_with_multiple_valid_values(self):
        # [10.0, 20.0] after filtering None — stdev should be non-zero
        result = calculate_stdev([10.0, None, 20.0])
        assert result == pytest.approx(calculate_stdev([10.0, 20.0]))


# --- calculate_confidence_interval ---


class TestCalculateConfidenceInterval:
    def test_empty_list_returns_zeros(self):
        assert calculate_confidence_interval([]) == (0.0, 0.0)

    def test_single_value_returns_zeros(self):
        assert calculate_confidence_interval([5.0]) == (0.0, 0.0)

    def test_two_values_returns_ordered_bounds(self):
        lower, upper = calculate_confidence_interval([10.0, 20.0])
        assert lower <= upper

    def test_normal_list_bounds_contain_mean(self):
        values = [10.0, 12.0, 11.0, 13.0, 10.5]
        lower, upper = calculate_confidence_interval(values)
        mean = calculate_mean(values)
        assert lower <= mean <= upper


# --- calculate_prediction_interval ---


class TestCalculatePredictionInterval:
    def test_empty_list_returns_zeros(self):
        assert calculate_prediction_interval([]) == (0.0, 0.0)

    def test_single_value_returns_zeros(self):
        assert calculate_prediction_interval([5.0]) == (0.0, 0.0)

    def test_two_values_returns_ordered_bounds(self):
        lower, upper = calculate_prediction_interval([10.0, 20.0])
        assert lower <= upper

    def test_prediction_interval_wider_than_confidence(self):
        values = [10.0, 12.0, 11.0, 13.0, 10.5]
        ci_lower, ci_upper = calculate_confidence_interval(values)
        pi_lower, pi_upper = calculate_prediction_interval(values)
        ci_width = ci_upper - ci_lower
        pi_width = pi_upper - pi_lower
        assert pi_width >= ci_width


# --- calculate_percentage_change ---


class TestCalculatePercentageChange:
    def test_normal_increase(self):
        assert calculate_percentage_change(150.0, 100.0) == 50.0

    def test_normal_decrease(self):
        assert calculate_percentage_change(80.0, 100.0) == -20.0

    def test_no_change(self):
        assert calculate_percentage_change(100.0, 100.0) == 0.0

    def test_zero_old_value(self):
        assert calculate_percentage_change(100.0, 0.0) == 0.0


# --- average_multiple_runs ---


class TestAverageMultipleRuns:
    def test_single_run_preserves_data_with_zero_stdev(self):
        data = [
            {
                "command": "GET",
                "pipeline": 1,
                "data_size": 64,
                "rps": 150000.0,
                "avg_latency_ms": 0.5,
                "p50_latency_ms": 0.4,
                "p95_latency_ms": 0.8,
                "p99_latency_ms": 1.2,
            }
        ]
        result = average_multiple_runs(data)
        assert len(result) == 1
        item = result[0]
        assert item["run_count"] == 1
        assert item["rps_stdev"] == 0.0
        assert item["avg_latency_ms_stdev"] == 0.0
        assert item["p50_latency_ms_stdev"] == 0.0
        assert item["p95_latency_ms_stdev"] == 0.0
        assert item["p99_latency_ms_stdev"] == 0.0

    def test_multiple_runs_averages_metrics(self):
        data = [
            {
                "command": "GET",
                "pipeline": 1,
                "data_size": 64,
                "rps": 100000.0,
                "avg_latency_ms": 1.0,
                "p50_latency_ms": 0.8,
                "p95_latency_ms": 1.5,
                "p99_latency_ms": 2.0,
            },
            {
                "command": "GET",
                "pipeline": 1,
                "data_size": 64,
                "rps": 200000.0,
                "avg_latency_ms": 0.5,
                "p50_latency_ms": 0.4,
                "p95_latency_ms": 0.8,
                "p99_latency_ms": 1.0,
            },
        ]
        result = average_multiple_runs(data)
        assert len(result) == 1
        item = result[0]
        assert item["run_count"] == 2
        assert item["rps"] == 150000.0
        assert item["avg_latency_ms"] == 0.75
        assert item["rps_stdev"] > 0.0

    def test_empty_data(self):
        assert average_multiple_runs([]) == []

    def test_different_configs_not_merged(self):
        data = [
            {
                "command": "GET",
                "pipeline": 1,
                "data_size": 64,
                "rps": 100000.0,
                "avg_latency_ms": 1.0,
                "p50_latency_ms": 0.8,
                "p95_latency_ms": 1.5,
                "p99_latency_ms": 2.0,
            },
            {
                "command": "SET",
                "pipeline": 1,
                "data_size": 64,
                "rps": 80000.0,
                "avg_latency_ms": 1.2,
                "p50_latency_ms": 1.0,
                "p95_latency_ms": 1.8,
                "p99_latency_ms": 2.5,
            },
        ]
        result = average_multiple_runs(data)
        assert len(result) == 2


# --- discover_config_keys ---


class TestDiscoverConfigKeys:
    def test_excludes_metric_fields(self):
        data = [
            {
                "command": "GET",
                "pipeline": 1,
                "rps": 150000.0,
                "avg_latency_ms": 0.5,
                "p50_latency_ms": 0.4,
                "p95_latency_ms": 0.8,
                "p99_latency_ms": 1.2,
                "timestamp": "2024-01-01",
                "commit": "abc123",
            }
        ]
        keys = discover_config_keys(data)
        assert "command" in keys
        assert "pipeline" in keys
        # Metric and metadata fields should be excluded
        assert "rps" not in keys
        assert "avg_latency_ms" not in keys
        assert "p50_latency_ms" not in keys
        assert "p95_latency_ms" not in keys
        assert "p99_latency_ms" not in keys
        assert "timestamp" not in keys
        assert "commit" not in keys

    def test_returns_sorted_keys(self):
        data = [{"zebra": "z", "alpha": "a", "middle": "m"}]
        keys = discover_config_keys(data)
        assert keys == sorted(keys)

    def test_empty_data(self):
        assert discover_config_keys([]) == []

    def test_excludes_stdev_and_ci_fields(self):
        data = [
            {
                "command": "GET",
                "rps_stdev": 100.0,
                "rps_ci_lower": 140000.0,
                "rps_ci_upper": 160000.0,
                "rps_pi_lower": 130000.0,
                "rps_pi_upper": 170000.0,
            }
        ]
        keys = discover_config_keys(data)
        assert "command" in keys
        assert "rps_stdev" not in keys
        assert "rps_ci_lower" not in keys
        assert "rps_ci_upper" not in keys
        assert "rps_pi_lower" not in keys
        assert "rps_pi_upper" not in keys


# --- group_by_command ---


class TestGroupByCommand:
    def test_groups_correctly(self):
        items = [
            {"command": "GET", "rps": 100000},
            {"command": "SET", "rps": 80000},
            {"command": "GET", "rps": 110000},
        ]
        grouped = group_by_command(items)
        assert set(grouped.keys()) == {"GET", "SET"}
        assert len(grouped["GET"]) == 2
        assert len(grouped["SET"]) == 1

    def test_empty_list(self):
        assert group_by_command([]) == {}

    def test_missing_command_uses_unknown(self):
        items = [{"rps": 100000}]
        grouped = group_by_command(items)
        assert "UNKNOWN" in grouped


# --- calculate_prediction_interval_percentage ---


class TestCalculatePredictionIntervalPercentage:
    def test_empty_list_returns_zero(self):
        assert calculate_prediction_interval_percentage([]) == 0.0

    def test_single_value_returns_zero(self):
        assert calculate_prediction_interval_percentage([5.0]) == 0.0

    def test_all_zeros_returns_zero(self):
        assert calculate_prediction_interval_percentage([0.0, 0.0, 0.0]) == 0.0

    def test_valid_list_returns_positive(self):
        result = calculate_prediction_interval_percentage([10.0, 12.0, 11.0, 13.0])
        assert result > 0.0

    def test_filters_none_values(self):
        # After filtering Nones, only one value remains -> 0.0
        assert calculate_prediction_interval_percentage([5.0, None]) == 0.0


# --- calculate_confidence_interval_percentage ---


class TestCalculateConfidenceIntervalPercentage:
    def test_empty_list_returns_zero(self):
        assert calculate_confidence_interval_percentage([]) == 0.0

    def test_single_value_returns_zero(self):
        assert calculate_confidence_interval_percentage([5.0]) == 0.0

    def test_all_zeros_returns_zero(self):
        assert calculate_confidence_interval_percentage([0.0, 0.0, 0.0]) == 0.0

    def test_valid_list_returns_positive(self):
        result = calculate_confidence_interval_percentage([10.0, 12.0, 11.0, 13.0])
        assert result > 0.0

    def test_filters_none_values(self):
        assert calculate_confidence_interval_percentage([5.0, None]) == 0.0


# --- extract_version_identifier ---


class TestExtractVersionIdentifier:
    def test_empty_data_returns_unknown(self):
        assert extract_version_identifier([]) == "Unknown"

    def test_none_data_returns_unknown(self):
        assert extract_version_identifier(None) == "Unknown"

    def test_commit_short_returned_as_is(self):
        data = [{"commit": "abc12345"}]
        assert extract_version_identifier(data) == "abc12345"

    def test_commit_long_truncated_to_8(self):
        data = [{"commit": "abcdef1234567890abcdef"}]
        assert extract_version_identifier(data) == "abcdef12"

    def test_commit_at_boundary_12_returned_as_is(self):
        data = [{"commit": "abcdef123456"}]
        assert extract_version_identifier(data) == "abcdef123456"

    def test_timestamp_with_t_returns_date(self):
        data = [{"timestamp": "2024-01-15T10:30:00Z"}]
        assert extract_version_identifier(data) == "2024-01-15"

    def test_timestamp_without_t_returns_first_10(self):
        data = [{"timestamp": "2024-01-15 10:30:00"}]
        assert extract_version_identifier(data) == "2024-01-15"

    def test_no_commit_no_timestamp_returns_unknown(self):
        data = [{"command": "GET"}]
        assert extract_version_identifier(data) == "Unknown"


# --- create_config_signature ---


class TestCreateConfigSignature:
    def test_returns_tuple_of_values(self):
        item = {"command": "GET", "pipeline": 1, "data_size": 64}
        keys = ["command", "pipeline", "data_size"]
        assert create_config_signature(item, keys) == ("GET", 1, 64)

    def test_missing_keys_return_none(self):
        item = {"command": "GET"}
        keys = ["command", "missing_key"]
        assert create_config_signature(item, keys) == ("GET", None)

    def test_empty_keys_returns_empty_tuple(self):
        item = {"command": "GET"}
        assert create_config_signature(item, []) == ()


# --- create_config_sort_key ---


class TestCreateConfigSortKey:
    def test_none_becomes_empty_string(self):
        assert create_config_sort_key((None,)) == ("",)

    def test_values_become_strings(self):
        assert create_config_sort_key(("GET", 1, 64)) == ("GET", "1", "64")

    def test_mixed_none_and_values(self):
        assert create_config_sort_key((None, "SET", None)) == ("", "SET", "")

    def test_empty_tuple(self):
        assert create_config_sort_key(()) == ()


# --- summarize_benchmark_results ---


class TestSummarizeBenchmarkResults:
    def test_empty_list_returns_zeros(self):
        result = summarize_benchmark_results([])
        assert result == {
            "rps": 0.0,
            "latency_avg_ms": 0.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_p99_ms": 0.0,
        }

    def test_single_item_returns_its_values(self):
        item = {
            "rps": 100000.0,
            "avg_latency_ms": 1.0,
            "p50_latency_ms": 0.8,
            "p95_latency_ms": 1.5,
            "p99_latency_ms": 2.0,
        }
        result = summarize_benchmark_results([item])
        assert result["rps"] == 100000.0
        assert result["latency_avg_ms"] == 1.0
        assert result["latency_p50_ms"] == 0.8
        assert result["latency_p95_ms"] == 1.5
        assert result["latency_p99_ms"] == 2.0

    def test_multiple_items_returns_means(self):
        items = [
            {
                "rps": 100000.0,
                "avg_latency_ms": 1.0,
                "p50_latency_ms": 0.8,
                "p95_latency_ms": 1.5,
                "p99_latency_ms": 2.0,
            },
            {
                "rps": 200000.0,
                "avg_latency_ms": 0.5,
                "p50_latency_ms": 0.4,
                "p95_latency_ms": 0.8,
                "p99_latency_ms": 1.0,
            },
        ]
        result = summarize_benchmark_results(items)
        assert result["rps"] == pytest.approx(150000.0)
        assert result["latency_avg_ms"] == pytest.approx(0.75)
        assert result["latency_p50_ms"] == pytest.approx(0.6)
        assert result["latency_p95_ms"] == pytest.approx(1.15)
        assert result["latency_p99_ms"] == pytest.approx(1.5)

    def test_new_field_naming_convention(self):
        item = {
            "rps": 100000.0,
            "latency_avg_ms": 1.0,
            "latency_p50_ms": 0.8,
            "latency_p95_ms": 1.5,
            "latency_p99_ms": 2.0,
        }
        result = summarize_benchmark_results([item])
        assert result["rps"] == 100000.0
        assert result["latency_avg_ms"] == 1.0
        assert result["latency_p50_ms"] == 0.8
        assert result["latency_p95_ms"] == 1.5
        assert result["latency_p99_ms"] == 2.0


# --- _format_with_sig_figs ---


class TestFormatWithSigFigs:
    """Tests for significant figures formatting based on uncertainty."""

    def test_zero_value(self):
        assert _format_with_sig_figs(0) == "0"
        assert _format_with_sig_figs(0, 100) == "0"

    def test_millions_no_uncertainty(self):
        assert _format_with_sig_figs(1_500_000) == "1.50M"

    def test_millions_large_uncertainty(self):
        # σ >= 1M -> 0 decimals
        assert _format_with_sig_figs(1_500_000, 1_000_000) == "2M"

    def test_millions_medium_uncertainty(self):
        # σ >= 0.1M -> 1 decimal
        assert _format_with_sig_figs(1_500_000, 100_000) == "1.5M"

    def test_millions_small_uncertainty(self):
        # σ >= 0.01M -> 2 decimals
        assert _format_with_sig_figs(1_548_000, 10_000) == "1.55M"

    def test_millions_very_small_uncertainty(self):
        # σ >= 0.001M -> 3 decimals
        assert _format_with_sig_figs(1_234_567, 5_000) == "1.235M"

    def test_millions_tiny_uncertainty(self):
        # σ < 0.001M -> 4 decimals
        assert _format_with_sig_figs(1_234_567, 50) == "1.2346M"

    def test_billions_no_uncertainty(self):
        assert _format_with_sig_figs(1_500_000_000) == "1.50B"

    def test_billions_with_uncertainty(self):
        assert _format_with_sig_figs(1_234_567_890, 10_000_000) == "1.23B"

    def test_trillions_no_uncertainty(self):
        assert _format_with_sig_figs(1_500_000_000_000) == "1.50T"

    def test_trillions_with_uncertainty(self):
        assert _format_with_sig_figs(1_234_567_890_000, 100_000_000_000) == "1.2T"

    def test_thousands_no_uncertainty(self):
        assert _format_with_sig_figs(234_567) == "235K"  # 3 sig figs

    def test_thousands_large_uncertainty(self):
        # σ >= 1K -> 0 decimals
        assert _format_with_sig_figs(229_000, 1_400) == "229K"

    def test_thousands_medium_uncertainty(self):
        # σ >= 0.1K -> 1 decimal
        assert _format_with_sig_figs(250_600, 850) == "250.6K"

    def test_thousands_small_uncertainty(self):
        # σ >= 0.01K -> 2 decimals
        assert _format_with_sig_figs(234_567, 50) == "234.57K"

    def test_thousands_tiny_uncertainty(self):
        # σ < 0.01K -> 3 decimals
        assert _format_with_sig_figs(234_567, 5) == "234.567K"

    def test_small_values_hundreds(self):
        assert _format_with_sig_figs(123.456) == "123"

    def test_small_values_tens(self):
        assert _format_with_sig_figs(12.345) == "12.3"

    def test_small_values_ones(self):
        assert _format_with_sig_figs(1.234) == "1.23"

    def test_small_values_sub_one(self):
        assert _format_with_sig_figs(0.123) == "0.123"

    def test_no_precision_loss_with_uncertainty(self):
        """Verify formatted value doesn't lose more than σ precision."""
        test_cases = [
            (1_234_567, 50),
            (1_234_567, 500),
            (1_234_567, 5_000),
            (250_600, 850),
            (229_000, 1_400),
        ]
        for value, stdev in test_cases:
            formatted = _format_with_sig_figs(value, stdev)
            # Parse back
            if "M" in formatted:
                parsed = float(formatted.replace("M", "")) * 1_000_000
            elif "K" in formatted:
                parsed = float(formatted.replace("K", "")) * 1_000
            else:
                parsed = float(formatted)
            diff = abs(value - parsed)
            assert diff <= stdev, f"{value} -> {formatted}: lost {diff}, σ={stdev}"


# --- _format_stats_only ---


class TestFormatStatsOnly:
    """Tests for stats-only formatting."""

    def test_single_run(self):
        assert _format_stats_only(1, 0.0) == "n=1"

    def test_zero_run_count(self):
        assert _format_stats_only(0, 0.0) == "n=1"

    def test_multiple_runs_basic(self):
        result = _format_stats_only(5, 1400, 0.6, 1.3, 3.1)
        assert "n=5" in result
        assert "σ=1.40K" in result
        assert "CV=0.6%" in result
        assert f"CI{CONFIDENCE_PERCENT}%=±1.3%" in result
        assert f"PI{CONFIDENCE_PERCENT}%=±3.1%" in result

    def test_skips_tiny_ci_pi(self):
        result = _format_stats_only(5, 1400, 0.6, 0.001, 0.001)
        assert f"CI{CONFIDENCE_PERCENT}%" not in result
        assert f"PI{CONFIDENCE_PERCENT}%" not in result


# --- _format_percent_change ---


class TestFormatPercentChange:
    """Statistical property tests for percent change with uncertainty propagation."""

    def test_identical_values_gives_zero_change_with_nonzero_margin(self):
        change, margin = calculate_percent_change_with_ci(
            1000.0, 50.0, 1000.0, 50.0, 5, 5
        )
        assert change == pytest.approx(0.0, abs=0.5)
        assert margin is not None and margin > 0

    def test_wider_stdev_gives_wider_margin(self):
        _, narrow = calculate_percent_change_with_ci(
            100000.0, 100.0, 105000.0, 100.0, 10, 10
        )
        _, wide = calculate_percent_change_with_ci(
            100000.0, 5000.0, 105000.0, 5000.0, 10, 10
        )
        assert wide > narrow

    def test_more_runs_gives_narrower_margin(self):
        _, few = calculate_percent_change_with_ci(
            100000.0, 5000.0, 105000.0, 5000.0, 3, 3
        )
        _, many = calculate_percent_change_with_ci(
            100000.0, 5000.0, 105000.0, 5000.0, 100, 100
        )
        assert many < few

    def test_zero_baseline_returns_zero_change_no_margin(self):
        change, margin = calculate_percent_change_with_ci(0, 10.0, 100.0, 10.0, 5, 5)
        assert change == 0.0
        assert margin is None

    def test_single_run_returns_no_margin(self):
        change, margin = calculate_percent_change_with_ci(
            1000.0, 0.0, 1050.0, 0.0, 1, 1
        )
        assert change == pytest.approx(5.0)
        assert margin is None

    def test_asymmetric_stdev_wider_than_narrower_input(self):
        _, margin = calculate_percent_change_with_ci(1000.0, 5.0, 1050.0, 200.0, 10, 10)
        assert margin > 0.5

    def test_format_with_margin_shows_plus_minus(self):
        result = _format_percent_change(1000.0, 50.0, 1050.0, 50.0, 5, 5)
        assert "±" in result
        assert "%" in result

    def test_format_without_margin_shows_plain_percent(self):
        result = _format_percent_change(1000.0, 0.0, 1050.0, 0.0, 1, 1)
        assert "±" not in result
        assert "%" in result

    def test_format_zero_baseline_returns_na(self):
        assert _format_percent_change(0, 10.0, 100.0, 10.0, 5, 5) == "N/A"


# --- _extract_common_and_unique_config ---


class TestExtractCommonAndUniqueConfig:
    """Tests for common/unique configuration extraction."""

    def test_empty_groups(self):
        common, groups = _extract_common_and_unique_config([])
        assert common == {}
        assert groups == []

    def test_single_group_all_common(self):
        config_groups = [
            {
                "config_dict": {"arch": "x86", "clients": 100},
                "config_keys": ["arch", "clients"],
                "table_rows": [],
            }
        ]
        common, groups = _extract_common_and_unique_config(config_groups)
        assert common == {"arch": "x86", "clients": 100}
        assert groups[0]["unique_config"] == {}

    def test_two_groups_with_common_and_unique(self):
        config_groups = [
            {
                "config_dict": {"arch": "x86", "data_size": 16},
                "config_keys": ["arch", "data_size"],
                "table_rows": [],
            },
            {
                "config_dict": {"arch": "x86", "data_size": 64},
                "config_keys": ["arch", "data_size"],
                "table_rows": [],
            },
        ]
        common, groups = _extract_common_and_unique_config(config_groups)
        assert common == {"arch": "x86"}
        assert groups[0]["unique_config"] == {"data_size": 16}
        assert groups[1]["unique_config"] == {"data_size": 64}

    def test_all_different_no_common(self):
        config_groups = [
            {
                "config_dict": {"data_size": 16},
                "config_keys": ["data_size"],
                "table_rows": [],
            },
            {
                "config_dict": {"data_size": 64},
                "config_keys": ["data_size"],
                "table_rows": [],
            },
        ]
        common, groups = _extract_common_and_unique_config(config_groups)
        assert common == {}
        assert groups[0]["unique_config"] == {"data_size": 16}
        assert groups[1]["unique_config"] == {"data_size": 64}
