"""Unit tests for utils/compare_benchmark_results.py statistical functions."""

import pytest

from valkey_benchmark import ClientRunner
from process_metrics import MetricsProcessor

from utils.compare_benchmark_results import (
    calculate_mean,
    calculate_stdev,
    calculate_confidence_interval,
    calculate_prediction_interval,
    calculate_percentage_change,
    average_multiple_runs,
    discover_config_keys,
    group_by_command,
    group_by_static_configuration,
    calculate_prediction_interval_percentage,
    calculate_confidence_interval_percentage,
    calculate_percent_change_with_ci,
    extract_version_identifier,
    create_config_signature,
    create_config_sort_key,
    summarize_benchmark_results,
    create_comparison_table_data,
    format_comparison_report,
    collect_failed_scenarios,
    _scenario_identity,
    _format_with_sig_figs,
    _format_stats_only,
    _format_percent_change,
    _extract_common_and_unique_config,
    _get_significance_indicator,
    CONFIDENCE_PERCENT,
)


def _row_stamper(module_commit=None, module_commit_timestamp=None, config_name=None):
    """Minimal runner shell for exercising the real row-stamping methods."""
    runner = object.__new__(ClientRunner)
    runner.module_commit = module_commit
    runner.module_commit_timestamp = module_commit_timestamp
    runner.config_name = config_name
    return runner


def _scenario_parts(test_id):
    group, _, scenario = str(test_id).partition("_")
    return (int(group) if group.isdigit() else 1), scenario or str(test_id)


def real_failure_row(
    test_id,
    error,
    *,
    command="HSET",
    phase="write",
    config_set=None,
    cluster_mode=False,
    tls_mode=False,
    io_threads=4,
    data_size=16,
    pipeline=1,
    clients=50,
    requests=1000,
    duration=None,
    commit="new123",
    repository=None,
    module_commit=None,
    module_commit_timestamp=None,
    config_name=None,
    **extra,
):
    """Build a failure row through the production marker path."""
    group_id, scenario_id = _scenario_parts(test_id)
    runner = _row_stamper(
        module_commit=module_commit,
        module_commit_timestamp=module_commit_timestamp,
        config_name=config_name,
    )
    processor = MetricsProcessor(
        commit_id=commit,
        cluster_mode=cluster_mode,
        tls_mode=tls_mode,
        commit_time="<TS>",
        io_threads=io_threads,
        repository=repository,
    )
    marker = runner._create_failure_marker(
        processor,
        {
            "command": command,
            "data_size": data_size,
            "pipeline": pipeline,
            "clients": clients,
            "duration": duration,
        },
        test_id=f"{group_id}_{scenario_id}",
        test_phase=phase,
        group_id=group_id,
        scenario_id=scenario_id,
        error=error,
        requests=requests,
        config_set=config_set,
    )
    marker.update(extra)
    return marker


def real_success_row(
    *,
    test_id=None,
    test_phase="write",
    command="GET",
    pipeline=1,
    io_threads=4,
    data_size=16,
    clients=50,
    rps=100000.0,
    avg_latency_ms=0.5,
    p50_latency_ms=0.4,
    p95_latency_ms=0.8,
    p99_latency_ms=1.2,
    config_set=None,
    cluster_mode=False,
    tls_mode=False,
    commit="base123",
    repository=None,
    **extra,
):
    """Build a success row through the production metrics path."""
    proc = MetricsProcessor(
        commit_id=commit,
        cluster_mode=cluster_mode,
        tls_mode=tls_mode,
        commit_time="<TS>",
        io_threads=io_threads,
        repository=repository,
    )
    bench = {
        "rps": str(rps),
        "avg_latency_ms": str(avg_latency_ms),
        "min_latency_ms": "0.1",
        "p50_latency_ms": str(p50_latency_ms),
        "p95_latency_ms": str(p95_latency_ms),
        "p99_latency_ms": str(p99_latency_ms),
        "max_latency_ms": "5.0",
    }
    row = proc.create_metrics(
        bench, command, data_size, pipeline, clients, requests=1000
    )
    if test_id is not None:
        group_id, scenario_id = _scenario_parts(test_id)
        row["status"] = "success"
        _row_stamper()._apply_row_metadata(
            row,
            test_id=test_id,
            test_phase=test_phase,
            group_id=group_id,
            scenario_id=scenario_id,
            config_set=config_set or {},
        )
    row.update(extra)
    return row


def _comparison(baseline, new, metrics_filter="all"):
    """Return failed descriptors, comparison groups, and the rendered report."""
    failed = collect_failed_scenarios(baseline, new, metrics_filter)
    result = create_comparison_table_data(baseline, new, metrics_filter)
    groups, baseline_version, new_version, baseline_repo, new_repo = result
    report = format_comparison_report(
        groups,
        baseline_version,
        new_version,
        baseline_repo,
        new_repo,
        failed_scenarios=failed,
    )
    return failed, groups, report


def _metric_rows(groups, metric="rps"):
    return [
        row
        for group in groups
        for row in group["table_rows"]
        if row["metric"] == metric
    ]


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

    def test_preserves_module_commit(self):
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
                "module_commit": "mod12345",
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
                "module_commit": "mod12345",
            },
        ]
        result = average_multiple_runs(data)
        assert len(result) == 1
        assert result[0]["module_commit"] == "mod12345"

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
                "module_commit": "def456",
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
        assert "module_commit" not in keys

    def test_test_id_sorted_first(self):
        data = [{"arch": "x86", "clients": 100, "test_id": "1_get"}]
        keys = discover_config_keys(data)
        assert keys[0] == "test_id"

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

    def test_excludes_only_volatile_env_field(self):
        # env_-prefixed reproducibility metadata (upstream PR #55) is split by
        # classification, NOT by a blanket prefix rule: only the VOLATILE live
        # reading (env_cpu_freq_mhz_at_setup) is excluded so same-host runs still
        # pair, while STABLE compatibility fields (cpu_model, kernel_version, ...)
        # participate in the signature so incompatible environments do not
        # silently compare.
        data = [
            {
                "command": "GET",
                "pipeline": 1,
                "env_cpu_model": "golden-cpu",
                "env_cpu_freq_mhz_at_setup": 3200,
                "env_kernel_version": "golden-kernel",
            }
        ]
        keys = discover_config_keys(data)
        assert "command" in keys
        assert "pipeline" in keys
        # Volatile live reading is excluded.
        assert "env_cpu_freq_mhz_at_setup" not in keys
        # Stable compatibility fields participate.
        assert "env_cpu_model" in keys
        assert "env_kernel_version" in keys

    def test_unknown_env_field_defaults_to_participating(self):
        # A future/unknown env_ field not enumerated in _VOLATILE_ENV_FIELDS
        # defaults to the STABLE side (participates) -- the safer direction, so a
        # newly-added environment axis is never silently ignored.
        data = [{"command": "GET", "env_some_future_knob": "x"}]
        keys = discover_config_keys(data)
        assert "env_some_future_knob" in keys

    def test_env_only_difference_yields_matching_signatures(self):
        # Two datasets identical except for a live env_ reading (e.g. CPU
        # frequency sampled at setup) must group under the SAME static config
        # signature, so group_by_static_configuration finds a shared group and
        # the comparison table is non-empty.
        baseline = [
            {
                "command": "GET",
                "pipeline": 1,
                "io_threads": 4,
                "clients": 50,
                "data_size": 16,
                "rps": 100000.0,
                "env_cpu_freq_mhz_at_setup": 3200,
            }
        ]
        new = [
            {
                "command": "GET",
                "pipeline": 1,
                "io_threads": 4,
                "clients": 50,
                "data_size": 16,
                "rps": 101000.0,
                "env_cpu_freq_mhz_at_setup": 3105,
            }
        ]

        baseline_groups = group_by_static_configuration(baseline)
        new_groups = group_by_static_configuration(new)
        shared = set(baseline_groups) & set(new_groups)
        assert len(shared) == 1, (
            "datasets differing only in an env_ value must share a config "
            f"signature; baseline={list(baseline_groups)} new={list(new_groups)}"
        )


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

    def test_module_commit_short_returned_as_is(self):
        data = [{"module_commit": "mod12345"}]
        assert extract_version_identifier(data) == "mod12345"

    def test_module_commit_long_truncated_to_8(self):
        data = [{"module_commit": "abcdef1234567890abcdef"}]
        assert extract_version_identifier(data) == "abcdef12"

    def test_module_commit_prioritized_over_commit(self):
        data = [{"module_commit": "mod12345", "commit": "core6789"}]
        assert extract_version_identifier(data) == "mod12345"


# --- create_config_signature ---


class TestCreateConfigSignature:
    def test_returns_tuple_of_values(self):
        # A trailing frozen config_set component (None when absent) is always
        # appended so distinct config_sets never collapse into one group.
        item = {"command": "GET", "pipeline": 1, "data_size": 64}
        keys = ["command", "pipeline", "data_size"]
        assert create_config_signature(item, keys) == ("GET", 1, 64, None)

    def test_missing_keys_return_none(self):
        item = {"command": "GET"}
        keys = ["command", "missing_key"]
        assert create_config_signature(item, keys) == ("GET", None, None)

    def test_empty_keys_returns_config_set_component_only(self):
        item = {"command": "GET"}
        # No config keys -> just the trailing (None) config_set component.
        assert create_config_signature(item, []) == (None,)

    def test_config_set_frozen_into_trailing_component(self):
        # A config_set dict is frozen into a stable, order-independent component
        # so two distinct config_sets produce two distinct signatures.
        keys = ["command"]
        sig_1gb = create_config_signature(
            {"command": "GET", "config_set": {"maxmemory": "1gb"}}, keys
        )
        sig_4gb = create_config_signature(
            {"command": "GET", "config_set": {"maxmemory": "4gb"}}, keys
        )
        assert sig_1gb != sig_4gb
        assert sig_1gb == ("GET", (("maxmemory", "1gb"),))

    def test_empty_config_set_matches_absent(self):
        # An empty config_set collapses to the same None component as an absent
        # one, so single-config runs key exactly as before.
        keys = ["command"]
        assert create_config_signature(
            {"command": "GET", "config_set": {}}, keys
        ) == create_config_signature({"command": "GET"}, keys)


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
            "avg_latency_ms": 0.0,
            "p50_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "p99_latency_ms": 0.0,
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
        assert result["avg_latency_ms"] == 1.0
        assert result["p50_latency_ms"] == 0.8
        assert result["p95_latency_ms"] == 1.5
        assert result["p99_latency_ms"] == 2.0

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
        assert result["avg_latency_ms"] == pytest.approx(0.75)
        assert result["p50_latency_ms"] == pytest.approx(0.6)
        assert result["p95_latency_ms"] == pytest.approx(1.15)
        assert result["p99_latency_ms"] == pytest.approx(1.5)


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


# --- TestComparisonPipeline ---


class TestComparisonPipeline:
    """End-to-end tests for the comparison pipeline."""

    def _make_run(
        self,
        rps,
        avg,
        p50,
        p95,
        p99,
        command="GET",
        pipeline=1,
        io_threads=1,
        commit="abc123",
    ):
        return {
            "command": command,
            "pipeline": pipeline,
            "io_threads": io_threads,
            "data_size": 32,
            "clients": 50,
            "rps": rps,
            "avg_latency_ms": avg,
            "p50_latency_ms": p50,
            "p95_latency_ms": p95,
            "p99_latency_ms": p99,
            "commit": commit,
        }

    def test_end_to_end_percentage_change(self):
        baseline_data = [
            self._make_run(98000, 1.02, 0.81, 1.48, 1.95, commit="base111"),
            self._make_run(102000, 0.98, 0.79, 1.52, 2.05, commit="base111"),
            self._make_run(100000, 1.00, 0.80, 1.50, 2.00, commit="base111"),
        ]
        new_data = [
            self._make_run(118000, 0.82, 0.61, 1.18, 1.58, commit="new2222"),
            self._make_run(122000, 0.78, 0.59, 1.22, 1.62, commit="new2222"),
            self._make_run(120000, 0.80, 0.60, 1.20, 1.60, commit="new2222"),
        ]

        baseline_averaged = average_multiple_runs(baseline_data)
        new_averaged = average_multiple_runs(new_data)

        config_groups, baseline_ver, new_ver, _, _ = create_comparison_table_data(
            baseline_averaged, new_averaged
        )

        assert len(config_groups) == 1
        rows = config_groups[0]["table_rows"]

        # RPS: mean 100000 -> 120000 = +20%
        rps_row = next(r for r in rows if r["metric"] == "rps")
        assert rps_row["baseline_value"] == pytest.approx(100000.0)
        assert rps_row["new_value"] == pytest.approx(120000.0)
        assert rps_row["change"] == pytest.approx(20.0)

        # avg latency: mean 1.0 -> 0.8 = -20%
        avg_row = next(r for r in rows if r["metric"] == "avg_latency")
        assert avg_row["baseline_value"] == pytest.approx(1.0)
        assert avg_row["new_value"] == pytest.approx(0.8)
        assert avg_row["change"] == pytest.approx(-20.0)

        # p99 latency: mean 2.0 -> 1.6 = -20%
        p99_row = next(r for r in rows if r["metric"] == "p99_latency")
        assert p99_row["baseline_value"] == pytest.approx(2.0)
        assert p99_row["new_value"] == pytest.approx(1.6)
        assert p99_row["change"] == pytest.approx(-20.0)

        # Verify version extraction worked
        assert baseline_ver == "base111"
        assert new_ver == "new2222"

    def test_significance_direction_by_metric_type(self):
        # Latency decrease = improvement
        assert (
            _get_significance_indicator(5, 5, 1.8, 2.2, 1.0, 1.4, -30.0, "avg_latency")
            == "✅"
        )
        # Latency increase = regression
        assert (
            _get_significance_indicator(5, 5, 1.0, 1.4, 1.8, 2.2, 50.0, "p99_latency")
            == "❌"
        )
        # RPS increase = improvement
        assert (
            _get_significance_indicator(
                5, 5, 90000.0, 100000.0, 110000.0, 120000.0, 20.0, "rps"
            )
            == "✅"
        )
        # RPS decrease = regression
        assert (
            _get_significance_indicator(
                5, 5, 110000.0, 120000.0, 90000.0, 100000.0, -15.0, "rps"
            )
            == "❌"
        )


# --- Union key discovery + failed-scenario reporting ---


class TestUnionKeyDiscoveryAndFailedScenarios:
    def _row(self, **overrides):
        return real_success_row(**overrides)

    def _failure(
        self,
        test_id,
        error,
        side_commit,
        phase="write",
        command="HSET",
        config_set=None,
        cluster_mode=False,
        **extra,
    ):
        return real_failure_row(
            test_id,
            error,
            phase=phase,
            command=command,
            config_set=config_set,
            cluster_mode=cluster_mode,
            commit=side_commit,
            **extra,
        )

    def test_union_key_discovery_matches_across_extra_field(self):
        baseline = [
            self._row(test_id="1_a"),
            self._row(test_id="1_b", command="SET"),
        ]
        new = [
            self._row(test_id="1_a", extra_field="only-in-new"),
            self._row(test_id="1_b", command="SET"),
        ]

        per_dataset_shared = set(group_by_static_configuration(baseline)) & set(
            group_by_static_configuration(new)
        )
        assert len(per_dataset_shared) == 0

        shared_keys = discover_config_keys(baseline + new)
        union_shared = set(group_by_static_configuration(baseline, shared_keys)) & set(
            group_by_static_configuration(new, shared_keys)
        )
        assert len(union_shared) == 1

    def test_union_keys_survive_metrics_schema_drift_through_pipeline(self):
        baseline = [
            self._row(test_id="1_a", rps=100000.0),
            self._row(test_id="1_b", rps=100000.0),
        ]
        new = [
            self._row(test_id="1_a", rps=120000.0, commit="new123"),
            self._row(
                test_id="1_b", rps=130000.0, commit="new123", keyspacelen=1000000
            ),
        ]

        assert "keyspacelen" not in discover_config_keys(baseline)
        assert "keyspacelen" in discover_config_keys(new)

        groups, *_ = create_comparison_table_data(baseline, new)
        matched = [
            r
            for g in groups
            if g["config_dict"].get("test_id") == "1_a"
            for r in g["table_rows"]
            if r["metric"] == "rps"
        ]
        assert len(matched) == 1
        assert matched[0]["baseline_value"] == pytest.approx(100000.0)
        assert matched[0]["new_value"] == pytest.approx(120000.0)
        assert matched[0]["change"] == pytest.approx(20.0)

        assert len(groups) == 3

    def test_failed_scenario_pairs_across_one_sided_schema_field(self):
        baseline = [self._row(test_id="1_a", rps=123456.0)]
        new = [self._failure("1_a", "client failed", "new123", keyspacelen=1000000)]

        failed, groups, report = _comparison(baseline, new, "rps")

        assert failed[0]["counterpart_present"] is True
        assert failed[0]["counterpart_value"] == pytest.approx(123456.0)
        assert _metric_rows(groups) == []
        assert "123K rps" in report
        assert "-100.0%" not in report

    def test_schema_fallback_refuses_ambiguous_one_to_many_match(self):
        baseline = [self._row(test_id="1_a", rps=123456.0)]
        new = [
            self._failure("1_a", "client failed", "new123", keyspacelen=1000),
            self._row(test_id="1_a", commit="new123", keyspacelen=2000, rps=200000.0),
        ]

        failed = collect_failed_scenarios(baseline, new, "rps")

        assert failed[0]["counterpart_present"] is False
        assert failed[0]["counterpart_value"] is None

    def test_failed_scenario_does_not_poison_healthy_scenario(self):
        baseline = [
            self._row(test_id="1_a", command="HSET"),
            self._row(test_id="1_b", command="FT.SEARCH"),
        ]
        new = [
            self._failure("1_a", "No results", "new123"),
            self._row(test_id="1_b", command="FT.SEARCH", commit="new123"),
        ]

        groups, *_ = create_comparison_table_data(baseline, new)

        assert len(groups) == 1
        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(100000.0)
        assert rps_rows[0]["new_value"] == pytest.approx(100000.0)
        assert rps_rows[0]["change"] == pytest.approx(0.0)

    def test_failed_in_new_labeled_not_regression(self):
        baseline = [
            self._row(test_id="1_a", command="HSET"),
            self._row(test_id="1_b", command="FT.SEARCH"),
        ]
        new = [
            self._failure("1_a", "boom benchmark crashed", "new123"),
            self._row(test_id="1_b", command="FT.SEARCH", commit="new123"),
        ]

        _, _, report = _comparison(baseline, new)

        assert "failed scenario" in report.lower()
        assert "1_a" in report
        assert "boom benchmark crashed" in report
        assert (
            "| Scenario | Phase | Command | ` base123 ` | ` new123 ` | Error |"
            in report
        )
        assert "100K rps" in report
        assert "**FAILED**" in report
        assert "-100.0%" not in report

    def test_failed_in_baseline_labeled_not_phantom_improvement(self):
        baseline = [
            self._failure("1_a", "baseline oom", "base123"),
            self._row(test_id="1_b", command="FT.SEARCH"),
        ]
        new = [
            self._row(test_id="1_a", command="HSET", commit="new123"),
            self._row(test_id="1_b", command="FT.SEARCH", commit="new123"),
        ]

        _, groups, report = _comparison(baseline, new)

        assert "1_a" in report
        assert "baseline oom" in report
        assert "100K rps" in report
        assert "**FAILED**" in report
        assert "-100.0%" not in report

        assert len(groups) == 1
        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["change"] == pytest.approx(0.0)

    def test_normal_comparison_unchanged_no_failures(self):
        baseline = [
            self._row(command="GET", rps=100000.0),
            self._row(command="SET", rps=80000.0),
        ]
        new = [
            self._row(command="GET", rps=120000.0, commit="new123"),
            self._row(command="SET", rps=88000.0, commit="new123"),
        ]

        failed, groups, report = _comparison(baseline, new)
        assert failed == []
        assert len(groups) == 1

        rows = groups[0]["table_rows"]
        get_rps = next(
            r for r in rows if r["command"] == "GET" and r["metric"] == "rps"
        )
        set_rps = next(
            r for r in rows if r["command"] == "SET" and r["metric"] == "rps"
        )
        assert get_rps["change"] == pytest.approx(20.0)
        assert set_rps["change"] == pytest.approx(10.0)

        assert "failed scenario" not in report.lower()

    def test_failed_in_new_captures_baseline_value(self):
        baseline = [self._row(test_id="1_a", command="HSET", rps=123456.0)]
        new = [self._failure("1_a", "boom", "new123")]

        failed, _, report = _comparison(baseline, new)
        assert len(failed) == 1
        entry = failed[0]
        assert entry["side"] == "new"
        assert entry["counterpart_present"] is True
        assert entry["counterpart_value"] == pytest.approx(123456.0)
        assert entry["metric_label"] == "rps"

        assert "123K rps" in report
        assert "**FAILED**" in report

    def test_failed_in_baseline_captures_new_value(self):
        baseline = [self._failure("1_a", "oom", "base123")]
        new = [self._row(test_id="1_a", command="HSET", rps=250000.0, commit="new123")]

        failed, _, report = _comparison(baseline, new)
        assert len(failed) == 1
        entry = failed[0]
        assert entry["side"] == "baseline"
        assert entry["counterpart_value"] == pytest.approx(250000.0)

        assert "250K rps" in report
        assert "**FAILED**" in report

    def test_both_sides_failed_rendering(self):
        baseline = [self._failure("1_a", "baseline oom", "base123")]
        new = [self._failure("1_a", "new segfault", "new123")]

        _, groups, report = _comparison(baseline, new)

        assert report.count("**FAILED**") == 2
        assert "baseline: baseline oom" in report
        assert "new: new segfault" in report
        assert "-100.0%" not in report
        assert all(not g["table_rows"] for g in groups)

    def test_failed_scenario_no_counterpart_renders_absent_marker(self):
        baseline = [self._row(test_id="1_b", command="FT.SEARCH")]
        new = [
            self._failure("1_a", "boom", "new123"),
            self._row(test_id="1_b", command="FT.SEARCH", commit="new123"),
        ]

        failed, groups, report = _comparison(baseline, new)
        a_entry = next(e for e in failed if e["test_id"] == "1_a")
        assert a_entry["counterpart_present"] is False
        assert a_entry["counterpart_value"] is None

        assert "n/a" in report
        assert "**FAILED**" in report
        assert "-100.0%" not in report
        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["change"] == pytest.approx(0.0)

    def test_no_negative_100_percent_for_any_failed_direction(self):
        directions = [
            (  # failed in new
                [self._row(test_id="1_a", command="HSET")],
                [self._failure("1_a", "boom", "new123")],
            ),
            (  # failed in baseline
                [self._failure("1_a", "oom", "base123")],
                [self._row(test_id="1_a", command="HSET", commit="new123")],
            ),
            (  # failed on both
                [self._failure("1_a", "oom", "base123")],
                [self._failure("1_a", "boom", "new123")],
            ),
        ]
        for baseline, new in directions:
            _, _, report = _comparison(baseline, new)
            assert "-100.0%" not in report
            assert "+100.0%" not in report

    @pytest.mark.parametrize(
        "variant_a, variant_b, label_a, label_b, err_a, err_b",
        [
            (  # config_set is part of the identity
                {"config_set": {"appendonly": "yes"}},
                {"config_set": {"appendonly": "no"}},
                "appendonly=yes",
                "appendonly=no",
                "err on yes",
                "err on no",
            ),
            (  # cluster_mode is part of the identity too
                {"cluster_mode": False},
                {"cluster_mode": True},
                "cluster=False",
                "cluster=True",
                "boom single",
                "boom cluster",
            ),
        ],
    )
    def test_same_test_id_two_variants_render_two_rows(
        self, variant_a, variant_b, label_a, label_b, err_a, err_b
    ):
        baseline = [
            self._row(test_id="1_a", command="HSET", rps=100000.0, **variant_a),
            self._row(test_id="1_a", command="HSET", rps=500000.0, **variant_b),
        ]
        new = [
            self._failure("1_a", err_a, "new123", **variant_a),
            self._failure("1_a", err_b, "new123", **variant_b),
        ]

        _, _, report = _comparison(baseline, new)

        assert "## ⚠️ 2 failed scenario(s)" in report
        assert label_a in report
        assert label_b in report
        assert err_a in report
        assert err_b in report
        assert "100K rps" in report
        assert "500K rps" in report
        assert "300K rps" not in report
        row_lines = [
            ln for ln in report.splitlines() if ln.startswith("| ") and "1_a" in ln
        ]
        assert len(row_lines) == 2

    def test_single_config_set_still_renders_one_row(self):
        baseline = [self._row(test_id="1_a", command="HSET", rps=100000.0)]
        new = [self._failure("1_a", "boom", "new123")]

        _, _, report = _comparison(baseline, new)

        assert "## ⚠️ 1 failed scenario(s)" in report
        row_lines = [
            ln for ln in report.splitlines() if ln.startswith("| ") and "1_a" in ln
        ]
        assert len(row_lines) == 1
        assert row_lines[0].startswith("| ` 1_a ` |")
        assert "appendonly=" not in report
        assert "cluster=" not in report


class TestScenarioIdentityAndCellSanitization:
    def _row(self, **overrides):
        overrides.setdefault("test_id", "1_a")
        return real_success_row(**overrides)

    def _failure(self, test_id, error, side_commit, command="HSET", **overrides):
        return real_failure_row(
            test_id, error, command=command, commit=side_commit, **overrides
        )

    @pytest.mark.parametrize(
        "variant_a, variant_b, base_rps, new_rps",
        [
            (
                {"config_set": {"maxmemory": "100mb"}},
                {"config_set": {"maxmemory": "200mb"}},
                200000.0,
                220000.0,
            ),
            ({"cluster_mode": False}, {"cluster_mode": True}, 300000.0, 330000.0),
        ],
    )
    def test_healthy_sibling_survives_when_one_variant_fails(
        self, variant_a, variant_b, base_rps, new_rps
    ):
        baseline = [
            self._row(test_id="1_a", rps=100000.0, **variant_a),
            self._row(test_id="1_a", rps=base_rps, **variant_b),
        ]
        new = [
            self._failure("1_a", "boom", "new123", **variant_a),
            self._row(test_id="1_a", rps=new_rps, commit="new123", **variant_b),
        ]

        failed, groups, report = _comparison(baseline, new)
        rps_rows = _metric_rows(groups)

        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(base_rps)
        assert rps_rows[0]["new_value"] == pytest.approx(new_rps)
        assert rps_rows[0]["change"] == pytest.approx(10.0)

    def test_counterpart_value_reflects_only_matching_config_set(self):
        cfg_a = {"maxmemory": "100mb"}
        cfg_b = {"maxmemory": "200mb"}
        baseline = [
            self._row(test_id="1_a", config_set=cfg_a, rps=100000.0),
            self._row(test_id="1_a", config_set=cfg_b, rps=900000.0),
        ]
        new = [
            self._failure("1_a", "boom", "new123", config_set=cfg_a),
            self._row(test_id="1_a", config_set=cfg_b, rps=950000.0, commit="new123"),
        ]

        failed = collect_failed_scenarios(baseline, new)
        assert len(failed) == 1
        entry = failed[0]
        assert entry["side"] == "new"
        assert entry["counterpart_value"] == pytest.approx(100000.0)

    def test_legacy_row_without_test_id_never_excluded_and_still_renders(self):
        legacy_ok = {
            "command": "GET",
            "pipeline": 1,
            "io_threads": 4,
            "data_size": 16,
            "clients": 50,
            "rps": 100000.0,
            "avg_latency_ms": 0.5,
        }
        failed_legacy = {
            "status": "failed",
            "error": "legacy runner died",
            "command": "SET",
            "timestamp": "<TS>",
        }
        baseline = [
            {**legacy_ok, "commit": "base123"},
            self._row(test_id="1_a", command="HSET"),
        ]
        new = [
            {**legacy_ok, "rps": 110000.0, "commit": "new123"},
            self._failure("1_a", "scenario boom", "new123"),
            failed_legacy,
        ]

        _, groups, report = _comparison(baseline, new)

        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(100000.0)
        assert rps_rows[0]["new_value"] == pytest.approx(110000.0)
        assert rps_rows[0]["change"] == pytest.approx(10.0)

        assert "legacy runner died" in report
        assert "1_a" in report

    def test_injection_in_command_and_error_is_inert(self):
        injected_command = (
            r"[click](http://evil.example) @octocat #83 GH-83 "
            r"a@example.com <img src=x> a|b"
        )
        injected_error = r"boom `rm -rf` ``two ticks`` *bold* _em_" + "\r done"
        baseline = [self._row(test_id="1_a", command="HSET")]
        new = [self._failure("1_a", injected_error, "new123", command=injected_command)]

        _, _, report = _comparison(baseline, new)

        assert (
            "` [click](http://evil.example) @octocat #83 GH-83 "
            "a@example.com <img src=x> a\\|b `"
        ) in report
        assert "``` boom `rm -rf` ``two ticks`` *bold* _em_  done ```" in report
        assert "\r" not in report
        assert report.count("| --- | --- | --- | --- | --- | --- |") == 1
        data_row = next(
            line
            for line in report.splitlines()
            if line.startswith("| ") and "1_a" in line
        )
        assert data_row.count(" | ") == 5
        assert data_row.count(r"\|") == 1

    def test_version_header_is_sanitized(self):
        baseline = [self._row(test_id="1_a", command="HSET")]
        new = [self._failure("1_a", "boom", "new123")]
        failed = collect_failed_scenarios(baseline, new)
        groups, _b, _n, brepo, nrepo = create_comparison_table_data(baseline, new)

        report = format_comparison_report(
            groups,
            "[evil](http://evil.example)",
            "v1|v2",
            brepo,
            nrepo,
            failed_scenarios=failed,
        )

        assert "` [evil](http://evil.example) `" in report
        assert r"` v1\|v2 `" in report


class TestRealProducerFailurePairing:
    def test_real_failure_marker_pairs_with_real_success_row(self):
        baseline = [
            real_success_row(
                test_id="1_a",
                command="HSET",
                cluster_mode=False,
                io_threads=4,
                rps=100000.0,
            )
        ]
        new = [
            real_failure_row(
                "1_a", "No results", command="HSET", cluster_mode=False, io_threads=4
            )
        ]

        assert _scenario_identity(baseline[0]) == _scenario_identity(new[0])

        _, groups, report = _comparison(baseline, new)

        assert _metric_rows(groups) == []
        assert "**FAILED**" in report
        assert "-100.0%" not in report

    def test_producer_failure_marker_carries_cluster_mode_and_io_threads(self):
        marker = real_failure_row(
            "1_a", "boom", command="HSET", cluster_mode=True, io_threads=8
        )
        assert marker["cluster_mode"] is True
        assert marker["io_threads"] == 8
        assert marker["status"] == "failed"
        assert "rps" not in marker

    def test_io_threads_sweep_sibling_survives_when_one_thread_count_fails(self):
        baseline = [
            real_success_row(test_id="1_a", command="HSET", io_threads=4, rps=100000.0),
            real_success_row(test_id="1_a", command="HSET", io_threads=8, rps=300000.0),
        ]
        new = [
            real_success_row(
                test_id="1_a", command="HSET", io_threads=4, rps=110000.0, commit="new"
            ),
            real_failure_row("1_a", "boom", command="HSET", io_threads=8, commit="new"),
        ]

        failed, groups, report = _comparison(baseline, new)
        rps_rows = _metric_rows(groups)

        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(100000.0)
        assert rps_rows[0]["new_value"] == pytest.approx(110000.0)
        assert rps_rows[0]["change"] == pytest.approx(10.0)


class TestAveragingStageConfigSet:
    @staticmethod
    def _run_pipeline(baseline, new):
        failed = collect_failed_scenarios(baseline, new)
        shared = discover_config_keys(baseline + new)
        baseline_avg = average_multiple_runs(baseline, shared)
        new_avg = average_multiple_runs(new, shared)
        groups, bver, nver, brepo, nrepo = create_comparison_table_data(
            baseline_avg, new_avg
        )
        return failed, baseline_avg, new_avg, groups, bver, nver, brepo, nrepo

    def test_config_set_sweep_not_collapsed_reproduction(self):
        baseline = [
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "1gb"},
                rps=100.0,
                commit="base",
            ),
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "4gb"},
                rps=200.0,
                commit="base",
            ),
        ]
        new = [
            real_failure_row(
                "1_a",
                "oom",
                command="HSET",
                config_set={"maxmemory": "1gb"},
                commit="new",
            ),
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "4gb"},
                rps=220.0,
                commit="new",
            ),
        ]

        failed, baseline_avg, new_avg, groups, bver, nver, brepo, nrepo = (
            self._run_pipeline(baseline, new)
        )

        assert len(baseline_avg) == 2
        assert {r["run_count"] for r in baseline_avg} == {1}
        assert sorted(r["rps"] for r in baseline_avg) == [100.0, 200.0]

        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(200.0)
        assert rps_rows[0]["new_value"] == pytest.approx(220.0)
        assert rps_rows[0]["change"] == pytest.approx(10.0)
        assert rps_rows[0]["change"] != pytest.approx(46.7, abs=1.0)

        report = format_comparison_report(
            groups, bver, nver, brepo, nrepo, failed_scenarios=failed
        )
        assert "1gb" in report
        assert "oom" in report
        assert "-100.0%" not in report
        assert "+46.7%" not in report

    def test_genuine_repeated_runs_same_config_set_still_average(self):
        baseline = [
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "1gb"},
                rps=100.0,
            ),
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "1gb"},
                rps=200.0,
            ),
            real_success_row(
                test_id="1_a",
                command="HSET",
                config_set={"maxmemory": "1gb"},
                rps=300.0,
            ),
        ]
        shared = discover_config_keys(baseline)
        averaged = average_multiple_runs(baseline, shared)

        assert len(averaged) == 1
        assert averaged[0]["run_count"] == 3
        assert averaged[0]["rps"] == pytest.approx(200.0)
        assert averaged[0]["rps_stdev"] > 0.0
        assert averaged[0]["config_set"] == {"maxmemory": "1gb"}

    def test_no_config_set_runs_average_as_before(self):
        baseline = [
            real_success_row(command="GET", rps=100.0),
            real_success_row(command="GET", rps=200.0),
        ]
        for row in baseline:
            assert "config_set" not in row

        shared = discover_config_keys(baseline)
        averaged = average_multiple_runs(baseline, shared)

        assert len(averaged) == 1
        assert averaged[0]["run_count"] == 2
        assert averaged[0]["rps"] == pytest.approx(150.0)
        assert "config_set" not in averaged[0]

    def test_two_config_sets_each_repeated_average_within_config_set(self):
        def runs(config_set, rps_pair):
            return [
                real_success_row(
                    test_id="1_a", command="HSET", config_set=config_set, rps=rps
                )
                for rps in rps_pair
            ]

        baseline = runs({"maxmemory": "1gb"}, [100.0, 120.0]) + runs(
            {"maxmemory": "4gb"}, [200.0, 240.0]
        )
        shared = discover_config_keys(baseline)
        averaged = average_multiple_runs(baseline, shared)

        assert len(averaged) == 2
        assert {r["run_count"] for r in averaged} == {2}
        by_cfg = {r["config_set"]["maxmemory"]: r["rps"] for r in averaged}
        assert by_cfg == {"1gb": pytest.approx(110.0), "4gb": pytest.approx(220.0)}


class TestStructuralIdentityAndAttribution:
    def test_tls_variants_pair_independently_and_failure_not_smeared(self):
        baseline = [
            real_success_row(test_id="1_a", tls_mode=False, rps=100.0),
            real_success_row(test_id="1_a", tls_mode=True, rps=500.0),
        ]
        new = [
            real_success_row(test_id="1_a", tls_mode=False, rps=110.0, commit="new123"),
            real_failure_row("1_a", "tls boom", tls_mode=True, commit="new123"),
        ]

        failed, groups, report = _comparison(baseline, new)
        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(100.0)
        assert rps_rows[0]["new_value"] == pytest.approx(110.0)
        assert rps_rows[0]["change"] == pytest.approx(10.0)

        assert len(failed) == 1
        assert failed[0]["counterpart_value"] == pytest.approx(500.0)
        assert "1_a (tls=True)" in report

    def test_command_difference_does_not_break_pairing(self):
        baseline = [
            real_success_row(test_id="1_a", command="MSET (10 keys)", rps=100.0)
        ]
        new = [real_failure_row("1_a", "boom", command="MSET", commit="new123")]

        failed = collect_failed_scenarios(baseline, new)
        assert len(failed) == 1
        assert failed[0]["counterpart_value"] == pytest.approx(100.0)

    def test_all_failed_module_dataset_labeled_with_module_commit(self):
        markers = [
            real_failure_row(
                "1_a",
                "boom",
                commit="core_sha",
                module_commit="module_sha_1234567890",
                module_commit_timestamp="2026-01-01T00:00:00Z",
                config_name="fts.json",
            )
        ]
        assert markers[0]["module_commit"] == "module_sha_1234567890"
        assert extract_version_identifier(markers) == "module_s"
        core_only = [real_failure_row("1_a", "boom", commit="corecommit123")]
        assert extract_version_identifier(core_only) == "corecomm"

    def test_mixed_failure_no_fabricated_regression_end_to_end(self):
        baseline = [
            real_success_row(
                test_id="1_mix_write_w", test_phase="mixed_write", rps=100.0
            ),
            real_success_row(
                test_id="1_mix_read_r1", test_phase="mixed_read", rps=200.0
            ),
        ]
        new = [
            real_failure_row(
                "1_mix_write_w", "write boom", phase="mixed_write", commit="new123"
            ),
            real_success_row(
                test_id="1_mix_read_r1",
                test_phase="mixed_read",
                rps=210.0,
                commit="new123",
            ),
        ]

        groups, *_ = create_comparison_table_data(baseline, new)
        rps_rows = _metric_rows(groups)
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(200.0)
        assert rps_rows[0]["new_value"] == pytest.approx(210.0)
        assert rps_rows[0]["change"] == pytest.approx(5.0)
        assert all(r["change"] != pytest.approx(-100.0) for r in rps_rows)

        failed = collect_failed_scenarios(baseline, new)
        assert len(failed) == 1
        assert failed[0]["test_id"] == "1_mix_write_w"
        assert failed[0]["counterpart_value"] == pytest.approx(100.0)


class TestVolatileVsStableEnvClassification:
    """Stable environment identity versus volatile live readings."""

    def test_stable_env_field_prevents_comparison(self):
        baseline = [
            real_success_row(test_id="1_a", rps=100000.0, env_cpu_model="AMD EPYC")
        ]
        new = [
            real_success_row(
                test_id="1_a",
                rps=200000.0,
                commit="new123",
                env_cpu_model="Intel Xeon",
            )
        ]

        groups, *_ = create_comparison_table_data(baseline, new)
        rps_rows = _metric_rows(groups)

        assert len(groups) == 2
        assert not any(r["baseline_value"] > 0 and r["new_value"] > 0 for r in rps_rows)

    def test_volatile_freq_field_still_compares(self):
        baseline = [
            real_success_row(
                test_id="1_a", rps=100000.0, env_cpu_freq_mhz_at_setup=3200
            )
        ]
        new = [
            real_success_row(
                test_id="1_a",
                rps=110000.0,
                commit="new123",
                env_cpu_freq_mhz_at_setup=3105,
            )
        ]

        groups, *_ = create_comparison_table_data(baseline, new)
        rps_rows = _metric_rows(groups)

        assert len(groups) == 1
        assert len(rps_rows) == 1
        assert rps_rows[0]["baseline_value"] == pytest.approx(100000.0)
        assert rps_rows[0]["new_value"] == pytest.approx(110000.0)
        assert rps_rows[0]["change"] == pytest.approx(10.0)

    def test_stable_env_field_is_identity_axis_and_pairs_same_host(self):
        baseline = [
            real_success_row(test_id="1_a", rps=100.0, env_cpu_model="AMD EPYC")
        ]
        new = [
            real_failure_row("1_a", "boom", commit="new123", env_cpu_model="AMD EPYC")
        ]

        assert ("env_cpu_model", "AMD EPYC") in _scenario_identity(baseline[0])
        failed = collect_failed_scenarios(baseline, new)
        assert len(failed) == 1
        assert failed[0]["counterpart_value"] == pytest.approx(100.0)


class TestConfigSetGroupHeadingAttribution:
    def test_two_config_sets_render_attributable_headings(self):
        cfg_1gb = {"maxmemory": "1gb"}
        cfg_4gb = {"maxmemory": "4gb"}
        baseline = [
            real_success_row(test_id="1_a", config_set=cfg_1gb, rps=100.0),
            real_success_row(test_id="1_a", config_set=cfg_4gb, rps=500.0),
        ]
        new = [
            real_success_row(
                test_id="1_a", config_set=cfg_1gb, rps=110.0, commit="new123"
            ),
            real_success_row(
                test_id="1_a", config_set=cfg_4gb, rps=550.0, commit="new123"
            ),
        ]

        _, _, report = _comparison(baseline, new)

        headings = [line for line in report.splitlines() if line.startswith("###")]
        assert "### config_set = ` maxmemory=1gb `" in headings
        assert "### config_set = ` maxmemory=4gb `" in headings
        base_no_cs = [
            real_success_row(test_id="1_a", data_size=16, rps=100.0),
            real_success_row(test_id="1_a", data_size=64, rps=200.0),
        ]
        new_no_cs = [
            real_success_row(test_id="1_a", data_size=16, rps=110.0, commit="new123"),
            real_success_row(test_id="1_a", data_size=64, rps=220.0, commit="new123"),
        ]
        _, _, report2 = _comparison(base_no_cs, new_no_cs)

        assert "config_set" not in report2
        headings2 = [line for line in report2.splitlines() if line.startswith("###")]
        assert "### data_size = 16" in headings2
        assert "### data_size = 64" in headings2
