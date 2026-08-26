#!/usr/bin/env python3
"""
Benchmark Results Comparison Tool

This tool compares benchmark results between two versions, automatically averaging
multiple runs for identical configurations and generating a comprehensive comparison report.
"""

import json
import statistics
import sys
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path

import math

from scipy import stats
from uncertainties import ufloat

# Optional dependencies for graphing functionality
try:
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.ticker import FuncFormatter

    GRAPHING_AVAILABLE = True
except ImportError:
    GRAPHING_AVAILABLE = False
    plt = None
    np = None
    FuncFormatter = None

# Shared by all CI/PI calculations and labels.
CONFIDENCE_LEVEL = 0.95
CONFIDENCE_PERCENT = int(CONFIDENCE_LEVEL * 100)


def load_benchmark_data(path: str) -> List[Dict[str, Any]]:
    """Load benchmark data from a JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File '{path}' not found", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in '{path}': {e}", file=sys.stderr)
        sys.exit(1)


def calculate_mean(values: List[float]) -> float:
    """Calculate mean of non-None values."""
    filtered_values = [v for v in values if v is not None]
    return statistics.mean(filtered_values) if filtered_values else 0.0


def calculate_stdev(values: List[float]) -> float:
    """Calculate standard deviation, returning 0.0 for single values or empty lists."""
    filtered_values = [v for v in values if v is not None]
    if len(filtered_values) <= 1:
        return 0.0
    return statistics.stdev(filtered_values)


def calculate_confidence_interval(
    values: List[float], confidence_level: float = CONFIDENCE_LEVEL
) -> Tuple[float, float]:
    """Return a t-distribution confidence interval, or zeros for fewer than 2 values."""
    filtered_values = [v for v in values if v is not None]
    n = len(filtered_values)

    if n <= 1:
        return (0.0, 0.0)

    mean_val = statistics.mean(filtered_values)
    stdev_val = statistics.stdev(filtered_values)

    standard_error = stdev_val / (n**0.5)

    degrees_of_freedom = n - 1
    lower_bound, upper_bound = stats.t.interval(
        confidence_level, degrees_of_freedom, loc=mean_val, scale=standard_error
    )
    return (lower_bound, upper_bound)


def calculate_prediction_interval(
    values: List[float], confidence_level: float = CONFIDENCE_LEVEL
) -> Tuple[float, float]:
    """Return a t prediction interval, or zeros for fewer than 2 values."""
    filtered_values = [v for v in values if v is not None]
    n = len(filtered_values)

    if n <= 1:
        return (0.0, 0.0)

    mean_val = statistics.mean(filtered_values)
    stdev_val = statistics.stdev(filtered_values)

    degrees_of_freedom = n - 1
    prediction_scale = stdev_val * (1 + 1 / n) ** 0.5

    lower_bound, upper_bound = stats.t.interval(
        confidence_level, degrees_of_freedom, loc=mean_val, scale=prediction_scale
    )
    return (lower_bound, upper_bound)


def calculate_prediction_interval_percentage(
    values: List[float], confidence_level: float = CONFIDENCE_LEVEL
) -> float:
    """Return the prediction-interval margin as a percentage of the mean."""
    filtered_values = [v for v in values if v is not None]
    n = len(filtered_values)

    if n <= 1:
        return 0.0

    mean_val = statistics.mean(filtered_values)
    if mean_val == 0.0:
        return 0.0

    stdev_val = statistics.stdev(filtered_values)

    prediction_error = stdev_val * (1 + 1 / n) ** 0.5

    degrees_of_freedom = n - 1
    alpha = 1 - confidence_level
    t_critical = stats.t.ppf(1 - alpha / 2, degrees_of_freedom)
    margin_of_error = t_critical * prediction_error

    return (margin_of_error / mean_val) * 100.0


def calculate_confidence_interval_percentage(
    values: List[float], confidence_level: float = CONFIDENCE_LEVEL
) -> float:
    """Return the confidence-interval margin as a percentage of the mean."""
    filtered_values = [v for v in values if v is not None]
    n = len(filtered_values)

    if n <= 1:
        return 0.0

    mean_val = statistics.mean(filtered_values)
    if mean_val == 0.0:
        return 0.0

    ci_lower, ci_upper = calculate_confidence_interval(values, confidence_level)
    if ci_lower == 0.0 and ci_upper == 0.0:
        return 0.0
    margin_of_error = (ci_upper - ci_lower) / 2.0
    return (margin_of_error / mean_val) * 100.0


# Metrics and run metadata excluded from both grouping and scenario identity.
_CONFIG_EXCLUDED_FIELDS = frozenset(
    {
        "timestamp",
        "commit",
        "module_commit",
        "module_commit_timestamp",
        "repository",
        "run_count",
        # Performance metrics
        "rps",
        "avg_latency_ms",
        "min_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "max_latency_ms",
        # Standard deviation fields
        "rps_stdev",
        "avg_latency_ms_stdev",
        "p50_latency_ms_stdev",
        "p95_latency_ms_stdev",
        "p99_latency_ms_stdev",
        # Coefficient of variation fields
        "rps_cv",
        "avg_latency_ms_cv",
        "p50_latency_ms_cv",
        "p95_latency_ms_cv",
        "p99_latency_ms_cv",
        # Confidence interval fields
        "rps_ci_lower",
        "rps_ci_upper",
        "rps_ci_percent",
        "avg_latency_ms_ci_lower",
        "avg_latency_ms_ci_upper",
        "avg_latency_ms_ci_percent",
        "p50_latency_ms_ci_lower",
        "p50_latency_ms_ci_upper",
        "p50_latency_ms_ci_percent",
        "p95_latency_ms_ci_lower",
        "p95_latency_ms_ci_upper",
        "p95_latency_ms_ci_percent",
        "p99_latency_ms_ci_lower",
        "p99_latency_ms_ci_upper",
        "p99_latency_ms_ci_percent",
        # Prediction interval fields
        "rps_pi_lower",
        "rps_pi_upper",
        "rps_pi_percent",
        "avg_latency_ms_pi_lower",
        "avg_latency_ms_pi_upper",
        "avg_latency_ms_pi_percent",
        "p50_latency_ms_pi_lower",
        "p50_latency_ms_pi_upper",
        "p50_latency_ms_pi_percent",
        "p95_latency_ms_pi_lower",
        "p95_latency_ms_pi_upper",
        "p95_latency_ms_pi_percent",
        "p99_latency_ms_pi_lower",
        "p99_latency_ms_pi_upper",
        "p99_latency_ms_pi_percent",
    }
)

# Failure state and display-only fields do not identify a scenario. In particular,
# success and failure rows may use different command labels.
_NON_IDENTITY_FIELDS = _CONFIG_EXCLUDED_FIELDS | {
    "status",
    "error",
    "command",
    "group_description",
    "scenario_description",
}

# Live readings may differ between compatible runs. Unknown environment fields
# remain identity axes so new compatibility metadata is conservative by default.
_VOLATILE_ENV_FIELDS = frozenset({"env_cpu_freq_mhz_at_setup"})


def discover_config_keys(data: List[Dict[str, Any]]) -> List[str]:
    """
    Dynamically discover configuration keys from benchmark data.

    Excludes performance metrics and metadata fields, keeping only
    configuration parameters that define test scenarios.
    """
    config_keys = set()

    for item in data:
        for key, value in item.items():
            if key not in _CONFIG_EXCLUDED_FIELDS and key not in _VOLATILE_ENV_FIELDS:
                # Only include keys with hashable values for grouping
                if isinstance(value, (str, int, float, bool, type(None))):
                    config_keys.add(key)

    # Sort with test_id first (if present) for natural test ordering
    sorted_keys = sorted(config_keys)
    if "test_id" in sorted_keys:
        sorted_keys.remove("test_id")
        sorted_keys.insert(0, "test_id")
    return sorted_keys


def create_config_signature(item: Dict[str, Any], config_keys: List[str]) -> Tuple:
    """Build a scalar signature plus a frozen ``config_set`` sweep value."""
    config_set = item.get("config_set")
    frozen_config_set = _make_hashable(config_set) if config_set else None
    return tuple(item.get(key) for key in config_keys) + (frozen_config_set,)


def group_by_command(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group benchmark items by command type (GET, SET, etc.)."""
    grouped = {}
    for item in items:
        command = item.get("command", "UNKNOWN")
        if command not in grouped:
            grouped[command] = []
        grouped[command].append(item)
    return grouped


def summarize_benchmark_results(data_items: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate summary statistics for a group of benchmark results."""
    if not data_items:
        return {
            "rps": 0.0,
            "avg_latency_ms": 0.0,
            "p50_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "p99_latency_ms": 0.0,
        }

    rps_values = [item.get("rps", 0.0) for item in data_items]
    avg_latency_values = [item.get("avg_latency_ms", 0.0) for item in data_items]
    p50_latency_values = [item.get("p50_latency_ms", 0.0) for item in data_items]
    p95_latency_values = [item.get("p95_latency_ms", 0.0) for item in data_items]
    p99_latency_values = [item.get("p99_latency_ms", 0.0) for item in data_items]

    return {
        "rps": calculate_mean(rps_values),
        "avg_latency_ms": calculate_mean(avg_latency_values),
        "p50_latency_ms": calculate_mean(p50_latency_values),
        "p95_latency_ms": calculate_mean(p95_latency_values),
        "p99_latency_ms": calculate_mean(p99_latency_values),
    }


def average_multiple_runs(
    data: List[Dict[str, Any]],
    shared_config_keys: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Average runs with identical signatures using optional shared config keys."""
    if not data:
        return []

    base_keys = (
        shared_config_keys
        if shared_config_keys is not None
        else discover_config_keys(data)
    )
    config_keys = [
        key
        for key in base_keys
        if key not in ["timestamp", "run_count"] and not key.endswith("_stdev")
    ]

    grouped_runs = {}
    for item in data:
        config_signature = create_config_signature(item, config_keys)
        grouped_runs.setdefault(config_signature, []).append(item)

    # Process each configuration group
    averaged_results = []
    for config_signature, runs in grouped_runs.items():
        averaged_item = dict(zip(config_keys, config_signature))
        averaged_item["run_count"] = len(runs)

        if len(runs) == 1:
            # Single run: preserve original data with zero standard deviations
            single_run = runs[0].copy()
            single_run["run_count"] = 1
            single_run.update(
                {
                    "rps_stdev": 0.0,
                    "avg_latency_ms_stdev": 0.0,
                    "p50_latency_ms_stdev": 0.0,
                    "p95_latency_ms_stdev": 0.0,
                    "p99_latency_ms_stdev": 0.0,
                }
            )
            averaged_results.append(single_run)
        else:
            metric_values = {
                "rps": [run.get("rps", 0.0) for run in runs],
                "avg_latency_ms": [run.get("avg_latency_ms", 0.0) for run in runs],
                "p50_latency_ms": [run.get("p50_latency_ms", 0.0) for run in runs],
                "p95_latency_ms": [run.get("p95_latency_ms", 0.0) for run in runs],
                "p99_latency_ms": [run.get("p99_latency_ms", 0.0) for run in runs],
            }

            for metric, values in metric_values.items():
                mean_val = calculate_mean(values)
                stdev_val = calculate_stdev(values)

                averaged_item[metric] = mean_val
                averaged_item[f"{metric}_stdev"] = stdev_val

                if mean_val == 0.0 or stdev_val == 0.0:
                    averaged_item[f"{metric}_cv"] = 0.0
                else:
                    averaged_item[f"{metric}_cv"] = (stdev_val / mean_val) * 100.0

                ci_lower, ci_upper = calculate_confidence_interval(values)
                averaged_item[f"{metric}_ci_lower"] = ci_lower
                averaged_item[f"{metric}_ci_upper"] = ci_upper

                ci_percentage = calculate_confidence_interval_percentage(values)
                averaged_item[f"{metric}_ci_percent"] = ci_percentage

                pi_lower, pi_upper = calculate_prediction_interval(values)
                averaged_item[f"{metric}_pi_lower"] = pi_lower
                averaged_item[f"{metric}_pi_upper"] = pi_upper

                pi_percentage = calculate_prediction_interval_percentage(values)
                averaged_item[f"{metric}_pi_percent"] = pi_percentage

            # Preserve the most recent timestamp and commit
            timestamps = [run.get("timestamp") for run in runs if run.get("timestamp")]
            if timestamps:
                averaged_item["timestamp"] = max(timestamps)

            commits = [run.get("commit") for run in runs if run.get("commit")]
            if commits:
                averaged_item["commit"] = commits[0]

            module_commits = [
                run.get("module_commit") for run in runs if run.get("module_commit")
            ]
            if module_commits:
                averaged_item["module_commit"] = module_commits[0]

            repositories = [
                run.get("repository") for run in runs if run.get("repository")
            ]
            if repositories:
                averaged_item["repository"] = repositories[0]

            # ``config_set`` is frozen outside ``config_keys``; retain its raw form.
            if "config_set" in runs[0]:
                averaged_item["config_set"] = runs[0]["config_set"]

            averaged_results.append(averaged_item)

    return averaged_results


def calculate_percentage_change(new_value: float, old_value: float) -> float:
    """Calculate percentage change between two values."""
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100.0


def create_config_sort_key(config_tuple: Tuple) -> Tuple[str, ...]:
    """Normalize mixed configuration values into a sortable string tuple."""

    def normalize_value(value):
        return "" if value is None else str(value)

    return tuple(normalize_value(item) for item in config_tuple)


def extract_version_identifier(data: List[Dict[str, Any]]) -> str:
    """
    Extract a version identifier from benchmark data.

    Prioritizes module_commit (for module benchmarks), then commit hash,
    falls back to a short timestamp format, or returns "Unknown".
    """
    if not data:
        return "Unknown"

    first_item = data[0]

    # Try module_commit first (module benchmarks)
    module_commit = first_item.get("module_commit")
    if module_commit:
        return module_commit if len(module_commit) <= 12 else module_commit[:8]

    # Try commit hash (core benchmarks)
    commit = first_item.get("commit")
    if commit:
        # Return short hash if already short, otherwise truncate to 8 characters
        return commit if len(commit) <= 12 else commit[:8]

    # Fallback to short timestamp format
    timestamp = first_item.get("timestamp")
    if timestamp:
        # Extract just the date part for cleaner display
        try:
            # Parse timestamp and extract date
            if "T" in timestamp:
                date_part = timestamp.split("T")[0]  # Get YYYY-MM-DD part
                return date_part
            else:
                return timestamp[:10]  # First 10 chars should be YYYY-MM-DD
        except:
            return f"ts-{timestamp[:10]}"

    return "Unknown"


def extract_version_with_repo(data: List[Dict[str, Any]]) -> Tuple[str, Optional[str]]:
    """Extract a version label and optional repository."""
    return extract_version_identifier(data), data[0].get("repository") if data else None


def format_version_link(version: str, repository: Optional[str]) -> str:
    """Link a version to its GitHub commit when a repository is available."""
    if repository:
        return f"[{version}](https://github.com/{repository}/commit/{version})"
    return version


def group_by_static_configuration(
    data: List[Dict[str, Any]],
    shared_config_keys: Optional[List[str]] = None,
) -> Dict[Tuple, Dict[str, Any]]:
    """Group rows by config, excluding command, pipeline, and I/O threads."""
    # Parameters that appear in the comparison table, not in config sections
    table_parameters = {"command", "pipeline", "io_threads"}

    base_keys = (
        shared_config_keys
        if shared_config_keys is not None
        else discover_config_keys(data)
    )
    config_keys = [key for key in base_keys if key not in table_parameters]

    grouped_configs = {}
    for item in data:
        config_signature = create_config_signature(item, config_keys)
        grouped_configs.setdefault(
            config_signature, {"items": [], "config_keys": config_keys}
        )
        grouped_configs[config_signature]["items"].append(item)

    return grouped_configs


# Performance metric fields that a comparable benchmark row is expected to carry.
# A row missing every one of these is not numerically comparable (e.g. a failure
# marker), but a row that merely holds a zero value still counts as comparable.
_PERFORMANCE_METRIC_KEYS = (
    "rps",
    "avg_latency_ms",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
)


def is_failed_row(item: Dict[str, Any]) -> bool:
    """Return whether a row is explicit failure or has no performance metrics."""
    return item.get("status") == "failed" or not any(
        key in item for key in _PERFORMANCE_METRIC_KEYS
    )


def partition_failed_rows(
    data: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split rows into ``(comparable, failed)`` using :func:`is_failed_row`."""
    comparable: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for item in data:
        (failed if is_failed_row(item) else comparable).append(item)
    return comparable, failed


def _make_hashable(value: Any) -> Any:
    """Recursively freeze dictionaries and sequences for use in identity tuples."""
    if isinstance(value, dict):
        return tuple(sorted((str(k), _make_hashable(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_make_hashable(v) for v in value)
    return value


def _scenario_identity(item: Dict[str, Any]) -> Optional[Tuple]:
    """Return the structural scenario identity, or ``None`` for legacy rows.

    All non-metric configuration and stable environment fields participate so
    sweeps pair independently. Failure state, display labels, and volatile host
    readings are excluded so matching success and failure rows still pair.
    """
    test_id = item.get("test_id")
    if test_id is None:
        return None
    config_items = tuple(
        sorted(
            (key, _make_hashable(value))
            for key, value in item.items()
            if key != "test_id"
            and key not in _NON_IDENTITY_FIELDS
            and key not in _VOLATILE_ENV_FIELDS
        )
    )
    return (test_id,) + config_items


def _identity_set(data: List[Dict[str, Any]]) -> set:
    """Return the distinct structural identities present in ``data``."""
    return {
        identity
        for identity in (_scenario_identity(row) for row in data)
        if identity is not None
    }


def _identities_compatible(left: Tuple, right: Tuple) -> bool:
    """Return whether identities agree on every field they have in common."""
    if left[0] != right[0]:
        return False
    left_fields = dict(left[1:])
    right_fields = dict(right[1:])
    if not (
        left_fields.keys() <= right_fields.keys()
        or right_fields.keys() <= left_fields.keys()
    ):
        return False
    return all(
        left_fields[key] == right_fields[key]
        for key in left_fields.keys() & right_fields.keys()
    )


def _resolve_counterpart_identity(
    identity: Optional[Tuple], own_ids: set, other_ids: set
) -> Optional[Tuple]:
    """Find an exact or unambiguous schema-compatible identity on the other side."""
    if identity is None:
        return None
    if identity in other_ids:
        return identity

    candidates = [
        candidate
        for candidate in other_ids
        if _identities_compatible(identity, candidate)
    ]
    if len(candidates) != 1:
        return None

    candidate = candidates[0]
    reverse_candidates = [
        own for own in own_ids if _identities_compatible(candidate, own)
    ]
    return candidate if reverse_candidates == [identity] else None


def _paired_identity_key(identity: Optional[Tuple], counterpart: Optional[Tuple]):
    """Build a shared report key for identities paired across schema versions."""
    if identity is None or counterpart is None or identity == counterpart:
        return identity
    return ("__schema_pair__",) + tuple(sorted((identity, counterpart), key=repr))


def _failed_scenario_ids(failed_rows: List[Dict[str, Any]]) -> set:
    """Collect the non-None scenario identities from a list of failed rows."""
    return {
        identity
        for identity in (_scenario_identity(row) for row in failed_rows)
        if identity is not None
    }


def _primary_failed_metric(metrics_filter: str) -> Tuple[str, str]:
    """Select the healthy counterpart value shown for a failed scenario."""
    if metrics_filter == "latency":
        return ("avg_latency_ms", "avg_latency")
    return ("rps", "rps")


def collect_failed_scenarios(
    baseline_data: List[Dict[str, Any]],
    new_data: List[Dict[str, Any]],
    metrics_filter: str = "all",
) -> List[Dict[str, Any]]:
    """Describe failures and the matching healthy side's primary metric value."""
    metric_key, metric_label = _primary_failed_metric(metrics_filter)

    def index(data: List[Dict[str, Any]]) -> Tuple[Dict[Tuple, List[Dict]], set]:
        comparable_by_id: Dict[Tuple, List[Dict[str, Any]]] = {}
        present_ids = _identity_set(data)
        for row in data:
            identity = _scenario_identity(row)
            if identity is None:
                continue
            if not is_failed_row(row):
                comparable_by_id.setdefault(identity, []).append(row)
        return comparable_by_id, present_ids

    baseline_comparable, baseline_ids = index(baseline_data)
    new_comparable, new_ids = index(new_data)

    # Use all rows so a lone failed member of a sweep still names its varying axes.
    identities_by_test: Dict[Any, List[Dict[str, Any]]] = {}
    for row in baseline_data + new_data:
        identity = _scenario_identity(row)
        if identity is not None:
            identities_by_test.setdefault(identity[0], []).append(dict(identity[1:]))
    varying_axes = {
        test_id: {
            key
            for key in set().union(*(fields.keys() for fields in identities))
            if key != "test_phase"
            if len({_make_hashable(fields.get(key)) for fields in identities}) > 1
        }
        for test_id, identities in identities_by_test.items()
    }

    descriptors: List[Dict[str, Any]] = []
    for side, data, own_ids, other_comparable, other_ids in (
        ("baseline", baseline_data, baseline_ids, new_comparable, new_ids),
        ("new", new_data, new_ids, baseline_comparable, baseline_ids),
    ):
        _, failed = partition_failed_rows(data)
        for row in failed:
            identity = _scenario_identity(row)
            display_axes = {
                key: row.get(key) for key in varying_axes.get(row.get("test_id"), set())
            }
            if row.get("config_set"):
                display_axes["config_set"] = row["config_set"]
            counterpart_identity = _resolve_counterpart_identity(
                identity, own_ids, other_ids
            )
            counterpart_rows = other_comparable.get(counterpart_identity, [])
            counterpart_value = (
                calculate_mean([r.get(metric_key) for r in counterpart_rows])
                if counterpart_rows
                else None
            )
            descriptors.append(
                {
                    "test_id": row.get("test_id"),
                    "test_phase": row.get("test_phase"),
                    "command": row.get("command"),
                    "side": side,
                    "error": row.get("error"),
                    "counterpart_value": counterpart_value,
                    "counterpart_present": counterpart_identity is not None,
                    "metric_key": metric_key,
                    "metric_label": metric_label,
                    "identity": identity,
                    "pairing_identity": _paired_identity_key(
                        identity, counterpart_identity
                    ),
                    "config_set": row.get("config_set"),
                    "cluster_mode": row.get("cluster_mode"),
                    "io_threads": row.get("io_threads"),
                    "display_axes": display_axes,
                }
            )
    return descriptors


def create_comparison_table_data(
    baseline_data: List[Dict[str, Any]],
    new_data: List[Dict[str, Any]],
    metrics_filter: str = "all",
) -> Tuple[List[Dict], str, str, Optional[str], Optional[str]]:
    """
    Create structured comparison data for benchmark results.

    Returns configuration groups with their comparison table rows,
    along with version identifiers and repositories for both datasets.
    """
    baseline_version, baseline_repo = extract_version_with_repo(baseline_data)
    new_version, new_repo = extract_version_with_repo(new_data)

    baseline_comparable, baseline_failed = partition_failed_rows(baseline_data)
    new_comparable, new_failed = partition_failed_rows(new_data)

    # Exclude both sides of a failure; the report renders them separately.
    baseline_ids = _identity_set(baseline_data)
    new_ids = _identity_set(new_data)
    baseline_failed_ids = _failed_scenario_ids(baseline_failed)
    new_failed_ids = _failed_scenario_ids(new_failed)
    baseline_excluded = set(baseline_failed_ids)
    new_excluded = set(new_failed_ids)
    for identity in new_failed_ids:
        counterpart = _resolve_counterpart_identity(identity, new_ids, baseline_ids)
        if counterpart is not None:
            baseline_excluded.add(counterpart)
    for identity in baseline_failed_ids:
        counterpart = _resolve_counterpart_identity(identity, baseline_ids, new_ids)
        if counterpart is not None:
            new_excluded.add(counterpart)

    if baseline_excluded or new_excluded:
        baseline_comparable = [
            row
            for row in baseline_comparable
            if _scenario_identity(row) not in baseline_excluded
        ]
        new_comparable = [
            row for row in new_comparable if _scenario_identity(row) not in new_excluded
        ]

    # Both datasets must use the same signature key space.
    shared_config_keys = discover_config_keys(baseline_comparable + new_comparable)

    # Group data by static configuration using the shared key list
    baseline_configs = group_by_static_configuration(
        baseline_comparable, shared_config_keys
    )
    new_configs = group_by_static_configuration(new_comparable, shared_config_keys)

    # Define available metrics with their display names
    available_metrics = [
        ("rps", "rps"),
        ("avg_latency_ms", "avg_latency"),
        ("p50_latency_ms", "p50_latency"),
        ("p95_latency_ms", "p95_latency"),
        ("p99_latency_ms", "p99_latency"),
    ]

    # Select metrics based on filter
    if metrics_filter == "rps":
        selected_metrics = [("rps", "rps")]
    elif metrics_filter == "latency":
        selected_metrics = [
            ("avg_latency_ms", "avg_latency"),
            ("p50_latency_ms", "p50_latency"),
            ("p95_latency_ms", "p95_latency"),
            ("p99_latency_ms", "p99_latency"),
        ]
    else:  # "all" or any other value
        selected_metrics = available_metrics

    # Process all unique configurations from both datasets
    all_config_signatures = sorted(
        set(baseline_configs.keys()) | set(new_configs.keys()),
        key=create_config_sort_key,
    )

    configuration_groups = []

    for config_signature in all_config_signatures:
        # Get configuration groups (may be empty for one dataset)
        baseline_group = baseline_configs.get(
            config_signature, {"items": [], "config_keys": []}
        )
        new_group = new_configs.get(config_signature, {"items": [], "config_keys": []})

        # Get configuration keys from either group
        config_keys = baseline_group.get("config_keys") or new_group.get(
            "config_keys", []
        )
        if not config_keys:
            continue

        # Create configuration dictionary for display
        config_dict = dict(zip(config_keys, config_signature))

        # ``config_set`` is frozen outside scalar keys; restore it for display.
        group_items = baseline_group["items"] or new_group["items"]
        config_set_label = _format_config_set(
            group_items[0].get("config_set") if group_items else None
        )
        if config_set_label:
            config_keys = list(config_keys) + ["config_set"]
            config_dict["config_set"] = config_set_label

        # Generate comparison table rows for this configuration
        table_rows = _generate_table_rows_for_config(
            baseline_group["items"], new_group["items"], selected_metrics
        )

        configuration_groups.append(
            {
                "config_dict": config_dict,
                "config_keys": config_keys,
                "table_rows": table_rows,
            }
        )

    return configuration_groups, baseline_version, new_version, baseline_repo, new_repo


def _generate_table_rows_for_config(
    baseline_items: List[Dict[str, Any]],
    new_items: List[Dict[str, Any]],
    metrics: List[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    """Generate comparison table rows for a specific configuration."""
    # Group by command type
    baseline_by_command = group_by_command(baseline_items)
    new_by_command = group_by_command(new_items)

    all_commands = sorted(set(baseline_by_command.keys()) | set(new_by_command.keys()))
    table_rows = []

    for command in all_commands:
        baseline_cmd_items = baseline_by_command.get(command, [])
        new_cmd_items = new_by_command.get(command, [])

        # Group by pipeline and io_threads parameters
        baseline_by_params = _group_by_table_parameters(baseline_cmd_items)
        new_by_params = _group_by_table_parameters(new_cmd_items)

        all_param_keys = set(baseline_by_params.keys()) | set(new_by_params.keys())

        for param_key in sorted(all_param_keys):
            pipeline, io_threads = param_key

            baseline_param_items = baseline_by_params.get(param_key, [])
            new_param_items = new_by_params.get(param_key, [])

            # Calculate summaries for comparison
            baseline_summary = summarize_benchmark_results(baseline_param_items)
            new_summary = summarize_benchmark_results(new_param_items)

            # Extract run count and standard deviation information
            baseline_stats = _extract_run_statistics(baseline_param_items)
            new_stats = _extract_run_statistics(new_param_items)

            # Create table rows for each metric
            for metric_key, metric_display in metrics:
                baseline_value = baseline_summary.get(metric_key, 0.0)
                new_value = new_summary.get(metric_key, 0.0)

                table_rows.append(
                    {
                        "command": command,
                        "metric": metric_display,
                        "pipeline": pipeline,
                        "io_threads": io_threads,
                        "baseline_value": baseline_value,
                        "new_value": new_value,
                        "diff": new_value - baseline_value,
                        "change": calculate_percentage_change(
                            new_value, baseline_value
                        ),
                        "baseline_run_count": baseline_stats["run_count"],
                        "new_run_count": new_stats["run_count"],
                        "baseline_stdev": baseline_stats.get(
                            f"{metric_key}_stdev", 0.0
                        ),
                        "new_stdev": new_stats.get(f"{metric_key}_stdev", 0.0),
                        "baseline_cv": baseline_stats.get(f"{metric_key}_cv", 0.0),
                        "new_cv": new_stats.get(f"{metric_key}_cv", 0.0),
                        "baseline_ci_lower": baseline_stats.get(
                            f"{metric_key}_ci_lower", 0.0
                        ),
                        "baseline_ci_upper": baseline_stats.get(
                            f"{metric_key}_ci_upper", 0.0
                        ),
                        "new_ci_lower": new_stats.get(f"{metric_key}_ci_lower", 0.0),
                        "new_ci_upper": new_stats.get(f"{metric_key}_ci_upper", 0.0),
                        "baseline_ci_percent": baseline_stats.get(
                            f"{metric_key}_ci_percent", 0.0
                        ),
                        "new_ci_percent": new_stats.get(
                            f"{metric_key}_ci_percent", 0.0
                        ),
                        "baseline_pi_lower": baseline_stats.get(
                            f"{metric_key}_pi_lower", 0.0
                        ),
                        "baseline_pi_upper": baseline_stats.get(
                            f"{metric_key}_pi_upper", 0.0
                        ),
                        "new_pi_lower": new_stats.get(f"{metric_key}_pi_lower", 0.0),
                        "new_pi_upper": new_stats.get(f"{metric_key}_pi_upper", 0.0),
                        "baseline_pi_percent": baseline_stats.get(
                            f"{metric_key}_pi_percent", 0.0
                        ),
                        "new_pi_percent": new_stats.get(
                            f"{metric_key}_pi_percent", 0.0
                        ),
                    }
                )

    return table_rows


def _group_by_table_parameters(
    items: List[Dict[str, Any]],
) -> Dict[Tuple, List[Dict[str, Any]]]:
    """Group items by table-level parameters (pipeline, io_threads)."""
    grouped = {}
    for item in items:
        key = (item.get("pipeline"), item.get("io_threads"))
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(item)
    return grouped


def _extract_run_statistics(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract run count, standard deviation, coefficient of variation, and confidence interval statistics from benchmark items."""
    if not items:
        return {"run_count": 0}

    # Use pre-calculated run_count if available, otherwise count items
    run_count = items[0].get("run_count", len(items))

    stats = {"run_count": run_count}

    # Extract standard deviations, coefficient of variations, and confidence intervals if available
    for metric_base in [
        "rps",
        "avg_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
    ]:
        stdev_key = f"{metric_base}_stdev"
        cv_key = f"{metric_base}_cv"
        ci_lower_key = f"{metric_base}_ci_lower"
        ci_upper_key = f"{metric_base}_ci_upper"

        if stdev_key in items[0]:
            # Use pre-calculated values
            stats[stdev_key] = items[0][stdev_key]
            stats[cv_key] = items[0].get(cv_key, 0.0)
            stats[ci_lower_key] = items[0].get(ci_lower_key, 0.0)
            stats[ci_upper_key] = items[0].get(ci_upper_key, 0.0)
            ci_percent_key = f"{metric_base}_ci_percent"
            stats[ci_percent_key] = items[0].get(ci_percent_key, 0.0)

            # Extract pre-calculated PI values
            pi_lower_key = f"{metric_base}_pi_lower"
            pi_upper_key = f"{metric_base}_pi_upper"
            pi_percent_key = f"{metric_base}_pi_percent"
            stats[pi_lower_key] = items[0].get(pi_lower_key, 0.0)
            stats[pi_upper_key] = items[0].get(pi_upper_key, 0.0)
            stats[pi_percent_key] = items[0].get(pi_percent_key, 0.0)
        elif run_count > 1:
            # Calculate from raw data if not pre-calculated
            values = [item.get(metric_base, 0.0) for item in items]
            mean_val = calculate_mean(values)
            stdev_val = calculate_stdev(values)

            stats[stdev_key] = stdev_val

            # Calculate CV directly from computed mean and stdev
            if mean_val == 0.0 or stdev_val == 0.0:
                stats[cv_key] = 0.0
            else:
                stats[cv_key] = (stdev_val / mean_val) * 100.0

            # Calculate confidence interval
            ci_lower, ci_upper = calculate_confidence_interval(values)
            stats[ci_lower_key] = ci_lower
            stats[ci_upper_key] = ci_upper

            # Calculate confidence interval as percentage of mean
            ci_percent_key = f"{metric_base}_ci_percent"
            ci_percentage = calculate_confidence_interval_percentage(values)
            stats[ci_percent_key] = ci_percentage

            # Calculate prediction interval
            pi_lower_key = f"{metric_base}_pi_lower"
            pi_upper_key = f"{metric_base}_pi_upper"
            pi_lower, pi_upper = calculate_prediction_interval(values)
            stats[pi_lower_key] = pi_lower
            stats[pi_upper_key] = pi_upper

            # Calculate prediction interval as percentage of mean
            pi_percent_key = f"{metric_base}_pi_percent"
            pi_percentage = calculate_prediction_interval_percentage(values)
            stats[pi_percent_key] = pi_percentage
        else:
            stats[stdev_key] = 0.0
            stats[cv_key] = 0.0
            stats[ci_lower_key] = 0.0
            stats[ci_upper_key] = 0.0
            pi_lower_key = f"{metric_base}_pi_lower"
            pi_upper_key = f"{metric_base}_pi_upper"
            pi_percent_key = f"{metric_base}_pi_percent"
            stats[pi_lower_key] = 0.0
            stats[pi_upper_key] = 0.0
            stats[pi_percent_key] = 0.0

    return stats


def _extract_common_and_unique_config(
    config_groups: List[Dict],
) -> Tuple[Dict[str, Any], List[Dict]]:
    """
    Extract common configuration shared by all groups and unique config per group.

    Returns:
        Tuple of (common_config dict, list of groups with unique_config added)
    """
    if not config_groups:
        return {}, []

    # Get all config keys (excluding statistical fields)
    def is_display_key(key: str) -> bool:
        return not (
            key.endswith("_cv")
            or key.endswith("_ci_lower")
            or key.endswith("_ci_upper")
            or key.endswith("_ci_percent")
        )

    # Collect all values for each key across all groups
    all_keys = set()
    for group in config_groups:
        all_keys.update(k for k in group["config_keys"] if is_display_key(k))

    # Find common config (same value across all groups)
    common_config = {}
    varying_keys = set()

    for key in all_keys:
        values = set()
        for group in config_groups:
            value = group["config_dict"].get(key)
            if value is not None:
                values.add(value)

        if len(values) == 1:
            # Same value in all groups - it's common
            common_config[key] = values.pop()
        elif len(values) > 1:
            # Different values - it varies
            varying_keys.add(key)

    # Add unique_config to each group (only varying keys)
    updated_groups = []
    for group in config_groups:
        unique_config = {}
        for key in varying_keys:
            value = group["config_dict"].get(key)
            if value is not None:
                unique_config[key] = value

        updated_group = group.copy()
        updated_group["unique_config"] = unique_config
        updated_groups.append(updated_group)

    return common_config, updated_groups


def _generate_summary(
    config_groups: List[Dict],
) -> Tuple[List[Dict], List[Dict], int, int]:
    """Collect significant changes and unchanged/insufficient counts."""
    improvements = []
    regressions = []
    no_change_count = 0
    insufficient_data_count = 0

    for group in config_groups:
        unique_config = group.get("unique_config", {})
        config_str = (
            ", ".join(f"{k}={v}" for k, v in sorted(unique_config.items()))
            if unique_config
            else ""
        )

        for row in group.get("table_rows", []):
            significance = _get_significance_indicator(
                row.get("baseline_run_count", 0),
                row.get("new_run_count", 0),
                row.get("baseline_ci_lower", 0.0),
                row.get("baseline_ci_upper", 0.0),
                row.get("new_ci_lower", 0.0),
                row.get("new_ci_upper", 0.0),
                row["change"],
                row["metric"],
            )

            test_label = f"{row['command']} {row['metric']} pipe={row['pipeline']} threads={row['io_threads']}"
            if config_str:
                test_label = f"{test_label} ({config_str})"

            change_formatted = _format_percent_change(
                row["baseline_value"],
                row.get("baseline_stdev", 0.0),
                row["new_value"],
                row.get("new_stdev", 0.0),
                row.get("baseline_run_count", 0),
                row.get("new_run_count", 0),
            )

            if significance == "✅":
                improvements.append(
                    {
                        "test": test_label,
                        "change": change_formatted,
                        "change_magnitude": abs(row["change"]),
                    }
                )
            elif significance == "❌":
                regressions.append(
                    {
                        "test": test_label,
                        "change": change_formatted,
                        "change_magnitude": abs(row["change"]),
                    }
                )
            elif significance == "❔":
                insufficient_data_count += 1
            else:
                no_change_count += 1

    return improvements, regressions, no_change_count, insufficient_data_count


def _sanitize_table_cell(text: Optional[str]) -> str:
    """Render untrusted text as a literal code span inside a Markdown table."""
    if text is None:
        return ""
    s = str(text).replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    s = s.strip()
    if not s:
        return ""

    # GitHub creates mentions and issue/email links after decoding character
    # references. Code spans suppress that post-processing. Use a fence longer
    # than any run in the value so embedded backticks cannot close the span.
    longest_run = current_run = 0
    for char in s:
        current_run = current_run + 1 if char == "`" else 0
        longest_run = max(longest_run, current_run)
    fence = "`" * (longest_run + 1)

    # GFM finds table separators before parsing inline code, so pipes still
    # need a backslash at the table layer. The backslash is not displayed.
    s = s.replace("|", r"\|")
    return f"{fence} {s} {fence}"


def _config_set_text(config_set: Optional[Dict[str, Any]]) -> str:
    """Return sorted ``config_set`` key/value pairs as plain text."""
    if not config_set:
        return ""
    return "; ".join(f"{k}={config_set[k]}" for k in sorted(config_set, key=str))


def _format_config_set(config_set: Optional[Dict[str, Any]]) -> str:
    """Render sorted ``config_set`` key/value pairs as literal text."""
    return _sanitize_table_cell(_config_set_text(config_set))


def _failed_side_cell(
    own_entry: Optional[Dict[str, Any]],
    other_entry: Optional[Dict[str, Any]],
) -> str:
    """Render ``FAILED``, the healthy counterpart value, or ``n/a``."""
    if own_entry is not None:
        return "**FAILED**"
    if other_entry is None:
        return "n/a"
    value = other_entry.get("counterpart_value")
    if value is None:
        return "n/a"
    label = other_entry.get("metric_label", "")
    return _sanitize_table_cell(f"{_format_with_sig_figs(value)} {label}".strip())


def _failed_error_cell(
    baseline_entry: Optional[Dict[str, Any]],
    new_entry: Optional[Dict[str, Any]],
) -> str:
    """Render one error, or side-prefixed errors when both sides failed."""
    parts = []
    if baseline_entry is not None:
        parts.append(("baseline", baseline_entry.get("error")))
    if new_entry is not None:
        parts.append(("new", new_entry.get("error")))

    def clean(err: Optional[str]) -> str:
        return str(err).strip() if err else "no error recorded"

    if len(parts) == 1:
        error_text = clean(parts[0][1])
    else:
        error_text = "; ".join(f"{side}: {clean(err)}" for side, err in parts)
    return _sanitize_table_cell(error_text)


def _failed_scenario_label(descriptor: Dict[str, Any]) -> str:
    """Render a scenario id plus config axes that distinguish failed rows."""
    label = str(descriptor.get("test_id") or "unknown")
    axis_bits: List[str] = []
    aliases = {"cluster_mode": "cluster"}
    for key, value in sorted(descriptor.get("display_axes", {}).items()):
        if key == "config_set":
            axis_bits.append(_config_set_text(value))
        elif value is not None:
            name = aliases.get(key, key)
            axis_bits.append(f"{name}={value}")

    if axis_bits:
        label = f"{label} ({'; '.join(axis_bits)})"
    return _sanitize_table_cell(label)


def _format_failed_scenarios_section(
    failed_scenarios: List[Dict[str, Any]],
    baseline_version: str,
    new_version: str,
) -> List[str]:
    """Render failures by full scenario identity as a two-sided table."""
    grouped: Dict[Any, List[Dict[str, Any]]] = {}
    for failure in failed_scenarios:
        key = failure.get("pairing_identity", failure.get("identity"))
        if key is None:
            key = ("__no_id__", id(failure))
        grouped.setdefault(key, []).append(failure)

    lines = [
        f"## ⚠️ {len(grouped)} failed scenario(s)",
        "",
        "Excluded from the numeric comparison (a failure has nothing to compare "
        "against); the measured value is shown for whichever side succeeded:",
        "",
        f"| Scenario | Phase | Command | "
        f"{_sanitize_table_cell(baseline_version)} | "
        f"{_sanitize_table_cell(new_version)} | Error |",
        "| --- | --- | --- | --- | --- | --- |",
    ]

    for entries in grouped.values():
        baseline_entry = next((e for e in entries if e["side"] == "baseline"), None)
        new_entry = next((e for e in entries if e["side"] == "new"), None)
        sample = entries[0]

        test_id = _failed_scenario_label(sample)
        phase = _sanitize_table_cell(sample.get("test_phase"))
        command = _sanitize_table_cell(sample.get("command"))
        baseline_cell = _failed_side_cell(baseline_entry, new_entry)
        new_cell = _failed_side_cell(new_entry, baseline_entry)
        error_cell = _failed_error_cell(baseline_entry, new_entry)

        lines.append(
            f"| {test_id} | {phase} | {command} | "
            f"{baseline_cell} | {new_cell} | {error_cell} |"
        )

    lines.append("")
    return lines


def format_comparison_report(
    config_groups: List[Dict],
    baseline_version: str,
    new_version: str,
    baseline_repo: Optional[str] = None,
    new_repo: Optional[str] = None,
    core_commit_baseline: Optional[str] = None,
    core_commit_new: Optional[str] = None,
    failed_scenarios: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Format summary, failures, and comparison tables as Markdown."""
    if not config_groups and not failed_scenarios:
        return "No data to compare."

    # Format version headers with links if repositories available
    baseline_header = format_version_link(baseline_version, baseline_repo)
    new_header = format_version_link(new_version, new_repo)

    # Extract common vs unique configuration
    common_config, groups_with_unique = _extract_common_and_unique_config(config_groups)

    # Generate summary
    improvements, regressions, no_change_count, insufficient_data_count = (
        _generate_summary(groups_with_unique)
    )

    report_lines = []

    # Summary section
    significant_changes = [
        ("✅", item["change"], item["test"], item["change_magnitude"])
        for item in improvements
    ] + [
        ("❌", item["change"], item["test"], item["change_magnitude"])
        for item in regressions
    ]
    significant_changes.sort(key=lambda x: x[3], reverse=True)

    total_tests = len(significant_changes) + no_change_count + insufficient_data_count

    if significant_changes:
        report_lines.append(f"## {len(significant_changes)} significant change(s)")
        report_lines.append("")
        for emoji, change, test, _ in significant_changes:
            report_lines.append(f"- {emoji} {change} {test}")
        report_lines.append("")
    else:
        report_lines.append("## No significant changes")
        report_lines.append("")
        report_lines.append(
            f"No statistically significant changes detected across {total_tests} test(s)."
        )
        report_lines.append("")

    # Summary counts
    summary_parts = []
    if no_change_count:
        summary_parts.append(f"{no_change_count} with no significant change")
    if insufficient_data_count:
        summary_parts.append(f"{insufficient_data_count} with insufficient data")
    if summary_parts:
        report_lines.append(f"*{', '.join(summary_parts)}*")
        report_lines.append("")

    if failed_scenarios:
        report_lines.extend(
            _format_failed_scenarios_section(
                failed_scenarios, baseline_version, new_version
            )
        )

    # Collapsible details section
    report_lines.append("<details>")
    report_lines.append("<summary>Click to expand full comparison tables</summary>")
    report_lines.append("")

    # Check if we have multiple groups with varying config
    has_varying_config = any(g.get("unique_config") for g in groups_with_unique)

    for group in groups_with_unique:
        unique_config = group.get("unique_config", {})
        table_rows = group["table_rows"]

        if not table_rows:
            continue

        # Only show heading if there are varying attributes across groups
        if has_varying_config and unique_config:
            config_str = ", ".join(
                f"{k} = {v}" for k, v in sorted(unique_config.items())
            )
            report_lines.append(f"### {config_str}")
            report_lines.append("")

        # Comparison table for this configuration
        report_lines.extend(
            [
                f"| | % Change | Test | {baseline_header} | {new_header} | {baseline_version} stats | {new_version} stats |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in table_rows:
            # Format metric values with uncertainty-based precision
            baseline_stdev = (
                row.get("baseline_stdev", 0.0)
                if row.get("baseline_run_count", 0) > 1
                else 0
            )
            new_stdev = (
                row.get("new_stdev", 0.0) if row.get("new_run_count", 0) > 1 else 0
            )
            baseline_display = _format_with_sig_figs(
                row["baseline_value"], baseline_stdev
            )
            new_display = _format_with_sig_figs(row["new_value"], new_stdev)

            # Determine significance indicator
            significance = _get_significance_indicator(
                row.get("baseline_run_count", 0),
                row.get("new_run_count", 0),
                row.get("baseline_ci_lower", 0.0),
                row.get("baseline_ci_upper", 0.0),
                row.get("new_ci_lower", 0.0),
                row.get("new_ci_upper", 0.0),
                row["change"],
                row["metric"],
            )

            # Format % change with uncertainty
            change_formatted = _format_percent_change(
                row["baseline_value"],
                row.get("baseline_stdev", 0.0),
                row["new_value"],
                row.get("new_stdev", 0.0),
                row.get("baseline_run_count", 0),
                row.get("new_run_count", 0),
            )

            # Create table row
            test_label = f"{row['command']} {row['metric']} P{row['pipeline']} T{row['io_threads']}"

            # Format stats separately
            baseline_stats_display = _format_stats_only(
                row.get("baseline_run_count", 0),
                row.get("baseline_stdev", 0.0),
                row.get("baseline_cv", 0.0),
                row.get("baseline_ci_percent", 0.0),
                row.get("baseline_pi_percent", 0.0),
            )
            new_stats_display = _format_stats_only(
                row.get("new_run_count", 0),
                row.get("new_stdev", 0.0),
                row.get("new_cv", 0.0),
                row.get("new_ci_percent", 0.0),
                row.get("new_pi_percent", 0.0),
            )

            report_lines.append(
                f"| {significance} | {change_formatted} | {test_label} | "
                f"{baseline_display} | {new_display} | {baseline_stats_display} | {new_stats_display} |"
            )

        report_lines.append("")

    # Add common configuration
    if common_config:
        report_lines.append("---")
        report_lines.append("")
        report_lines.append("**Configuration:**")
        for key in sorted(common_config.keys()):
            report_lines.append(f"- {key}: {common_config[key]}")
        report_lines.append("")

    # Add core commit metadata
    if core_commit_baseline or core_commit_new:
        if core_commit_baseline == core_commit_new:
            report_lines.append(f"**Core commit:** {core_commit_baseline}")
        else:
            report_lines.append(
                f"**Core commit:** {core_commit_baseline} (baseline) → "
                f"{core_commit_new} (new)"
            )
        report_lines.append("")

    # Add legend
    report_lines.append("**Legend:**")
    report_lines.append(
        "- **Test column**: Command, metric, P=pipeline depth, T=io-threads"
    )
    report_lines.append(
        "- **Significance**: ✅ significant improvement, ❌ significant regression, ➖ not significant, ❔ insufficient data"
    )
    report_lines.append("")
    report_lines.append("**Statistical Notes:**")
    report_lines.append(
        "- **CV**: Coefficient of Variation - relative variability (σ/μ × 100%)"
    )
    report_lines.append(
        f"- **CI{CONFIDENCE_PERCENT}%**: {CONFIDENCE_PERCENT}% Confidence Interval - range where the true population mean is likely to fall"
    )
    report_lines.append(
        f"- **PI{CONFIDENCE_PERCENT}%**: {CONFIDENCE_PERCENT}% Prediction Interval - range where a single future observation is likely to fall"
    )
    report_lines.append("")

    # Close collapsible section
    report_lines.append("</details>")

    return "\n".join(report_lines)


def _get_significance_indicator(
    baseline_run_count: int,
    new_run_count: int,
    baseline_ci_lower: float,
    baseline_ci_upper: float,
    new_ci_lower: float,
    new_ci_upper: float,
    change_percent: float,
    metric: str,
) -> str:
    """Classify CI overlap, accounting for whether lower values are better."""
    if baseline_run_count <= 1 or new_run_count <= 1:
        return "❔"

    lower_is_better = "latency" in metric

    if new_ci_lower > baseline_ci_upper:
        return "❌" if lower_is_better else "✅"
    if new_ci_upper < baseline_ci_lower:
        return "✅" if lower_is_better else "❌"
    return "➖"


def calculate_percent_change_with_ci(
    baseline_value: float,
    baseline_stdev: float,
    new_value: float,
    new_stdev: float,
    baseline_run_count: int,
    new_run_count: int,
) -> Tuple[float, Optional[float]]:
    """Return percentage change and an optional propagated CI margin."""
    if baseline_value == 0:
        return (0.0, None)

    change_percent = ((new_value - baseline_value) / baseline_value) * 100

    if (
        baseline_run_count > 1
        and new_run_count > 1
        and baseline_stdev > 0
        and new_stdev > 0
    ):
        baseline_se = baseline_stdev / math.sqrt(baseline_run_count)
        new_se = new_stdev / math.sqrt(new_run_count)

        baseline = ufloat(baseline_value, baseline_se)
        new = ufloat(new_value, new_se)

        change_with_uncertainty = (new - baseline) / baseline * 100

        # Conservative: use the smaller df for a wider (safer) CI
        df = min(baseline_run_count - 1, new_run_count - 1)
        alpha = 1 - CONFIDENCE_LEVEL
        t_crit = stats.t.ppf(1 - alpha / 2, df)

        return (
            change_with_uncertainty.nominal_value,
            t_crit * change_with_uncertainty.std_dev,
        )

    return (change_percent, None)


def _format_percent_change(
    baseline_value: float,
    baseline_stdev: float,
    new_value: float,
    new_stdev: float,
    baseline_run_count: int,
    new_run_count: int,
) -> str:
    """Format percentage change with uncertainty as a display string."""
    if baseline_value == 0:
        return "N/A"

    change, ci_margin = calculate_percent_change_with_ci(
        baseline_value,
        baseline_stdev,
        new_value,
        new_stdev,
        baseline_run_count,
        new_run_count,
    )

    if ci_margin is not None:
        scaled = ufloat(change, ci_margin)
        return f"{scaled:+.1uP}%"

    return f"{change:+.1f}%"


# Unit suffixes for 1000x scaling (largest first)
_UNIT_SUFFIXES = [
    (1e12, "T"),
    (1e9, "B"),
    (1e6, "M"),
    (1e3, "K"),
]


def _format_with_sig_figs(value: float, uncertainty: float = 0.0) -> str:
    """Format a value using uncertainty-aware precision and unit suffixes."""
    if value == 0:
        return "0"

    abs_value = abs(value)

    # Find appropriate unit suffix
    suffix = ""
    divisor = 1
    scaled = value
    for div, unit in _UNIT_SUFFIXES:
        if abs_value >= div:
            divisor = div
            suffix = unit
            scaled = value / divisor
            break

    # Determine decimal places
    if uncertainty > 0:
        scaled_uncertainty = uncertainty / divisor
        decimals = max(0, min(4, -math.floor(math.log10(scaled_uncertainty))))
    else:
        # 3 significant figures
        decimals = max(0, 2 - math.floor(math.log10(abs(scaled))))

    return f"{scaled:.{decimals}f}{suffix}"


def _format_stats_only(
    run_count: int,
    stdev: float,
    cv: float = 0.0,
    ci_percent: float = 0.0,
    pi_percent: float = 0.0,
) -> str:
    """Format statistical information only (no value)."""
    if run_count <= 1:
        return "n=1"

    formatted_stdev = _format_with_sig_figs(stdev)

    # Format percentages with 1 decimal place
    stats_parts = [f"n={run_count}", f"σ={formatted_stdev}", f"CV={cv:.1f}%"]
    if ci_percent > 0.01:
        stats_parts.append(f"CI{CONFIDENCE_PERCENT}%=±{ci_percent:.1f}%")
    if pi_percent > 0.01:
        stats_parts.append(f"PI{CONFIDENCE_PERCENT}%=±{pi_percent:.1f}%")

    return ", ".join(stats_parts)


def generate_comparison_graphs(
    config_groups: List[Dict],
    baseline_version: str,
    new_version: str,
    output_dir: str = ".",
    raw_baseline_data: Optional[List[Dict]] = None,
    raw_new_data: Optional[List[Dict]] = None,
    metrics_filter: str = "all",
) -> List[str]:
    """
    Generate consolidated comparison graphs for benchmark results.

    Creates a single comprehensive graph to reduce duplication, showing all metrics
    in one consolidated view with proper version - config legend format.
    Also generates line graphs showing variance across runs.

    Returns list of generated file paths.
    """
    if not GRAPHING_AVAILABLE:
        print(
            "WARNING: Graphing dependencies (matplotlib, numpy, scipy) not available. Skipping graph generation."
        )
        return []

    if not config_groups:
        return []

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    generated_files = []

    # Collect all data for graphing
    all_rows = []
    for group in config_groups:
        all_rows.extend(group["table_rows"])

    if not all_rows:
        return []

    # Generate single consolidated metrics comparison graph
    comprehensive_graph_path = generate_consolidated_metrics_graph(
        all_rows, baseline_version, new_version, output_path
    )
    if comprehensive_graph_path:
        generated_files.append(comprehensive_graph_path)

    # Generate variance line graphs if raw data is available
    if raw_baseline_data and raw_new_data:
        variance_graph_paths = generate_variance_line_graphs(
            raw_baseline_data,
            raw_new_data,
            baseline_version,
            new_version,
            output_path,
            metrics_filter,
        )
        generated_files.extend(variance_graph_paths)

    return generated_files


def generate_variance_line_graphs(
    raw_baseline_data: List[Dict],
    raw_new_data: List[Dict],
    baseline_version: str,
    new_version: str,
    output_path: Path,
    metrics_filter: str = "all",
) -> List[str]:
    """
    Generate line graphs showing variance across runs for each command and configuration.

    Shows individual run values with error bars for standard deviation to visualize
    consistency and variance in benchmark results.
    """
    generated_files = []

    try:
        # Group raw data by configuration and command
        baseline_grouped = _group_raw_data_for_variance(raw_baseline_data)
        new_grouped = _group_raw_data_for_variance(raw_new_data)

        # Get all unique config-command combinations
        all_keys = set(baseline_grouped.keys()) | set(new_grouped.keys())

        for config_key in sorted(all_keys):
            baseline_runs = baseline_grouped.get(config_key, [])
            new_runs = new_grouped.get(config_key, [])

            if not baseline_runs and not new_runs:
                continue

            # Generate variance graph for this config-command combination
            graph_path = _generate_single_variance_graph(
                config_key,
                baseline_runs,
                new_runs,
                baseline_version,
                new_version,
                output_path,
                metrics_filter,
            )
            if graph_path:
                generated_files.append(graph_path)

    except Exception:
        pass  # Silently handle errors in graph generation

    return generated_files


def _group_raw_data_for_variance(data: List[Dict]) -> Dict[str, List[Dict]]:
    """Group raw data by configuration and command for variance analysis."""
    grouped = {}

    for item in data:
        # Create a key combining config and command info
        command = item.get("command", "UNKNOWN")
        pipeline = item.get("pipeline", "Unknown")
        io_threads = item.get("io_threads", "Unknown")

        # Include key config parameters for grouping
        config_parts = [
            f"cmd_{command}",
            f"p{pipeline}",
            f"io{io_threads}",
        ]

        # Add other significant config parameters
        for key in ["data_size", "clients", "duration", "tls", "cluster_mode"]:
            value = item.get(key)
            if value is not None:
                config_parts.append(f"{key}_{value}")

        config_key = "_".join(str(part) for part in config_parts)

        if config_key not in grouped:
            grouped[config_key] = []
        grouped[config_key].append(item)

    return grouped


def _generate_single_variance_graph(
    config_key: str,
    baseline_runs: List[Dict],
    new_runs: List[Dict],
    baseline_version: str,
    new_version: str,
    output_path: Path,
    metrics_filter: str = "all",
) -> Optional[str]:
    """Generate a single variance line graph for a specific config-command combination."""
    try:
        if not baseline_runs and not new_runs:
            return None

        # Select metrics based on filter
        all_metrics = [
            "rps",
            "avg_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
        ]

        if metrics_filter == "rps":
            metrics = ["rps"]
        elif metrics_filter == "latency":
            metrics = [
                "avg_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
            ]
        else:  # "all" or any other value
            metrics = all_metrics

        # Create subplots for each metric
        _, axes = plt.subplots(len(metrics), 1, figsize=(12, 4 * len(metrics)))
        if len(metrics) == 1:
            axes = [axes]

        for idx, metric in enumerate(metrics):
            ax = axes[idx]

            # Extract values for baseline and new versions
            baseline_values = []
            new_values = []

            for run in baseline_runs:
                baseline_values.append(run.get(metric, 0.0))

            for run in new_runs:
                new_values.append(run.get(metric, 0.0))

            # Plot baseline runs
            if baseline_values:
                baseline_x = list(range(1, len(baseline_values) + 1))
                ax.plot(
                    baseline_x,
                    baseline_values,
                    "o-",
                    label=f"{baseline_version} (n={len(baseline_values)})",
                    color="steelblue",
                    alpha=0.8,
                    linewidth=2,
                    markersize=6,
                )

                # Add mean line and prediction interval
                if len(baseline_values) > 1:
                    mean_val = statistics.mean(baseline_values)
                    pi_lower, pi_upper = calculate_prediction_interval(baseline_values)
                    ax.axhline(y=mean_val, color="steelblue", linestyle="--", alpha=0.6)
                    ax.fill_between(
                        baseline_x,
                        [pi_lower] * len(baseline_x),
                        [pi_upper] * len(baseline_x),
                        color="steelblue",
                        alpha=0.2,
                        label=f"{baseline_version} {CONFIDENCE_PERCENT}% PI",
                    )

            # Plot new version runs
            if new_values:
                new_x = list(range(1, len(new_values) + 1))
                ax.plot(
                    new_x,
                    new_values,
                    "s-",
                    label=f"{new_version} (n={len(new_values)})",
                    color="mediumseagreen",
                    alpha=0.8,
                    linewidth=2,
                    markersize=6,
                )

                # Add mean line and prediction interval
                if len(new_values) > 1:
                    mean_val = statistics.mean(new_values)
                    pi_lower, pi_upper = calculate_prediction_interval(new_values)
                    ax.axhline(
                        y=mean_val, color="mediumseagreen", linestyle="--", alpha=0.6
                    )
                    ax.fill_between(
                        new_x,
                        [pi_lower] * len(new_x),
                        [pi_upper] * len(new_x),
                        color="mediumseagreen",
                        alpha=0.2,
                        label=f"{new_version} {CONFIDENCE_PERCENT}% PI",
                    )

            # Formatting
            ax.set_xlabel("Run Number")

            if metric == "rps":
                ax.set_ylabel("Requests per Second")
                # Format y-axis for RPS
                ax.yaxis.set_major_formatter(
                    FuncFormatter(
                        lambda x, p: f"{x/1e6:.2f}M" if x >= 1e6 else f"{x/1e3:.0f}K"
                    )
                )
            else:
                ax.set_ylabel(f'{metric.replace("_", " ").title()} (ms)')

            ax.set_title(f'{metric.replace("_", " ").title()} Variance: {config_key}')
            ax.legend()
            ax.grid(True, alpha=0.3)

            # Set integer ticks for x-axis
            max_runs = max(len(baseline_values), len(new_values))
            if max_runs > 0:
                ax.set_xticks(range(1, max_runs + 1))

        plt.tight_layout()

        # Create safe filename
        safe_config_key = config_key.replace("/", "_").replace(" ", "_")
        graph_path = output_path / f"variance_line_graph_{safe_config_key}.png"
        plt.savefig(graph_path, dpi=300, bbox_inches="tight")
        plt.close()

        return str(graph_path)

    except Exception:
        return None


def generate_consolidated_metrics_graph(
    rows: List[Dict],
    baseline_version: str,
    new_version: str,
    output_path: Path,
) -> Optional[str]:
    """Generate a single consolidated comparison graph for all metrics with proper legend format."""
    try:
        if not rows:
            return None

        # Group data by metric type
        metrics_data = {}
        for row in rows:
            metric = row["metric"]
            if metric not in metrics_data:
                metrics_data[metric] = []
            metrics_data[metric].append(row)

        # Create subplots for each metric
        num_metrics = len(metrics_data)
        _, axes = plt.subplots(num_metrics, 1, figsize=(14, 6 * num_metrics))
        if num_metrics == 1:
            axes = [axes]

        for idx, (metric, metric_rows) in enumerate(sorted(metrics_data.items())):
            ax = axes[idx]

            # Create labels and data for this metric
            labels = []
            baseline_values = []
            new_values = []

            # Get pipeline and io_threads for legend format
            pipeline = metric_rows[0]["pipeline"] if metric_rows else "Unknown"
            io_threads = metric_rows[0]["io_threads"] if metric_rows else "Unknown"

            for row in metric_rows:
                label = f"{row['command']}\nP{row['pipeline']}/T{row['io_threads']}"
                labels.append(label)

                # Convert RPS to millions, keep other metrics as-is
                if metric == "rps":
                    baseline_values.append(row["baseline_value"] / 1_000_000)
                    new_values.append(row["new_value"] / 1_000_000)
                else:
                    baseline_values.append(row["baseline_value"])
                    new_values.append(row["new_value"])

            # Create the bar chart with proper legend format: "commit-P{pipeline}/IO{io_threads}"
            x = np.arange(len(labels))
            width = 0.35

            bars1 = ax.bar(
                x - width / 2,
                baseline_values,
                width,
                label=f"{baseline_version}-P{pipeline}/IO{io_threads}",
                alpha=0.8,
                color="steelblue",
            )
            bars2 = ax.bar(
                x + width / 2,
                new_values,
                width,
                label=f"{new_version}-P{pipeline}/IO{io_threads}",
                alpha=0.8,
                color="mediumseagreen",
            )

            # Add value labels on bars
            for bar in bars1:
                height = bar.get_height()
                if metric == "rps":
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.3f}M",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )
                else:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.3f}",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )

            for bar in bars2:
                height = bar.get_height()
                if metric == "rps":
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.3f}M",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )
                else:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.3f}",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )

            # Set labels and formatting
            ax.set_xlabel("Command/Configuration")

            if metric == "rps":
                ax.set_ylabel("Requests per Second (Millions)")
                ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.2f}M"))
            else:
                ax.set_ylabel(f'{metric.replace("_", " ").title()} (ms)')

            ax.set_title(
                f'{metric.replace("_", " ").title()} Comparison: {baseline_version} vs {new_version}'
            )
            ax.set_xticks(x)
            ax.set_xticklabels(labels, rotation=45, ha="right")
            ax.legend()
            ax.grid(True, alpha=0.3)

        plt.tight_layout()

        # Save the graph
        graph_path = output_path / "benchmark_comparison_consolidated.png"
        plt.savefig(graph_path, dpi=300, bbox_inches="tight")
        plt.close()

        return str(graph_path)

    except Exception:
        return None


def main():
    """
    Main entry point for the benchmark comparison tool.

    Automatically averages multiple runs and generates a comprehensive comparison report.
    """
    if len(sys.argv) < 3:
        print(
            "Usage: compare_benchmark_results.py --baseline FILE --new FILE [OPTIONS]",
            file=sys.stderr,
        )
        print("\nOptions:")
        print("  --baseline  Path to baseline benchmark results JSON file")
        print("  --new       Path to new benchmark results JSON file")
        print(
            "  --output    Optional output file path (prints to stdout if not specified)"
        )
        print(
            "  --metrics   Filter metrics to display: 'all' (default), 'rps', or 'latency'"
        )
        print("  --graphs    Generate comparison graphs")
        print("  --graph-dir Directory to save graphs (default: current directory)")
        sys.exit(1)

    baseline_file = None
    new_file = None
    out_file = None
    metrics_filter = "all"
    generate_graphs = False
    graph_dir = "."

    # Parse arguments
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--baseline":
            if i + 1 < len(sys.argv):
                baseline_file = sys.argv[i + 1]
                i += 1  # Skip the file argument
            else:
                print("ERROR: --baseline requires a file path", file=sys.stderr)
                sys.exit(1)
        elif arg == "--new":
            if i + 1 < len(sys.argv):
                new_file = sys.argv[i + 1]
                i += 1  # Skip the file argument
            else:
                print("ERROR: --new requires a file path", file=sys.stderr)
                sys.exit(1)
        elif arg == "--output":
            if i + 1 < len(sys.argv):
                out_file = sys.argv[i + 1]
                i += 1  # Skip the file argument
            else:
                print("ERROR: --output requires a file path", file=sys.stderr)
                sys.exit(1)
        elif arg == "--metrics":
            if i + 1 < len(sys.argv):
                metrics_filter = sys.argv[i + 1]
                if metrics_filter not in ["all", "rps", "latency"]:
                    print(
                        f"ERROR: Invalid metrics filter '{metrics_filter}'. "
                        f"Must be one of: all, rps, latency",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                i += 1  # Skip the metrics value argument
            else:
                print(
                    "ERROR: --metrics requires a value (all, rps, or latency)",
                    file=sys.stderr,
                )
                sys.exit(1)
        elif arg == "--graphs":
            generate_graphs = True
        elif arg == "--graph-dir":
            if i + 1 < len(sys.argv):
                graph_dir = sys.argv[i + 1]
                i += 1  # Skip the directory argument
            else:
                print("ERROR: --graph-dir requires a directory path", file=sys.stderr)
                sys.exit(1)
        else:
            print(f"ERROR: Unknown argument '{arg}'", file=sys.stderr)
            sys.exit(1)
        i += 1

    # Validate required arguments
    if not baseline_file:
        print("ERROR: --baseline is required", file=sys.stderr)
        sys.exit(1)
    if not new_file:
        print("ERROR: --new is required", file=sys.stderr)
        sys.exit(1)

    # Load benchmark data
    baseline_data = load_benchmark_data(baseline_file)
    new_data = load_benchmark_data(new_file)

    # Collect failed / non-comparable scenarios from the RAW rows (before
    # averaging) so their recorded error text is preserved for the report. The
    # metrics_filter selects which surviving-side metric to surface.
    failed_scenarios = collect_failed_scenarios(baseline_data, new_data, metrics_filter)

    # Union-based key discovery over both raw datasets, threaded into the
    # run-averaging path so both sides group runs over an identical key space.
    shared_config_keys = discover_config_keys(baseline_data + new_data)

    # Always apply dynamic averaging for consistent comparisons
    baseline_data = average_multiple_runs(baseline_data, shared_config_keys)
    new_data = average_multiple_runs(new_data, shared_config_keys)

    # Generate comparison data
    config_groups, baseline_version, new_version, baseline_repo, new_repo = (
        create_comparison_table_data(baseline_data, new_data, metrics_filter)
    )

    # Generate graphs if requested
    if generate_graphs:
        # Load raw data again for variance analysis
        raw_baseline_data = load_benchmark_data(baseline_file)
        raw_new_data = load_benchmark_data(new_file)

        generated_files = generate_comparison_graphs(
            config_groups,
            baseline_version,
            new_version,
            graph_dir,
            raw_baseline_data or [],
            raw_new_data or [],
            metrics_filter,
        )
        if generated_files:
            print(f"Generated {len(generated_files)} graph(s):")
            for file_path in generated_files:
                print(f"  - {file_path}")

    # Format the comparison report
    # Extract core commit if module_commit is used as the version identifier
    core_commit_baseline = None
    core_commit_new = None
    if baseline_data and baseline_data[0].get("module_commit"):
        core_commit_baseline = baseline_data[0].get("commit")
    if new_data and new_data[0].get("module_commit"):
        core_commit_new = new_data[0].get("commit")

    comparison_table = format_comparison_report(
        config_groups,
        baseline_version,
        new_version,
        baseline_repo,
        new_repo,
        core_commit_baseline=core_commit_baseline,
        core_commit_new=core_commit_new,
        failed_scenarios=failed_scenarios,
    )

    # Create final report with metadata
    if metrics_filter == "rps":
        title_prefix = "RPS "
    elif metrics_filter == "latency":
        title_prefix = "Latency "
    else:
        title_prefix = ""

    # Format version links for title
    baseline_title = format_version_link(baseline_version, baseline_repo)
    new_title = format_version_link(new_version, new_repo)

    final_report = (
        f"# {title_prefix}Benchmark Comparison: {baseline_title} vs {new_title}\n\n"
        f"{comparison_table}\n"
    )

    # Output the report
    if out_file:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(final_report)
        print(f"Comparison report written to: {out_file}")
    else:
        print(final_report)


if __name__ == "__main__":
    main()
