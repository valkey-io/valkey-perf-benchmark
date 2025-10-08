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
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


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


def discover_config_keys(data: List[Dict[str, Any]]) -> List[str]:
    """
    Dynamically discover configuration keys from benchmark data.

    Excludes performance metrics and metadata fields, keeping only
    configuration parameters that define test scenarios.
    """
    config_keys = set()

    # Fields that are metrics or metadata, not configuration
    excluded_fields = {
        "timestamp",
        "commit",
        "run_count",
        # Performance metrics
        "rps",
        "avg_latency_ms",
        "min_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "max_latency_ms",
        "latency_avg_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        # Standard deviation fields
        "rps_stdev",
        "avg_latency_ms_stdev",
        "p50_latency_ms_stdev",
        "p95_latency_ms_stdev",
        "p99_latency_ms_stdev",
    }

    for item in data:
        for key, value in item.items():
            if key not in excluded_fields:
                # Only include keys with hashable values for grouping
                if isinstance(value, (str, int, float, bool, type(None))):
                    config_keys.add(key)

    return sorted(config_keys)


def create_config_signature(item: Dict[str, Any], config_keys: List[str]) -> Tuple:
    """Create a configuration signature tuple for grouping identical configurations."""
    return tuple(item.get(key) for key in config_keys)


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
    """
    Calculate summary statistics for a group of benchmark results.

    Handles both old and new field naming conventions for latency metrics.
    """
    if not data_items:
        return {
            "rps": 0.0,
            "latency_avg_ms": 0.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_p99_ms": 0.0,
        }

    # Extract values with fallback for different field names
    rps_values = [item.get("rps", 0.0) for item in data_items]

    avg_latency_values = [
        item.get("avg_latency_ms", item.get("latency_avg_ms", 0.0))
        for item in data_items
    ]

    p50_latency_values = [
        item.get("p50_latency_ms", item.get("latency_p50_ms", 0.0))
        for item in data_items
    ]

    p95_latency_values = [
        item.get("p95_latency_ms", item.get("latency_p95_ms", 0.0))
        for item in data_items
    ]

    p99_latency_values = [
        item.get("p99_latency_ms", item.get("latency_p99_ms", 0.0))
        for item in data_items
    ]

    return {
        "rps": calculate_mean(rps_values),
        "latency_avg_ms": calculate_mean(avg_latency_values),
        "latency_p50_ms": calculate_mean(p50_latency_values),
        "latency_p95_ms": calculate_mean(p95_latency_values),
        "latency_p99_ms": calculate_mean(p99_latency_values),
    }


def average_multiple_runs(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Automatically average multiple benchmark runs with identical configurations.

    Groups runs by configuration parameters and calculates means and standard deviations
    for performance metrics. Always applied to ensure consistent comparisons.
    """
    if not data:
        return []

    # Get configuration keys (excluding metrics and metadata)
    config_keys = [
        key
        for key in discover_config_keys(data)
        if key not in ["timestamp", "run_count"] and not key.endswith("_stdev")
    ]

    # Group runs by identical configurations
    grouped_runs = {}
    for item in data:
        config_signature = create_config_signature(item, config_keys)
        if config_signature not in grouped_runs:
            grouped_runs[config_signature] = []
        grouped_runs[config_signature].append(item)

    # Process each configuration group
    averaged_results = []
    for config_signature, runs in grouped_runs.items():
        # Create base configuration item
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
            # Multiple runs: calculate averages and standard deviations
            metric_values = {
                "rps": [run.get("rps", 0.0) for run in runs],
                "avg_latency_ms": [
                    run.get("avg_latency_ms", run.get("latency_avg_ms", 0.0))
                    for run in runs
                ],
                "p50_latency_ms": [
                    run.get("p50_latency_ms", run.get("latency_p50_ms", 0.0))
                    for run in runs
                ],
                "p95_latency_ms": [
                    run.get("p95_latency_ms", run.get("latency_p95_ms", 0.0))
                    for run in runs
                ],
                "p99_latency_ms": [
                    run.get("p99_latency_ms", run.get("latency_p99_ms", 0.0))
                    for run in runs
                ],
            }

            # Calculate means and standard deviations
            for metric, values in metric_values.items():
                averaged_item[metric] = calculate_mean(values)
                averaged_item[f"{metric}_stdev"] = calculate_stdev(values)

            # Preserve the most recent timestamp and commit
            timestamps = [run.get("timestamp") for run in runs if run.get("timestamp")]
            if timestamps:
                averaged_item["timestamp"] = max(timestamps)

            # Preserve commit information from any run (they should all be the same)
            commits = [run.get("commit") for run in runs if run.get("commit")]
            if commits:
                averaged_item["commit"] = commits[
                    0
                ]  # Use first commit (should be same for all runs)

            averaged_results.append(averaged_item)

    return averaged_results


def calculate_percentage_change(new_value: float, old_value: float) -> float:
    """Calculate percentage change between two values."""
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100.0


def create_config_sort_key(config_tuple: Tuple) -> Tuple[str, ...]:
    """
    Create a sorting key for configuration tuples that handles None values and mixed types.

    Converts all values to strings for consistent comparison, with None values sorting first.
    """

    def normalize_value(value):
        return "" if value is None else str(value)

    return tuple(normalize_value(item) for item in config_tuple)


def extract_version_identifier(data: List[Dict[str, Any]]) -> str:
    """
    Extract a version identifier from benchmark data.

    Prioritizes commit hash, falls back to a short timestamp format, or returns "Unknown".
    """
    if not data:
        return "Unknown"

    first_item = data[0]

    # Try commit hash first
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
            from datetime import datetime

            if "T" in timestamp:
                date_part = timestamp.split("T")[0]  # Get YYYY-MM-DD part
                return date_part
            else:
                return timestamp[:10]  # First 10 chars should be YYYY-MM-DD
        except:
            return f"ts-{timestamp[:10]}"

    return "Unknown"


def group_by_static_configuration(
    data: List[Dict[str, Any]],
) -> Dict[Tuple, Dict[str, Any]]:
    """
    Group benchmark results by static configuration parameters.

    Excludes table-level parameters (command, pipeline, io_threads) that vary
    within the same test configuration.
    """
    # Parameters that appear in the comparison table, not in config sections
    table_parameters = {"command", "pipeline", "io_threads"}

    # Get configuration keys excluding table parameters
    config_keys = [
        key for key in discover_config_keys(data) if key not in table_parameters
    ]

    grouped_configs = {}
    for item in data:
        config_signature = create_config_signature(item, config_keys)
        if config_signature not in grouped_configs:
            grouped_configs[config_signature] = {
                "items": [],
                "config_keys": config_keys,
            }
        grouped_configs[config_signature]["items"].append(item)

    return grouped_configs


def create_comparison_table_data(
    baseline_data: List[Dict[str, Any]],
    new_data: List[Dict[str, Any]],
    metrics_filter: str = "all",
) -> Tuple[List[Dict], str, str]:
    """
    Create structured comparison data for benchmark results.

    Returns configuration groups with their comparison table rows,
    along with version identifiers for both datasets.
    """
    baseline_version = extract_version_identifier(baseline_data)
    new_version = extract_version_identifier(new_data)

    # Group data by static configuration
    baseline_configs = group_by_static_configuration(baseline_data)
    new_configs = group_by_static_configuration(new_data)

    # Define available metrics with their display names
    available_metrics = [
        ("rps", "rps"),
        ("latency_avg_ms", "avg_latency"),
        ("latency_p50_ms", "p50_latency"),
        ("latency_p95_ms", "p95_latency"),
        ("latency_p99_ms", "p99_latency"),
    ]

    # Select metrics based on filter
    if metrics_filter == "rps":
        selected_metrics = [("rps", "rps")]
    elif metrics_filter == "latency":
        selected_metrics = [
            ("latency_avg_ms", "avg_latency"),
            ("latency_p50_ms", "p50_latency"),
            ("latency_p95_ms", "p95_latency"),
            ("latency_p99_ms", "p99_latency"),
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

    return configuration_groups, baseline_version, new_version


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
    """Extract run count and standard deviation statistics from benchmark items."""
    if not items:
        return {"run_count": 0}

    # Use pre-calculated run_count if available, otherwise count items
    run_count = items[0].get("run_count", len(items))

    stats = {"run_count": run_count}

    # Extract standard deviations if available
    for stdev_key in [
        "rps_stdev",
        "avg_latency_ms_stdev",
        "p50_latency_ms_stdev",
        "p95_latency_ms_stdev",
        "p99_latency_ms_stdev",
    ]:
        if stdev_key in items[0]:
            stats[stdev_key] = items[0][stdev_key]
        elif run_count > 1:
            # Calculate from raw data if not pre-calculated
            metric_key = stdev_key.replace("_stdev", "")
            values = [item.get(metric_key, 0.0) for item in items]
            stats[stdev_key] = calculate_stdev(values)
        else:
            stats[stdev_key] = 0.0

    return stats


def format_comparison_report(
    config_groups: List[Dict], baseline_version: str, new_version: str
) -> str:
    """
    Format the comparison data as a markdown report with configuration sections.

    Each configuration gets its own section with a comparison table showing
    performance differences across commands and parameters.
    """
    if not config_groups:
        return "No data to compare."

    report_lines = []

    for group in config_groups:
        config_dict = group["config_dict"]
        config_keys = group["config_keys"]
        table_rows = group["table_rows"]

        if not table_rows:
            continue

        # Configuration section header
        report_lines.append("**Configuration:**")
        for key in sorted(config_keys):
            value = config_dict.get(key)
            if value is not None:
                report_lines.append(f"- {key}: {value}")
        report_lines.append("")

        # Comparison table for this configuration
        report_lines.extend(
            [
                f"| Command | Metric | Pipeline | io_threads | {baseline_version} | {new_version} | Diff | % Change |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in table_rows:
            # Format metric values with statistical information
            baseline_display = _format_metric_value(
                row["baseline_value"],
                row.get("baseline_run_count", 0),
                row.get("baseline_stdev", 0.0),
            )

            new_display = _format_metric_value(
                row["new_value"], row.get("new_run_count", 0), row.get("new_stdev", 0.0)
            )

            # Create table row
            report_lines.append(
                f"| {row['command']} | {row['metric']} | {row['pipeline']} | {row['io_threads']} | "
                f"{baseline_display} | {new_display} | "
                f"{row['diff']:.2f} | {row['change']:+.2f}% |"
            )

        report_lines.append("")

    return "\n".join(report_lines)


def _format_metric_value(value: float, run_count: int, stdev: float) -> str:
    """Format a metric value with optional run count and standard deviation."""
    formatted_value = f"{value:.2f}"

    # Add statistical information for multiple runs
    if run_count > 1:
        formatted_value += f" (n={run_count}, σ={stdev:.2f})"

    return formatted_value


def generate_comparison_graphs(
    config_groups: List[Dict],
    baseline_version: str,
    new_version: str,
    output_dir: str = ".",
    raw_baseline_data: List[Dict] = None,
    raw_new_data: List[Dict] = None,
    metrics_filter: str = "all",
) -> List[str]:
    """
    Generate consolidated comparison graphs for benchmark results.

    Creates a single comprehensive graph to reduce duplication, showing all metrics
    in one consolidated view with proper version - config legend format.
    Also generates line graphs showing variance across runs.

    Returns list of generated file paths.
    """
    if not config_groups:
        print("No data available for graph generation")
        return []

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    generated_files = []

    # Collect all data for graphing
    all_rows = []
    config_info = []
    for group in config_groups:
        all_rows.extend(group["table_rows"])
        # Extract config info for legend
        config_dict = group["config_dict"]
        config_str = ", ".join(
            [f"{k}={v}" for k, v in config_dict.items() if v is not None]
        )
        config_info.append(config_str)

    if not all_rows:
        print("No table rows available for graph generation")
        return []

    # Get unique config string for legends
    unique_configs = list(set(config_info))
    config_label = unique_configs[0] if len(unique_configs) == 1 else "mixed_configs"

    # Generate single consolidated metrics comparison graph
    comprehensive_graph_path = generate_consolidated_metrics_graph(
        all_rows, baseline_version, new_version, output_path, config_label
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

    except Exception as e:
        print(f"Error generating variance line graphs: {e}")

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
        fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 4 * len(metrics)))
        if len(metrics) == 1:
            axes = [axes]

        for idx, metric in enumerate(metrics):
            ax = axes[idx]

            # Extract values for baseline and new versions
            baseline_values = []
            new_values = []

            for run in baseline_runs:
                value = run.get(
                    metric, run.get(f'latency_{metric.split("_")[-1]}', 0.0)
                )
                baseline_values.append(value)

            for run in new_runs:
                value = run.get(
                    metric, run.get(f'latency_{metric.split("_")[-1]}', 0.0)
                )
                new_values.append(value)

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

                # Add mean line and error bars
                if len(baseline_values) > 1:
                    mean_val = statistics.mean(baseline_values)
                    std_val = statistics.stdev(baseline_values)
                    ax.axhline(y=mean_val, color="steelblue", linestyle="--", alpha=0.6)
                    ax.fill_between(
                        baseline_x,
                        [mean_val - std_val] * len(baseline_x),
                        [mean_val + std_val] * len(baseline_x),
                        color="steelblue",
                        alpha=0.2,
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

                # Add mean line and error bars
                if len(new_values) > 1:
                    mean_val = statistics.mean(new_values)
                    std_val = statistics.stdev(new_values)
                    ax.axhline(
                        y=mean_val, color="mediumseagreen", linestyle="--", alpha=0.6
                    )
                    ax.fill_between(
                        new_x,
                        [mean_val - std_val] * len(new_x),
                        [mean_val + std_val] * len(new_x),
                        color="mediumseagreen",
                        alpha=0.2,
                    )

            # Formatting
            ax.set_xlabel("Run Number")

            if metric == "rps":
                ax.set_ylabel("Requests per Second")
                # Format y-axis for RPS
                ax.yaxis.set_major_formatter(
                    plt.FuncFormatter(
                        lambda x, p: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1e3:.0f}K"
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

        print(f"Variance line graph saved to: {graph_path}")
        return str(graph_path)

    except Exception as e:
        print(f"Error generating variance graph for {config_key}: {e}")
        return None


def generate_consolidated_metrics_graph(
    rows: List[Dict],
    baseline_version: str,
    new_version: str,
    output_path: Path,
    config_label: str,
) -> Optional[str]:
    """Generate a single consolidated comparison graph for all metrics with proper legend format."""
    try:
        if not rows:
            print("No data available for consolidated metrics graph")
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
        fig, axes = plt.subplots(num_metrics, 1, figsize=(14, 6 * num_metrics))
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
                        f"{height:.2f}M",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )
                else:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.2f}",
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
                        f"{height:.2f}M",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )
                else:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height,
                        f"{height:.2f}",
                        ha="center",
                        va="bottom",
                        fontsize=9,
                    )

            # Set labels and formatting
            ax.set_xlabel("Command/Configuration")

            if metric == "rps":
                ax.set_ylabel("Requests per Second (Millions)")
                ax.yaxis.set_major_formatter(
                    plt.FuncFormatter(lambda x, p: f"{x:.1f}M")
                )
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

        print(f"Consolidated benchmark comparison graph saved to: {graph_path}")
        return str(graph_path)

    except Exception as e:
        print(f"Error generating consolidated metrics graph: {e}")
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
    print("Loading benchmark data...")
    baseline_data = load_benchmark_data(baseline_file)
    new_data = load_benchmark_data(new_file)

    # Track original data sizes for summary
    original_baseline_count = len(baseline_data)
    original_new_count = len(new_data)

    # Always apply dynamic averaging for consistent comparisons
    print("Processing and averaging multiple runs...")
    baseline_data = average_multiple_runs(baseline_data)
    new_data = average_multiple_runs(new_data)

    # Calculate averaging statistics
    baseline_avg_runs = (
        original_baseline_count / len(baseline_data) if baseline_data else 0
    )
    new_avg_runs = original_new_count / len(new_data) if new_data else 0

    print(
        f"Baseline: {original_baseline_count} runs → {len(baseline_data)} configurations "
        f"(avg {baseline_avg_runs:.1f} runs per config)"
    )
    print(
        f"New: {original_new_count} runs → {len(new_data)} configurations "
        f"(avg {new_avg_runs:.1f} runs per config)"
    )

    # Generate comparison data
    print("Generating comparison report...")
    config_groups, baseline_version, new_version = create_comparison_table_data(
        baseline_data, new_data, metrics_filter
    )

    # Generate graphs if requested
    if generate_graphs:
        print("Generating consolidated comparison graphs...")

        # Load raw data again for variance analysis
        raw_baseline_data = load_benchmark_data(baseline_file)
        raw_new_data = load_benchmark_data(new_file)

        generated_files = generate_comparison_graphs(
            config_groups,
            baseline_version,
            new_version,
            graph_dir,
            raw_baseline_data,
            raw_new_data,
            metrics_filter,
        )
        if generated_files:
            print(f"Generated {len(generated_files)} graph(s):")
            for file_path in generated_files:
                print(f"  - {file_path}")
        else:
            print("No graphs were generated")

    # Format the comparison report
    comparison_table = format_comparison_report(
        config_groups, baseline_version, new_version
    )

    # Create final report with metadata
    metrics_info = f" - {metrics_filter} metrics" if metrics_filter != "all" else ""

    run_summary = (
        f"\n\n**Run Summary:**\n"
        f"- {baseline_version}: {original_baseline_count} total runs, "
        f"{len(baseline_data)} configurations (avg {baseline_avg_runs:.1f} runs per config)\n"
        f"- {new_version}: {original_new_count} total runs, "
        f"{len(new_data)} configurations (avg {new_avg_runs:.1f} runs per config)\n\n"
        f"*Note: Values with (n=X, σ=Y) indicate averages from X runs with standard deviation Y*"
    )

    final_report = (
        f"# Benchmark Comparison: {baseline_version} vs {new_version} (averaged){metrics_info}"
        f"{run_summary}\n\n{comparison_table}\n"
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
