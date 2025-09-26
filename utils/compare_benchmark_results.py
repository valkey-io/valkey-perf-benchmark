#!/usr/bin/env python3

import json
import statistics
import sys


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _mean(values):
    values = [v for v in values if v is not None]
    return statistics.mean(values) if values else 0.0


def get_config_keys(data):
    """Dynamically discover all configuration keys from the data."""
    config_keys = set()
    exclude_keys = {
        "timestamp",
        "commit",
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
    }

    for item in data:
        for key in item.keys():
            if key not in exclude_keys:
                # Only include keys with hashable values
                value = item[key]
                if isinstance(value, (str, int, float, bool, type(None))):
                    config_keys.add(key)

    return sorted(config_keys)


def get_config_signature(item, config_keys):
    """Create a configuration signature for grouping."""
    return tuple(item.get(key) for key in config_keys)


def group_by_config_excluding_command(data):
    """Group benchmark results by configuration excluding command."""
    config_keys = [k for k in get_config_keys(data) if k != "command"]
    result = {}

    for item in data:
        config_sig = tuple(item.get(key) for key in config_keys)
        if config_sig not in result:
            result[config_sig] = {"items": [], "config_keys": config_keys}
        result[config_sig]["items"].append(item)

    return result


def group_by_command(items):
    """Group items by command."""
    result = {}
    for item in items:
        command = item.get("command", "UNKNOWN")
        if command not in result:
            result[command] = []
        result[command].append(item)
    return result


def format_config_display(config_sig, config_keys):
    """Format configuration signature for display."""
    config_dict = dict(zip(config_keys, config_sig))

    # Format all config parameters
    config_lines = []
    for key, value in config_dict.items():
        if value is not None:
            config_lines.append(f"- {key}: {value}")

    return config_lines


def summarize(data_items):
    """Summarize one or more benchmark result items."""
    if not isinstance(data_items, list):
        data_items = [data_items]

    rps = _mean([item.get("rps", 0.0) for item in data_items])
    latency_avg = _mean(
        [
            item.get("avg_latency_ms", item.get("latency_avg_ms", 0.0))
            for item in data_items
        ]
    )
    latency_p50 = _mean(
        [
            item.get("p50_latency_ms", item.get("latency_p50_ms", 0.0))
            for item in data_items
        ]
    )
    latency_p95 = _mean(
        [
            item.get("p95_latency_ms", item.get("latency_p95_ms", 0.0))
            for item in data_items
        ]
    )
    latency_p99 = _mean(
        [
            item.get("p99_latency_ms", item.get("latency_p99_ms", 0.0))
            for item in data_items
        ]
    )

    return {
        "rps": rps,
        "latency_avg_ms": latency_avg,
        "latency_p50_ms": latency_p50,
        "latency_p95_ms": latency_p95,
        "latency_p99_ms": latency_p99,
    }


def pct_change(new_v, old_v):
    return ((new_v - old_v) / old_v * 100.0) if old_v else 0.0


def sort_config_tuples(config_tuple):
    """Custom sorting key function that handles None values and mixed types in config tuples."""

    # Convert all values to strings for consistent sorting
    # This ensures all types can be compared
    def convert_item(item):
        if item is None:
            return ""  # None values sort first
        else:
            return str(item)  # Convert everything else to string

    return tuple(convert_item(item) for item in config_tuple)


def get_version_from_data(data):
    """Extract version information from benchmark data."""
    if not data:
        return "Unknown"
    
    # Try to get version from commit field first
    first_item = data[0]
    if 'commit' in first_item and first_item['commit']:
        commit = first_item['commit']
        # If it's a short commit hash, return it
        if len(commit) <= 12:
            return commit
        # If it's a long commit hash, return first 8 characters
        return commit[:8]
    
    # Try to get from timestamp as fallback
    if 'timestamp' in first_item:
        return f"ts-{first_item['timestamp']}"
    
    return "Unknown"


def group_by_static_config(data):
    """Group benchmark results by static configuration (excluding command, pipeline, io_threads)."""
    # Define which keys should be in the table vs configuration section
    table_keys = {'command', 'pipeline', 'io_threads'}
    
    config_keys = [k for k in get_config_keys(data) if k not in table_keys]
    result = {}

    for item in data:
        config_sig = tuple(item.get(key) for key in config_keys)
        if config_sig not in result:
            result[config_sig] = {"items": [], "config_keys": config_keys}
        result[config_sig]["items"].append(item)

    return result


def create_unified_comparison_table(baseline_data, new_data):
    """Create a unified comparison table with all results."""
    baseline_version = get_version_from_data(baseline_data)
    new_version = get_version_from_data(new_data)
    
    baseline_by_config = group_by_static_config(baseline_data)
    new_by_config = group_by_static_config(new_data)
    
    metrics = [
        ("rps", "rps"),
        ("latency_avg_ms", "avg_latency"),
        ("latency_p50_ms", "p50_latency"),
        ("latency_p95_ms", "p95_latency"),
        ("latency_p99_ms", "p99_latency"),
    ]
    
    # Collect all configuration groups and their table rows
    config_groups = []
    
    all_configs = sorted(
        set(baseline_by_config.keys()) | set(new_by_config.keys()),
        key=sort_config_tuples,
    )
    
    for config_sig in all_configs:
        baseline_group = baseline_by_config.get(
            config_sig, {"items": [], "config_keys": []}
        )
        new_group = new_by_config.get(config_sig, {"items": [], "config_keys": []})
        
        config_keys = baseline_group.get("config_keys") or new_group.get(
            "config_keys", []
        )
        if not config_keys:
            continue
            
        # Create config dict for easy access
        config_dict = dict(zip(config_keys, config_sig))
        
        # Group by command within this configuration
        baseline_by_cmd = group_by_command(baseline_group["items"])
        new_by_cmd = group_by_command(new_group["items"])
        all_commands = sorted(set(baseline_by_cmd.keys()) | set(new_by_cmd.keys()))
        
        # Collect table rows for this configuration
        table_rows = []
        
        for command in all_commands:
            baseline_items = baseline_by_cmd.get(command, [])
            new_items = new_by_cmd.get(command, [])
            
            # Group by pipeline and io_threads within each command
            baseline_by_pipeline = {}
            new_by_pipeline = {}
            
            for item in baseline_items:
                key = (item.get('pipeline'), item.get('io_threads'))
                if key not in baseline_by_pipeline:
                    baseline_by_pipeline[key] = []
                baseline_by_pipeline[key].append(item)
                
            for item in new_items:
                key = (item.get('pipeline'), item.get('io_threads'))
                if key not in new_by_pipeline:
                    new_by_pipeline[key] = []
                new_by_pipeline[key].append(item)
            
            # Get all unique pipeline/io_threads combinations
            all_pipeline_keys = set(baseline_by_pipeline.keys()) | set(new_by_pipeline.keys())
            
            for pipeline_key in sorted(all_pipeline_keys):
                pipeline, io_threads = pipeline_key
                
                baseline_pipeline_items = baseline_by_pipeline.get(pipeline_key, [])
                new_pipeline_items = new_by_pipeline.get(pipeline_key, [])
                
                baseline_summary = summarize(baseline_pipeline_items)
                new_summary = summarize(new_pipeline_items)
                
                for metric_key, metric_display in metrics:
                    baseline_value = baseline_summary.get(metric_key, 0.0)
                    new_value = new_summary.get(metric_key, 0.0)
                    diff = new_value - baseline_value
                    change = pct_change(new_value, baseline_value)
                    
                    # Create row with only table parameters
                    row = {
                        'command': command,
                        'metric': metric_display,
                        'pipeline': pipeline,
                        'io_threads': io_threads,
                        'baseline_value': baseline_value,
                        'new_value': new_value,
                        'diff': diff,
                        'change': change
                    }
                    
                    table_rows.append(row)
        
        config_groups.append({
            'config_dict': config_dict,
            'config_keys': config_keys,
            'table_rows': table_rows
        })
    
    return config_groups, baseline_version, new_version


def format_unified_table(config_groups, baseline_version, new_version):
    """Format the unified table as markdown with configuration sections."""
    if not config_groups:
        return "No data to compare."
    
    lines = []
    
    for group in config_groups:
        config_dict = group['config_dict']
        config_keys = group['config_keys']
        table_rows = group['table_rows']
        
        if not table_rows:
            continue
            
        # Show configuration section
        lines.append("**Configuration:**")
        for key in sorted(config_keys):
            value = config_dict.get(key)
            if value is not None:
                lines.append(f"- {key}: {value}")
        lines.append("")
        
        # Show table for this configuration
        lines.append(f"| Command | Metric | Pipeline | io_threads | {baseline_version} | {new_version} | Diff | % Change |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        
        for row in table_rows:
            lines.append(
                f"| {row['command']} | {row['metric']} | {row['pipeline']} | {row['io_threads']} | "
                f"{row['baseline_value']:.2f} | {row['new_value']:.2f} | "
                f"{row['diff']:.2f} | {row['change']:+.2f}% |"
            )
        
        lines.append("")
    
    return '\n'.join(lines)


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: compare_benchmark_results.py BASELINE NEW [OUT_FILE]",
            file=sys.stderr,
        )
        sys.exit(1)

    baseline_file = sys.argv[1]
    new_file = sys.argv[2]
    out_file = sys.argv[3] if len(sys.argv) > 3 else None

    baseline_data = load(baseline_file)
    new_data = load(new_file)

    # Create unified comparison table
    table_rows, baseline_version, new_version = create_unified_comparison_table(baseline_data, new_data)
    
    # Format as markdown table
    table = format_unified_table(table_rows, baseline_version, new_version)
    
    # Add header
    output = f"# Benchmark Comparison: {baseline_version} vs {new_version}\n\n{table}\n"

    if out_file:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(output)
    print(output)


if __name__ == "__main__":
    main()
