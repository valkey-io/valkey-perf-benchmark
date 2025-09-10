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
    exclude_keys = {'timestamp', 'commit', 'rps', 'avg_latency_ms', 'min_latency_ms', 
                   'p50_latency_ms', 'p95_latency_ms', 'p99_latency_ms', 'max_latency_ms',
                   'latency_avg_ms', 'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms'}
    
    for item in data:
        for key in item.keys():
            if key not in exclude_keys:
                config_keys.add(key)
    
    return sorted(config_keys)


def get_config_signature(item, config_keys):
    """Create a configuration signature for grouping."""
    return tuple(item.get(key) for key in config_keys)


def group_by_config_excluding_command(data):
    """Group benchmark results by configuration excluding command."""
    config_keys = [k for k in get_config_keys(data) if k != 'command']
    result = {}
    
    for item in data:
        config_sig = tuple(item.get(key) for key in config_keys)
        if config_sig not in result:
            result[config_sig] = {'items': [], 'config_keys': config_keys}
        result[config_sig]['items'].append(item)
    
    return result


def group_by_command(items):
    """Group items by command."""
    result = {}
    for item in items:
        command = item.get('command', 'UNKNOWN')
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

    baseline_by_config = group_by_config_excluding_command(baseline_data)
    new_by_config = group_by_config_excluding_command(new_data)

    metrics = [
        "rps",
        "latency_avg_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
    ]

    lines = ["# Benchmark Comparison by Configuration\n"]
    all_configs = sorted(set(baseline_by_config.keys()) | set(new_by_config.keys()))
    
    for config_sig in all_configs:
        baseline_group = baseline_by_config.get(config_sig, {'items': [], 'config_keys': []})
        new_group = new_by_config.get(config_sig, {'items': [], 'config_keys': []})
        
        config_keys = baseline_group.get('config_keys') or new_group.get('config_keys', [])
        if not config_keys:
            continue
        
        # Show configuration once at the top
        config_lines = format_config_display(config_sig, config_keys)
        if config_lines:
            lines.append("**Configuration:**")
            lines.extend(config_lines)
            lines.append("")
        
        # Group by command within this configuration
        baseline_by_cmd = group_by_command(baseline_group['items'])
        new_by_cmd = group_by_command(new_group['items'])
        all_commands = sorted(set(baseline_by_cmd.keys()) | set(new_by_cmd.keys()))
        
        for command in all_commands:
            lines.append(f"## {command}\n")
            lines.append("| Metric | Baseline | PR | Diff | % Change |")
            lines.append("| --- | --- | --- | --- | --- |")

            baseline_items = baseline_by_cmd.get(command, [])
            new_items = new_by_cmd.get(command, [])
            
            baseline_summary = summarize(baseline_items)
            new_summary = summarize(new_items)

            for metric in metrics:
                baseline_value = baseline_summary.get(metric, 0.0)
                new_value = new_summary.get(metric, 0.0)
                diff = new_value - baseline_value
                change = pct_change(new_value, baseline_value)

                lines.append(
                    f"| {metric} | {baseline_value:.2f} | {new_value:.2f} | {diff:.2f} | {change:+.2f}% |"
                )

            lines.append("")

    table = "\n".join(lines)

    if out_file:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(table)
    print(table)


if __name__ == "__main__":
    main()
