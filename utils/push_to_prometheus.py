#!/usr/bin/env python3
"""Convert benchmark metrics to Prometheus format and push to AWS Managed Prometheus."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import boto3
import requests
import snappy
from requests_aws4auth import AWS4Auth


def build_labels(metric):
    """Build label dictionary from metric data."""
    commit_time = metric["timestamp"].replace("Z", "+00:00")

    labels = {
        "commit": metric.get("commit", ""),
        "commit_time": commit_time,
        "command": metric.get("command", ""),
        "data_size": str(metric.get("data_size", "")),
        "pipeline": str(metric.get("pipeline", "")),
        "clients": str(metric.get("clients", "")),
        "cluster_mode": str(metric.get("cluster_mode", "")).lower(),
        "tls": str(metric.get("tls", "")).lower(),
    }

    optional_fields = {
        "io_threads": "io_threads",
        "valkey-benchmark-threads": "benchmark_threads",
        "benchmark_mode": "benchmark_mode",
        "duration": "duration",
        "requests": "requests",
        "warmup": "warmup",
    }

    for field, label_name in optional_fields.items():
        if field in metric:
            labels[label_name] = str(metric[field])

    return labels


def get_metric_values(metric):
    """Extract metric values from benchmark data."""
    return {
        "valkey_rps": metric.get("rps"),
        "valkey_avg_latency_ms": metric.get("avg_latency_ms"),
        "valkey_min_latency_ms": metric.get("min_latency_ms"),
        "valkey_p50_latency_ms": metric.get("p50_latency_ms"),
        "valkey_p95_latency_ms": metric.get("p95_latency_ms"),
        "valkey_p99_latency_ms": metric.get("p99_latency_ms"),
        "valkey_max_latency_ms": metric.get("max_latency_ms"),
    }


def print_metrics_dry_run(metrics_data):
    """Print metrics in Prometheus format for dry-run."""
    current_timestamp_ms = int(datetime.now().timestamp() * 1000)

    for metric in metrics_data:
        labels = build_labels(metric)
        metrics_map = get_metric_values(metric)

        for metric_name, value in metrics_map.items():
            if value is not None:
                label_str = ",".join([f'{k}="{v}"' for k, v in labels.items()])
                print(f"{metric_name}{{{label_str}}} {value} {current_timestamp_ms}")


def create_prometheus_payload(metrics_data):
    """Create Prometheus remote write protobuf payload."""
    import remote_pb2

    write_request = remote_pb2.WriteRequest()
    current_timestamp_ms = int(datetime.now().timestamp() * 1000)

    for metric in metrics_data:
        labels = build_labels(metric)
        metrics_map = get_metric_values(metric)

        for metric_name, value in metrics_map.items():
            if value is not None:
                ts = write_request.timeseries.add()

                label = ts.labels.add()
                label.name = "__name__"
                label.value = metric_name

                for k, v in labels.items():
                    label = ts.labels.add()
                    label.name = k
                    label.value = v

                sample = ts.samples.add()
                sample.value = float(value)
                sample.timestamp = current_timestamp_ms

    return snappy.compress(write_request.SerializeToString())


def push_to_amp(metrics_data, workspace_url, region, debug=False):
    """Push metrics to AWS Managed Prometheus using remote write API."""
    payload = create_prometheus_payload(metrics_data)

    if debug:
        print(f"Payload size: {len(payload)} bytes")

    session = boto3.Session()
    credentials = session.get_credentials()
    auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "aps",
        session_token=credentials.token,
    )

    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
    }

    url = f"{workspace_url}/api/v1/remote_write"
    if debug:
        print(f"Pushing to: {url}")

    response = requests.post(url, data=payload, auth=auth, headers=headers)

    if debug:
        print(f"Response status: {response.status_code}")

    response.raise_for_status()


def process_commit_metrics(commit_dir, dry_run, workspace_url, region, debug=False):
    """Process metrics for a single commit directory."""
    metrics_file = commit_dir / "metrics.json"
    if not metrics_file.exists():
        print(f"Skipping {commit_dir.name}: no metrics.json found")
        return 0

    with open(metrics_file) as f:
        metrics_data = json.load(f)

    print(f"\n=== Metrics for {commit_dir.name} ===")

    if dry_run:
        print_metrics_dry_run(metrics_data)
        print(f"Would push {len(metrics_data)} metrics")
    else:
        push_to_amp(metrics_data, workspace_url, region, debug=debug)
        print(f"Pushed {len(metrics_data)} metrics")

    return len(metrics_data)


def main():
    parser = argparse.ArgumentParser(
        description="Push benchmark metrics to AWS Managed Prometheus"
    )
    parser.add_argument(
        "--results-dir", required=True, help="Path to results directory"
    )
    parser.add_argument(
        "--workspace-url",
        help="AWS Managed Prometheus workspace URL (not required for --dry-run)",
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be pushed without actually pushing",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug information",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.workspace_url:
        parser.error("--workspace-url is required unless --dry-run is specified")

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    total_pushed = 0
    for commit_dir in sorted(results_dir.iterdir()):
        if not commit_dir.is_dir():
            continue

        try:
            count = process_commit_metrics(
                commit_dir, args.dry_run, args.workspace_url, args.region, args.debug
            )
            total_pushed += count
        except Exception as e:
            print(f"Error processing {commit_dir.name}: {e}", file=sys.stderr)

    status = "[DRY RUN] Would push" if args.dry_run else "Successfully pushed"
    print(f"\n{status} {total_pushed} total metrics to Prometheus")


if __name__ == "__main__":
    main()
