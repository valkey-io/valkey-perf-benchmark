#!/usr/bin/env python3
"""Convert benchmark metrics to Prometheus format and push to VictoriaMetrics."""

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
    labels = {
        "commit": metric.get("commit", ""),
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
    for metric in metrics_data:
        timestamp_ms = int(datetime.fromisoformat(metric['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
        labels = build_labels(metric)
        metrics_map = get_metric_values(metric)

        for metric_name, value in metrics_map.items():
            if value is not None:
                label_str = ",".join([f'{k}="{v}"' for k, v in labels.items()])
                print(f"{metric_name}{{{label_str}}} {value} {timestamp_ms}")


def create_prometheus_payload(metrics_data):
    """Create Prometheus remote write protobuf payload."""
    import remote_pb2

    write_request = remote_pb2.WriteRequest()

    for metric in metrics_data:
        timestamp_ms = int(datetime.fromisoformat(metric['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
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
                sample.timestamp = timestamp_ms

    return snappy.compress(write_request.SerializeToString())


def push_to_victoriametrics(metrics_data, url, use_aws_auth=False, region=None, debug=False):
    """Push metrics to VictoriaMetrics using remote write API."""
    
    # Print first few metrics being pushed
    print(f"\nPushing {len(metrics_data)} metrics:")
    for i, metric in enumerate(metrics_data[:3]):
        timestamp_ms = int(datetime.fromisoformat(metric['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
        print(f"  [{i+1}] commit={metric.get('commit', '')[:8]}, cmd={metric.get('command', '')}, "
              f"pipeline={metric.get('pipeline', '')}, rps={metric.get('rps', '')}, timestamp={timestamp_ms}")
    if len(metrics_data) > 3:
        print(f"  ... and {len(metrics_data) - 3} more")
    
    payload = create_prometheus_payload(metrics_data)

    if debug:
        print(f"\nPayload size: {len(payload)} bytes")

    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
    }

    if debug:
        print(f"Pushing to: {url}")

    if use_aws_auth:
        session = boto3.Session()
        credentials = session.get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            "aps",
            session_token=credentials.token,
        )
        response = requests.post(url, data=payload, auth=auth, headers=headers)
    else:
        response = requests.post(url, data=payload, headers=headers)

    print(f"\n=== Response ===")
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    if response.text:
        print(f"Body:")
        try:
            import json as json_lib
            print(json_lib.dumps(json_lib.loads(response.text), indent=2))
        except:
            print(response.text)
    else:
        print("Body: (empty)")
    
    if response.status_code not in [200, 204]:
        print(f"\nError: Expected status 200 or 204, got {response.status_code}", file=sys.stderr)
        response.raise_for_status()
    else:
        print(f"\n✓ Successfully pushed metrics")


def process_commit_metrics(commit_dir, dry_run, url, use_aws_auth, region, debug=False):
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
        push_to_victoriametrics(metrics_data, url, use_aws_auth, region, debug=debug)
        print(f"Pushed {len(metrics_data)} metrics")

    return len(metrics_data)


def main():
    parser = argparse.ArgumentParser(
        description="Push benchmark metrics to VictoriaMetrics"
    )
    parser.add_argument(
        "--results-dir", required=True, help="Path to results directory"
    )
    parser.add_argument(
        "--url",
        help="VictoriaMetrics remote write URL (not required for --dry-run)",
    )
    parser.add_argument(
        "--aws-auth",
        action="store_true",
        help="Use AWS SigV4 authentication for AWS Managed Prometheus",
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region (for AWS auth)")
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

    if not args.dry_run and not args.url:
        parser.error("--url is required unless --dry-run is specified")

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Sort commit directories by timestamp (oldest first)
    commit_dirs = []
    for commit_dir in results_dir.iterdir():
        if not commit_dir.is_dir():
            continue
        metrics_file = commit_dir / "metrics.json"
        if metrics_file.exists():
            with open(metrics_file) as f:
                metrics_data = json.load(f)
                if metrics_data:
                    timestamp = datetime.fromisoformat(metrics_data[0]['timestamp'].replace('Z', '+00:00'))
                    commit_dirs.append((timestamp, commit_dir))
    
    commit_dirs.sort(key=lambda x: x[0])
    
    total_pushed = 0
    for _, commit_dir in commit_dirs:

        try:
            count = process_commit_metrics(
                commit_dir, args.dry_run, args.url, args.aws_auth, args.region, args.debug
            )
            total_pushed += count
        except Exception as e:
            print(f"Error processing {commit_dir.name}: {e}", file=sys.stderr)

    status = "[DRY RUN] Would push" if args.dry_run else "Successfully pushed"
    print(f"\n{status} {total_pushed} total metrics to VictoriaMetrics")


if __name__ == "__main__":
    main()
