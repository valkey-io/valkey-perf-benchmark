#!/usr/bin/env python3
"""Convert benchmark metrics to Prometheus format and push to AWS Managed Prometheus."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
import requests
from requests_aws4auth import AWS4Auth
import boto3
import snappy
from prometheus_client.core import CollectorRegistry, Gauge
from prometheus_client.exposition import generate_latest
from prometheus_client import write_to_textfile


def create_prometheus_payload(metrics_data):
    """Create Prometheus remote write protobuf payload."""
    from prometheus_client.core import Sample, Metric
    from prometheus_client.openmetrics.exposition import generate_latest as openmetrics_generate
    
    # Build WriteRequest protobuf
    timeseries = []
    
    for metric in metrics_data:
        timestamp_ms = int(datetime.fromisoformat(metric['timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
        
        labels = {
            "commit": metric.get("commit", ""),
            "command": metric.get("command", ""),
            "data_size": str(metric.get("data_size", "")),
            "pipeline": str(metric.get("pipeline", "")),
            "clients": str(metric.get("clients", "")),
            "cluster_mode": str(metric.get("cluster_mode", "")).lower(),
            "tls": str(metric.get("tls", "")).lower(),
        }
        
        if "io_threads" in metric:
            labels["io_threads"] = str(metric["io_threads"])
        if "valkey-benchmark-threads" in metric:
            labels["benchmark_threads"] = str(metric["valkey-benchmark-threads"])
        if "benchmark_mode" in metric:
            labels["benchmark_mode"] = str(metric["benchmark_mode"])
        if "duration" in metric:
            labels["duration"] = str(metric["duration"])
        if "requests" in metric:
            labels["requests"] = str(metric["requests"])
        if "warmup" in metric:
            labels["warmup"] = str(metric["warmup"])
        
        metrics_map = {
            "valkey_rps": metric.get("rps"),
            "valkey_avg_latency_ms": metric.get("avg_latency_ms"),
            "valkey_min_latency_ms": metric.get("min_latency_ms"),
            "valkey_p50_latency_ms": metric.get("p50_latency_ms"),
            "valkey_p95_latency_ms": metric.get("p95_latency_ms"),
            "valkey_p99_latency_ms": metric.get("p99_latency_ms"),
            "valkey_max_latency_ms": metric.get("max_latency_ms"),
        }
        
        for metric_name, value in metrics_map.items():
            if value is not None:
                ts_labels = [{"name": "__name__", "value": metric_name}]
                ts_labels.extend([{"name": k, "value": v} for k, v in labels.items()])
                
                timeseries.append({
                    "labels": ts_labels,
                    "samples": [{"value": float(value), "timestamp": timestamp_ms}]
                })
    
    # Create protobuf WriteRequest
    from prometheus_client.samples import Sample as PromSample
    import struct
    
    # Simplified: use snappy compression on JSON payload
    payload = json.dumps({"timeseries": timeseries}).encode('utf-8')
    compressed = snappy.compress(payload)
    
    return compressed


def push_to_amp(metrics_data, workspace_url, region):
    """Push metrics to AWS Managed Prometheus using remote write API."""
    session = boto3.Session()
    credentials = session.get_credentials()
    auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        'aps',
        session_token=credentials.token
    )
    
    url = f"{workspace_url}api/v1/remote_write"
    
    payload = create_prometheus_payload(metrics_data)
    
    headers = {
        'Content-Encoding': 'snappy',
        'Content-Type': 'application/x-protobuf',
        'X-Prometheus-Remote-Write-Version': '0.1.0'
    }
    
    response = requests.post(url, data=payload, auth=auth, headers=headers)
    response.raise_for_status()
    
    return response


def main():
    parser = argparse.ArgumentParser(description="Push benchmark metrics to AWS Managed Prometheus")
    parser.add_argument("--results-dir", required=True, help="Path to results directory")
    parser.add_argument("--workspace-url", required=True, help="AWS Managed Prometheus workspace URL")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    
    args = parser.parse_args()
    
    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)
    
    total_pushed = 0
    for commit_dir in results_dir.iterdir():
        if not commit_dir.is_dir():
            continue
        
        metrics_file = commit_dir / "metrics.json"
        if not metrics_file.exists():
            print(f"Skipping {commit_dir.name}: no metrics.json found")
            continue
        
        try:
            with open(metrics_file) as f:
                metrics_data = json.load(f)
            
            push_to_amp(metrics_data, args.workspace_url, args.region)
            print(f"Pushed {len(metrics_data)} metrics for commit {commit_dir.name}")
            total_pushed += len(metrics_data)
        except Exception as e:
            print(f"Error pushing metrics for {commit_dir.name}: {e}", file=sys.stderr)
    
    print(f"Successfully pushed {total_pushed} total metrics to Prometheus")


if __name__ == "__main__":
    main()
