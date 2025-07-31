"""Helpers for parsing benchmark results."""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import logging


class MetricsProcessor:
    """Process metric output from ``valkey-benchmark``.

    Parameters
    ----------
    commit_id : str
        Git commit identifier of the tested build.
    cluster_mode : bool
        Whether cluster mode was enabled for the benchmark.
    tls_mode : bool
        Whether TLS was enabled for the benchmark.
    commit_time : str
        ISO8601 commit timestamp.
    """

    def __init__(
        self, commit_id: str, cluster_mode: bool, tls_mode: bool, commit_time: str
    ) -> None:
        self.commit_id = commit_id
        self.cluster_mode = cluster_mode
        self.tls_mode = tls_mode
        self.commit_time = commit_time

    def parse_csv_output(
        self,
        output: str,
        command: str,
        data_size: int,
        pipeline: int,
        clients: int,
        requests: int,
    ) -> Optional[Dict[str, object]]:
        """Return a metrics dictionary parsed from CSV output.

        Parameters
        ----------
        output : str
            Raw CSV output from ``valkey-benchmark``.
        command : str
            Benchmark command that was executed.
        data_size : int
            Size of the payload in bytes.
        pipeline : int
            Number of commands pipelined.
        clients : int
            Concurrent client connections used.
        requests : int
            Total number of requests issued.
        """
        lines = output.strip().split("\n")
        if len(lines) < 2:
            logging.warning("Unexpected CSV format in benchmark output.")
            return None

        labels = lines[0].replace('"', "").split(",")
        values = lines[1].replace('"', "").split(",")

        if len(values) != len(labels):
            logging.warning("Mismatch between CSV labels and values")
            return None

        data = dict(zip(labels, values))

        return {
            "timestamp": self.commit_time,
            "commit": self.commit_id,
            "command": command,
            "data_size": int(data_size),
            "pipeline": int(pipeline),
            "clients": int(clients),
            "requests": int(requests),
            "rps": float(data.get("rps", 0)),
            "avg_latency_ms": float(data.get("avg_latency_ms", 0)),
            "min_latency_ms": float(data.get("min_latency_ms", 0)),
            "p50_latency_ms": float(data.get("p50_latency_ms", 0)),
            "p95_latency_ms": float(data.get("p95_latency_ms", 0)),
            "p99_latency_ms": float(data.get("p99_latency_ms", 0)),
            "max_latency_ms": float(data.get("max_latency_ms", 0)),
            "cluster_mode": self.cluster_mode,
            "tls": self.tls_mode,
        }

    def write_metrics(
        self, results_dir: Path, new_metrics: List[Dict[str, object]]
    ) -> None:
        """Append metrics to ``results_dir/metrics.json``."""
        metrics_file = results_dir / "metrics.json"
        metrics = []

        if metrics_file.exists() and metrics_file.stat().st_size > 0:
            try:
                with metrics_file.open("r", encoding="utf-8") as f:
                    metrics = json.load(f)
            except json.JSONDecodeError:
                logging.warning(
                    f"Could not decode JSON from {metrics_file}, starting fresh."
                )

        # Extend metrics with new_metrics as it's a list.
        metrics.extend(new_metrics)

        with metrics_file.open("w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=4)

        logging.info(f"Metrics written to {metrics_file}")
