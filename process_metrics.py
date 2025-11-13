"""Helpers for parsing benchmark results."""

import json
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
        self,
        commit_id: str,
        cluster_mode: bool,
        tls_mode: bool,
        commit_time: str,
        io_threads: Optional[int] = None,
        benchmark_threads: Optional[int] = None,
        architecture: Optional[str] = None,
    ) -> None:
        self.commit_id = commit_id
        self.cluster_mode = cluster_mode
        self.tls_mode = tls_mode
        self.commit_time = commit_time
        self.io_threads = io_threads
        self.benchmark_threads = benchmark_threads
        self.architecture = architecture

    def create_metrics(
        self,
        output: str,
        command: str,
        data_size: int,
        pipeline: int,
        clients: int,
        requests: Optional[int] = None,
        warmup: Optional[int] = None,
        duration: Optional[int] = None,
    ) -> Optional[Dict[str, object]]:
        """Create a complete metrics dictionary from CSV output and benchmark parameters.

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
        warmup : int, optional
            Warmup time in seconds.
        """
        if not output or not output.strip():
            logging.warning("Empty benchmark output received")
            return None

        try:
            lines = output.strip().split("\n")
            if len(lines) < 2:
                logging.warning(f"Unexpected CSV format in benchmark output: {output}")
                return None

            labels = [label.strip().replace('"', "") for label in lines[0].split(",")]
            values = [value.strip().replace('"', "") for value in lines[1].split(",")]

            if len(values) != len(labels):
                logging.warning(
                    f"Mismatch between CSV labels ({len(labels)}) and values ({len(values)})"
                )
                logging.debug(f"Labels: {labels}")
                logging.debug(f"Values: {values}")
                return None

            data = dict(zip(labels, values))

            # Helper function to safely convert to float
            def safe_float(value, default=0.0):
                try:
                    return float(value) if value else default
                except (ValueError, TypeError):
                    logging.warning(
                        f"Could not convert '{value}' to float, using {default}"
                    )
                    return default

            metrics_dict = {
                "timestamp": self.commit_time,
                "commit": self.commit_id,
                "command": command,
                "data_size": int(data_size),
                "pipeline": int(pipeline),
                "clients": int(clients),
                "rps": safe_float(data.get("rps")),
                "avg_latency_ms": safe_float(data.get("avg_latency_ms")),
                "min_latency_ms": safe_float(data.get("min_latency_ms")),
                "p50_latency_ms": safe_float(data.get("p50_latency_ms")),
                "p95_latency_ms": safe_float(data.get("p95_latency_ms")),
                "p99_latency_ms": safe_float(data.get("p99_latency_ms")),
                "max_latency_ms": safe_float(data.get("max_latency_ms")),
                "cluster_mode": self.cluster_mode,
                "tls": self.tls_mode,
            }

            # Add requests or duration based on benchmark mode
            if requests is not None:
                metrics_dict["requests"] = int(requests)
                metrics_dict["benchmark_mode"] = "requests"
            elif duration is not None:
                metrics_dict["duration"] = int(duration)
                metrics_dict["benchmark_mode"] = "duration"
            else:
                logging.warning("Neither requests nor duration specified")
                metrics_dict["benchmark_mode"] = "unknown"

            # Add io_threads to metrics if it was specified
            if self.io_threads is not None:
                metrics_dict["io_threads"] = self.io_threads

            # Add benchmark_threads to metrics if it was specified
            if self.benchmark_threads is not None:
                metrics_dict["valkey-benchmark-threads"] = self.benchmark_threads

            # Add warmup to metrics if it was specified
            if warmup is not None:
                metrics_dict["warmup"] = warmup

            # Add architecture to metrics if it was specified
            if self.architecture is not None:
                metrics_dict["architecture"] = self.architecture

            return metrics_dict
        except Exception:
            logging.exception(f"Error parsing CSV output")
            logging.debug(f"Raw output: {output}")
            return None

    def write_metrics(
        self, results_dir: Path, new_metrics: List[Dict[str, object]]
    ) -> None:
        """Append metrics to ``results_dir/metrics.json``."""
        if not new_metrics:
            logging.warning("No metrics to write")
            return

        metrics_file = results_dir / "metrics.json"
        metrics = []

        # Ensure results directory exists
        results_dir.mkdir(parents=True, exist_ok=True)

        # Load existing metrics if file exists
        if metrics_file.exists() and metrics_file.stat().st_size > 0:
            try:
                with metrics_file.open("r", encoding="utf-8") as f:
                    metrics = json.load(f)
                if not isinstance(metrics, list):
                    logging.warning(
                        f"Existing metrics file contains non-list data, starting fresh"
                    )
                    metrics = []
            except json.JSONDecodeError as e:
                logging.warning(
                    f"Could not decode JSON from {metrics_file}: {e}, starting fresh."
                )
                metrics = []
            except Exception as e:
                logging.error(f"Error reading existing metrics file: {e}")
                raise

        # Extend metrics with new_metrics
        metrics.extend(new_metrics)

        # Write metrics with atomic operation
        temp_file = metrics_file.with_suffix(".tmp")
        try:
            with temp_file.open("w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=4, ensure_ascii=False)
            temp_file.replace(metrics_file)
            logging.info(
                f"Successfully wrote {len(new_metrics)} metrics to {metrics_file}"
            )
        except Exception as e:
            logging.error(f"Error writing metrics to {metrics_file}: {e}")
            if temp_file.exists():
                temp_file.unlink()
            raise
