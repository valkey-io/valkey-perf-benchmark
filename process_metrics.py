"""Helpers for parsing benchmark results."""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any

import logging


class MetricsProcessor:
    """Build and persist metric rows from ``valkey-benchmark`` output."""

    def __init__(
        self,
        commit_id: str,
        cluster_mode: bool,
        tls_mode: bool,
        commit_time: str,
        io_threads: Optional[int] = None,
        benchmark_threads: Optional[int] = None,
        architecture: Optional[str] = None,
        repository: Optional[str] = None,
        environment_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.commit_id = commit_id
        self.cluster_mode = cluster_mode
        self.tls_mode = tls_mode
        self.commit_time = commit_time
        self.io_threads = io_threads
        self.benchmark_threads = benchmark_threads
        self.architecture = architecture
        self.repository = repository
        self.environment_metadata = environment_metadata

    def build_base_metadata(
        self,
        command: str,
        data_size: int,
        pipeline: int,
        clients: int,
        requests: Optional[int] = None,
        warmup: Optional[int] = None,
        duration: Optional[int] = None,
    ) -> Dict[str, object]:
        """Build metadata shared by successful and failed metric rows.

        Performance fields must remain absent; their absence identifies failed
        rows. Optional identity fields are emitted only when configured.
        """
        metadata: Dict[str, object] = {
            "timestamp": self.commit_time,
            "commit": self.commit_id,
            "repository": self.repository,
            "command": command,
            "data_size": int(data_size),
            "pipeline": int(pipeline),
            "clients": int(clients),
            "cluster_mode": self.cluster_mode,
            "tls": self.tls_mode,
        }

        if duration is not None:
            metadata["duration"] = int(duration)
            metadata["benchmark_mode"] = "duration"
        elif requests is not None:
            metadata["requests"] = int(requests)
            metadata["benchmark_mode"] = "requests"
        else:
            logging.warning("Neither requests nor duration specified")
            metadata["benchmark_mode"] = "unknown"

        if self.io_threads is not None:
            metadata["io_threads"] = self.io_threads

        if self.benchmark_threads is not None:
            metadata["valkey_benchmark_threads"] = self.benchmark_threads

        if warmup is not None:
            metadata["warmup"] = warmup

        if self.architecture is not None:
            metadata["architecture"] = self.architecture

        # Downstream tables require flat columns.
        if self.environment_metadata:
            for key, value in self.environment_metadata.items():
                metadata[f"env_{key}"] = value

        return metadata

    def create_metrics(
        self,
        benchmark_data: Dict[str, Any],
        command: str,
        data_size: int,
        pipeline: int,
        clients: int,
        requests: Optional[int] = None,
        warmup: Optional[int] = None,
        duration: Optional[int] = None,
    ) -> Optional[Dict[str, object]]:
        """Add parsed performance measurements to the row metadata."""
        if not benchmark_data:
            logging.warning("Empty benchmark output received")
            return None

        try:

            def safe_float(value, default=0.0):
                try:
                    return float(value) if value else default
                except (ValueError, TypeError):
                    logging.warning(
                        f"Could not convert '{value}' to float, using {default}"
                    )
                    return default

            metrics_dict = self.build_base_metadata(
                command,
                data_size,
                pipeline,
                clients,
                requests=requests,
                warmup=warmup,
                duration=duration,
            )

            metrics_dict.update(
                {
                    "rps": safe_float(benchmark_data.get("rps")),
                    "avg_latency_ms": safe_float(benchmark_data.get("avg_latency_ms")),
                    "min_latency_ms": safe_float(benchmark_data.get("min_latency_ms")),
                    "p50_latency_ms": safe_float(benchmark_data.get("p50_latency_ms")),
                    "p95_latency_ms": safe_float(benchmark_data.get("p95_latency_ms")),
                    "p99_latency_ms": safe_float(benchmark_data.get("p99_latency_ms")),
                    "max_latency_ms": safe_float(benchmark_data.get("max_latency_ms")),
                }
            )

            return metrics_dict
        except Exception:
            logging.exception("Error parsing CSV output")
            logging.debug(f"Raw output: {benchmark_data}")
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

        results_dir.mkdir(parents=True, exist_ok=True)

        if metrics_file.exists() and metrics_file.stat().st_size > 0:
            try:
                with metrics_file.open("r", encoding="utf-8") as f:
                    metrics = json.load(f)
                if not isinstance(metrics, list):
                    logging.warning(
                        "Existing metrics file contains non-list data, starting fresh"
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

        metrics.extend(new_metrics)

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
