"""Unit tests for process_metrics.py — MetricsProcessor.create_metrics."""

import json

import pytest

from process_metrics import MetricsProcessor


@pytest.fixture
def processor():
    """A MetricsProcessor with typical constructor args."""
    return MetricsProcessor(
        commit_id="abc123",
        cluster_mode=False,
        tls_mode=True,
        commit_time="2024-01-15T10:00:00Z",
    )


@pytest.fixture
def processor_with_optionals():
    """A MetricsProcessor with all optional constructor args set."""
    return MetricsProcessor(
        commit_id="def456",
        cluster_mode=True,
        tls_mode=False,
        commit_time="2024-06-01T12:00:00Z",
        io_threads=4,
        benchmark_threads=2,
        architecture="x86_64",
    )


# ---------------------------------------------------------------------------
# create_metrics — valid benchmark data
# ---------------------------------------------------------------------------


class TestCreateMetricsValid:
    def test_returns_all_required_fields(self, processor, sample_benchmark_data):
        result = processor.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=10000,
        )

        assert result is not None
        assert result["timestamp"] == "2024-01-15T10:00:00Z"
        assert result["commit"] == "abc123"
        assert result["command"] == "GET"
        assert result["data_size"] == 64
        assert result["pipeline"] == 1
        assert result["clients"] == 50
        assert result["rps"] == 150000.0
        assert result["avg_latency_ms"] == 0.5
        assert result["min_latency_ms"] == 0.1
        assert result["p50_latency_ms"] == 0.4
        assert result["p95_latency_ms"] == 0.8
        assert result["p99_latency_ms"] == 1.2
        assert result["max_latency_ms"] == 5.0
        assert result["cluster_mode"] is False
        assert result["tls"] is True

    def test_optional_fields_present(
        self, processor_with_optionals, sample_benchmark_data
    ):
        result = processor_with_optionals.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="SET",
            data_size=128,
            pipeline=4,
            clients=100,
            requests=5000,
            warmup=10,
        )

        assert result["io_threads"] == 4
        assert result["valkey_benchmark_threads"] == 2
        assert result["architecture"] == "x86_64"
        assert result["warmup"] == 10

    def test_optional_fields_absent_when_not_set(
        self, processor, sample_benchmark_data
    ):
        result = processor.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )

        assert "io_threads" not in result
        assert "valkey_benchmark_threads" not in result
        assert "architecture" not in result
        assert "warmup" not in result


# ---------------------------------------------------------------------------
# create_metrics — empty / None data
# ---------------------------------------------------------------------------


class TestCreateMetricsEmpty:
    def test_empty_dict_returns_none(self, processor):
        result = processor.create_metrics(
            benchmark_data={},
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )
        assert result is None

    def test_none_returns_none(self, processor):
        result = processor.create_metrics(
            benchmark_data=None,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )
        assert result is None


# ---------------------------------------------------------------------------
# create_metrics — non-numeric values use defaults
# ---------------------------------------------------------------------------


class TestCreateMetricsNonNumeric:
    def test_non_numeric_rps_defaults_to_zero(self, processor):
        data = {"rps": "not_a_number", "avg_latency_ms": "0.5"}
        result = processor.create_metrics(
            benchmark_data=data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )

        assert result["rps"] == 0.0
        assert result["avg_latency_ms"] == 0.5

    def test_all_non_numeric_values_default(self, processor):
        data = {
            "rps": "bad",
            "avg_latency_ms": "bad",
            "min_latency_ms": "bad",
            "p50_latency_ms": "bad",
            "p95_latency_ms": "bad",
            "p99_latency_ms": "bad",
            "max_latency_ms": "bad",
        }
        result = processor.create_metrics(
            benchmark_data=data,
            command="SET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )

        assert result["rps"] == 0.0
        assert result["avg_latency_ms"] == 0.0
        assert result["min_latency_ms"] == 0.0
        assert result["p50_latency_ms"] == 0.0
        assert result["p95_latency_ms"] == 0.0
        assert result["p99_latency_ms"] == 0.0
        assert result["max_latency_ms"] == 0.0

    def test_missing_keys_default_to_zero(self, processor):
        data = {"rps": "100000.0"}
        result = processor.create_metrics(
            benchmark_data=data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=1000,
        )

        assert result["rps"] == 100000.0
        assert result["avg_latency_ms"] == 0.0
        assert result["min_latency_ms"] == 0.0


# ---------------------------------------------------------------------------
# create_metrics — requests mode vs duration mode
# ---------------------------------------------------------------------------


class TestCreateMetricsBenchmarkMode:
    def test_requests_mode(self, processor, sample_benchmark_data):
        result = processor.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            requests=10000,
        )

        assert result["benchmark_mode"] == "requests"
        assert result["requests"] == 10000
        assert "duration" not in result

    def test_duration_mode(self, processor, sample_benchmark_data):
        result = processor.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
            duration=30,
        )

        assert result["benchmark_mode"] == "duration"
        assert result["duration"] == 30
        assert "requests" not in result

    def test_neither_mode(self, processor, sample_benchmark_data):
        result = processor.create_metrics(
            benchmark_data=sample_benchmark_data,
            command="GET",
            data_size=64,
            pipeline=1,
            clients=50,
        )

        assert result["benchmark_mode"] == "unknown"
        assert "requests" not in result
        assert "duration" not in result


# ---------------------------------------------------------------------------
# write_metrics — file I/O behaviour
# ---------------------------------------------------------------------------


class TestWriteMetrics:
    """Tests for MetricsProcessor.write_metrics."""

    def test_writes_to_new_directory(self, processor, tmp_path):
        results_dir = tmp_path / "new_results"
        new_metrics = [{"command": "GET", "rps": 100000}]

        processor.write_metrics(results_dir, new_metrics)

        metrics_file = results_dir / "metrics.json"
        assert metrics_file.exists()

        data = json.loads(metrics_file.read_text(encoding="utf-8"))
        assert data == new_metrics

    def test_appends_to_existing_file(self, processor, tmp_path):
        metrics_file = tmp_path / "metrics.json"
        existing = [{"command": "SET", "rps": 50000}]
        metrics_file.write_text(json.dumps(existing), encoding="utf-8")

        new_metrics = [{"command": "GET", "rps": 100000}]
        processor.write_metrics(tmp_path, new_metrics)

        data = json.loads(metrics_file.read_text(encoding="utf-8"))
        assert len(data) == 2
        assert data[0] == existing[0]
        assert data[1] == new_metrics[0]

    def test_empty_metrics_does_nothing(self, processor, tmp_path):
        results_dir = tmp_path / "empty_test"

        processor.write_metrics(results_dir, [])

        assert not results_dir.exists()

    def test_corrupt_json_starts_fresh(self, processor, tmp_path):
        metrics_file = tmp_path / "metrics.json"
        metrics_file.write_text("{not valid json!!!", encoding="utf-8")

        new_metrics = [{"command": "GET", "rps": 100000}]
        processor.write_metrics(tmp_path, new_metrics)

        data = json.loads(metrics_file.read_text(encoding="utf-8"))
        assert data == new_metrics

    def test_non_list_json_starts_fresh(self, processor, tmp_path):
        metrics_file = tmp_path / "metrics.json"
        metrics_file.write_text(json.dumps({"key": "value"}), encoding="utf-8")

        new_metrics = [{"command": "GET", "rps": 100000}]
        processor.write_metrics(tmp_path, new_metrics)

        data = json.loads(metrics_file.read_text(encoding="utf-8"))
        assert data == new_metrics
