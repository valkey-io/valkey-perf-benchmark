"""Integration tests for benchmark execution flow."""

import csv
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from .conftest import PROJECT_ROOT

from benchmark import load_configs
from valkey_benchmark import ClientRunner
from process_metrics import MetricsProcessor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_config(tmp_path: Path, config: list, name: str = "config.json") -> Path:
    """Write a benchmark config list to a JSON file and return its path."""
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2))
    return path


def _make_runner(config, *, tls_mode=False, cores=None) -> ClientRunner:
    """Build a ClientRunner with sensible test defaults."""
    return ClientRunner(
        commit_id="abc123",
        config=config,
        cluster_mode=False,
        tls_mode=tls_mode,
        target_ip="127.0.0.1",
        results_dir=Path("/tmp"),
        valkey_path="/tmp/valkey",
        valkey_benchmark_path="/tmp/valkey-benchmark",
        cores=cores,
    )


_DEFAULT_BUILD_SCENARIO = dict(
    test="GET",
    requests=100,
    keyspacelen=1000,
    data_size=16,
    pipeline=1,
    clients=10,
)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


class TestBenchmarkConfigLoading:
    """Test benchmark configuration loading and validation."""

    def test_load_minimal_config(self, minimal_config_file):
        configs = load_configs(str(minimal_config_file))
        assert len(configs) == 1
        assert configs[0]["commands"] == ["GET", "SET"]

    def test_config_validation_rejects_invalid(self, tmp_path):
        config_path = _write_config(tmp_path, [{"commands": ["GET"]}], "invalid.json")
        with pytest.raises(ValueError):
            load_configs(str(config_path))

    def test_config_with_duration_mode(self, tmp_path):
        config_path = _write_config(
            tmp_path,
            [
                {
                    "keyspacelen": [100],
                    "data_sizes": [16],
                    "pipelines": [1],
                    "clients": [1],
                    "commands": ["GET"],
                    "cluster_mode": False,
                    "tls_mode": False,
                    "warmup": 0,
                    "duration": 1,
                }
            ],
            "duration.json",
        )

        assert load_configs(str(config_path))[0]["duration"] == 1


# ---------------------------------------------------------------------------
# Mock benchmark binary
# ---------------------------------------------------------------------------


class TestMockBenchmarkExecution:
    """Test benchmark execution with mock binary."""

    def _run_mock(self, binary, extra_args=None):
        """Run the mock benchmark binary and return stdout lines."""
        cmd = [sys.executable, binary.executable, "-t", "GET", "--csv"]
        if extra_args:
            cmd.extend(extra_args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0
        return result.stdout.strip().split("\n")

    def test_mock_benchmark_produces_valid_output(self, mock_benchmark_binary):
        lines = self._run_mock(mock_benchmark_binary)
        rows = list(csv.DictReader(lines))
        assert len(rows) >= 1
        assert float(rows[0]["rps"]) > 0

    def test_mock_benchmark_handles_multiple_commands(self, mock_benchmark_binary):
        cmd = [
            sys.executable,
            mock_benchmark_binary.executable,
            "-t",
            "GET,SET,LPUSH",
            "--csv",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0
        assert len(result.stdout.strip().split("\n")) == 4  # header + 3

    def test_mock_benchmark_pipeline_affects_rps(self, mock_benchmark_binary):
        lines_p1 = self._run_mock(mock_benchmark_binary, ["-P", "1", "--seed", "42"])
        lines_p10 = self._run_mock(mock_benchmark_binary, ["-P", "10", "--seed", "42"])

        rps_p1 = float(list(csv.DictReader(lines_p1))[0]["rps"])
        rps_p10 = float(list(csv.DictReader(lines_p10))[0]["rps"])
        assert rps_p10 > rps_p1 * 5


# ---------------------------------------------------------------------------
# Command building
# ---------------------------------------------------------------------------


class TestBenchmarkCommandBuilding:
    """Test benchmark command construction."""

    def test_build_test_workload_command(self, minimal_benchmark_config):
        runner = _make_runner(minimal_benchmark_config)
        cmd = runner._build_benchmark_command(
            dict(_DEFAULT_BUILD_SCENARIO), tls=False, seed_val=12345
        )

        assert "/tmp/valkey-benchmark" in cmd
        for token in ("-n", "100", "-t", "GET", "--csv"):
            assert token in cmd

    def test_build_tls_command(self, minimal_benchmark_config):
        runner = _make_runner(minimal_benchmark_config, tls_mode=True)
        cmd = runner._build_benchmark_command(
            dict(_DEFAULT_BUILD_SCENARIO), tls=True, seed_val=12345
        )

        for token in ("--tls", "--cert", "--key", "--cacert"):
            assert token in cmd

    def test_build_command_with_cpu_pinning(self, minimal_benchmark_config):
        runner = _make_runner(minimal_benchmark_config, cores="0-3")
        cmd = runner._build_benchmark_command(
            dict(_DEFAULT_BUILD_SCENARIO), tls=False, seed_val=12345
        )

        assert "taskset" in cmd
        assert "-c" in cmd
        assert "0-3" in cmd


# ---------------------------------------------------------------------------
# Metrics processing
# ---------------------------------------------------------------------------


class TestMetricsProcessing:
    """Test metrics processing from benchmark output."""

    _SAMPLE_CSV_DATA = {
        "rps": "150000.00",
        "avg_latency_ms": "0.500",
        "min_latency_ms": "0.100",
        "p50_latency_ms": "0.400",
        "p95_latency_ms": "0.800",
        "p99_latency_ms": "1.200",
        "max_latency_ms": "5.000",
    }

    def test_create_metrics_from_csv_data(self, metrics_processor):
        metrics = metrics_processor.create_metrics(
            self._SAMPLE_CSV_DATA,
            command="GET",
            data_size=16,
            pipeline=1,
            clients=10,
            requests=100000,
        )

        assert metrics is not None
        assert metrics["commit"] == "abc123"
        assert metrics["command"] == "GET"
        assert metrics["rps"] == 150000.0
        assert metrics["avg_latency_ms"] == 0.5
        assert metrics["cluster_mode"] is False
        assert metrics["tls"] is False

    def test_write_and_read_metrics(self, tmp_path, metrics_processor):
        metrics = [
            {
                "commit": "abc123",
                "command": "GET",
                "rps": 100000.0,
                "avg_latency_ms": 0.5,
                "min_latency_ms": 0.1,
                "p50_latency_ms": 0.4,
                "p95_latency_ms": 0.8,
                "p99_latency_ms": 1.2,
                "max_latency_ms": 2.0,
            }
        ]

        results_dir = tmp_path / "results"
        metrics_processor.write_metrics(results_dir, metrics)

        loaded = json.loads((results_dir / "metrics.json").read_text())
        assert len(loaded) == 1
        assert loaded[0]["commit"] == "abc123"

    def test_append_metrics_to_existing(self, tmp_path, metrics_processor):
        results_dir = tmp_path / "results"
        metrics_processor.write_metrics(
            results_dir, [{"commit": "abc123", "command": "GET", "rps": 100000.0}]
        )
        metrics_processor.write_metrics(
            results_dir, [{"commit": "abc123", "command": "SET", "rps": 80000.0}]
        )

        loaded = json.loads((results_dir / "metrics.json").read_text())
        assert {m["command"] for m in loaded} == {"GET", "SET"}


# ---------------------------------------------------------------------------
# End-to-end with mock
# ---------------------------------------------------------------------------


class TestEndToEndWithMock:
    """End-to-end tests using mock benchmark binary."""

    def test_full_benchmark_flow_with_mock(self, mock_valkey_repo, tmp_path):
        """Validate CLI arg parsing and config loading proceed to server wait."""
        config_path = _write_config(
            tmp_path,
            [
                {
                    "requests": [10],
                    "keyspacelen": [100],
                    "data_sizes": [16],
                    "pipelines": [1],
                    "clients": [1],
                    "commands": ["SET"],
                    "cluster_mode": False,
                    "tls_mode": False,
                    "warmup": 0,
                }
            ],
        )

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "benchmark.py",
                    "--valkey-path",
                    str(mock_valkey_repo.path),
                    "--valkey-benchmark-path",
                    str(mock_valkey_repo.path / "src" / "valkey-benchmark"),
                    "--config",
                    str(config_path),
                    "--results-dir",
                    str(tmp_path / "results"),
                    "--use-running-server",
                    "--mode",
                    "client",
                ],
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT,
                timeout=5,
            )
            combined = result.stdout + result.stderr
            assert result.returncode != 0 or "error" in combined.lower()
        except subprocess.TimeoutExpired as e:
            # Timeout means it got past config loading — expected
            output = (e.stdout or b"").decode() + (e.stderr or b"").decode()
            assert len(output) > 0, "Expected benchmark to start, got no output"

    def test_benchmark_generates_metrics_with_mock_server(
        self, mock_valkey_repo, tmp_path
    ):
        """Test metrics generation using component-level calls."""
        processor = MetricsProcessor(
            commit_id=mock_valkey_repo.get_current_commit()[:8],
            cluster_mode=False,
            tls_mode=False,
            commit_time="2024-01-01T00:00:00Z",
        )

        metrics = processor.create_metrics(
            {
                "rps": "95432.10",
                "avg_latency_ms": "0.523",
                "min_latency_ms": "0.102",
                "p50_latency_ms": "0.412",
                "p95_latency_ms": "0.834",
                "p99_latency_ms": "1.245",
                "max_latency_ms": "4.532",
            },
            command="SET",
            data_size=16,
            pipeline=1,
            clients=1,
            requests=10,
        )

        results_dir = tmp_path / "results" / "test_commit"
        processor.write_metrics(results_dir, [metrics])

        loaded = json.loads((results_dir / "metrics.json").read_text())
        assert loaded[0]["command"] == "SET"
        assert loaded[0]["rps"] == 95432.10

    def test_benchmark_cli_help(self):
        result = subprocess.run(
            [sys.executable, "benchmark.py", "--help"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        assert result.returncode == 0
        for flag in ("--config", "--valkey-path", "--mode"):
            assert flag in result.stdout

    def test_benchmark_cli_validates_args(self):
        result = subprocess.run(
            [sys.executable, "benchmark.py", "--use-running-server"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        assert result.returncode != 0


class TestCompiledBasicFormatRun:
    _CSV_HEADER = (
        '"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms",'
        '"p95_latency_ms","p99_latency_ms","max_latency_ms"'
    )
    _CSV_ROW = '"100000.00","0.500","0.100","0.400","0.800","1.200","2.000"'

    _EXPECTED_BASIC_KEYS = {
        "timestamp",
        "commit",
        "repository",
        "command",
        "data_size",
        "pipeline",
        "clients",
        "rps",
        "avg_latency_ms",
        "min_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "max_latency_ms",
        "cluster_mode",
        "tls",
        "requests",
        "benchmark_mode",
        "warmup",
        "architecture",
    }
    _TEST_GROUPS_ONLY_KEYS = {
        "test_id",
        "test_phase",
        "group",
        "scenario",
        "config_set",
        "status",
        "group_description",
        "scenario_description",
        "config_name",
        "dataset",
        "module_commit",
        "module_commit_timestamp",
    }

    def _canned_csv(self, cmd):
        name = cmd[cmd.index("-t") + 1] if "-t" in cmd else "SCENARIO"
        return f'{self._CSV_HEADER}\n"{name}",{self._CSV_ROW}\n'

    def test_run_ordering_and_metrics_schema(self, tmp_path):
        config_path = _write_config(
            tmp_path,
            [
                {
                    "requests": [100],
                    "keyspacelen": [1000],
                    "data_sizes": [16],
                    "pipelines": [1],
                    "clients": [1],
                    "commands": ["SET", "GET"],
                    "cluster_mode": False,
                    "tls_mode": False,
                    "warmup": 0,
                }
            ],
        )
        cfg = load_configs(str(config_path))[0]

        runner = ClientRunner(
            commit_id="abc123",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=tmp_path,
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            architecture="test-arch",
        )

        invocations = []

        def fake_run(command=None, *args, **kwargs):
            invocations.append(list(command))
            if kwargs.get("capture_output"):
                return SimpleNamespace(stdout=self._canned_csv(list(command)))
            return None

        with (
            patch.object(runner, "_run", side_effect=fake_run),
            patch.object(runner, "_flush_database"),
            patch.object(ClientRunner, "get_commit_time", lambda self, cid: "<TS>"),
            patch(
                "valkey_benchmark.collect_environment_metadata",
                lambda **kwargs: {"numa_nodes": "1"},
            ),
            patch("valkey_benchmark.random.randint", return_value=555),
        ):
            runner.run_benchmark_config()

        order = [(c[c.index("-t") + 1], "--sequential" in c) for c in invocations]
        assert order == [("SET", False), ("SET", True), ("GET", False)]

        metrics = json.loads((tmp_path / "metrics.json").read_text())
        assert [m["command"] for m in metrics] == ["SET", "GET"]
        for row in metrics:
            non_env = {k for k in row if not k.startswith("env_")}
            assert non_env == self._EXPECTED_BASIC_KEYS
            assert not (set(row) & self._TEST_GROUPS_ONLY_KEYS)
