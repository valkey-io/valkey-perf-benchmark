"""Shared fixtures and path setup for the valkey-perf-benchmark test suite."""

import sys
from pathlib import Path

# Add repo root to sys.path so source modules are importable
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from valkey_benchmark import ClientRunner


@pytest.fixture
def minimal_valid_config():
    """Minimal valid config in commands format."""
    return {
        "keyspacelen": [1000],
        "data_sizes": [64],
        "pipelines": [1],
        "clients": [50],
        "commands": ["GET", "SET"],
        "cluster_mode": False,
        "tls_mode": False,
        "warmup": 0,
        "requests": [1000],
    }


@pytest.fixture
def minimal_test_groups_config():
    """Minimal valid config in test_groups format."""
    return {
        "cluster_mode": False,
        "tls_mode": False,
        "test_groups": [
            {
                "group": 1,
                "scenarios": [
                    {"id": "test1", "command": "SET foo bar", "type": "write"}
                ],
            }
        ],
    }


@pytest.fixture
def sample_benchmark_data():
    """Sample benchmark CSV data dict."""
    return {
        "rps": "150000.00",
        "avg_latency_ms": "0.500",
        "min_latency_ms": "0.100",
        "p50_latency_ms": "0.400",
        "p95_latency_ms": "0.800",
        "p99_latency_ms": "1.200",
        "max_latency_ms": "5.000",
    }


@pytest.fixture
def minimal_client_runner(minimal_valid_config):
    """Create a minimal ClientRunner instance with sensible defaults."""
    return ClientRunner(
        commit_id="abc123",
        config=minimal_valid_config,
        cluster_mode=False,
        tls_mode=False,
        target_ip="127.0.0.1",
        results_dir=Path("/tmp/test_results"),
        valkey_path="/tmp/valkey",
        valkey_benchmark_path="src/valkey-benchmark",
    )
