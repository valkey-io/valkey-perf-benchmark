"""Fixtures and utilities for integration tests."""

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import pytest

# Make project root importable for all integration tests
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Mock valkey-benchmark script
# ---------------------------------------------------------------------------

MOCK_BENCHMARK_SCRIPT = '''#!/usr/bin/env python3
"""Mock valkey-benchmark that outputs valid CSV without running real benchmarks."""

import argparse
import random
import sys

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", dest="host", default="127.0.0.1")
    parser.add_argument("-p", dest="port", type=int, default=6379)
    parser.add_argument("-c", dest="clients", type=int, default=50)
    parser.add_argument("-n", dest="requests", type=int, default=100000)
    parser.add_argument("-d", dest="data_size", type=int, default=3)
    parser.add_argument("-P", dest="pipeline", type=int, default=1)
    parser.add_argument("-r", dest="keyspacelen", type=int, default=1)
    parser.add_argument("-t", dest="tests", default="GET")
    parser.add_argument("--csv", action="store_true")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--cert", default=None)
    parser.add_argument("--key", default=None)
    parser.add_argument("--cacert", default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--duration", type=int, default=None)
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--cluster", action="store_true")
    
    args, unknown = parser.parse_known_args()
    
    # Generate mock benchmark results
    commands = args.tests.split(",") if args.tests else ["GET"]
    
    # Use seed for reproducible results if provided
    if args.seed:
        random.seed(args.seed)
    
    # CSV header
    print('"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"')
    
    for cmd in commands:
        # Generate plausible mock metrics
        base_rps = 100000 + random.randint(-10000, 10000)
        rps = base_rps * args.pipeline  # Pipeline increases throughput
        avg_latency = 0.5 + random.random() * 0.5
        min_latency = avg_latency * 0.2
        p50_latency = avg_latency * 0.9
        p95_latency = avg_latency * 1.5
        p99_latency = avg_latency * 2.0
        max_latency = avg_latency * 5.0
        
        print(f'"{cmd}","{rps:.2f}","{avg_latency:.3f}","{min_latency:.3f}","{p50_latency:.3f}","{p95_latency:.3f}","{p99_latency:.3f}","{max_latency:.3f}"')

if __name__ == "__main__":
    main()
'''


class GitRepoFixture:
    """Helper for creating and managing temporary git repositories."""

    def __init__(self, path: Path):
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self._init_repo()

    def _init_repo(self):
        """Initialize git repository with initial commit."""
        self._run_git("init")
        self._run_git("config", "user.email", "test@example.com")
        self._run_git("config", "user.name", "Test User")
        # Create initial file and commit
        (self.path / "README.md").write_text("# Test Repo\n")
        self._run_git("add", ".")
        self._run_git("commit", "-m", "Initial commit")

    def _run_git(self, *args) -> subprocess.CompletedProcess:
        """Run git command in repo directory."""
        return subprocess.run(
            ["git"] + list(args),
            cwd=self.path,
            capture_output=True,
            text=True,
            check=True,
        )

    def create_commit(
        self, message: str, files: Optional[Dict[str, str]] = None
    ) -> str:
        """Create a commit with optional file changes. Returns commit SHA."""
        if files:
            for filename, content in files.items():
                filepath = self.path / filename
                filepath.parent.mkdir(parents=True, exist_ok=True)
                filepath.write_text(content)
                self._run_git("add", filename)
        else:
            # Touch a file to create a change
            marker = self.path / ".marker"
            marker.write_text(message)
            self._run_git("add", ".marker")

        self._run_git("commit", "-m", message)
        result = self._run_git("rev-parse", "HEAD")
        return result.stdout.strip()

    def create_branch(self, branch_name: str, from_ref: str = "HEAD") -> None:
        """Create a new branch from a reference."""
        self._run_git("checkout", "-b", branch_name, from_ref)

    def checkout(self, ref: str) -> None:
        """Checkout a branch or commit."""
        self._run_git("checkout", ref)

    def get_current_commit(self) -> str:
        """Get current HEAD commit SHA."""
        result = self._run_git("rev-parse", "HEAD")
        return result.stdout.strip()

    def create_mock_valkey_structure(self) -> None:
        """Create minimal Valkey directory structure for testing."""
        src_dir = self.path / "src"
        src_dir.mkdir(exist_ok=True)

        # Create mock valkey-server (just needs to exist)
        server = src_dir / "valkey-server"
        server.write_text("#!/bin/bash\necho 'mock server'\n")
        server.chmod(server.stat().st_mode | stat.S_IEXEC)

        # Create mock valkey-benchmark using our mock script
        benchmark = src_dir / "valkey-benchmark"
        benchmark.write_text(MOCK_BENCHMARK_SCRIPT)
        benchmark.chmod(benchmark.stat().st_mode | stat.S_IEXEC)

        # Create TLS directory structure (for TLS tests)
        tls_dir = self.path / "tests" / "tls"
        tls_dir.mkdir(parents=True, exist_ok=True)
        (tls_dir / "valkey.crt").write_text("mock cert")
        (tls_dir / "valkey.key").write_text("mock key")
        (tls_dir / "ca.crt").write_text("mock ca")

        self._run_git("add", ".")
        self._run_git("commit", "-m", "Add mock valkey structure")


class MockBenchmarkBinary:
    """Creates a standalone mock valkey-benchmark executable."""

    def __init__(self, path: Path):
        self.path = path
        self._create_mock()

    def _create_mock(self):
        """Create the mock benchmark script."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(MOCK_BENCHMARK_SCRIPT)
        self.path.chmod(self.path.stat().st_mode | stat.S_IEXEC)

    @property
    def executable(self) -> str:
        return str(self.path)


# ---------------------------------------------------------------------------
# Pytest Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_dir(tmp_path):
    """Provide a temporary directory that's cleaned up after test."""
    yield tmp_path
    # Cleanup handled by pytest tmp_path


@pytest.fixture
def git_repo(tmp_path) -> GitRepoFixture:
    """Create a temporary git repository."""
    repo_path = tmp_path / "test_repo"
    return GitRepoFixture(repo_path)


@pytest.fixture
def mock_valkey_repo(tmp_path) -> GitRepoFixture:
    """Create a mock Valkey repository with necessary structure."""
    repo_path = tmp_path / "valkey"
    repo = GitRepoFixture(repo_path)
    repo.create_mock_valkey_structure()
    return repo


@pytest.fixture
def mock_benchmark_binary(tmp_path) -> MockBenchmarkBinary:
    """Create a standalone mock valkey-benchmark binary."""
    binary_path = tmp_path / "bin" / "valkey-benchmark"
    return MockBenchmarkBinary(binary_path)


@pytest.fixture
def minimal_benchmark_config() -> Dict:
    """Minimal benchmark configuration for fast tests."""
    return {
        "requests": [10],
        "keyspacelen": [100],
        "data_sizes": [16],
        "pipelines": [1],
        "clients": [1],
        "commands": ["GET", "SET"],
        "cluster_mode": False,
        "tls_mode": False,
        "warmup": 0,
    }


@pytest.fixture
def minimal_config_file(tmp_path, minimal_benchmark_config) -> Path:
    """Create a minimal config file for testing."""
    config_path = tmp_path / "configs" / "test-config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps([minimal_benchmark_config], indent=2))
    return config_path


@pytest.fixture
def results_dir(tmp_path) -> Path:
    """Create a results directory for test output."""
    results = tmp_path / "results"
    results.mkdir(parents=True, exist_ok=True)
    return results


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def create_sample_metrics(
    commit: str,
    command: str = "GET",
    rps: float = 100000.0,
    pipeline: int = 1,
) -> Dict:
    """Create a sample metrics dict for testing."""
    return {
        "timestamp": "2024-01-01T00:00:00Z",
        "commit": commit,
        "command": command,
        "data_size": 16,
        "pipeline": pipeline,
        "clients": 1,
        "requests": 10,
        "rps": rps,
        "avg_latency_ms": 0.5,
        "min_latency_ms": 0.1,
        "p50_latency_ms": 0.4,
        "p95_latency_ms": 0.8,
        "p99_latency_ms": 1.2,
        "max_latency_ms": 2.0,
        "cluster_mode": False,
        "tls": False,
    }


def write_metrics_file(path: Path, metrics: List[Dict]) -> None:
    """Write metrics to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2))


def read_metrics_file(path: Path) -> List[Dict]:
    """Read metrics from a JSON file."""
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Comparison Helper
# ---------------------------------------------------------------------------


def run_comparison(
    baseline_path: Path,
    new_path: Path,
    output_path: Path,
    extra_args: Optional[List[str]] = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    """Run compare_benchmark_results.py and return the result.

    Args:
        baseline_path: Path to baseline metrics JSON.
        new_path: Path to new metrics JSON.
        output_path: Path for the markdown output.
        extra_args: Additional CLI arguments (e.g. ["--metrics", "rps"]).
        check: If True, raise on non-zero exit code.
    """
    cmd = [
        sys.executable,
        "utils/compare_benchmark_results.py",
        "--baseline",
        str(baseline_path),
        "--new",
        str(new_path),
        "--output",
        str(output_path),
    ]
    if extra_args:
        cmd.extend(extra_args)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        check=check,
    )


def run_comparison_with_metrics(
    tmp_path: Path,
    baseline_metrics: List[Dict],
    new_metrics: List[Dict],
    extra_args: Optional[List[str]] = None,
) -> str:
    """Write metrics files, run comparison, return output markdown content.

    Convenience wrapper for the common pattern of creating two metrics files,
    running the comparison script, and reading the result.

    Returns:
        The markdown content of the comparison output.

    Raises:
        AssertionError: If the comparison script exits with non-zero status.
    """
    baseline_path = tmp_path / "baseline" / "metrics.json"
    new_path = tmp_path / "new" / "metrics.json"
    output_path = tmp_path / "comparison.md"

    write_metrics_file(baseline_path, baseline_metrics)
    write_metrics_file(new_path, new_metrics)

    result = run_comparison(baseline_path, new_path, output_path, extra_args)
    assert result.returncode == 0, f"Comparison failed: {result.stderr}"
    return output_path.read_text()


# ---------------------------------------------------------------------------
# Metrics Processor Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def metrics_processor():
    """Create a default MetricsProcessor for testing."""
    from process_metrics import MetricsProcessor

    return MetricsProcessor(
        commit_id="abc123",
        cluster_mode=False,
        tls_mode=False,
        commit_time="2024-01-01T00:00:00Z",
    )
