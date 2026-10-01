"""Tests for configs/tiering-zipfian-80-20.json, the data tiering port."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchmark import load_configs, validate_cpu_allocation
from utils.cpu_utils import calculate_client_cpu_ranges, calculate_server_cpu_ranges
from valkey_benchmark import ClientRunner

CONFIG_PATH = "configs/tiering-zipfian-80-20.json"


@pytest.fixture
def config():
    return load_configs(CONFIG_PATH)[0]


@pytest.fixture
def scenario(config):
    return config["test_groups"][0]["scenarios"][0]


@pytest.fixture
def runner(config):
    runner = ClientRunner(
        commit_id="abc123",
        config=config,
        cluster_mode=False,
        tls_mode=False,
        target_ip="127.0.0.1",
        results_dir=Path("/tmp/test_results"),
        valkey_path="/tmp/valkey",
        valkey_benchmark_path="src/valkey-benchmark",
    )
    runner.client_cpu_ranges = calculate_client_cpu_ranges(config)
    return runner


def test_config_validates(config):
    validate_cpu_allocation(config)
    assert config["test_name"] == "tiering-zipfian-80-20"


def test_populate_argv(runner, scenario):
    captured = []

    def fake_run(command=None, *args, **kwargs):
        captured.append(command)
        return MagicMock()

    with (
        patch.object(runner, "_run", side_effect=fake_run),
        patch.object(runner, "_count_loaded_keys", return_value=4000000),
    ):
        runner._populate_scenario_keyspace(scenario, 111)

    argv = captured[0]
    assert argv[argv.index("--keysize") + 1] == "100"
    assert "--sequential" in argv
    assert "--zipfian" not in argv
    assert argv[argv.index("-c") + 1] == "50"
    assert argv[argv.index("-n") + 1] == "4000000"


def test_mixed_children_carry_the_zipfian_passthrough(runner, scenario):
    writes, reads = runner._normalize_mixed_configs(scenario)

    for child in writes + reads:
        argv = runner._build_benchmark_command(child)
        zipfian = argv.index("--zipfian")
        assert argv[zipfian : zipfian + 4] == ["--zipfian", "1.0", "--keysize", "100"]
        assert zipfian < argv.index("--csv")
        assert argv[argv.index("-r") + 1] == "4000000"
        assert argv[argv.index("-d") + 1] == "512"


def test_mixed_processes_pin_to_the_two_client_ranges(runner):
    assert calculate_server_cpu_ranges(runner.config) == ["0-7"]
    assert runner._get_cpu_for_mixed_process(0) == "8-31"
    assert runner._get_cpu_for_mixed_process(1) == "32-55"
