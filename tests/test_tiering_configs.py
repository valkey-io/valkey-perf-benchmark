"""Tests for every data tiering scenario in configs/tiering.json."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from benchmark import load_configs, validate_config, validate_cpu_allocation
from utils.cpu_utils import calculate_client_cpu_ranges
from valkey_benchmark import ClientRunner

CONFIGS = load_configs("configs/tiering.json")


def _runner(config):
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


def _assert_100_byte_keys(argv):
    """Keys are 100 bytes, through --keysize or spelled out in the command."""
    if "--" in argv:
        assert "--keysize" not in argv
        assert len(argv[argv.index("--") + 2]) == 100
    else:
        assert argv[argv.index("--keysize") + 1] == "100"


def _scenarios():
    for config in CONFIGS:
        for group in config["test_groups"]:
            for scenario in group["scenarios"]:
                yield pytest.param(config, scenario, id=scenario["id"])


def test_all_dashboard_tests_present():
    groups = {
        group["group"]: [s["id"] for s in group["scenarios"]]
        for config in CONFIGS
        for group in config["test_groups"]
    }
    assert groups == {
        1: ["zipf80"],
        2: ["unif80"],
        3: ["bal50"],
        4: ["zipfttl"],
        5: ["v500", "v5k"],
    }


@pytest.mark.parametrize("config", CONFIGS, ids=["1gb", "value-sizes"])
def test_config_validates(config):
    validate_config(config)
    validate_cpu_allocation(config)
    assert config["test_name"] == "tiering"
    assert config["build_args"] == ["BUILD_EXT_STORAGE=yes"]
    assert config["custom-server-configs"]["ext-storage-enabled"] == "yes"


@pytest.mark.parametrize("config, scenario", _scenarios())
def test_populate_loads_the_whole_keyspace(config, scenario):
    runner = _runner(config)
    captured = []

    def fake_run(command=None, *args, **kwargs):
        captured.append(command)
        return MagicMock()

    with (
        patch.object(runner, "_run", side_effect=fake_run),
        patch.object(
            runner, "_count_loaded_keys", return_value=scenario["keyspacelen"]
        ),
    ):
        runner._populate_scenario_keyspace(scenario, 111)

    argv = captured[0]
    assert argv[argv.index("-n") + 1] == str(scenario["keyspacelen"])
    assert argv[argv.index("-d") + 1] == str(scenario["data_size"])
    _assert_100_byte_keys(argv)
    assert "--zipfian" not in argv


@pytest.mark.parametrize("config, scenario", _scenarios())
def test_mixed_children_argv(config, scenario):
    runner = _runner(config)
    writes, reads = runner._normalize_mixed_configs(scenario)
    zipfian = "--zipfian 1.0" in scenario["benchmark_args"]

    for child in writes + reads:
        argv = runner._build_benchmark_command(child)
        assert argv[argv.index("-r") + 1] == str(scenario["keyspacelen"])
        assert argv[argv.index("-d") + 1] == str(scenario["data_size"])
        _assert_100_byte_keys(argv)
        assert ("--zipfian" in argv) == zipfian
    assert sum(c["clients"] for c in writes + reads) == 200


def test_ttl_keys_match_the_loaded_keys_and_expire_during_the_run():
    scenario = next(
        s
        for c in CONFIGS
        for g in c["test_groups"]
        for s in g["scenarios"]
        if s["id"] == "zipfttl"
    )
    key = "a" * 88 + "__rand_int__"
    assert scenario["populate_with"] == f"SET {key} __data__ EX 120"
    assert scenario["writes"][0]["command"] == f"SET {key} __data__ EX 120"
    assert scenario["reads"][0]["command"] == f"GET {key}"
    assert scenario["duration"] > 120
