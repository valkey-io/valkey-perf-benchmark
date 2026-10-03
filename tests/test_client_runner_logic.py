"""Unit tests for pure logic methods on ClientRunner from valkey_benchmark.py."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from valkey_benchmark import ClientRunner, ORIGIN_FIELD, ORIGIN_SIMPLE
from process_metrics import MetricsProcessor


def _make_csv(rows):
    """Build CSV stdout from metric dictionaries."""
    header = '"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"'
    lines = [header]
    for r in rows:
        lines.append(
            f'"{r.get("test","GET")}","{r["rps"]}","{r["avg_latency_ms"]}",'
            f'"{r["min_latency_ms"]}","{r["p50_latency_ms"]}","{r["p95_latency_ms"]}",'
            f'"{r["p99_latency_ms"]}","{r["max_latency_ms"]}"'
        )
    return "\n".join(lines)


def _metric_row(test="GET", rps="1000.0"):
    return {
        "test": test,
        "rps": rps,
        "avg_latency_ms": "1.0",
        "min_latency_ms": "0.5",
        "p50_latency_ms": "1.0",
        "p95_latency_ms": "2.0",
        "p99_latency_ms": "3.0",
        "max_latency_ms": "5.0",
    }


def _invoke_scenario(
    runner,
    scenario,
    *,
    metrics_processor,
    profiler=None,
    group_id=1,
    config_set=None,
    config_suffix="default",
):
    """Run a scenario with common test defaults."""
    return runner._run_single_scenario(
        scenario,
        group_id=group_id,
        profiler=profiler,
        metrics_processor=metrics_processor,
        config_set={} if config_set is None else config_set,
        config_suffix=config_suffix,
    )


def _run_mixed(
    runner,
    scenario,
    *,
    metrics_processor,
    group_id=1,
    config_set=None,
    warmup_duration=0,
):
    """Run a mixed scenario with common test defaults."""
    return runner._run_mixed_workload(
        scenario,
        group_id=group_id,
        config_set={} if config_set is None else config_set,
        metrics_processor=metrics_processor,
        warmup_duration=warmup_duration,
    )


def _seed_of(command):
    assert "--seed" in command, f"no --seed in command: {command}"
    return command[command.index("--seed") + 1]


# ---------------------------------------------------------------------------
# _aggregate_parallel_results
# ---------------------------------------------------------------------------


class TestAggregateParallelResults:
    """Tests for ClientRunner._aggregate_parallel_results."""

    def _make_result(self, rps, avg, mn, p50, p95, p99, mx, port=6379, test="GET"):
        """Return a (stdout, stderr, port) tuple with valid CSV."""
        csv_str = _make_csv(
            [
                {
                    "test": test,
                    "rps": rps,
                    "avg_latency_ms": avg,
                    "min_latency_ms": mn,
                    "p50_latency_ms": p50,
                    "p95_latency_ms": p95,
                    "p99_latency_ms": p99,
                    "max_latency_ms": mx,
                }
            ]
        )
        return (csv_str, "", port)

    def test_single_node(self, minimal_client_runner):
        results = [
            self._make_result(
                "100000", "0.5", "0.1", "0.4", "0.8", "1.2", "5.0", port=6379
            )
        ]
        scenario = {"command": "GET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert agg["test"] == "GET"
        assert float(agg["rps"]) == pytest.approx(100000.0)
        assert float(agg["avg_latency_ms"]) == pytest.approx(0.5)
        assert float(agg["min_latency_ms"]) == pytest.approx(0.1)
        assert float(agg["max_latency_ms"]) == pytest.approx(5.0)

    def test_two_nodes_sums_rps(self, minimal_client_runner):
        results = [
            self._make_result(
                "60000", "0.4", "0.1", "0.3", "0.7", "1.0", "4.0", port=6379
            ),
            self._make_result(
                "40000", "0.6", "0.2", "0.5", "0.9", "1.4", "6.0", port=6380
            ),
        ]
        scenario = {"command": "SET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert float(agg["rps"]) == pytest.approx(100000.0)

    def test_two_nodes_weighted_avg_latency(self, minimal_client_runner):
        # node1: rps=60000, avg=0.4  node2: rps=40000, avg=0.6
        # weighted avg = (60000*0.4 + 40000*0.6) / 100000 = 0.48
        results = [
            self._make_result(
                "60000", "0.4", "0.1", "0.3", "0.7", "1.0", "4.0", port=6379
            ),
            self._make_result(
                "40000", "0.6", "0.2", "0.5", "0.9", "1.4", "6.0", port=6380
            ),
        ]
        scenario = {"command": "SET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert float(agg["avg_latency_ms"]) == pytest.approx(0.48)

    def test_min_of_min_latency(self, minimal_client_runner):
        results = [
            self._make_result(
                "50000", "0.5", "0.3", "0.4", "0.8", "1.2", "5.0", port=6379
            ),
            self._make_result(
                "50000", "0.5", "0.1", "0.4", "0.8", "1.2", "5.0", port=6380
            ),
        ]
        scenario = {"command": "GET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert float(agg["min_latency_ms"]) == pytest.approx(0.1)

    def test_max_of_max_latency(self, minimal_client_runner):
        results = [
            self._make_result(
                "50000", "0.5", "0.1", "0.4", "0.8", "1.2", "3.0", port=6379
            ),
            self._make_result(
                "50000", "0.5", "0.1", "0.4", "0.8", "1.2", "7.0", port=6380
            ),
        ]
        scenario = {"command": "GET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert float(agg["max_latency_ms"]) == pytest.approx(7.0)

    def test_returns_string_values(self, minimal_client_runner):
        results = [
            self._make_result("100000", "0.5", "0.1", "0.4", "0.8", "1.2", "5.0")
        ]
        scenario = {"command": "GET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        for key in (
            "rps",
            "avg_latency_ms",
            "min_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
            "max_latency_ms",
        ):
            assert isinstance(agg[key], str)

    def test_no_valid_metrics_raises(self, minimal_client_runner):
        # Empty stdout → _parse_csv_row returns None → no metrics
        results = [("", "", 6379)]
        scenario = {"command": "GET"}

        with pytest.raises(RuntimeError, match="No valid metrics"):
            minimal_client_runner._aggregate_parallel_results(results, scenario)

    def test_skips_unparseable_rows(self, minimal_client_runner):
        good = self._make_result(
            "80000", "0.5", "0.1", "0.4", "0.8", "1.2", "5.0", port=6379
        )
        bad = ("not csv at all", "", 6380)
        results = [good, bad]
        scenario = {"command": "GET"}

        agg = minimal_client_runner._aggregate_parallel_results(results, scenario)

        assert float(agg["rps"]) == pytest.approx(80000.0)


# ---------------------------------------------------------------------------
# _is_cme
# ---------------------------------------------------------------------------


class TestIsCme:
    """Tests for ClientRunner._is_cme."""

    def test_not_cluster_mode(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = False
        assert minimal_client_runner._is_cme() is False

    def test_cluster_mode_single_node(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config["cluster_nodes"] = 1
        assert minimal_client_runner._is_cme() is False

    def test_cluster_mode_multiple_nodes(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config["cluster_nodes"] = 3
        assert minimal_client_runner._is_cme() is True

    def test_cluster_mode_no_cluster_nodes_key(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config.pop("cluster_nodes", None)
        # defaults to 1 → not CME
        assert minimal_client_runner._is_cme() is False


# ---------------------------------------------------------------------------
# _should_use_parallel
# ---------------------------------------------------------------------------


class TestShouldUseParallel:
    """Tests for ClientRunner._should_use_parallel."""

    def test_cme_with_parallel(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config["cluster_nodes"] = 3
        scenario = {"cluster_execution": "parallel"}
        assert minimal_client_runner._should_use_parallel(scenario) is True

    def test_cme_with_single(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config["cluster_nodes"] = 3
        scenario = {"cluster_execution": "single"}
        assert minimal_client_runner._should_use_parallel(scenario) is False

    def test_cme_default_execution(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config["cluster_nodes"] = 3
        scenario = {}  # defaults to "single"
        assert minimal_client_runner._should_use_parallel(scenario) is False

    def test_not_cme(self, minimal_client_runner):
        minimal_client_runner.cluster_mode = False
        scenario = {"cluster_execution": "parallel"}
        assert minimal_client_runner._should_use_parallel(scenario) is False


# ---------------------------------------------------------------------------
# compile_simple_config (benchmark.py) replaces _generate_combinations:
# the basic 'commands' format now compiles into generated test_groups.
# ---------------------------------------------------------------------------


class TestCompileSimpleConfig:
    """Tests for benchmark.compile_simple_config."""

    def test_default_config(self, minimal_valid_config):
        from benchmark import compile_simple_config

        compile_simple_config(minimal_valid_config)
        groups = minimal_valid_config["test_groups"]
        assert len(groups) == 2
        assert all(len(g["scenarios"]) == 1 for g in groups)

    def test_cartesian_product_count(self, minimal_valid_config):
        from benchmark import compile_simple_config

        minimal_valid_config["data_sizes"] = [64, 128]
        minimal_valid_config["pipelines"] = [1, 10]
        compile_simple_config(minimal_valid_config)
        assert len(minimal_valid_config["test_groups"]) == 8

    def test_generated_scenario_structure(self, minimal_valid_config):
        from benchmark import compile_simple_config
        from valkey_benchmark import ORIGIN_FIELD, ORIGIN_SIMPLE

        compile_simple_config(minimal_valid_config)
        first = minimal_valid_config["test_groups"][0]["scenarios"][0]

        assert first[ORIGIN_FIELD] == ORIGIN_SIMPLE
        assert first["test"] == "GET"
        assert first["requests"] == 1000
        assert first["keyspacelen"] == 1000
        assert first["data_size"] == 64
        assert first["pipeline"] == 1
        assert first["clients"] == 50
        assert first["warmup_inline"] == 0
        assert first["restart_before"] is True
        assert first["populate_with"] == "SET"
        second = minimal_valid_config["test_groups"][1]["scenarios"][0]
        assert second["test"] == "SET"
        assert "populate_with" not in second

    def test_duration_mode(self, minimal_valid_config):
        from benchmark import compile_simple_config

        del minimal_valid_config["requests"]
        minimal_valid_config["duration"] = 30
        compile_simple_config(minimal_valid_config)

        first = minimal_valid_config["test_groups"][0]["scenarios"][0]
        assert first["duration"] == 30
        assert "requests" not in first

    def test_unsupported_command_dropped(self, minimal_valid_config):
        from benchmark import compile_simple_config

        minimal_valid_config["commands"] = ["GET", "XRANGE"]
        compile_simple_config(minimal_valid_config)

        tests = [
            s["test"]
            for g in minimal_valid_config["test_groups"]
            for s in g["scenarios"]
        ]
        assert tests == ["GET"]

    def test_groups_numbered_sequentially(self, minimal_valid_config):
        from benchmark import compile_simple_config

        compile_simple_config(minimal_valid_config)
        assert [g["group"] for g in minimal_valid_config["test_groups"]] == [1, 2]

    def test_compiled_config_passes_validation(self, minimal_valid_config):
        from benchmark import compile_simple_config, validate_test_groups

        compile_simple_config(minimal_valid_config)
        validate_test_groups(minimal_valid_config)  # should not raise


# ---------------------------------------------------------------------------
# _create_failure_marker
# ---------------------------------------------------------------------------


class TestCreateFailureMarker:
    @staticmethod
    def _processor(runner, commit_time="<TS>"):
        return MetricsProcessor(
            commit_id=runner.commit_id,
            cluster_mode=runner.cluster_mode,
            tls_mode=runner.tls_mode,
            commit_time=commit_time,
            io_threads=runner.io_threads,
            benchmark_threads=runner.benchmark_threads,
            architecture=runner.architecture,
            repository=runner.repository,
        )

    def _marker(self, runner, **overrides):
        fields = dict(
            test_id="1_test1",
            test_phase="write",
            group_id=1,
            scenario_id="test1",
            error="timeout",
            command="SET foo bar",
            data_size=16,
            pipeline=1,
            clients=50,
            requests=1000,
            config_set={"io_threads": 4},
        )
        fields.update(overrides)
        workload = {
            "command": fields.pop("command"),
            "data_size": fields.pop("data_size"),
            "pipeline": fields.pop("pipeline"),
            "clients": fields.pop("clients"),
        }
        if "iteration" in fields:
            workload["iteration"] = fields.pop("iteration")
        return runner._create_failure_marker(
            self._processor(runner), workload, **fields
        )

    def test_marker_base_identity_and_config_fields(self, minimal_client_runner):
        marker = self._marker(minimal_client_runner, config_set={}, iteration=2)

        assert marker["test_id"] == "1_test1"
        assert marker["test_phase"] == "write"
        assert marker["status"] == "failed"
        assert marker["error"] == "timeout"
        assert marker["command"] == "SET foo bar"
        assert marker["group"] == 1
        assert marker["scenario"] == "test1"
        assert marker["config_set"] == {}
        assert marker["cluster_mode"] is False
        assert marker["tls"] is False
        assert marker["commit"] == "abc123"
        assert marker["repository"] is None
        assert marker["data_size"] == 16
        assert marker["pipeline"] == 1
        assert marker["clients"] == 50
        assert marker["requests"] == 1000
        assert marker["benchmark_mode"] == "requests"
        assert marker["iteration"] == 2

    def test_marker_has_no_performance_fields_and_unset_axes_absent(
        self, minimal_client_runner
    ):
        marker = self._marker(minimal_client_runner, config_set={})

        for key in (
            "rps",
            "avg_latency_ms",
            "min_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
            "max_latency_ms",
        ):
            assert key not in marker
        assert "io_threads" not in marker
        assert "valkey_benchmark_threads" not in marker
        assert "architecture" not in marker

    def test_marker_module_and_optional_axis_attribution(self):
        module_runner = ClientRunner(
            commit_id="core_sha",
            config={},
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            config_name="fts.json",
            module_commit="mod_sha",
            module_commit_timestamp="2026-01-01T00:00:00Z",
        )
        module_marker = self._marker(module_runner, command="FT.SEARCH", config_set={})
        assert module_marker["module_commit"] == "mod_sha"
        assert module_marker["module_commit_timestamp"] == "2026-01-01T00:00:00Z"
        assert module_marker["config_name"] == "fts.json"
        assert module_marker["status"] == "failed"
        assert "rps" not in module_marker

        axis_runner = ClientRunner(
            commit_id="c1",
            config={"cluster_mode": True, "tls_mode": True},
            cluster_mode=True,
            tls_mode=True,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            io_threads=8,
            benchmark_threads=2,
            architecture="aarch64",
            repository="valkey-io/valkey",
        )
        axis_marker = self._marker(
            axis_runner,
            test_id="2_b",
            group_id=2,
            scenario_id="b",
            test_phase="read",
            error="err",
            command="GET",
            config_set={"maxmemory": "1gb"},
        )
        assert axis_marker["cluster_mode"] is True
        assert axis_marker["tls"] is True
        assert axis_marker["io_threads"] == 8
        assert axis_marker["valkey_benchmark_threads"] == 2
        assert axis_marker["architecture"] == "aarch64"
        assert axis_marker["repository"] == "valkey-io/valkey"
        assert axis_marker["config_set"] == {"maxmemory": "1gb"}


# ---------------------------------------------------------------------------
# _iterate_test_groups_scenarios
# ---------------------------------------------------------------------------


class TestIterateTestGroupsScenarios:
    """Tests for ClientRunner._iterate_test_groups_scenarios.

    Verifies that group_description (and per-scenario description) from
    the config flow through to each yielded scenario item so they can
    be picked up downstream when metrics are written.
    """

    def _runner_with_groups(self, minimal_valid_config, test_groups):
        cfg = {**minimal_valid_config, "test_groups": test_groups}
        return ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )

    def test_builds_iteration_sequence(self, minimal_valid_config):
        test_groups = [
            {
                "group": 1,
                "scenarios": [
                    {"id": "load", "command": "SET item:0 value", "type": "write"}
                ],
                "iterations": {
                    "count": 3,
                    "scenarios": [
                        {
                            "id": "mutate",
                            "command": "SET item:{iteration} value",
                            "dataset": "updates-{iteration}.csv",
                            "type": "write",
                        },
                        {
                            "id": "sample",
                            "command": "GET item:{iteration}",
                            "on_iterations": [1, 3],
                            "type": "read",
                        },
                    ],
                },
            }
        ]
        runner = self._runner_with_groups(minimal_valid_config, test_groups)

        scenarios = [
            item["scenario"] for item in runner._iterate_test_groups_scenarios()
        ]

        assert [scenario["id"] for scenario in scenarios] == [
            "load",
            "mutate",
            "sample",
            "mutate",
            "mutate",
            "sample",
        ]
        assert [scenario.get("iteration") for scenario in scenarios] == [
            None,
            1,
            1,
            2,
            3,
            3,
        ]
        assert scenarios[1]["command"] == "SET item:1 value"
        assert scenarios[4]["dataset"] == "updates-3.csv"
        assert test_groups[0]["iterations"]["scenarios"][0]["command"] == (
            "SET item:{iteration} value"
        )

    def test_yields_group_description(self, minimal_valid_config):
        runner = self._runner_with_groups(
            minimal_valid_config,
            [
                {
                    "group": 1,
                    "description": "small payload latency",
                    "scenarios": [{"id": "s1", "command": "GET", "type": "read"}],
                }
            ],
        )

        items = list(runner._iterate_test_groups_scenarios())

        assert len(items) == 1
        assert items[0]["group_id"] == 1
        assert items[0]["group_description"] == "small payload latency"

    def test_group_description_is_none_when_missing(self, minimal_valid_config):
        runner = self._runner_with_groups(
            minimal_valid_config,
            [
                {
                    "group": 2,
                    "scenarios": [{"id": "s1", "command": "SET", "type": "write"}],
                }
            ],
        )

        items = list(runner._iterate_test_groups_scenarios())

        assert items[0]["group_description"] is None

    def test_description_propagates_to_every_scenario(self, minimal_valid_config):
        runner = self._runner_with_groups(
            minimal_valid_config,
            [
                {
                    "group": 3,
                    "description": "shared desc",
                    "scenarios": [
                        {"id": "a", "command": "GET", "type": "read"},
                        {"id": "b", "command": "SET", "type": "write"},
                    ],
                }
            ],
        )

        items = list(runner._iterate_test_groups_scenarios())

        assert len(items) == 2
        assert all(i["group_description"] == "shared desc" for i in items)

    def test_distinct_descriptions_across_groups(self, minimal_valid_config):
        runner = self._runner_with_groups(
            minimal_valid_config,
            [
                {
                    "group": 1,
                    "description": "first",
                    "scenarios": [{"id": "s1", "command": "GET", "type": "read"}],
                },
                {
                    "group": 2,
                    "description": "second",
                    "scenarios": [{"id": "s2", "command": "SET", "type": "write"}],
                },
            ],
        )

        items = list(runner._iterate_test_groups_scenarios())

        by_group = {i["group_id"]: i["group_description"] for i in items}
        assert by_group == {1: "first", 2: "second"}

    def test_both_group_and_scenario_description_present(self, minimal_valid_config):
        # group_description rides on the yielded item; per-scenario
        # "description" stays on the inner scenario dict and is read later
        # by _run_single_scenario when it builds the metrics row.
        runner = self._runner_with_groups(
            minimal_valid_config,
            [
                {
                    "group": 4,
                    "description": "latency suite",
                    "scenarios": [
                        {
                            "id": "s1",
                            "command": "GET",
                            "type": "read",
                            "description": "GET, 64B, pipeline=1",
                        }
                    ],
                }
            ],
        )

        items = list(runner._iterate_test_groups_scenarios())

        assert len(items) == 1
        assert items[0]["group_description"] == "latency suite"
        assert items[0]["scenario"]["description"] == "GET, 64B, pipeline=1"


# ---------------------------------------------------------------------------
# Compiled basic-format scenarios through _iterate_test_groups_scenarios:
# replaces the deleted _iterate_simple_scenarios path.
# ---------------------------------------------------------------------------


class TestIterateCompiledScenarios:
    """Compiled basic-format groups flow through the unified iterator."""

    def _compiled_runner(self, config, cluster_mode=False):
        from benchmark import compile_simple_config

        compile_simple_config(config)
        return ClientRunner(
            commit_id="abc123",
            config=config,
            cluster_mode=cluster_mode,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )

    def test_compiled_scenarios_yielded_per_group(self, minimal_valid_config):
        runner = self._compiled_runner(minimal_valid_config)

        items = list(runner._iterate_test_groups_scenarios())

        assert len(items) == 2  # GET, SET
        assert [i["scenario"]["test"] for i in items] == ["GET", "SET"]
        assert [i["group_id"] for i in items] == [1, 2]
        assert all(i["group_description"] is None for i in items)

    def test_mset_mget_skipped_in_cluster_mode(self, minimal_valid_config):
        minimal_valid_config["commands"] = ["MSET", "MGET", "SET"]
        runner = self._compiled_runner(minimal_valid_config, cluster_mode=True)

        items = list(runner._iterate_test_groups_scenarios())

        assert [i["scenario"]["test"] for i in items] == ["SET"]

    def test_mset_mget_kept_outside_cluster_mode(self, minimal_valid_config):
        minimal_valid_config["commands"] = ["MSET", "MGET", "SET"]
        runner = self._compiled_runner(minimal_valid_config, cluster_mode=False)

        items = list(runner._iterate_test_groups_scenarios())

        assert [i["scenario"]["test"] for i in items] == ["MSET", "MGET", "SET"]

    def test_runs_repeat_each_compiled_group_consecutively(self, minimal_valid_config):
        from benchmark import compile_simple_config

        compile_simple_config(minimal_valid_config)
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            runs=2,
        )

        items = list(runner._iterate_test_groups_scenarios())

        assert [i["scenario"]["test"] for i in items] == ["GET", "GET", "SET", "SET"]


# ---------------------------------------------------------------------------
# Mixed workload: _normalize_mixed_configs, _get_cpu_for_mixed_process,
# _run_mixed_workload
# ---------------------------------------------------------------------------


@pytest.fixture
def mixed_runner(minimal_valid_config):
    """ClientRunner configured for mixed-workload tests.

    Adds cpu_allocation (used by _get_cpu_for_mixed_process) and a set of
    client CPU ranges wide enough to test partitioning.
    """
    cfg = {
        **minimal_valid_config,
        "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 2},
    }
    runner = ClientRunner(
        commit_id="abc123",
        config=cfg,
        cluster_mode=False,
        tls_mode=False,
        target_ip="127.0.0.1",
        results_dir=Path("/tmp/test_results"),
        valkey_path="/tmp/valkey",
        valkey_benchmark_path="src/valkey-benchmark",
        config_name="fts-mixed-test.json",
    )
    runner.client_cpu_ranges = ["10-11", "12-13", "14-15"]
    return runner


@pytest.fixture
def mixed_scenario():
    """Representative mixed scenario with 1 write + 2 read sub-scenarios."""
    return {
        "id": "j",
        "type": "mixed",
        "duration": 60,
        "pipeline": 2,
        "writes": [{"id": "w1", "command": "HSET doc:x f v", "clients": 3}],
        "reads": [
            {"id": "r1", "command": "FT.SEARCH idx q1", "clients": 5},
            {"id": "r2", "command": "FT.SEARCH idx q2", "clients": 5},
        ],
    }


class TestNormalizeMixedConfigs:
    """Tests for ClientRunner._normalize_mixed_configs."""

    def test_sub_scenario_duration_overrides_parent(self, mixed_runner):
        """Sub-scenario duration must NOT be replaced by parent duration."""
        scenario = {
            "id": "j",
            "duration": 60,
            "writes": [
                {"id": "w1", "command": "HSET k f v", "clients": 1, "duration": 10}
            ],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert writes[0]["duration"] == 10  # override preserved
        assert reads[0]["duration"] == 60  # parent propagated

    def test_cluster_execution_only_propagated_when_parent_sets_it(self, mixed_runner):
        """cluster_execution is set on subs only when the parent has it."""
        with_ce = {
            "id": "j",
            "cluster_execution": "single",
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 1}],
            "reads": [],
        }
        writes, _ = mixed_runner._normalize_mixed_configs(with_ce)
        assert writes[0]["cluster_execution"] == "single"

        without_ce = {
            "id": "j",
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 1}],
            "reads": [],
        }
        writes, _ = mixed_runner._normalize_mixed_configs(without_ce)
        assert "cluster_execution" not in writes[0]

    def test_parent_data_size_propagates_to_subs(self, mixed_runner):
        """A parent data_size reaches subs that do not set their own (piece 1).

        Without this, _create_mixed_metric falls back to data_size=100 and every
        mixed row misreports the payload.
        """
        scenario = {
            "id": "j",
            "data_size": 256,
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 1}],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert writes[0]["data_size"] == 256
        assert reads[0]["data_size"] == 256

    def test_sub_scenario_data_size_overrides_parent(self, mixed_runner):
        """A sub-scenario's own data_size is not replaced by the parent's."""
        scenario = {
            "id": "j",
            "data_size": 256,
            "writes": [
                {"id": "w1", "command": "HSET k f v", "clients": 1, "data_size": 16}
            ],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert writes[0]["data_size"] == 16  # override preserved
        assert reads[0]["data_size"] == 256  # parent propagated

    def test_no_parent_data_size_leaves_subs_untouched(self, mixed_runner):
        """When the parent sets no data_size, subs gain none (FTS "j" is
        byte-for-byte unchanged and still relies on the metric default)."""
        scenario = {
            "id": "j",
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 1}],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert "data_size" not in writes[0]
        assert "data_size" not in reads[0]

    def test_parent_requests_propagates_to_subs(self, mixed_runner):
        """Finding 1: a request-bounded parent's 'requests' reaches subs that do
        not set their own; without it a predefined child hits the requires-
        requests-or-duration guard and an arbitrary child falls back to 60s."""
        scenario = {
            "id": "j",
            "requests": 500000,
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 1}],
            "reads": [{"id": "r1", "test": "GET", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert writes[0]["requests"] == 500000
        assert reads[0]["requests"] == 500000

    def test_sub_scenario_requests_overrides_parent(self, mixed_runner):
        """A sub-scenario's own 'requests' is not replaced by the parent's."""
        scenario = {
            "id": "j",
            "requests": 500000,
            "writes": [
                {"id": "w1", "command": "HSET k f v", "clients": 1, "requests": 10}
            ],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 1}],
        }
        writes, reads = mixed_runner._normalize_mixed_configs(scenario)
        assert writes[0]["requests"] == 10  # override preserved
        assert reads[0]["requests"] == 500000  # parent propagated


class TestGetCpuForMixedProcess:
    """Tests for ClientRunner._get_cpu_for_mixed_process."""

    def test_partitions_pool_by_cores_per_client(self, mixed_runner):
        """cores_per_client=2 starting at core 10 → 10-11, 12-13, 14-15."""
        assert mixed_runner._get_cpu_for_mixed_process(0) == "10-11"
        assert mixed_runner._get_cpu_for_mixed_process(1) == "12-13"
        assert mixed_runner._get_cpu_for_mixed_process(2) == "14-15"

    def test_single_core_per_client_returns_bare_number(self, minimal_valid_config):
        """When only 1 core per client, output must be '20' not '20-20'."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 1},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["20-25"]
        assert runner._get_cpu_for_mixed_process(0) == "20"
        assert runner._get_cpu_for_mixed_process(1) == "21"

    def test_first_range_as_single_core_parses_correctly(self, minimal_valid_config):
        """When client_cpu_ranges[0] has no dash it still parses as an int.

        The pool spans 30-33 (four dashless single-core entries) so two
        two-core processes fit without overrunning it (see the finding-3 raise
        test for the undersized case).
        """
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 2},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["30", "31", "32", "33"]
        assert runner._get_cpu_for_mixed_process(0) == "30-31"
        assert runner._get_cpu_for_mixed_process(1) == "32-33"

    def test_non_contiguous_range_parses(self, minimal_valid_config):
        """Finding 2: a valid non-contiguous range (e.g. '10-11,20-21') must
        parse via parse_core_range, not raise ValueError from split('-')."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 2},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["10-11,20-21"]
        # start=10 (pool[0]), end=21 (pool[-1]); process 0 -> 10-11.
        assert runner._get_cpu_for_mixed_process(0) == "10-11"

    def test_undersized_pool_spills_and_warns(self, minimal_valid_config, caplog):
        """A process whose cores fall past the pool end must spill onto extra
        cores and warn (original behavior) rather than raise. Pool '10-11' (2
        cores, cpc=2) fits one process; process 1 spills to '12-13'."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 2},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["10-11"]  # only 2 cores, one process fits
        assert runner._get_cpu_for_mixed_process(0) == "10-11"
        with caplog.at_level(logging.WARNING):
            assert runner._get_cpu_for_mixed_process(1) == "12-13"
        assert "exceeds the client CPU pool" in caplog.text

    def test_non_contiguous_pool_allocates_only_real_cores(self, minimal_valid_config):
        """Finding 1: the reported pool '10-11,20-21' (cores 12-13 absent) must
        give process 1 the second real block '20-21', not the contiguous
        '12-13' the old first..last arithmetic produced."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 2},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["10-11,20-21"]
        assert runner._get_cpu_for_mixed_process(0) == "10-11"
        assert runner._get_cpu_for_mixed_process(1) == "20-21"

    def test_slice_running_out_mid_pool_spills_and_warns(
        self, minimal_valid_config, caplog
    ):
        """Finding 1: when cores_per_client straddles a gap the last full block
        exhausts the pool, so the next process spills past the last pool core
        and warns. Pool [10,11,20,21], cores_per_client=3 -> process 0 gets the
        real cores '10-11,20'; process 1 spills to '22-24' (past last core 21)
        and warns instead of raising."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 3},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["10-11,20-21"]
        assert runner._get_cpu_for_mixed_process(0) == "10-11,20"
        with caplog.at_level(logging.WARNING):
            assert runner._get_cpu_for_mixed_process(1) == "22-24"
        assert "exceeds the client CPU pool" in caplog.text

    def test_fts_shipped_config_spill_values(self, minimal_valid_config, caplog):
        """Acceptance: the shipped fts-benchmarks-arm.json client pool (40 cores
        at cores_per_client=8) must keep its original behavior -- processes 0-4
        map to the in-pool blocks 40-47..72-79 and process 5 spills to 80-87."""
        cfg = {
            **minimal_valid_config,
            "cpu_allocation": {"cores_per_server": 1, "cores_per_client": 8},
        }
        runner = ClientRunner(
            commit_id="abc",
            config=cfg,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp",
        )
        runner.client_cpu_ranges = ["40-47", "48-55", "56-63", "64-71", "72-79"]
        assert runner._get_cpu_for_mixed_process(0) == "40-47"
        assert runner._get_cpu_for_mixed_process(1) == "48-55"
        assert runner._get_cpu_for_mixed_process(2) == "56-63"
        assert runner._get_cpu_for_mixed_process(3) == "64-71"
        assert runner._get_cpu_for_mixed_process(4) == "72-79"
        with caplog.at_level(logging.WARNING):
            assert runner._get_cpu_for_mixed_process(5) == "80-87"
        assert "exceeds the client CPU pool" in caplog.text


def _popen_mock(stdout: str, returncode: int = 0):
    """Return a MagicMock that mimics ``subprocess.Popen`` for one child."""
    proc = MagicMock()
    proc.communicate.return_value = (stdout, "")
    proc.returncode = returncode
    return proc


_MIXED_CSV = _make_csv([_metric_row("HSET")])


class TestRunMixedWorkload:
    """Tests for ClientRunner._run_mixed_workload (Popen mocked)."""

    def test_metrics_have_correct_test_id_and_phase_per_sub_scenario(
        self, mixed_runner, mixed_scenario
    ):
        """Every produced metric must be attributable to its sub-scenario id."""
        # Use side_effect (not return_value) so each call returns a *fresh*
        # dict. _create_mixed_metric mutates the dict returned by
        # create_metrics; a shared return_value would cause every metric to
        # end up as the same dict with the last-written test_id.
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [_popen_mock(_MIXED_CSV) for _ in range(3)]

            result = _run_mixed(
                mixed_runner, mixed_scenario, metrics_processor=metrics_processor
            )

        assert result is not None and len(result) == 3
        test_ids = {m["test_id"] for m in result}
        assert test_ids == {"1_j_write_w1", "1_j_read_r1", "1_j_read_r2"}

        by_phase = {m["test_id"]: m["test_phase"] for m in result}
        assert by_phase["1_j_write_w1"] == "mixed_write"
        assert by_phase["1_j_read_r1"] == "mixed_read"
        assert by_phase["1_j_read_r2"] == "mixed_read"

        assert all(m.get("config_name") == "fts-mixed-test.json" for m in result)

    @pytest.mark.parametrize("returncode", [0, 1])
    def test_warmup_drains_processes_and_returns_no_metrics(
        self, mixed_runner, mixed_scenario, returncode
    ):
        """Warmup (metrics_processor=None) must still drain every child process
        and record nothing -- even when the children fail (returncode=1), no
        markers are produced."""
        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [
                _popen_mock(_MIXED_CSV, returncode=returncode) for _ in range(3)
            ]

            result = _run_mixed(mixed_runner, mixed_scenario, metrics_processor=None)

        assert mock_popen.call_count == 3
        assert result is None

    def test_partial_launch_terminates_and_reaps_started_children(
        self, mixed_runner, mixed_scenario
    ):
        metrics_processor = MagicMock()
        metrics_processor.build_base_metadata.side_effect = lambda *a, **kw: {}
        started = _popen_mock("")
        started.poll.return_value = None

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [started, RuntimeError("second launch failed")]
            result = _run_mixed(
                mixed_runner, mixed_scenario, metrics_processor=metrics_processor
            )

        assert all(row["status"] == "failed" for row in result)
        started.terminate.assert_called_once_with()
        started.wait.assert_called_once_with(timeout=5)

    def test_partial_failure_emits_marker_for_missing_child_only(
        self, mixed_runner, mixed_scenario
    ):
        """Finding 1(b): a non-zero exit for one sub-scenario must emit a failure
        marker for THAT child (so its healthy baseline is excluded instead of
        rendering as a fabricated -100%) while the survivors keep their success
        metrics."""
        # side_effect returns a fresh dict per call (see other test for why).
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}
        # The failed child's marker is built from the shared base-metadata
        # assembly; make the mock return a real dict so the marker is a real row.
        metrics_processor.build_base_metadata.side_effect = lambda *a, **kw: {}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [
                _popen_mock("", returncode=1),  # w1 fails
                _popen_mock(_MIXED_CSV),  # r1 ok
                _popen_mock(_MIXED_CSV),  # r2 ok
            ]

            result = _run_mixed(
                mixed_runner, mixed_scenario, metrics_processor=metrics_processor
            )

        by_status = {m["test_id"]: m.get("status") for m in result}
        # The failed write child now has an honest failure marker...
        assert by_status["1_j_write_w1"] == "failed"
        # ...and the read survivors keep their success metrics.
        assert by_status["1_j_read_r1"] == "success"
        assert by_status["1_j_read_r2"] == "success"
        # The marker carries the child's identity and no performance keys.
        w_marker = next(m for m in result if m["test_id"] == "1_j_write_w1")
        assert w_marker["test_phase"] == "mixed_write"
        assert "rps" not in w_marker

    @pytest.mark.parametrize(
        "mode, err_substr",
        [
            ("all_fail", "produced no successful result"),  # Finding 1(a)
            ("raise_mid_run", "launch exploded"),  # Finding 1(c)
        ],
    )
    def test_total_failure_emits_marker_per_expected_child(
        self, mixed_runner, mixed_scenario, mode, err_substr
    ):
        """Findings 1(a)/1(c): whether EVERY sub-scenario exits non-zero, or a
        launch/aggregation exception blows up mid-run, the mixed path returns a
        marker per expected child keyed by sub-scenario identity -- never None,
        and never the old mis-keyed parent ``1_j`` marker -- so they pair with
        the children they replaced."""
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}
        metrics_processor.build_base_metadata.side_effect = lambda *a, **kw: {}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            if mode == "raise_mid_run":
                mock_popen.side_effect = RuntimeError("launch exploded")
            else:
                mock_popen.side_effect = [
                    _popen_mock("", returncode=1) for _ in range(3)
                ]

            result = _run_mixed(
                mixed_runner, mixed_scenario, metrics_processor=metrics_processor
            )

        assert result is not None
        ids = {m["test_id"] for m in result}
        assert ids == {"1_j_write_w1", "1_j_read_r1", "1_j_read_r2"}
        # The mis-keyed parent marker must NOT appear.
        assert "1_j" not in ids
        assert all(m["status"] == "failed" for m in result)
        assert all(err_substr in m["error"] for m in result)

    def test_empty_writes_and_reads_returns_none_without_spawning(self, mixed_runner):
        """A scenario with no sub-scenarios must not spawn anything."""
        scenario = {"id": "j", "duration": 60, "writes": [], "reads": []}
        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            result = _run_mixed(mixed_runner, scenario, metrics_processor=MagicMock())

        assert result is None
        assert mock_popen.call_count == 0


# ---------------------------------------------------------------------------
# _create_mixed_metric — signature guard using the REAL MetricsProcessor
# ---------------------------------------------------------------------------
#
# Other mixed-workload tests mock MetricsProcessor via ``side_effect``. That
# accepts any positional/keyword combination, so a drift between
# _create_mixed_metric's call site and MetricsProcessor.create_metrics's real
# signature would pass mocked tests but blow up at runtime.
#
# This test wires the two together with a real MetricsProcessor to catch that
# drift class of bug.


class TestCreateMixedMetricRealProcessor:
    """Signature-drift guard: _create_mixed_metric ↔ MetricsProcessor.create_metrics."""

    def test_real_processor_accepts_all_positional_args(self, mixed_runner):
        from process_metrics import MetricsProcessor

        real_processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2026-01-01T00:00:00Z",
        )

        # Minimal well-formed CSV row (matches _parse_csv_row output shape).
        row = {
            "test": "FT.SEARCH",
            "rps": "1000.0",
            "avg_latency_ms": "1.0",
            "min_latency_ms": "0.5",
            "p50_latency_ms": "1.0",
            "p95_latency_ms": "2.0",
            "p99_latency_ms": "3.0",
            "max_latency_ms": "5.0",
        }
        sub_cfg = {"command": "FT.SEARCH rd0 hello", "clients": 10, "pipeline": 1}
        parent_scenario = {"duration": 60}

        metric = mixed_runner._create_mixed_metric(
            row=row,
            sub_cfg=sub_cfg,
            parent_scenario=parent_scenario,
            group_id=1,
            scenario_id="j",
            sub_id="r1",
            phase="read",
            config_set={},
            warmup_duration=0,
            metrics_processor=real_processor,
        )

        # If create_metrics's signature drifts (arg reorder, rename, added
        # required kwarg), the call above raises TypeError before we get here.
        assert metric is not None
        assert metric["test_id"] == "1_j_read_r1"
        assert metric["test_phase"] == "mixed_read"
        assert metric["rps"] == 1000.0

    def test_test_subscenario_records_predefined_name(self, mixed_runner):
        """A predefined ``test:`` sub-scenario no longer raises KeyError and
        records the predefined name in the metric ``command`` field, matching
        _build_scenario_metrics' aggregated-row convention (row["test"] is set
        to the canonical test name by _aggregate_parallel_results)."""
        real_processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2026-01-01T00:00:00Z",
        )
        # Aggregated row: _aggregate_parallel_results stamps the canonical name.
        row = _metric_row("SET")
        sub_cfg = {"test": "SET", "clients": 10, "pipeline": 1}

        metric = mixed_runner._create_mixed_metric(
            row=row,
            sub_cfg=sub_cfg,
            parent_scenario={"duration": 60},
            group_id=1,
            scenario_id="j",
            sub_id="w",
            phase="write",
            config_set={},
            warmup_duration=0,
            metrics_processor=real_processor,
        )

        assert metric is not None
        assert metric["command"] == "SET"  # predefined name, not a KeyError
        assert metric["test_id"] == "1_j_write_w"
        assert metric["test_phase"] == "mixed_write"


class TestBuildBenchmarkCommandPredefinedMixedSub:
    """A predefined ``test:`` mixed sub-scenario SHALL be launched with ``-t
    NAME`` -- the same predefined argv the rest of the framework emits."""

    def test_predefined_sub_uses_dash_t(self, minimal_client_runner):
        sub = {
            "id": "w",
            "test": "SET",
            "clients": 10,
            "pipeline": 2,
            "data_size": 16,
            "duration": 60,
        }
        argv = minimal_client_runner._build_benchmark_command(
            scenario=sub, port=6379, seed_val=42
        )
        assert argv == [
            "src/valkey-benchmark",
            "-h",
            "127.0.0.1",
            "-p",
            "6379",
            "--duration",
            "60",
            "-r",
            "1000",
            "-d",
            "16",
            "-P",
            "2",
            "-c",
            "10",
            "-t",
            "SET",
            "--seed",
            "42",
            "--csv",
        ]
        # A predefined sub is a '-t' workload, never an arbitrary '--' command.
        assert "--" not in argv


class TestMixedDataSizeReachesMetric:
    """A parent data_size reaches the mixed metric row."""

    def test_parent_data_size_lands_in_create_metrics(self, mixed_runner):
        scenario = {
            "id": "j",
            "type": "mixed",
            "duration": 60,
            "pipeline": 2,
            "data_size": 256,
            "writes": [{"id": "w1", "command": "HSET doc:x f v", "clients": 3}],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q1", "clients": 5}],
        }
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [_popen_mock(_MIXED_CSV) for _ in range(2)]
            result = _run_mixed(
                mixed_runner, scenario, metrics_processor=metrics_processor
            )

        assert result is not None and len(result) == 2
        # data_size is create_metrics' third positional arg.
        seen_sizes = {
            call.args[2] for call in metrics_processor.create_metrics.call_args_list
        }
        assert seen_sizes == {256}


class TestMixedRequestsReachesMetric:
    """A request-bounded mixed parent's request count reaches the metric row
    (create_metrics' requests arg) instead of being dropped."""

    def test_parent_requests_lands_in_create_metrics(self, mixed_runner):
        scenario = {
            "id": "j",
            "type": "mixed",
            "requests": 500000,
            "pipeline": 2,
            "writes": [{"id": "w1", "command": "HSET doc:x f v", "clients": 3}],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q1", "clients": 5}],
        }
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [_popen_mock(_MIXED_CSV) for _ in range(2)]
            result = _run_mixed(
                mixed_runner, scenario, metrics_processor=metrics_processor
            )

        assert result is not None and len(result) == 2
        # requests is create_metrics' sixth positional arg (index 5).
        seen = {
            call.args[5] for call in metrics_processor.create_metrics.call_args_list
        }
        assert seen == {500000}


class TestMixedFailureMarkerRequests:
    """Mixed failure markers carry the request count too, so a failed
    request-bounded child records requests/benchmark_mode like a success row."""

    def test_marker_carries_requests(self, mixed_runner):
        scenario = {
            "id": "j",
            "type": "mixed",
            "requests": 500000,
            "pipeline": 2,
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 3}],
            "reads": [{"id": "r1", "command": "FT.SEARCH idx q", "clients": 5}],
        }
        real_processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2026-01-01T00:00:00Z",
        )
        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [_popen_mock("", returncode=1) for _ in range(2)]
            result = _run_mixed(
                mixed_runner, scenario, metrics_processor=real_processor
            )

        assert result is not None and len(result) == 2
        assert all(r["status"] == "failed" for r in result)
        assert all(r["requests"] == 500000 for r in result)
        assert all(r["benchmark_mode"] == "requests" for r in result)


class TestWarmupWritesOnly:
    """warmup_writes_only drops reads from the mixed warmup copy while the
    default keeps both. Verified by capturing the scenario handed to
    _run_mixed_workload from _run_scenario_warmup."""

    @staticmethod
    def _capture_warmup_scenario(runner, scenario):
        captured = {}

        def _fake_mixed(
            warmup_scenario,
            group_id,
            config_set,
            metrics_processor=None,
            warmup_duration=0,
            group_description=None,
        ):
            captured["scenario"] = warmup_scenario
            return None

        with patch.object(runner, "_run_mixed_workload", side_effect=_fake_mixed):
            runner._run_scenario_warmup(scenario, group_id=1, config_set={})
        return captured.get("scenario")

    def _mixed(self, **extra):
        return {
            "id": "j",
            "type": "mixed",
            "warmup": 30,
            "writes": [{"id": "w1", "command": "HSET k f v", "clients": 3}],
            "reads": [
                {"id": "r1", "command": "FT.SEARCH idx q1", "clients": 5},
                {"id": "r2", "command": "FT.SEARCH idx q2", "clients": 5},
            ],
            **extra,
        }

    def test_opt_in_drops_reads(self, mixed_runner):
        warmup = self._capture_warmup_scenario(
            mixed_runner, self._mixed(warmup_writes_only=True)
        )
        assert warmup is not None
        assert warmup["reads"] == []  # reads dropped
        assert len(warmup["writes"]) == 1  # writes preserved

    def test_default_keeps_both(self, mixed_runner):
        warmup = self._capture_warmup_scenario(mixed_runner, self._mixed())
        assert warmup is not None
        assert len(warmup["reads"]) == 2  # today's behavior preserved
        assert len(warmup["writes"]) == 1

    def test_false_keeps_both(self, mixed_runner):
        warmup = self._capture_warmup_scenario(
            mixed_runner, self._mixed(warmup_writes_only=False)
        )
        assert warmup is not None
        assert len(warmup["reads"]) == 2
        assert len(warmup["writes"]) == 1

    def test_opt_in_does_not_mutate_original_scenario(self, mixed_runner):
        """The warmup transformation works on a copy; the real run still reads."""
        scenario = self._mixed(warmup_writes_only=True)
        self._capture_warmup_scenario(mixed_runner, scenario)
        assert len(scenario["reads"]) == 2  # original untouched


def _populate_scenario(**workload):
    return {
        "id": "s1",
        "requests": 100,
        "keyspacelen": 100,
        "data_size": 64,
        "pipeline": 1,
        "clients": 1,
        **workload,
    }


def _capture_scenario_commands(runner, scenario):
    captured = []

    def fake_run(command=None, *args, **kwargs):
        captured.append(command)
        return MagicMock()

    with (
        patch.object(runner, "_run", side_effect=fake_run),
        patch("valkey_benchmark.random.randint", side_effect=[111, 222, 333]),
    ):
        _invoke_scenario(runner, scenario, metrics_processor=None)
    return captured


class TestPopulateWith:
    @pytest.mark.parametrize(
        "scenario, expected_tokens",
        [
            (_populate_scenario(test="GET", populate_with="SET"), ["-t", "SET"]),
            (
                _populate_scenario(
                    command="GET key:__rand_int__",
                    populate_with="SET key:__rand_int__ __data__",
                ),
                ["--", "SET", "key:__rand_int__", "__data__"],
            ),
        ],
    )
    def test_populate_argv_and_shared_seed(
        self, minimal_client_runner, scenario, expected_tokens
    ):
        populate_cmd, main_cmd = _capture_scenario_commands(
            minimal_client_runner, scenario
        )

        start = populate_cmd.index(expected_tokens[0])
        assert populate_cmd[start : start + len(expected_tokens)] == expected_tokens
        assert ("--" in populate_cmd) is (expected_tokens[0] == "--")
        assert "--sequential" in populate_cmd
        assert _seed_of(populate_cmd) == _seed_of(main_cmd) == "111"


class TestPopulateWithNotInMetrics:
    def test_populate_with_absent_from_metrics(self, minimal_client_runner):
        real_processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2026-01-01T00:00:00Z",
        )

        proc = MagicMock()
        proc.stdout = _make_csv([_metric_row()])

        metrics = minimal_client_runner._build_scenario_metrics(
            _populate_scenario(test="GET", populate_with="SET"),
            proc,
            None,
            group_id=1,
            config_set={},
            warmup_duration=0,
            group_description=None,
            metrics_processor=real_processor,
        )

        assert metrics is not None
        assert "populate_with" not in metrics


class TestRestartReappliesConfigSet:
    @staticmethod
    def _scenario(**extra):
        return _populate_scenario(test="SET", **extra)

    def _run(self, runner, scenario, config_set):
        calls = []
        with (
            patch.object(runner, "_run", return_value=MagicMock()),
            patch.object(
                runner, "_restart_server", side_effect=lambda: calls.append("restart")
            ),
            patch.object(
                runner, "_flush_database", side_effect=lambda: calls.append("flush")
            ),
            patch.object(
                runner,
                "_apply_config_set",
                side_effect=lambda cs: calls.append(f"apply:{cs}"),
            ),
        ):
            _invoke_scenario(
                runner,
                scenario,
                metrics_processor=None,
                config_set=config_set,
            )
        return calls

    @pytest.mark.parametrize(
        "extra, has_launcher, config_set, expected",
        [
            (
                {ORIGIN_FIELD: ORIGIN_SIMPLE, "restart_before": True},
                True,
                {"appendonly": "no"},
                ["restart", "apply:{'appendonly': 'no'}"],
            ),
            (
                {"flush_before": True},
                True,
                {"appendonly": "no"},
                ["restart", "apply:{'appendonly': 'no'}"],
            ),
            (
                {"restart_before": True, "flush_before": True},
                True,
                {"appendonly": "no"},
                ["restart", "apply:{'appendonly': 'no'}"],
            ),
            ({"restart_before": True}, True, {}, ["restart"]),
            ({"restart_before": True}, False, {"appendonly": "no"}, ["flush"]),
        ],
    )
    def test_restart_and_config_reapplication(
        self, minimal_client_runner, extra, has_launcher, config_set, expected
    ):
        minimal_client_runner.server_launcher = MagicMock() if has_launcher else None
        assert (
            self._run(minimal_client_runner, self._scenario(**extra), config_set)
            == expected
        )


FORBIDDEN_SIMPLE_KEYS = {
    "test_id",
    "test_phase",
    "group",
    "scenario",
    "group_description",
    "scenario_description",
    "config_set",
    "status",
    "error",
    "config_name",
    "module_commit",
    "module_commit_timestamp",
    "dataset",
}


class TestRunSingleScenarioErrorPaths:
    @staticmethod
    def _command_scenario(**extra):
        return _populate_scenario(type="write", command="SET", **extra)

    @staticmethod
    def _origin_simple(**extra):
        return TestRunSingleScenarioErrorPaths._command_scenario(
            **{ORIGIN_FIELD: ORIGIN_SIMPLE, **extra}
        )

    @staticmethod
    def _invoke(runner, scenario, metrics_processor):
        return _invoke_scenario(
            runner,
            scenario,
            metrics_processor=metrics_processor,
            config_set={"appendonly": "no"},
        )

    def test_origin_simple_no_results_returns_none(self, minimal_client_runner):
        runner = minimal_client_runner
        metrics_processor = MagicMock()

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=None),
        ):
            result = self._invoke(runner, self._origin_simple(), metrics_processor)

        assert result is None
        metrics_processor.create_metrics.assert_not_called()

    def test_handwritten_no_results_returns_failure_marker(self, minimal_client_runner):
        runner = minimal_client_runner
        metrics_processor = MetricsProcessor("abc123", False, False, "<TS>")

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=None),
        ):
            result = self._invoke(runner, self._command_scenario(), metrics_processor)

        assert isinstance(result, dict)
        assert result["test_id"] == "1_s1"
        assert result["test_phase"] == "write"
        assert result["status"] == "failed"

    def test_origin_simple_parse_error_returns_none(self, minimal_client_runner):
        runner = minimal_client_runner
        metrics_processor = MagicMock()

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=MagicMock()),
            patch.object(runner, "_parse_csv_row", side_effect=ValueError("bad csv")),
        ):
            result = self._invoke(runner, self._origin_simple(), metrics_processor)

        assert result is None

    def test_origin_simple_invocation_error_propagates(self, minimal_client_runner):
        runner = minimal_client_runner
        metrics_processor = MagicMock()

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(
                runner, "_run", side_effect=RuntimeError("Command failed: vb")
            ),
        ):
            with pytest.raises(RuntimeError, match="Command failed"):
                self._invoke(runner, self._origin_simple(), metrics_processor)

    def test_origin_simple_never_leaks_scenario_keys(self, minimal_client_runner):
        runner = minimal_client_runner

        success_processor = MagicMock()
        success_processor.create_metrics.return_value = {
            "rps": 1000.0,
            "avg_latency_ms": 0.5,
        }
        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=MagicMock()),
            patch.object(
                runner, "_parse_csv_row", return_value={"test": "SET", "rps": "1000"}
            ),
        ):
            success = self._invoke(runner, self._origin_simple(), success_processor)

        assert isinstance(success, dict)
        assert FORBIDDEN_SIMPLE_KEYS.isdisjoint(success)

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=None),
        ):
            no_results = self._invoke(runner, self._origin_simple(), MagicMock())

        assert no_results is None

    def test_handwritten_populate_failure_returns_failure_marker(
        self, minimal_client_runner
    ):
        runner = minimal_client_runner
        metrics_processor = MetricsProcessor("abc123", False, False, "<TS>")
        scenario = self._command_scenario(populate_with="SET key:__rand_int__ v")

        with patch.object(
            runner,
            "_populate_scenario_keyspace",
            side_effect=RuntimeError("populate boom"),
        ):
            result = self._invoke(runner, scenario, metrics_processor)

        assert isinstance(result, dict)
        assert result["status"] == "failed"
        assert result["test_id"] == "1_s1"
        assert "populate boom" in result["error"]

    def test_origin_simple_populate_failure_propagates(self, minimal_client_runner):
        runner = minimal_client_runner
        scenario = self._origin_simple(populate_with="SET")

        with patch.object(
            runner,
            "_populate_scenario_keyspace",
            side_effect=RuntimeError("populate boom"),
        ):
            with pytest.raises(RuntimeError, match="populate boom"):
                self._invoke(runner, scenario, MagicMock())

    def test_successful_populate_runs_before_main_invocation(
        self, minimal_client_runner
    ):
        runner = minimal_client_runner
        metrics_processor = MagicMock()
        scenario = self._command_scenario(populate_with="SET key:__rand_int__ v")

        calls = []

        def record_populate(*args, **kwargs):
            calls.append("populate")

        def record_main(*args, **kwargs):
            calls.append("main")
            return MagicMock(), None

        with (
            patch.object(
                runner, "_populate_scenario_keyspace", side_effect=record_populate
            ),
            patch.object(runner, "_execute_benchmark_run", side_effect=record_main),
            patch.object(runner, "_build_scenario_metrics", return_value={"rps": 1.0}),
        ):
            result = self._invoke(runner, scenario, metrics_processor)

        assert calls == ["populate", "main"]
        assert result == {"rps": 1.0}


# ---------------------------------------------------------------------------
# Parallel execution of ``test:`` scenarios (P1-3)
# ---------------------------------------------------------------------------


def _cme_parallel_runner(minimal_client_runner, nodes=2):
    runner = minimal_client_runner
    runner.cluster_mode = True
    runner.config["cluster_nodes"] = nodes
    ports = [6379 + i for i in range(nodes)]
    runner.config["cluster_ports"] = ports
    runner.client_cpu_ranges = [str(i) for i in range(nodes)]
    return runner


class TestAggregateParallelTestScenario:
    def _result(self, test="GET", rps="100000"):
        return (_make_csv([_metric_row(test, rps)]), "", 6379)

    @pytest.mark.parametrize("workload", [{"test": "GET"}, {"command": "SET key v"}])
    def test_labels_from_workload_key(self, minimal_client_runner, workload):
        scenario = {"id": "a", "cluster_execution": "parallel", **workload}
        agg = minimal_client_runner._aggregate_parallel_results(
            [self._result(test="SET")], scenario
        )

        assert agg["test"] == next(iter(workload.values()))
        assert float(agg["rps"]) == pytest.approx(100000.0)


class TestBuildScenarioMetricsAggregatedRow:
    @staticmethod
    def _aggregated_row(test="GET"):
        return _metric_row(test, "100000")

    @pytest.mark.parametrize("workload", [{"test": "GET"}, {"command": "GET key"}])
    def test_parallel_scenario_uses_aggregated_row(
        self, minimal_client_runner, workload
    ):
        runner = minimal_client_runner
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.return_value = {"rps": 100000.0}
        scenario = {
            "id": "a",
            "type": "read",
            "cluster_execution": "parallel",
            "iteration": 2,
            **workload,
        }
        label = next(iter(workload.values()))
        aggregated = self._aggregated_row(label)

        result = runner._build_scenario_metrics(
            scenario,
            None,
            aggregated,
            group_id=1,
            config_set={},
            warmup_duration=0,
            group_description=None,
            metrics_processor=metrics_processor,
        )

        assert result is not None
        assert result["status"] == "success"
        assert result["test_id"] == "1_a"
        assert result["iteration"] == 2
        metrics_processor.create_metrics.assert_called_once()
        call = metrics_processor.create_metrics.call_args
        assert call.args[:2] == (aggregated, label)


class TestParallelSeedSharing:
    @staticmethod
    def _seeds(commands):
        return [cmd[cmd.index("--seed") + 1] for cmd in commands if "--seed" in cmd]

    def test_populate_and_parallel_readers_share_one_seed(self, minimal_client_runner):
        runner = _cme_parallel_runner(minimal_client_runner, nodes=2)
        scenario = _populate_scenario(
            id="a",
            type="read",
            test="GET",
            populate_with="SET",
            cluster_execution="parallel",
        )

        built = []
        real_build = runner._build_benchmark_command

        def spy_build(*args, **kwargs):
            cmd = real_build(*args, **kwargs)
            built.append(cmd)
            return cmd

        with (
            patch.object(runner, "_build_benchmark_command", side_effect=spy_build),
            patch.object(runner, "_run", return_value=MagicMock()),
            patch(
                "valkey_benchmark.subprocess.Popen",
                return_value=_popen_mock(_MIXED_CSV),
            ),
            patch("valkey_benchmark.random.randint", side_effect=[999, 11, 22, 33, 44]),
        ):
            _invoke_scenario(runner, scenario, metrics_processor=None)

        seeds = self._seeds(built)
        assert len(seeds) == 3
        assert seeds == ["999", "999", "999"]

    def test_parallel_without_populate_keeps_draw_count(self, minimal_client_runner):
        runner = _cme_parallel_runner(minimal_client_runner, nodes=2)
        scenario = _populate_scenario(
            id="a", type="read", test="GET", cluster_execution="parallel"
        )

        with (
            patch(
                "valkey_benchmark.subprocess.Popen",
                return_value=_popen_mock(_MIXED_CSV),
            ),
            patch(
                "valkey_benchmark.random.randint", side_effect=[11, 22, 33, 44]
            ) as randint,
        ):
            _invoke_scenario(runner, scenario, metrics_processor=None)

        assert randint.call_count == 2


class TestProfilingAlwaysStopped:
    @staticmethod
    def _invoke(runner, scenario, profiler, metrics_processor):
        return _invoke_scenario(
            runner,
            scenario,
            profiler=profiler,
            metrics_processor=metrics_processor,
        )

    @staticmethod
    def _command_scenario(**extra):
        return _populate_scenario(type="write", command="SET", **extra)

    @pytest.mark.parametrize("origin_simple", [False, True])
    def test_stops_on_invocation_error(self, minimal_client_runner, origin_simple):
        runner = minimal_client_runner
        profiler = MagicMock()
        scenario = self._command_scenario(
            **({ORIGIN_FIELD: ORIGIN_SIMPLE} if origin_simple else {})
        )
        processor = MetricsProcessor("abc123", False, False, "<TS>")
        with (
            patch.object(
                runner, "_resolve_effective_profiling", return_value={"enabled": True}
            ),
            patch.object(
                runner,
                "_execute_benchmark_run",
                side_effect=RuntimeError("invocation boom"),
            ),
        ):
            if origin_simple:
                with pytest.raises(RuntimeError, match="invocation boom"):
                    self._invoke(runner, scenario, profiler, processor)
            else:
                result = self._invoke(runner, scenario, profiler, processor)
                assert result["status"] == "failed"

        assert profiler.stop_profiling.call_count == 1

    @pytest.mark.parametrize(
        "scenario, method, method_result, expected",
        [
            (
                _populate_scenario(type="write", command="SET"),
                "_execute_benchmark_run",
                (MagicMock(), None),
                {"rps": 1.0},
            ),
            (
                {"id": "s1", "type": "mixed", "warmup": 0},
                "_run_mixed_workload",
                [{"rps": 1.0}],
                [{"rps": 1.0}],
            ),
        ],
    )
    def test_stops_once_on_success(
        self, minimal_client_runner, scenario, method, method_result, expected
    ):
        runner = minimal_client_runner
        profiler = MagicMock()
        with (
            patch.object(
                runner, "_resolve_effective_profiling", return_value={"enabled": True}
            ),
            patch.object(runner, method, return_value=method_result),
            patch.object(runner, "_build_scenario_metrics", return_value={"rps": 1.0}),
        ):
            result = self._invoke(runner, scenario, profiler, MagicMock())

        assert result == expected
        assert profiler.stop_profiling.call_count == 1
