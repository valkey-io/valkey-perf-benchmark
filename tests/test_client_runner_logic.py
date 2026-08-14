"""Unit tests for pure logic methods on ClientRunner from valkey_benchmark.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from valkey_benchmark import ClientRunner


def _make_csv(rows):
    """Build CSV stdout string from a list of metric dicts.

    Each dict should contain keys like rps, avg_latency_ms, etc.
    Returns a string with a header line and one data line per dict.
    """
    header = '"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"'
    lines = [header]
    for r in rows:
        lines.append(
            f'"{r.get("test","GET")}","{r["rps"]}","{r["avg_latency_ms"]}",'
            f'"{r["min_latency_ms"]}","{r["p50_latency_ms"]}","{r["p95_latency_ms"]}",'
            f'"{r["p99_latency_ms"]}","{r["max_latency_ms"]}"'
        )
    return "\n".join(lines)


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
        # requests=[1000], keyspacelen=[1000], data_sizes=[64], pipelines=[1],
        # clients=[50], commands=["GET","SET"] -> one group per combination
        groups = minimal_valid_config["test_groups"]
        assert len(groups) == 2  # 1*1*1*1*1*2
        assert all(len(g["scenarios"]) == 1 for g in groups)

    def test_cartesian_product_count(self, minimal_valid_config):
        from benchmark import compile_simple_config

        minimal_valid_config["data_sizes"] = [64, 128]
        minimal_valid_config["pipelines"] = [1, 10]
        compile_simple_config(minimal_valid_config)
        # 1 * 1 * 2 * 2 * 1 * 2 = 8
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
        # GET is a read command -> populated via SET
        assert first["populate_with"] == "SET"
        # SET is a write command -> no populate
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
    """Tests for ClientRunner._create_failure_marker."""

    def test_basic_marker(self, minimal_client_runner):
        marker = minimal_client_runner._create_failure_marker(
            group_id=1,
            scenario_id="test1",
            scenario_type="write",
            error="timeout",
            command="SET foo bar",
            timestamp="2024-01-01T00:00:00",
            config_set={"io_threads": 4},
        )

        assert marker["test_id"] == "1_test1"
        assert marker["test_phase"] == "write"
        assert marker["status"] == "failed"
        assert marker["error"] == "timeout"
        assert marker["command"] == "SET foo bar"
        assert marker["timestamp"] == "2024-01-01T00:00:00"
        assert marker["config_set"] == {"io_threads": 4}

    def test_test_id_format(self, minimal_client_runner):
        marker = minimal_client_runner._create_failure_marker(
            group_id=5,
            scenario_id="search_idx",
            scenario_type="read",
            error="err",
            command="FT.SEARCH",
            timestamp="ts",
            config_set={},
        )
        assert marker["test_id"] == "5_search_idx"

    def test_empty_config_set(self, minimal_client_runner):
        marker = minimal_client_runner._create_failure_marker(
            group_id=1,
            scenario_id="s1",
            scenario_type="test",
            error="e",
            command="c",
            timestamp="t",
            config_set={},
        )
        assert marker["config_set"] == {}

    def test_marker_includes_group_and_scenario(self, minimal_client_runner):
        marker = minimal_client_runner._create_failure_marker(
            group_id=7,
            scenario_id="search_idx",
            scenario_type="read",
            error="boom",
            command="FT.SEARCH",
            timestamp="2024-01-01T00:00:00",
            config_set={},
        )
        assert marker["group"] == 7
        assert marker["scenario"] == "search_idx"


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

        # One group per combination + group-level runs loop -> A,A then B,B
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
        """When client_cpu_ranges[0] has no dash it still parses as an int."""
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
        runner.client_cpu_ranges = ["30"]
        assert runner._get_cpu_for_mixed_process(0) == "30-31"
        assert runner._get_cpu_for_mixed_process(1) == "32-33"


def _popen_mock(stdout: str, returncode: int = 0):
    """Return a MagicMock that mimics ``subprocess.Popen`` for one child."""
    proc = MagicMock()
    proc.communicate.return_value = (stdout, "")
    proc.returncode = returncode
    return proc


_MIXED_CSV = _make_csv(
    [
        {
            "test": "HSET",
            "rps": "1000.0",
            "avg_latency_ms": "1.0",
            "min_latency_ms": "0.5",
            "p50_latency_ms": "1.0",
            "p95_latency_ms": "1.5",
            "p99_latency_ms": "2.0",
            "max_latency_ms": "3.0",
        }
    ]
)


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

            result = mixed_runner._run_mixed_workload(
                mixed_scenario,
                group_id=1,
                config_set={},
                metrics_processor=metrics_processor,
                warmup_duration=0,
                commit_time="2026-01-01T00:00:00Z",
                scenario_id="j",
            )

        assert result is not None and len(result) == 3
        test_ids = {m["test_id"] for m in result}
        assert test_ids == {"1_j_write_w1", "1_j_read_r1", "1_j_read_r2"}

        by_phase = {m["test_id"]: m["test_phase"] for m in result}
        assert by_phase["1_j_write_w1"] == "mixed_write"
        assert by_phase["1_j_read_r1"] == "mixed_read"
        assert by_phase["1_j_read_r2"] == "mixed_read"

        assert all(m.get("config_name") == "fts-mixed-test.json" for m in result)

    def test_warmup_mode_still_spawns_processes_but_returns_no_metrics(
        self, mixed_runner, mixed_scenario
    ):
        """Warmup (metrics_processor=None) must still drain workload processes."""
        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [_popen_mock(_MIXED_CSV) for _ in range(3)]

            result = mixed_runner._run_mixed_workload(
                mixed_scenario,
                group_id=1,
                config_set={},
                metrics_processor=None,
                warmup_duration=0,
                commit_time="2026-01-01T00:00:00Z",
                scenario_id="j",
            )

        assert mock_popen.call_count == 3
        assert result is None

    def test_failed_process_produces_no_metric_for_that_sub_scenario(
        self, mixed_runner, mixed_scenario
    ):
        """Non-zero exit for one process must not poison metrics for others."""
        # side_effect returns a fresh dict per call (see other test for why).
        metrics_processor = MagicMock()
        metrics_processor.create_metrics.side_effect = lambda *a, **kw: {"rps": 1000.0}

        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [
                _popen_mock("", returncode=1),  # w1 fails
                _popen_mock(_MIXED_CSV),  # r1 ok
                _popen_mock(_MIXED_CSV),  # r2 ok
            ]

            result = mixed_runner._run_mixed_workload(
                mixed_scenario,
                group_id=1,
                config_set={},
                metrics_processor=metrics_processor,
                warmup_duration=0,
                commit_time="2026-01-01T00:00:00Z",
                scenario_id="j",
            )

        test_ids = {m["test_id"] for m in result}
        assert "1_j_write_w1" not in test_ids
        assert "1_j_read_r1" in test_ids
        assert "1_j_read_r2" in test_ids

    def test_empty_writes_and_reads_returns_none_without_spawning(self, mixed_runner):
        """A scenario with no sub-scenarios must not spawn anything."""
        scenario = {"id": "j", "duration": 60, "writes": [], "reads": []}
        with patch("valkey_benchmark.subprocess.Popen") as mock_popen:
            result = mixed_runner._run_mixed_workload(
                scenario,
                group_id=1,
                config_set={},
                metrics_processor=MagicMock(),
                warmup_duration=0,
                commit_time="2026-01-01T00:00:00Z",
            )

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


# ---------------------------------------------------------------------------
# Shared-seed behavior for populate_with scenarios (finding 3)
# ---------------------------------------------------------------------------


class TestPopulateWithSharedSeed:
    """A hand-written ``test:`` scenario using ``populate_with`` must run its
    populate pass and its main run with the SAME ``--seed``, matching the
    documented "shares the main run's seed" behavior."""

    @staticmethod
    def _seed_of(command):
        """Return the value following --seed in a benchmark command list."""
        assert "--seed" in command, f"no --seed in command: {command}"
        return command[command.index("--seed") + 1]

    def test_populate_and_main_share_one_seed(self, minimal_client_runner):
        runner = minimal_client_runner
        # No _origin marker -> this is a hand-written scenario, the path that
        # previously drew two independent lazy seeds.
        scenario = {
            "id": "s1",
            "test": "GET",
            "populate_with": "SET",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }

        captured = []

        def fake_run(command=None, *args, **kwargs):
            # metrics_processor=None below means the returned proc is only
            # checked for truthiness, so a bare MagicMock is sufficient.
            captured.append(command)
            return MagicMock()

        # Distinct values per draw: a single shared draw yields identical
        # seeds on both invocations; separate lazy draws would differ.
        with (
            patch.object(runner, "_run", side_effect=fake_run),
            patch("valkey_benchmark.random.randint", side_effect=[111, 222, 333]),
        ):
            runner._run_single_scenario(
                scenario,
                group_id=1,
                profiler=None,
                metrics_processor=None,
                profiling_enabled=False,
                commit_time="2026-01-01T00:00:00Z",
                config_set={},
                config_suffix="default",
            )

        assert len(captured) == 2  # populate invocation + main invocation
        populate_seed = self._seed_of(captured[0])
        main_seed = self._seed_of(captured[1])
        assert populate_seed == main_seed == "111"


class TestPopulateWithArbitraryCommand:
    """A ``command:`` scenario using ``populate_with`` must run its populate
    pass as the arbitrary write command (after ``--``), sequentially, sharing
    the main run's ``--seed``. Before the fix this path raised
    ``KeyError: 'test'`` because the populate helper read ``scenario["test"]``.
    """

    @staticmethod
    def _seed_of(command):
        assert "--seed" in command, f"no --seed in command: {command}"
        return command[command.index("--seed") + 1]

    def test_arbitrary_command_populate_argv(self, minimal_client_runner):
        runner = minimal_client_runner
        # command: (no test:) -> populate_with is an arbitrary write command
        # string. This is the exact shape that raised KeyError before the fix.
        scenario = {
            "id": "s1",
            "command": "GET key:__rand_int__",
            "populate_with": "SET key:__rand_int__ __data__",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }

        captured = []

        def fake_run(command=None, *args, **kwargs):
            captured.append(command)
            return MagicMock()

        with (
            patch.object(runner, "_run", side_effect=fake_run),
            patch("valkey_benchmark.random.randint", side_effect=[111, 222, 333]),
        ):
            runner._run_single_scenario(
                scenario,
                group_id=1,
                profiler=None,
                metrics_processor=None,
                profiling_enabled=False,
                commit_time="2026-01-01T00:00:00Z",
                config_set={},
                config_suffix="default",
            )

        assert len(captured) == 2  # populate invocation + main invocation
        populate_cmd = captured[0]

        # The populate command runs the write string after the -- separator.
        assert "--" in populate_cmd
        tokens_after_sep = populate_cmd[populate_cmd.index("--") + 1 :]
        assert tokens_after_sep == ["SET", "key:__rand_int__", "__data__"]

        # Sequential seeding, and the SAME seed as the main run.
        assert "--sequential" in populate_cmd
        assert self._seed_of(populate_cmd) == self._seed_of(captured[1]) == "111"


class TestPopulateWithTestArgvUnchanged:
    """A ``test:`` scenario using ``populate_with`` must still seed via the
    predefined write workload (``-t NAME``), sequentially, with the shared
    seed. This locks the compiled basic-format populate argv shape that the
    golden fixtures depend on."""

    @staticmethod
    def _seed_of(command):
        assert "--seed" in command, f"no --seed in command: {command}"
        return command[command.index("--seed") + 1]

    def test_test_format_populate_argv(self, minimal_client_runner):
        runner = minimal_client_runner
        scenario = {
            "id": "s1",
            "test": "GET",
            "populate_with": "SET",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }

        captured = []

        def fake_run(command=None, *args, **kwargs):
            captured.append(command)
            return MagicMock()

        with (
            patch.object(runner, "_run", side_effect=fake_run),
            patch("valkey_benchmark.random.randint", side_effect=[111, 222, 333]),
        ):
            runner._run_single_scenario(
                scenario,
                group_id=1,
                profiler=None,
                metrics_processor=None,
                profiling_enabled=False,
                commit_time="2026-01-01T00:00:00Z",
                config_set={},
                config_suffix="default",
            )

        assert len(captured) == 2
        populate_cmd = captured[0]

        # Predefined -t write workload, never the -- separator.
        assert "-t" in populate_cmd
        assert populate_cmd[populate_cmd.index("-t") + 1] == "SET"
        assert "--" not in populate_cmd
        assert "--sequential" in populate_cmd
        assert self._seed_of(populate_cmd) == self._seed_of(captured[1]) == "111"


class TestPopulateWithNotInMetrics:
    """``populate_with`` is a scenario input, not a metrics output. A scenario
    carrying it must not leak the field into the produced metrics row."""

    def test_populate_with_absent_from_metrics(self, minimal_client_runner):
        from process_metrics import MetricsProcessor

        real_processor = MetricsProcessor(
            commit_id="abc123",
            cluster_mode=False,
            tls_mode=False,
            commit_time="2026-01-01T00:00:00Z",
        )

        scenario = {
            "id": "s1",
            "test": "GET",
            "populate_with": "SET",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }

        csv_stdout = _make_csv(
            [
                {
                    "test": "GET",
                    "rps": "1000.0",
                    "avg_latency_ms": "1.0",
                    "min_latency_ms": "0.5",
                    "p50_latency_ms": "1.0",
                    "p95_latency_ms": "2.0",
                    "p99_latency_ms": "3.0",
                    "max_latency_ms": "5.0",
                }
            ]
        )
        proc = MagicMock()
        proc.stdout = csv_stdout

        metrics = minimal_client_runner._build_scenario_metrics(
            scenario,
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
    """Restarting a server discards CONFIG SET values, so any scenario that
    restarts must re-apply the active ``config_set`` afterwards. Compiled
    basic-format scenarios set ``restart_before`` on every combination, so
    without this the sweep values from ``config_sets`` would be silently lost
    for the rest of the run."""

    @staticmethod
    def _scenario(**extra):
        scenario = {
            "id": "s1",
            "test": "SET",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }
        scenario.update(extra)
        return scenario

    def _run(self, runner, scenario, config_set):
        """Run one scenario, returning the ordered server-side call names."""
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
            runner._run_single_scenario(
                scenario,
                group_id=1,
                profiler=None,
                metrics_processor=None,
                profiling_enabled=False,
                commit_time="2026-01-01T00:00:00Z",
                config_set=config_set,
                config_suffix="default",
            )
        return calls

    def test_restart_before_reapplies_config_set(self, minimal_client_runner):
        from valkey_benchmark import ORIGIN_FIELD, ORIGIN_SIMPLE

        runner = minimal_client_runner
        runner.server_launcher = MagicMock()
        config_set = {"appendonly": "no"}

        calls = self._run(
            runner,
            self._scenario(**{ORIGIN_FIELD: ORIGIN_SIMPLE, "restart_before": True}),
            config_set,
        )

        # Restart first, then re-apply: applying before the restart would be
        # discarded by it.
        assert calls == ["restart", f"apply:{config_set}"]

    def test_flush_before_reapplies_config_set(self, minimal_client_runner):
        runner = minimal_client_runner
        runner.server_launcher = MagicMock()
        config_set = {"appendonly": "no"}

        calls = self._run(runner, self._scenario(flush_before=True), config_set)

        assert calls == ["restart", f"apply:{config_set}"]

    def test_restart_and_flush_flags_together_restart_once(self, minimal_client_runner):
        runner = minimal_client_runner
        runner.server_launcher = MagicMock()

        calls = self._run(
            runner,
            self._scenario(restart_before=True, flush_before=True),
            {"appendonly": "no"},
        )

        assert calls.count("restart") == 1

    def test_empty_config_set_skips_reapply(self, minimal_client_runner):
        runner = minimal_client_runner
        runner.server_launcher = MagicMock()

        calls = self._run(runner, self._scenario(restart_before=True), {})

        assert calls == ["restart"]

    def test_without_launcher_flushes_and_skips_reapply(self, minimal_client_runner):
        runner = minimal_client_runner
        runner.server_launcher = None

        calls = self._run(
            runner, self._scenario(restart_before=True), {"appendonly": "no"}
        )

        # No launcher means we cannot restart, so there is nothing to restore.
        assert calls == ["flush"]


# ---------------------------------------------------------------------------
# _run_single_scenario error-path parity between origin-simple (compiled
# basic-format) and hand-written scenarios. The old simple path: a failing
# benchmark COMMAND aborted the whole run (semantic a); proc-is-None skipped
# the combination with no row (semantic b); a CSV-parse/metrics error was
# logged and the run continued (semantic c). Compiled basic scenarios must
# reproduce those semantics AND never leak scenario-only schema keys into the
# basic-format metrics.json.
# ---------------------------------------------------------------------------

# Scenario-only keys that must never appear on an origin-simple metrics row;
# one leaked row permanently pollutes the dynamically-created core metrics
# table and the comparison tool's auto-discovered grouping keys.
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
    """Error-path parity for compiled basic (origin-simple) vs hand-written
    scenarios in ``_run_single_scenario``."""

    @staticmethod
    def _command_scenario(**extra):
        """A minimal command-type scenario (no populate/restart side effects)."""
        scenario = {
            "id": "s1",
            "type": "write",
            "command": "SET",
            "requests": 100,
            "keyspacelen": 100,
            "data_size": 64,
            "pipeline": 1,
            "clients": 1,
        }
        scenario.update(extra)
        return scenario

    @staticmethod
    def _origin_simple(**extra):
        from valkey_benchmark import ORIGIN_FIELD, ORIGIN_SIMPLE

        return TestRunSingleScenarioErrorPaths._command_scenario(
            **{ORIGIN_FIELD: ORIGIN_SIMPLE, **extra}
        )

    @staticmethod
    def _invoke(runner, scenario, metrics_processor):
        return runner._run_single_scenario(
            scenario,
            group_id=1,
            profiler=None,
            metrics_processor=metrics_processor,
            profiling_enabled=False,
            commit_time="2026-01-01T00:00:00Z",
            config_set={"appendonly": "no"},
            config_suffix="default",
        )

    def test_origin_simple_no_results_returns_none(self, minimal_client_runner):
        """Finding 1: an origin-simple scenario whose benchmark returns no
        results skips the combination (returns None, no metrics row, no
        exception) instead of emitting a failure marker."""
        runner = minimal_client_runner
        metrics_processor = MagicMock()

        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=None),
        ):
            result = self._invoke(runner, self._origin_simple(), metrics_processor)

        assert result is None
        # No row was produced, so metrics were never constructed.
        metrics_processor.create_metrics.assert_not_called()

    def test_handwritten_no_results_returns_failure_marker(self, minimal_client_runner):
        """Unchanged behavior: a hand-written scenario in the same no-results
        situation still returns a failure-marker dict."""
        runner = minimal_client_runner
        metrics_processor = MagicMock()

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
        """Finding 2 (semantic c): a parse/metrics-phase error on an
        origin-simple scenario is logged and yields None so the run continues
        -- it must NOT propagate."""
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
        """Finding 2 (semantic a): a benchmark-invocation failure on an
        origin-simple scenario propagates out of ``_run_single_scenario`` and
        aborts the run."""
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
        """Schema-fence guard: across both the success path and the no-results
        path, an origin-simple metrics result never contains scenario-only
        keys."""
        runner = minimal_client_runner

        # Success path: create_metrics returns a clean basic-format row and
        # _run_single_scenario must return it verbatim for origin-simple.
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

        # No-results path: must return None (a failure marker WOULD carry the
        # forbidden keys).
        with (
            patch.object(runner, "_build_benchmark_command", return_value=["vb"]),
            patch.object(runner, "_run", return_value=None),
        ):
            no_results = self._invoke(runner, self._origin_simple(), MagicMock())

        assert no_results is None
