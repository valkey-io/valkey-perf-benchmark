"""Unit tests for pure logic methods on ClientRunner from valkey_benchmark.py."""

import pytest
from pathlib import Path

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
# _generate_combinations
# ---------------------------------------------------------------------------


class TestGenerateCombinations:
    """Tests for ClientRunner._generate_combinations."""

    def test_default_config(self, minimal_client_runner):
        combos = minimal_client_runner._generate_combinations()
        # requests=[1000], keyspacelen=[1000], data_sizes=[64], pipelines=[1],
        # clients=[50], commands=["GET","SET"], warmup=0, duration=None
        assert len(combos) == 2  # 1*1*1*1*1*2*1*1

    def test_cartesian_product_count(self, minimal_valid_config):
        minimal_valid_config["data_sizes"] = [64, 128]
        minimal_valid_config["pipelines"] = [1, 10]
        runner = ClientRunner(
            commit_id="abc",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )
        combos = runner._generate_combinations()
        # 1 * 1 * 2 * 2 * 1 * 2 * 1 * 1 = 8
        assert len(combos) == 8

    def test_tuple_structure(self, minimal_client_runner):
        combos = minimal_client_runner._generate_combinations()
        first = combos[0]
        # (requests, keyspacelen, data_size, pipeline, clients, command, warmup, duration)
        assert len(first) == 8
        assert first[0] == 1000  # requests
        assert first[1] == 1000  # keyspacelen
        assert first[2] == 64  # data_size
        assert first[3] == 1  # pipeline
        assert first[4] == 50  # clients
        assert first[5] in ("GET", "SET")
        assert first[6] == 0  # warmup
        assert first[7] is None  # duration

    def test_no_requests_key(self, minimal_valid_config):
        del minimal_valid_config["requests"]
        runner = ClientRunner(
            commit_id="abc",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )
        combos = runner._generate_combinations()
        # requests defaults to [None]
        assert combos[0][0] is None


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
# module_commit_id and config_name conditional output
# ---------------------------------------------------------------------------


class TestModuleCommitAndConfigName:
    """Verify module_commit and config_name are only present when provided."""

    def test_module_commit_present_when_set(self, minimal_valid_config):
        """When module_commit_id is provided, metrics should contain module_commit."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            module_commit_id="deadbeef1234",
        )
        assert runner.module_commit_id == "deadbeef1234"

    def test_module_commit_absent_when_not_set(self, minimal_valid_config):
        """When module_commit_id is not provided, it should be None."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )
        assert runner.module_commit_id is None

    def test_config_name_present_when_set(self, minimal_valid_config):
        """When config_name is provided, it should be stored on the runner."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            config_name="fts-benchmarks-arm.json",
        )
        assert runner.config_name == "fts-benchmarks-arm.json"

    def test_config_name_absent_when_not_set(self, minimal_valid_config):
        """When config_name is not provided, it should be None."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )
        assert runner.config_name is None

    def test_both_set_for_module_run(self, minimal_valid_config):
        """Module runs should have both module_commit_id and config_name set."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            module_commit_id="deadbeef1234",
            config_name="fts-benchmarks-arm.json",
        )
        assert runner.module_commit_id == "deadbeef1234"
        assert runner.config_name == "fts-benchmarks-arm.json"

    def test_neither_set_for_core_run(self, minimal_valid_config):
        """Core runs should have neither module_commit_id nor config_name."""
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )
        assert runner.module_commit_id is None
        assert runner.config_name is None

    def test_module_commit_defaults_to_head_for_module_run(self, minimal_valid_config):
        """Module runs without explicit --module-commit should use 'HEAD'.

        This mirrors how core's commit defaults to 'HEAD' when --commits is not passed.
        The default is applied in benchmark.py: (args.module_commit or "HEAD") if args.module
        Here we verify that passing "HEAD" works correctly on the runner.
        """
        runner = ClientRunner(
            commit_id="abc123",
            config=minimal_valid_config,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
            module_commit_id="HEAD",
            config_name="fts-benchmarks-arm.json",
        )
        assert runner.module_commit_id == "HEAD"
        assert runner.config_name == "fts-benchmarks-arm.json"


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
<<<<<<< HEAD
<<<<<<< HEAD
                    "scenarios": [{"id": "s1", "command": "GET", "type": "read"}],
=======
                    "scenarios": [
                        {"id": "s1", "command": "GET", "type": "read"}
                    ],
>>>>>>> c2961f0 (add additional unit tests for new features)
=======
                    "scenarios": [{"id": "s1", "command": "GET", "type": "read"}],
>>>>>>> 5529b33 (Sync with PR branch: review feedback updates)
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
<<<<<<< HEAD
<<<<<<< HEAD
                    "scenarios": [{"id": "s1", "command": "SET", "type": "write"}],
=======
                    "scenarios": [
                        {"id": "s1", "command": "SET", "type": "write"}
                    ],
>>>>>>> c2961f0 (add additional unit tests for new features)
=======
                    "scenarios": [{"id": "s1", "command": "SET", "type": "write"}],
>>>>>>> 5529b33 (Sync with PR branch: review feedback updates)
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
# _iterate_simple_scenarios — regression: simple (non-test_groups) format
# must not pick up group_description / group_id / scenario fields.
# ---------------------------------------------------------------------------


class TestIterateSimpleScenarios:
    """Tests for ClientRunner._iterate_simple_scenarios.

    The simple/core config format (commands + data_sizes + ... cartesian)
    does not have groups or descriptions. These tests lock in that the
    new group/scenario_description plumbing did not leak into the simple
    path.
    """

    def test_simple_yields_no_group_fields(self, minimal_client_runner):
        items = list(minimal_client_runner._iterate_simple_scenarios())

        assert len(items) > 0
        for item in items:
            assert item["format"] == "simple"
            assert "group_description" not in item
            assert "group_id" not in item
            assert "scenario" not in item

    def test_simple_format_has_combination_keys(self, minimal_client_runner):
        # Sanity: simple-format dicts carry the per-run combination keys
        # (command, data_size, pipeline, ...) instead of group/scenario.
        items = list(minimal_client_runner._iterate_simple_scenarios())

        first = items[0]
        for key in ("command", "data_size", "pipeline", "clients", "requests"):
            assert key in first
