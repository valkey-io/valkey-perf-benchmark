"""Unit tests for ClientRunner._build_benchmark_command."""

from unittest.mock import MagicMock, patch

import pytest

from benchmark import validate_config


def _compiled_basic_scenario(command):
    """Return the scenario produced by compiling a one-command basic config."""
    cfg = {
        "requests": [100],
        "keyspacelen": [1000],
        "data_sizes": [16],
        "pipelines": [1],
        "clients": [1],
        "commands": [command],
        "cluster_mode": False,
        "tls_mode": False,
        "warmup": 0,
    }
    validate_config(cfg)
    return cfg["test_groups"][0]["scenarios"][0]


_COMPILED_ARGV_PREFIX = [
    "src/valkey-benchmark",
    "-h",
    "127.0.0.1",
    "-p",
    "6379",
    "-n",
    "100",
    "-r",
    "1000",
    "-d",
    "16",
    "-P",
    "1",
    "-c",
    "1",
]


@pytest.fixture
def base_test_scenario():
    """Common predefined-workload (-t) scenario shared across builder tests."""
    return {
        "test": "GET",
        "requests": 100,
        "keyspacelen": 100,
        "data_size": 32,
        "pipeline": 1,
        "clients": 10,
    }


class TestBuildBenchmarkCommandTestFormat:
    """Test predefined-workload scenarios ('test' field) produce correct flags."""

    @pytest.mark.parametrize(
        "cluster_mode, drop_nodes, expect_cluster",
        [
            (True, False, True),
            (True, True, True),  # must not depend on cluster_nodes metadata
            (False, False, False),
        ],
    )
    def test_test_format_cluster_flag(
        self,
        minimal_client_runner,
        base_test_scenario,
        cluster_mode,
        drop_nodes,
        expect_cluster,
    ):
        """--cluster tracks cluster_mode and never depends on cluster_nodes."""
        minimal_client_runner.cluster_mode = cluster_mode
        if drop_nodes:
            minimal_client_runner.config.pop("cluster_nodes", None)

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert ("--cluster" in cmd) is expect_cluster

    @pytest.mark.parametrize("warmup_inline, expect_flag", [(10, True), (0, False)])
    def test_test_format_warmup_inline(
        self, minimal_client_runner, base_test_scenario, warmup_inline, expect_flag
    ):
        """warmup_inline > 0 emits --warmup N on the main invocation; 0 omits it."""
        base_test_scenario["warmup_inline"] = warmup_inline

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert ("--warmup" in cmd) is expect_flag
        if expect_flag:
            assert cmd[cmd.index("--warmup") + 1] == str(warmup_inline)

    def test_test_format_benchmark_threads(
        self, minimal_client_runner, base_test_scenario
    ):
        """benchmark-threads emits --threads for test-format scenarios."""
        minimal_client_runner.benchmark_threads = 4

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--threads" in cmd
        assert cmd[cmd.index("--threads") + 1] == "4"

    def test_test_format_requires_requests_or_duration(
        self, minimal_client_runner, base_test_scenario
    ):
        """A test-format scenario without requests or duration is rejected."""
        del base_test_scenario["requests"]

        with pytest.raises(ValueError):
            minimal_client_runner._build_benchmark_command(
                base_test_scenario, seed_val=1
            )

    def test_test_format_keyspacelen_falls_back_to_config(
        self, minimal_client_runner, base_test_scenario
    ):
        """Without per-scenario keyspacelen, config-level keyspacelen[0] is used."""
        del base_test_scenario["keyspacelen"]

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        # minimal_valid_config has keyspacelen [1000]
        assert cmd[cmd.index("-r") + 1] == "1000"

    def test_write_workload_exact_argv(self, minimal_client_runner):
        scenario = _compiled_basic_scenario("SET")
        with patch("valkey_benchmark.random.randint", return_value=42):
            cmd = minimal_client_runner._build_benchmark_command(scenario, tls=False)

        assert cmd == [
            *_COMPILED_ARGV_PREFIX,
            "-t",
            "SET",
            "--seed",
            "42",
            "--csv",
        ]

    def test_read_workload_populate_and_main_exact_argv(self, minimal_client_runner):
        scenario = _compiled_basic_scenario("GET")
        captured = []

        def fake_run(command=None, *args, **kwargs):
            captured.append(list(command))
            return MagicMock()

        with (
            patch.object(minimal_client_runner, "_run", side_effect=fake_run),
            patch.object(minimal_client_runner, "_flush_database"),
            patch("valkey_benchmark.random.randint", return_value=777),
        ):
            minimal_client_runner._run_single_scenario(
                scenario,
                group_id=1,
                profiler=None,
                metrics_processor=None,
                config_set={},
                config_suffix="default",
            )

        assert len(captured) == 2
        populate_cmd, main_cmd = captured

        assert populate_cmd == [
            *_COMPILED_ARGV_PREFIX,
            "-t",
            "SET",
            "--sequential",
            "--seed",
            "777",
            "--csv",
        ]
        assert main_cmd == [
            *_COMPILED_ARGV_PREFIX,
            "-t",
            "GET",
            "--seed",
            "777",
            "--csv",
        ]
        assert (
            populate_cmd[populate_cmd.index("--seed") + 1]
            == main_cmd[main_cmd.index("--seed") + 1]
            == "777"
        )
        assert "--sequential" in populate_cmd
        assert "--sequential" not in main_cmd


class TestBuildBenchmarkCommandTLS:
    """Test TLS mode includes TLS flags."""

    def test_tls_flags_present(self, minimal_client_runner, base_test_scenario):
        """When tls=True, TLS cert/key/cacert flags are included."""
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, tls=True, seed_val=1
        )

        assert "--tls" in cmd
        assert "--cert" in cmd
        assert cmd[cmd.index("--cert") + 1] == "./tests/tls/valkey.crt"
        assert "--key" in cmd
        assert cmd[cmd.index("--key") + 1] == "./tests/tls/valkey.key"
        assert "--cacert" in cmd
        assert cmd[cmd.index("--cacert") + 1] == "./tests/tls/ca.crt"

    def test_no_tls_flags_when_disabled(
        self, minimal_client_runner, base_test_scenario
    ):
        """When tls=False, no TLS flags appear."""
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, tls=False, seed_val=1
        )
        assert "--tls" not in cmd
        assert "--cert" not in cmd


class TestBuildBenchmarkCommandCPUPinning:
    """Test CPU pinning prepends taskset."""

    def test_cpu_range_param_prepends_taskset(
        self, minimal_client_runner, base_test_scenario
    ):
        """Passing cpu_range prepends taskset -c <range> to the command."""
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, cpu_range="0-3", seed_val=1
        )

        assert cmd[0] == "taskset"
        assert cmd[1] == "-c"
        assert cmd[2] == "0-3"
        assert cmd[3] == "src/valkey-benchmark"

    def test_self_cores_prepends_taskset(
        self, minimal_client_runner, base_test_scenario
    ):
        """When self.cores is set, taskset is prepended."""
        minimal_client_runner.cores = "4-7"
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert cmd[0] == "taskset"
        assert cmd[1] == "-c"
        assert cmd[2] == "4-7"


class TestBuildBenchmarkCommandDuration:
    """Test duration mode uses --duration instead of -n."""

    def test_duration_flag_replaces_requests(
        self, minimal_client_runner, base_test_scenario
    ):
        """When duration is provided, --duration is used instead of -n."""
        base_test_scenario.pop("requests")
        base_test_scenario["duration"] = 30

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--duration" in cmd
        assert cmd[cmd.index("--duration") + 1] == "30"
        assert "-n" not in cmd

    def test_no_duration_uses_requests(self, minimal_client_runner):
        """Without duration, -n flag is used with requests count."""
        cmd = minimal_client_runner._build_benchmark_command(
            {
                "test": "GET",
                "requests": 5000,
                "keyspacelen": 100,
                "data_size": 32,
                "pipeline": 1,
                "clients": 10,
            },
            seed_val=1,
        )

        assert "-n" in cmd
        assert cmd[cmd.index("-n") + 1] == "5000"
        assert "--duration" not in cmd


class TestBuildBenchmarkCommandScenarios:
    """Test scenario-based command construction."""

    def test_arbitrary_command_omits_data_size_when_unset(self, minimal_client_runner):
        """Scenarios that do not ask for a payload size keep their existing argv."""
        cmd = minimal_client_runner._build_benchmark_command(
            scenario={"command": "SET foo bar", "type": "write"}
        )

        assert "-d" not in cmd

    def test_arbitrary_command_omits_threads_when_unconfigured(
        self, minimal_client_runner
    ):
        """No benchmark-threads in config means valkey-benchmark's own default."""
        minimal_client_runner.benchmark_threads = None

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={"command": "SET foo bar", "type": "write"}
        )

        assert "--threads" not in cmd

    @pytest.mark.parametrize(
        "cluster_mode, execution, drop_nodes, expect_cluster",
        [
            (True, "single", False, True),
            (True, "single", True, True),  # routing must not depend on cluster_nodes
            (True, "parallel", False, False),
            (False, "single", False, False),
        ],
    )
    def test_scenario_cluster_flag(
        self,
        minimal_client_runner,
        cluster_mode,
        execution,
        drop_nodes,
        expect_cluster,
    ):
        """--cluster is added only for single-node execution inside cluster mode,
        never for parallel execution, and never depends on cluster_nodes."""
        minimal_client_runner.cluster_mode = cluster_mode
        if drop_nodes:
            minimal_client_runner.config.pop("cluster_nodes", None)

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET foo bar",
                "type": "write",
                "cluster_execution": execution,
            }
        )

        assert ("--cluster" in cmd) is expect_cluster

    def test_arbitrary_command_exact_argv(self, minimal_client_runner):
        minimal_client_runner.benchmark_threads = 4
        scenario = {
            "command": "SET key:__rand_int__ __data__",
            "type": "write",
            "data_size": 128,
            "requests": 100,
            "pipeline": 1,
            "clients": 1,
            "keyspacelen": 1000,
        }

        with patch("valkey_benchmark.random.randint", return_value=99):
            cmd = minimal_client_runner._build_benchmark_command(scenario, tls=False)

        assert cmd == [
            "src/valkey-benchmark",
            "-h",
            "127.0.0.1",
            "-p",
            "6379",
            "-n",
            "100",
            "-c",
            "1",
            "-P",
            "1",
            "-r",
            "1000",
            "-d",
            "128",
            "--threads",
            "4",
            "--seed",
            "99",
            "--csv",
            "--",
            "SET",
            "key:__rand_int__",
            "__data__",
        ]
