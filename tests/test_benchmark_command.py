"""Unit tests for ClientRunner._build_benchmark_command."""

import pytest


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

    def test_test_format_contains_all_flags(self, minimal_client_runner):
        """Test-format command includes all expected flags."""
        cmd = minimal_client_runner._build_benchmark_command(
            {
                "test": "GET",
                "requests": 1000,
                "keyspacelen": 5000,
                "data_size": 64,
                "pipeline": 1,
                "clients": 50,
            },
            tls=False,
            seed_val=42,
        )

        assert cmd[0] == "src/valkey-benchmark"
        assert "-h" in cmd
        assert cmd[cmd.index("-h") + 1] == "127.0.0.1"
        assert "-p" in cmd
        assert cmd[cmd.index("-p") + 1] == "6379"
        assert "-n" in cmd
        assert cmd[cmd.index("-n") + 1] == "1000"
        assert "-r" in cmd
        assert cmd[cmd.index("-r") + 1] == "5000"
        assert "-d" in cmd
        assert cmd[cmd.index("-d") + 1] == "64"
        assert "-P" in cmd
        assert cmd[cmd.index("-P") + 1] == "1"
        assert "-c" in cmd
        assert cmd[cmd.index("-c") + 1] == "50"
        assert "-t" in cmd
        assert cmd[cmd.index("-t") + 1] == "GET"
        assert "--seed" in cmd
        assert cmd[cmd.index("--seed") + 1] == "42"
        assert "--csv" in cmd
        # test-format commands never use the -- separator
        assert "--" not in cmd

    def test_test_format_no_taskset_by_default(
        self, minimal_client_runner, base_test_scenario
    ):
        """Without CPU pinning, taskset should not appear."""
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )
        assert "taskset" not in cmd

    def test_test_format_includes_cluster_flag_in_cluster_mode(
        self, minimal_client_runner, base_test_scenario
    ):
        """Test-format commands include --cluster when cluster mode is enabled."""
        minimal_client_runner.cluster_mode = True

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--cluster" in cmd

    def test_test_format_omits_cluster_flag_when_cluster_mode_disabled(
        self, minimal_client_runner, base_test_scenario
    ):
        """Test-format commands should not include --cluster outside cluster mode."""
        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--cluster" not in cmd

    def test_test_format_includes_cluster_flag_without_cluster_nodes_config(
        self, minimal_client_runner, base_test_scenario
    ):
        """Issue #27 regression: cluster mode should not depend on cluster_nodes metadata."""
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config.pop("cluster_nodes", None)

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--cluster" in cmd

    def test_test_format_warmup_inline_flag(
        self, minimal_client_runner, base_test_scenario
    ):
        """warmup_inline emits --warmup N on the main invocation."""
        base_test_scenario["warmup_inline"] = 10

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--warmup" in cmd
        assert cmd[cmd.index("--warmup") + 1] == "10"

    def test_test_format_zero_warmup_inline_omitted(
        self, minimal_client_runner, base_test_scenario
    ):
        """warmup_inline of 0 does not emit the --warmup flag."""
        base_test_scenario["warmup_inline"] = 0

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--warmup" not in cmd

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

    def test_test_format_sequential_flag(
        self, minimal_client_runner, base_test_scenario
    ):
        """sequential: true emits --sequential (used by populate passes)."""
        base_test_scenario["sequential"] = True

        cmd = minimal_client_runner._build_benchmark_command(
            base_test_scenario, seed_val=1
        )

        assert "--sequential" in cmd

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

    def test_arbitrary_command_includes_data_size(self, minimal_client_runner):
        """data_size reaches -d so the __data__ placeholder is sized correctly."""
        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET key:__rand_int__ __data__",
                "type": "write",
                "data_size": 1024,
            }
        )

        assert "-d" in cmd
        assert cmd[cmd.index("-d") + 1] == "1024"
        # The command itself must still be passed as argv after "--".
        assert cmd[cmd.index("--") + 1 :] == ["SET", "key:__rand_int__", "__data__"]

    def test_arbitrary_command_omits_data_size_when_unset(self, minimal_client_runner):
        """Scenarios that do not ask for a payload size keep their existing argv."""
        cmd = minimal_client_runner._build_benchmark_command(
            scenario={"command": "SET foo bar", "type": "write"}
        )

        assert "-d" not in cmd

    def test_arbitrary_command_includes_benchmark_threads(self, minimal_client_runner):
        """Client thread count comes from the config, as it does for -t workloads,
        so arbitrary results stay comparable to predefined ones."""
        minimal_client_runner.benchmark_threads = 90

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={"command": "SET key:__rand_int__ __data__", "type": "write"}
        )

        assert "--threads" in cmd
        assert cmd[cmd.index("--threads") + 1] == "90"
        # The flag must precede "--", otherwise valkey-benchmark treats it as
        # part of the command rather than a client option.
        assert cmd.index("--threads") < cmd.index("--")

    def test_arbitrary_command_omits_threads_when_unconfigured(
        self, minimal_client_runner
    ):
        """No benchmark-threads in config means valkey-benchmark's own default."""
        minimal_client_runner.benchmark_threads = None

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={"command": "SET foo bar", "type": "write"}
        )

        assert "--threads" not in cmd

    def test_single_node_scenario_includes_cluster_flag_in_cluster_mode(
        self, minimal_client_runner
    ):
        """Scenario commands include --cluster for single-node execution in cluster mode."""
        minimal_client_runner.cluster_mode = True

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET foo bar",
                "type": "write",
                "cluster_execution": "single",
            }
        )

        assert "--cluster" in cmd

    def test_single_node_scenario_includes_cluster_flag_without_cluster_nodes_config(
        self, minimal_client_runner
    ):
        """Scenario cluster routing should not depend on cluster_nodes metadata."""
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config.pop("cluster_nodes", None)

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET foo bar",
                "type": "write",
                "cluster_execution": "single",
            }
        )

        assert "--cluster" in cmd

    def test_parallel_scenario_omits_cluster_flag(self, minimal_client_runner):
        """Parallel cluster execution should not pass --cluster to a single command."""
        minimal_client_runner.cluster_mode = True

        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET foo bar",
                "type": "write",
                "cluster_execution": "parallel",
            }
        )

        assert "--cluster" not in cmd

    def test_single_node_scenario_omits_cluster_flag_when_cluster_mode_disabled(
        self, minimal_client_runner
    ):
        """Scenario commands should not include --cluster outside cluster mode."""
        cmd = minimal_client_runner._build_benchmark_command(
            scenario={
                "command": "SET foo bar",
                "type": "write",
                "cluster_execution": "single",
            }
        )

        assert "--cluster" not in cmd
