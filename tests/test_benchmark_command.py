"""Unit tests for ClientRunner._build_benchmark_command."""

import pytest


@pytest.fixture
def base_cmd_params():
    """Common parameters shared across _build_benchmark_command tests."""
    return dict(
        requests=100, keyspacelen=100, data_size=32, pipeline=1, clients=10, seed_val=1
    )


class TestBuildBenchmarkCommandSimpleFormat:
    """Test simple format (no scenario) produces correct flags."""

    def test_simple_format_contains_all_flags(self, minimal_client_runner):
        """Simple format command includes all expected positional flags."""
        cmd = minimal_client_runner._build_benchmark_command(
            tls=False,
            requests=1000,
            keyspacelen=5000,
            data_size=64,
            pipeline=1,
            clients=50,
            command="GET",
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

    def test_simple_format_no_taskset_by_default(
        self, minimal_client_runner, base_cmd_params
    ):
        """Without CPU pinning, taskset should not appear."""
        cmd = minimal_client_runner._build_benchmark_command(
            command="SET", **base_cmd_params
        )
        assert "taskset" not in cmd

    def test_simple_format_includes_cluster_flag_in_cluster_mode(
        self, minimal_client_runner, base_cmd_params
    ):
        """Simple format commands include --cluster when cluster mode is enabled."""
        minimal_client_runner.cluster_mode = True

        cmd = minimal_client_runner._build_benchmark_command(
            command="SET", **base_cmd_params
        )

        assert "--cluster" in cmd

    def test_simple_format_omits_cluster_flag_when_cluster_mode_disabled(
        self, minimal_client_runner, base_cmd_params
    ):
        """Simple format commands should not include --cluster outside cluster mode."""
        cmd = minimal_client_runner._build_benchmark_command(
            command="SET", **base_cmd_params
        )

        assert "--cluster" not in cmd

    def test_simple_format_includes_cluster_flag_without_cluster_nodes_config(
        self, minimal_client_runner, base_cmd_params
    ):
        """Issue #27 regression: cluster mode should not depend on cluster_nodes metadata."""
        minimal_client_runner.cluster_mode = True
        minimal_client_runner.config.pop("cluster_nodes", None)

        cmd = minimal_client_runner._build_benchmark_command(
            command="SET", **base_cmd_params
        )

        assert "--cluster" in cmd


class TestBuildBenchmarkCommandTLS:
    """Test TLS mode includes TLS flags."""

    def test_tls_flags_present(self, minimal_client_runner, base_cmd_params):
        """When tls=True, TLS cert/key/cacert flags are included."""
        cmd = minimal_client_runner._build_benchmark_command(
            tls=True, command="GET", **base_cmd_params
        )

        assert "--tls" in cmd
        assert "--cert" in cmd
        assert cmd[cmd.index("--cert") + 1] == "./tests/tls/valkey.crt"
        assert "--key" in cmd
        assert cmd[cmd.index("--key") + 1] == "./tests/tls/valkey.key"
        assert "--cacert" in cmd
        assert cmd[cmd.index("--cacert") + 1] == "./tests/tls/ca.crt"

    def test_no_tls_flags_when_disabled(self, minimal_client_runner, base_cmd_params):
        """When tls=False, no TLS flags appear."""
        cmd = minimal_client_runner._build_benchmark_command(
            tls=False, command="GET", **base_cmd_params
        )
        assert "--tls" not in cmd
        assert "--cert" not in cmd


class TestBuildBenchmarkCommandCPUPinning:
    """Test CPU pinning prepends taskset."""

    def test_cpu_range_param_prepends_taskset(
        self, minimal_client_runner, base_cmd_params
    ):
        """Passing cpu_range prepends taskset -c <range> to the command."""
        cmd = minimal_client_runner._build_benchmark_command(
            command="GET", cpu_range="0-3", **base_cmd_params
        )

        assert cmd[0] == "taskset"
        assert cmd[1] == "-c"
        assert cmd[2] == "0-3"
        assert cmd[3] == "src/valkey-benchmark"

    def test_self_cores_prepends_taskset(self, minimal_client_runner, base_cmd_params):
        """When self.cores is set, taskset is prepended."""
        minimal_client_runner.cores = "4-7"
        cmd = minimal_client_runner._build_benchmark_command(
            command="SET", **base_cmd_params
        )

        assert cmd[0] == "taskset"
        assert cmd[1] == "-c"
        assert cmd[2] == "4-7"


class TestBuildBenchmarkCommandDuration:
    """Test duration mode uses --duration instead of -n."""

    def test_duration_flag_replaces_requests(
        self, minimal_client_runner, base_cmd_params
    ):
        """When duration is provided, --duration is used instead of -n."""
        base_cmd_params["requests"] = None
        cmd = minimal_client_runner._build_benchmark_command(
            command="GET", duration=30, **base_cmd_params
        )

        assert "--duration" in cmd
        assert cmd[cmd.index("--duration") + 1] == "30"
        assert "-n" not in cmd

    def test_no_duration_uses_requests(self, minimal_client_runner):
        """Without duration, -n flag is used with requests count."""
        cmd = minimal_client_runner._build_benchmark_command(
            requests=5000,
            keyspacelen=100,
            data_size=32,
            pipeline=1,
            clients=10,
            command="GET",
            seed_val=1,
        )

        assert "-n" in cmd
        assert cmd[cmd.index("-n") + 1] == "5000"
        assert "--duration" not in cmd


class TestBuildBenchmarkCommandScenarios:
    """Test scenario-based command construction."""

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
