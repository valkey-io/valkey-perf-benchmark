"""Unit tests for environment_metadata module."""

import platform
from unittest.mock import patch, MagicMock

import pytest

from environment_metadata import (
    collect_environment_metadata,
    get_cpu_governor,
    get_turbo_boost_status,
    get_cpu_frequency_mhz,
    get_benchmark_tool_version,
    get_idle_states_status,
)


class TestGetCpuGovernor:
    def test_returns_string(self):
        result = get_cpu_governor()
        assert isinstance(result, str)
        assert len(result) > 0

    @patch("environment_metadata._read_sysfs", return_value="performance")
    def test_performance_governor(self, mock_read):
        assert get_cpu_governor() == "performance"

    @patch("environment_metadata._read_sysfs", return_value="")
    def test_not_available_when_no_cpufreq(self, mock_read):
        assert get_cpu_governor() == "not_available"


class TestGetTurboBoostStatus:
    """Each test mocks the sysfs reads to force a specific, assertable outcome."""

    @staticmethod
    def _sysfs(values):
        """Return a _read_sysfs side_effect serving canned per-path values."""

        def read(path, default=""):
            return values.get(path, default)

        return read

    @patch("environment_metadata._read_sysfs")
    def test_intel_turbo_enabled(self, mock_read):
        mock_read.side_effect = self._sysfs(
            {"/sys/devices/system/cpu/intel_pstate/no_turbo": "0"}
        )
        assert get_turbo_boost_status() == "enabled"

    @patch("environment_metadata._read_sysfs")
    def test_intel_turbo_disabled(self, mock_read):
        # no_turbo has inverted semantics: 1 means turbo is OFF
        mock_read.side_effect = self._sysfs(
            {"/sys/devices/system/cpu/intel_pstate/no_turbo": "1"}
        )
        assert get_turbo_boost_status() == "disabled"

    @patch("environment_metadata._read_sysfs")
    def test_amd_boost_enabled(self, mock_read):
        mock_read.side_effect = self._sysfs(
            {"/sys/devices/system/cpu/cpufreq/boost": "1"}
        )
        assert get_turbo_boost_status() == "enabled"

    @patch("environment_metadata._read_sysfs")
    def test_amd_boost_disabled(self, mock_read):
        mock_read.side_effect = self._sysfs(
            {"/sys/devices/system/cpu/cpufreq/boost": "0"}
        )
        assert get_turbo_boost_status() == "disabled"

    @patch("environment_metadata._read_sysfs", return_value="")
    @patch("platform.machine", return_value="aarch64")
    def test_arm_no_turbo_mechanism(self, mock_machine, mock_read):
        assert get_turbo_boost_status() == "not_available"

    @patch("environment_metadata._read_sysfs", return_value="")
    @patch("platform.machine", return_value="x86_64")
    def test_x86_without_either_sysfs_path(self, mock_machine, mock_read):
        assert get_turbo_boost_status() == "unknown"


class TestGetCpuFrequencyMhz:
    @patch("environment_metadata._read_sysfs", return_value="2600000")
    def test_parses_khz_to_mhz(self, mock_read):
        assert get_cpu_frequency_mhz() == 2600

    @patch("environment_metadata._read_sysfs", return_value="")
    def test_returns_none_on_failure(self, mock_read):
        assert get_cpu_frequency_mhz() is None

    @patch("environment_metadata._read_sysfs", return_value="not_a_number")
    def test_returns_none_on_invalid(self, mock_read):
        assert get_cpu_frequency_mhz() is None


class TestGetIdleStatesStatus:
    """Build a fake cpuidle sysfs tree in tmp_path and assert exact outcomes."""

    @staticmethod
    def _make_states(root, disable_values):
        for i, val in enumerate(disable_values, start=1):
            state = root / f"state{i}"
            state.mkdir()
            (state / "disable").write_text(f"{val}\n")

    def test_all_disabled(self, tmp_path):
        self._make_states(tmp_path, ["1", "1", "1"])
        with patch("environment_metadata._CPUIDLE_DIR", tmp_path):
            assert get_idle_states_status() == "all_disabled"

    def test_partially_disabled(self, tmp_path):
        self._make_states(tmp_path, ["1", "0", "1"])
        with patch("environment_metadata._CPUIDLE_DIR", tmp_path):
            assert get_idle_states_status() == "partially_disabled"

    def test_all_enabled(self, tmp_path):
        self._make_states(tmp_path, ["0", "0", "0"])
        with patch("environment_metadata._CPUIDLE_DIR", tmp_path):
            assert get_idle_states_status() == "all_enabled"

    def test_not_available_when_dir_missing(self, tmp_path):
        with patch("environment_metadata._CPUIDLE_DIR", tmp_path / "missing"):
            assert get_idle_states_status() == "not_available"

    def test_unknown_when_no_states(self, tmp_path):
        with patch("environment_metadata._CPUIDLE_DIR", tmp_path):
            assert get_idle_states_status() == "unknown"


class TestGetBenchmarkToolVersion:
    def test_nonexistent_path(self):
        assert get_benchmark_tool_version("/nonexistent/path") == "unknown"

    @patch("environment_metadata._run_cmd", return_value="abc123def456")
    def test_returns_short_sha(self, mock_cmd, tmp_path):
        binary = tmp_path / "src" / "valkey-benchmark"
        binary.parent.mkdir(parents=True)
        binary.touch()
        (tmp_path / ".git").mkdir()
        result = get_benchmark_tool_version(str(binary))
        assert result == "abc123def456"


class TestCollectEnvironmentMetadata:
    def test_returns_dict_with_required_keys(self):
        metadata = collect_environment_metadata()
        assert "kernel_version" in metadata
        assert "cpu_governor" in metadata
        assert "turbo_boost" in metadata
        assert "idle_states" in metadata
        assert "numa_nodes" in metadata
        assert "cpu_model" in metadata
        assert "os" in metadata

    def test_includes_benchmark_version_when_path_given(self):
        metadata = collect_environment_metadata(benchmark_path="/nonexistent")
        assert "benchmark_tool_version" in metadata

    def test_no_benchmark_version_without_path(self):
        metadata = collect_environment_metadata()
        assert "benchmark_tool_version" not in metadata

    def test_includes_cpu_ranges_when_provided(self):
        metadata = collect_environment_metadata(
            server_cpu_range="0-8", client_cpu_range="96-191"
        )
        assert metadata["server_cpu_range"] == "0-8"
        assert metadata["client_cpu_range"] == "96-191"

    def test_no_cpu_ranges_when_not_provided(self):
        metadata = collect_environment_metadata()
        assert "server_cpu_range" not in metadata
        assert "client_cpu_range" not in metadata

    def test_includes_aslr_status(self):
        metadata = collect_environment_metadata()
        assert "aslr" in metadata
        assert metadata["aslr"] in ("full", "partial", "disabled", "unknown")

    def test_includes_thp_status(self):
        metadata = collect_environment_metadata()
        assert "thp" in metadata
        assert metadata["thp"] in ("always", "madvise", "never", "unknown")
