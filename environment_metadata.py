"""Collect environment metadata for benchmark reproducibility.

Records system state (CPU governor, turbo boost, kernel version, etc.)
and benchmark tool version so results can be compared accurately across
runs with different configurations.
"""

import logging
import platform
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional


def _run_cmd(cmd: str, default: str = "unknown") -> str:
    """Run a shell command and return stripped stdout, or default on failure."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() if result.returncode == 0 else default
    except Exception:
        return default


def _read_sysfs(path: str, default: str = "") -> str:
    """Read a single-line sysfs/procfs file, returning default if unreadable."""
    try:
        return Path(path).read_text().strip()
    except OSError:
        return default


def get_cpu_governor() -> str:
    """Return the active CPU frequency governor, or 'not_available' for fixed-frequency CPUs."""
    result = _read_sysfs("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    return result if result else "not_available"


_INTEL_NO_TURBO_PATH = "/sys/devices/system/cpu/intel_pstate/no_turbo"
_AMD_BOOST_PATH = "/sys/devices/system/cpu/cpufreq/boost"


def get_turbo_boost_status() -> str:
    """Return turbo/boost status: 'enabled', 'disabled', or 'not_available'."""
    # Intel (note inverted semantics: no_turbo=0 means turbo enabled)
    result = _read_sysfs(_INTEL_NO_TURBO_PATH)
    if result == "0":
        return "enabled"
    elif result == "1":
        return "disabled"

    # AMD
    result = _read_sysfs(_AMD_BOOST_PATH)
    if result == "1":
        return "enabled"
    elif result == "0":
        return "disabled"

    # ARM (Graviton) — no turbo mechanism
    if platform.machine() in ("aarch64", "arm64"):
        return "not_available"

    return "unknown"


def get_cpu_frequency_mhz() -> Optional[int]:
    """Return current CPU frequency in MHz, or None if unavailable."""
    freq_str = _read_sysfs("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq")
    if freq_str.isdigit():
        return int(freq_str) // 1000  # kHz -> MHz
    return None


_CPUIDLE_DIR = Path("/sys/devices/system/cpu/cpu0/cpuidle")


def get_idle_states_status() -> str:
    """Return whether deep CPU idle states (C-states) are disabled.

    Checks cpu0 as representative. Returns:
    - 'all_disabled': all idle states beyond C0 are disabled
    - 'partially_disabled': some states disabled
    - 'all_enabled': all idle states are enabled (default kernel behavior)
    - 'unknown': cannot determine
    """
    states_dir = _CPUIDLE_DIR
    if not states_dir.exists():
        return "not_available"

    try:
        states = sorted(states_dir.glob("state[1-9]*"))
        if not states:
            return "unknown"

        disabled_count = 0
        for state in states:
            disable_file = state / "disable"
            if disable_file.exists():
                val = disable_file.read_text().strip()
                if val == "1":
                    disabled_count += 1

        if disabled_count == len(states):
            return "all_disabled"
        elif disabled_count > 0:
            return "partially_disabled"
        else:
            return "all_enabled"
    except Exception:
        return "unknown"


def get_benchmark_tool_version(benchmark_path: str) -> str:
    """Get the version string of the valkey-benchmark binary.

    Runs `valkey-benchmark --version` and extracts the version + git SHA.
    Falls back to 'unknown' if the binary doesn't exist or has no version output.
    """
    binary = Path(benchmark_path)
    if not binary.exists():
        return "unknown"

    version_output = _run_cmd(f"{benchmark_path} --version", default="")
    if version_output:
        return version_output.strip()
    return "unknown"


def get_aslr_status() -> str:
    """Return ASLR status: 'full' (2), 'partial' (1), or 'disabled' (0)."""
    val = _read_sysfs("/proc/sys/kernel/randomize_va_space")
    return {"0": "disabled", "1": "partial", "2": "full"}.get(val, "unknown")


def get_thp_status() -> str:
    """Return THP mode: 'always', 'madvise', or 'never'."""
    content = _read_sysfs("/sys/kernel/mm/transparent_hugepage/enabled")
    if "[always]" in content:
        return "always"
    elif "[madvise]" in content:
        return "madvise"
    elif "[never]" in content:
        return "never"
    return "unknown"


def _get_os_pretty_name() -> str:
    """Return PRETTY_NAME from /etc/os-release, or 'unknown'."""
    for line in _read_sysfs("/etc/os-release").splitlines():
        if line.startswith("PRETTY_NAME="):
            return line.split("=", 1)[1].strip().strip('"')
    return "unknown"


def collect_environment_metadata(
    benchmark_path: Optional[str] = None,
    server_cpu_range: Optional[str] = None,
    client_cpu_range: Optional[str] = None,
) -> Dict[str, Any]:
    """Collect all environment metadata for a benchmark run.

    Returns a dict suitable for embedding in metrics.json entries.
    """
    metadata: Dict[str, Any] = {
        "kernel_version": platform.release(),
        "os": _get_os_pretty_name(),
        "cpu_model": _run_cmd("lscpu | grep 'Model name' | sed 's/.*: *//'"),
        "cpu_governor": get_cpu_governor(),
        "turbo_boost": get_turbo_boost_status(),
        "idle_states": get_idle_states_status(),
        "aslr": get_aslr_status(),
        "thp": get_thp_status(),
        "numa_nodes": _run_cmd("lscpu | grep 'NUMA node(s)' | awk '{print $NF}'"),
    }

    freq = get_cpu_frequency_mhz()
    if freq:
        metadata["cpu_freq_mhz_at_setup"] = freq

    # Record CPU pinning configuration (intent from config)
    if server_cpu_range:
        metadata["server_cpu_range"] = server_cpu_range
    if client_cpu_range:
        metadata["client_cpu_range"] = client_cpu_range

    if benchmark_path:
        metadata["benchmark_tool_version"] = get_benchmark_tool_version(benchmark_path)

    return metadata
