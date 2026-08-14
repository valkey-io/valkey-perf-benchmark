"""Argv-capture harness for golden characterization tests.

This module drives ``ClientRunner.run_benchmark_config`` for real config
entries with EVERY side effect mocked, and records:

* ``argv``    -- ordered list of every command line the runner would execute
                 (both ``ClientRunner._run`` and ``subprocess.Popen``),
                 rendered with ``shlex.join`` for readability.
* ``events``  -- ordered trace of all side-effect calls (run/popen/flush/
                 restart/setup) so interleaving is locked too.
* ``metrics`` -- the metrics dicts written to ``results_dir/metrics.json``
                 through the real ``MetricsProcessor.write_metrics`` path.

The snapshots in ``tests/fixtures/golden/`` are the parity contract for the
config-format unification refactor: they capture CURRENT behavior of the
simple-commands path (``_iterate_simple_scenarios`` -> ``_generate_combinations``
-> ``_execute_simple_scenario``) and the test_groups path
(``_iterate_test_groups_scenarios`` -> ``_run_single_scenario``) and must pass
unchanged after the two paths are merged.

Determinism / normalization rules (applied identically at fixture generation
and replay time; also recorded in each fixture's ``normalization`` block):

* ``valkey_benchmark.random.randint`` is patched to a deterministic counter
  returning 1, 2, 3, ... per capture. Every ``--seed`` value in captured argv
  is therefore the ordinal of that randint draw. This intentionally locks how
  many seeds are drawn and in what order (e.g. populate + benchmark of one
  scenario share a single draw).
* ``pathlib.Path.cwd`` (as used by ``valkey_benchmark``) is patched to return
  ``/golden-cwd`` so test_groups dataset paths are machine-independent.
* ``ClientRunner.get_commit_time`` is patched to return ``<TS>`` so the
  metrics ``timestamp`` field is already normalized.
* Fixed identity inputs: ``commit_id=golden-commit``,
  ``architecture=golden-arch`` (production always passes
  ``platform.machine()``), ``repository=None``, default
  ``valkey_benchmark_path`` (``src/valkey-benchmark``).
* ``ClientRunner._run`` / ``subprocess.Popen`` return canned valkey-benchmark
  CSV. For simple-format runs the row's test name equals the benchmarked
  command (taken from the token after ``-t``) so the ``startswith`` row filter
  in ``_execute_simple_scenario`` matches; scenario-format runs get a fixed
  ``SCENARIO`` row (the first CSV row is used regardless of name).

Runner wiring mirrors ``benchmark.py::_execute_benchmark_run``:
``cores=config['client_cpu_range']``,
``benchmark_threads=config['benchmark-threads']``, ``io_threads`` = first
value of the config's ``io-threads`` list (the sweep over remaining values is
a benchmark.py-level loop that only changes the ``io_threads`` metrics field,
never the argv), ``server_launcher=None`` (so per-scenario cleanup takes the
``_flush_database`` branch), and ``client_cpu_ranges`` computed by
``calculate_client_cpu_ranges`` from the RAW config entry -- exactly like
``run_benchmark_matrix``, which computes it before cluster_mode arrays are
scalarized (so e.g. module-test-arm.json yields ``["2-2", "3-3"]`` for both
cluster and non-cluster runs).

Regenerate all fixtures with:

    python tests/golden_harness.py
"""

import copy
import inspect
import itertools
import json
import shlex
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark import load_configs, validate_config  # noqa: E402
from utils.cpu_utils import calculate_client_cpu_ranges  # noqa: E402
from valkey_benchmark import ClientRunner  # noqa: E402

FIXTURES_DIR = TESTS_DIR / "fixtures" / "golden"

CSV_HEADER = (
    '"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms",'
    '"p95_latency_ms","p99_latency_ms","max_latency_ms"'
)
CANNED_ROW_VALUES = '"100000.00","0.500","0.100","0.400","0.800","1.200","2.000"'

# Deterministic environment metadata for capture. Upstream PR #55
# (environment_metadata.collect_environment_metadata) shells out (lscpu, sysfs
# reads) at metrics-processor setup time; subprocess.run internally constructs
# a subprocess.Popen, which this harness patches globally to record every
# command into the argv trace. Left unmocked, those host-probe shell strings
# leak into argv[0..] and also make the flattened env_* metric fields
# machine-dependent. We therefore replace the WHOLE collect_environment_metadata
# function (patched at the valkey_benchmark lookup site) with a fixed dict, so
# no shell/sysfs from environment_metadata ever reaches the capture mocks and
# the recorded env_* fields are deterministic. Note the omission of
# cpu_freq_mhz_at_setup (a LIVE reading) -- excluding it keeps fixtures stable.
GOLDEN_ENV = {
    "kernel_version": "golden-kernel",
    "os": "golden-os",
    "cpu_model": "golden-cpu",
    "cpu_governor": "golden-governor",
    "turbo_boost": "golden-turbo",
    "idle_states": "golden-idle",
    "aslr": "golden-aslr",
    "thp": "golden-thp",
    "numa_nodes": "1",
}

NORMALIZATION_DOC = {
    "seed": (
        "valkey_benchmark.random.randint patched to a deterministic counter "
        "(1, 2, 3, ...) per capture; each --seed token in argv is the ordinal "
        "of that draw"
    ),
    "cwd": (
        "pathlib.Path.cwd() patched to /golden-cwd "
        "(affects test_groups dataset paths)"
    ),
    "timestamp": "ClientRunner.get_commit_time patched to return <TS>",
    "identity": (
        "commit_id=golden-commit, architecture=golden-arch, repository=None, "
        "valkey_benchmark_path defaults to src/valkey-benchmark"
    ),
    "csv": (
        "canned valkey-benchmark CSV: rps=100000.00 avg=0.500 min=0.100 "
        "p50=0.400 p95=0.800 p99=1.200 max=2.000; row test name = token "
        "after -t (simple format) or SCENARIO (test_groups format)"
    ),
    "environment_metadata": (
        "valkey_benchmark.collect_environment_metadata patched (whole-function "
        "replacement, at the ClientRunner lookup site) to return a fixed dict "
        f"{GOLDEN_ENV}. environment_metadata's real implementation shells out "
        "(lscpu, sysfs) via subprocess.run, whose internal Popen this harness "
        "records; replacing the function keeps those host probes out of argv "
        "and makes the flattened env_* metric fields deterministic. "
        "cpu_freq_mhz_at_setup (a live reading) is intentionally omitted."
    ),
}


def canned_csv_for(argv):
    """Return canned valkey-benchmark CSV stdout for a captured command.

    For simple-format commands the row's test name must equal the benchmarked
    command (the token after ``-t``) so the ``startswith`` filter in
    ``_execute_simple_scenario`` matches. Scenario-format commands (``--``
    separator, no ``-t``) parse the first row regardless of name.
    """
    if "-t" in argv:
        test_name = argv[argv.index("-t") + 1]
    else:
        test_name = "SCENARIO"
    return f'{CSV_HEADER}\n"{test_name}",{CANNED_ROW_VALUES}\n'


class _Recorder:
    """Ordered trace of every side-effect the runner performs."""

    def __init__(self):
        self.argv = []
        self.events = []

    def record_run(self, cmd):
        joined = shlex.join(list(cmd))
        self.argv.append(joined)
        self.events.append(f"run: {joined}")

    def record_popen(self, cmd):
        joined = shlex.join(list(cmd))
        self.argv.append(joined)
        self.events.append(f"popen: {joined}")


class _FakePopen:
    """Stand-in for subprocess.Popen used by parallel/mixed workloads."""

    def __init__(self, cmd, recorder):
        recorder.record_popen(cmd)
        self._stdout = canned_csv_for(list(cmd))
        self.returncode = 0

    def communicate(self):
        return self._stdout, ""


def _derive_io_threads(cfg):
    """First io-threads value, mirroring benchmark.py's iteration order.

    benchmark.py sweeps every value in the list; the sweep only changes the
    ``io_threads`` metrics field (never the client argv), so capturing the
    first value is representative for the parity contract.
    """
    io = cfg.get("io-threads")
    if io is None:
        return None
    if isinstance(io, int):
        return io
    return io[0]


def capture(config, *, cluster_mode, runs, results_dir):
    """Run run_benchmark_config for one config entry with all side effects mocked.

    Returns {"argv": [...], "events": [...], "metrics": [...]}.
    """
    raw_cfg = copy.deepcopy(config)
    exec_cfg = copy.deepcopy(config)
    # benchmark.py scalarizes cluster_mode per execution config.
    exec_cfg["cluster_mode"] = cluster_mode

    recorder = _Recorder()
    seed_counter = itertools.count(1)

    def fake_run(self, command, cwd=None, capture_output=False, text=True, timeout=300):
        cmd_list = list(command)
        recorder.record_run(cmd_list)
        if capture_output:
            return SimpleNamespace(stdout=canned_csv_for(cmd_list), stderr="")
        return None

    def fake_flush(self):
        recorder.events.append("flush_database")

    def fake_restart(self):
        recorder.events.append("restart_server")

    def fake_setup(self, cmd_str):
        recorder.events.append(f"setup_command: {cmd_str}")

    results_dir = Path(results_dir)
    runner_kwargs = dict(
        commit_id="golden-commit",
        config=exec_cfg,
        cluster_mode=cluster_mode,
        tls_mode=exec_cfg["tls_mode"],
        target_ip="127.0.0.1",
        results_dir=results_dir,
        valkey_path="/golden-valkey",
        cores=exec_cfg.get("client_cpu_range"),
        io_threads=_derive_io_threads(exec_cfg),
        valkey_benchmark_path=None,
        benchmark_threads=exec_cfg.get("benchmark-threads"),
        runs=runs,
        server_launcher=None,
        architecture="golden-arch",
        repository=None,
    )
    # Cross-tree compatibility: the pre-refactor tree (which the fixtures
    # characterize) selects the execution path via a ``uses_test_groups``
    # kwarg that ClientRunner branches on; benchmark.py sets it to
    # ``"test_groups" in cfg``. Our branch removed that kwarg -- simple
    # configs are compiled into test_groups at load time and the path is
    # auto-detected. Pass the kwarg ONLY when the constructor still accepts it
    # (base tree), mirroring benchmark.py, so one harness drives both trees.
    if "uses_test_groups" in inspect.signature(ClientRunner).parameters:
        runner_kwargs["uses_test_groups"] = "test_groups" in exec_cfg
    runner = ClientRunner(**runner_kwargs)
    # Mirror run_benchmark_matrix: client_cpu_ranges computed from the RAW
    # config (cluster_mode may still be an array there, which is truthy).
    client_ranges = calculate_client_cpu_ranges(raw_cfg)
    if client_ranges:
        runner.client_cpu_ranges = client_ranges

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(ClientRunner, "get_commit_time", lambda self, cid: "<TS>")
        )
        stack.enter_context(patch.object(ClientRunner, "_run", fake_run))
        stack.enter_context(patch.object(ClientRunner, "_flush_database", fake_flush))
        stack.enter_context(patch.object(ClientRunner, "_restart_server", fake_restart))
        stack.enter_context(
            patch.object(ClientRunner, "_execute_setup_command", fake_setup)
        )
        stack.enter_context(
            patch(
                "valkey_benchmark.subprocess.Popen",
                lambda cmd, **kwargs: _FakePopen(cmd, recorder),
            )
        )
        stack.enter_context(
            patch(
                "valkey_benchmark.random.randint",
                lambda a, b: next(seed_counter),
            )
        )
        stack.enter_context(
            patch("valkey_benchmark.Path.cwd", lambda: Path("/golden-cwd"))
        )
        # Replace the whole environment probe (see GOLDEN_ENV). Patched at the
        # valkey_benchmark lookup site because ClientRunner imports the symbol
        # into that namespace. This keeps environment_metadata's lscpu/sysfs
        # shell calls (whose internal Popen the harness records) out of argv
        # and makes the flattened env_* metric fields deterministic.
        stack.enter_context(
            patch(
                "valkey_benchmark.collect_environment_metadata",
                lambda **kwargs: dict(GOLDEN_ENV),
            )
        )
        runner.run_benchmark_config()

    metrics_file = results_dir / "metrics.json"
    metrics = json.loads(metrics_file.read_text()) if metrics_file.exists() else []
    return {"argv": recorder.argv, "events": recorder.events, "metrics": metrics}


# ---------------------------------------------------------------------------
# Case definitions
# ---------------------------------------------------------------------------

# Representative inline mixed-workload config. The repo's real mixed-workload
# configs (fts-benchmarks-arm.json) depend on generated dataset files and
# config_sets/profiling_sets sweeps that live in benchmark.py, so a minimal
# inline test_groups config is used instead to lock the subprocess.Popen
# mixed-workload path (_run_mixed_workload).
INLINE_MIXED_CONFIG = {
    "test_name": "Inline mixed workload",
    "cluster_mode": False,
    "tls_mode": False,
    "test_groups": [
        {
            "group": 9,
            "description": "Inline mixed workload group",
            "scenarios": [
                {
                    "id": "mix",
                    "type": "mixed",
                    "duration": 10,
                    "writes": [
                        {
                            "id": "w1",
                            "command": "SET key:__rand_int__ val",
                            "clients": 2,
                        }
                    ],
                    "reads": [
                        {
                            "id": "r1",
                            "command": "GET key:__rand_int__",
                            "clients": 2,
                        }
                    ],
                }
            ],
        }
    ],
}

# Each case pins one ClientRunner.run_benchmark_config invocation.
# cluster_mode must be given explicitly when the config stores an array
# (benchmark.py iterates the array outside ClientRunner).
CASES = {
    "benchmark-configs": {
        "config_file": "configs/benchmark-configs.json",
        "entry": 0,
    },
    "benchmark-configs_runs2": {
        "config_file": "configs/benchmark-configs.json",
        "entry": 0,
        "runs": 2,
    },
    "benchmark-config-arm": {
        "config_file": "configs/benchmark-config-arm.json",
        "entry": 0,
    },
    "benchmark-configs-cluster-tls": {
        "config_file": "configs/benchmark-configs-cluster-tls.json",
        "entry": 0,
    },
    "module-test-arm_cluster-false": {
        "config_file": "configs/module-test-arm.json",
        "entry": 0,
        "cluster_mode": False,
    },
    "module-test-arm_cluster-true": {
        "config_file": "configs/module-test-arm.json",
        "entry": 0,
        "cluster_mode": True,
    },
    "module-test-arm_cluster-false_runs2": {
        "config_file": "configs/module-test-arm.json",
        "entry": 0,
        "cluster_mode": False,
        "runs": 2,
    },
    "inline-mixed": {
        "inline_config": INLINE_MIXED_CONFIG,
    },
}

# Guard: number of entries we expect in each fixtured config file. If an
# entry is added to a config file, this forces the golden set to be extended.
EXPECTED_ENTRY_COUNTS = {
    "configs/benchmark-configs.json": 1,
    "configs/benchmark-config-arm.json": 1,
    "configs/benchmark-configs-cluster-tls.json": 1,
    "configs/module-test-arm.json": 1,
}


def load_case_config(spec):
    """Resolve a case spec to a validated config dict (production load path)."""
    if "inline_config" in spec:
        cfg = copy.deepcopy(spec["inline_config"])
        validate_config(cfg)
        return cfg
    return load_configs(str(REPO_ROOT / spec["config_file"]))[spec["entry"]]


def run_case(name, results_dir):
    """Execute one named case and return its capture result."""
    spec = CASES[name]
    cfg = load_case_config(spec)
    cluster_mode = spec.get("cluster_mode", cfg.get("cluster_mode"))
    if isinstance(cluster_mode, list):
        raise ValueError(
            f"Case {name}: config has a cluster_mode array; the case spec "
            "must select a scalar cluster_mode explicitly"
        )
    return capture(
        cfg,
        cluster_mode=bool(cluster_mode),
        runs=spec.get("runs", 1),
        results_dir=results_dir,
    )


def generate_fixtures():
    """Regenerate every golden fixture from current code behavior."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    for name, spec in CASES.items():
        with tempfile.TemporaryDirectory() as tmp:
            result = run_case(name, tmp)
        fixture = {
            "case": name,
            "source": spec.get("config_file", "<inline test_groups config>"),
            "runs": spec.get("runs", 1),
            "normalization": NORMALIZATION_DOC,
            "argv": result["argv"],
            "events": result["events"],
            "metrics": result["metrics"],
        }
        path = FIXTURES_DIR / f"{name}.json"
        path.write_text(json.dumps(fixture, indent=2) + "\n")
        print(
            f"wrote {path.relative_to(REPO_ROOT)}: "
            f"{len(result['argv'])} argv, {len(result['metrics'])} metrics"
        )


if __name__ == "__main__":
    generate_fixtures()
