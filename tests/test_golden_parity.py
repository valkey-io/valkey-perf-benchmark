"""Golden characterization (parity) tests for ClientRunner.run_benchmark_config.

These tests replay each fixture in tests/fixtures/golden/ through the
argv-capture harness (tests/golden_harness.py) and assert EXACT equality of:

* the ordered argv sequence the runner would execute,
* the ordered side-effect event trace (flush/restart/setup interleaving),
* the metrics dicts written to metrics.json.

They are the parity contract for the config-format unification refactor
(merging the simple-commands path with the test_groups path): they capture
CURRENT behavior and must pass unchanged after the refactor.

If a test fails after an INTENTIONAL behavior change, inspect the diff, then
regenerate fixtures with ``python tests/golden_harness.py``. Regenerating
redefines the contract -- never do it just to make the suite green.
"""

import json
import sys
from pathlib import Path

import pytest

# tests/ has no __init__.py; make the harness importable regardless of how
# pytest was invoked.
TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from golden_harness import (  # noqa: E402
    CASES,
    EXPECTED_ENTRY_COUNTS,
    FIXTURES_DIR,
    REPO_ROOT,
    load_case_config,
    run_case,
)

CASE_NAMES = sorted(CASES)


def load_fixture(name):
    path = FIXTURES_DIR / f"{name}.json"
    assert path.exists(), (
        f"Missing golden fixture {path}. "
        "Generate it with: python tests/golden_harness.py"
    )
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Fixture inventory guards
# ---------------------------------------------------------------------------


def test_no_orphan_fixtures():
    """Every fixture file corresponds to a defined case (and vice versa)."""
    on_disk = {p.stem for p in FIXTURES_DIR.glob("*.json")}
    assert on_disk == set(CASES), (
        "Fixture files and harness CASES are out of sync. "
        f"only on disk: {sorted(on_disk - set(CASES))}, "
        f"only in CASES: {sorted(set(CASES) - on_disk)}"
    )


@pytest.mark.parametrize("config_file", sorted(EXPECTED_ENTRY_COUNTS))
def test_config_entry_count_is_fixtured(config_file):
    """New entries in a fixtured config file must get golden coverage."""
    entries = json.loads((REPO_ROOT / config_file).read_text())
    assert len(entries) == EXPECTED_ENTRY_COUNTS[config_file], (
        f"{config_file} now has {len(entries)} entries but the golden set "
        f"covers {EXPECTED_ENTRY_COUNTS[config_file]}. Add a case to "
        "tests/golden_harness.py and regenerate fixtures."
    )


# ---------------------------------------------------------------------------
# Parity: replay each case and compare against its golden snapshot
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case_name", CASE_NAMES)
def test_golden_parity(case_name, tmp_path):
    """Current code must reproduce the golden argv/events/metrics exactly."""
    fixture = load_fixture(case_name)
    result = run_case(case_name, tmp_path)

    assert (
        result["argv"] == fixture["argv"]
    ), f"argv sequence for case '{case_name}' diverged from golden snapshot"
    assert result["events"] == fixture["events"], (
        f"side-effect event trace for case '{case_name}' diverged from "
        "golden snapshot"
    )
    assert (
        result["metrics"] == fixture["metrics"]
    ), f"metrics for case '{case_name}' diverged from golden snapshot"


# ---------------------------------------------------------------------------
# Schema contract: basic-format metrics key set
# ---------------------------------------------------------------------------

# Keys always emitted by MetricsProcessor.create_metrics for basic format.
BASIC_METRICS_BASE_KEYS = {
    "timestamp",
    "commit",
    "repository",
    "command",
    "data_size",
    "pipeline",
    "clients",
    "rps",
    "avg_latency_ms",
    "min_latency_ms",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "max_latency_ms",
    "cluster_mode",
    "tls",
    "benchmark_mode",
}

# Keys that must never appear on basic-format (simple commands) metrics --
# they belong to the test_groups path only.
TEST_GROUPS_ONLY_KEYS = {
    "test_id",
    "test_phase",
    "group",
    "scenario",
    "config_set",
    "status",
    "group_description",
    "scenario_description",
    "config_name",
    "dataset",
    "module_commit",
    "module_commit_timestamp",
}

BASIC_FORMAT_CASES = [
    "benchmark-configs",
    "benchmark-configs_runs2",
    "benchmark-config-arm",
    "benchmark-configs-cluster-tls",
]


def expected_basic_keys(case_name):
    """Derive the exact expected key set from the case's config + harness wiring.

    Mirrors what the current code emits:
    * duration configs -> duration + benchmark_mode; requests configs ->
      requests + benchmark_mode,
    * io_threads only when the config has io-threads (harness passes the
      first list value, as benchmark.py does per sweep iteration),
    * valkey_benchmark_threads only when the config has benchmark-threads,
    * warmup whenever the config warmup is not None (0 still emits the key),
    * architecture always (benchmark.py always passes platform.machine();
      the harness pins it to golden-arch).
    """
    cfg = load_case_config(CASES[case_name])
    keys = set(BASIC_METRICS_BASE_KEYS)
    keys.add("duration" if cfg.get("duration") is not None else "requests")
    if cfg.get("io-threads") is not None:
        keys.add("io_threads")
    if cfg.get("benchmark-threads") is not None:
        keys.add("valkey_benchmark_threads")
    if cfg.get("warmup") is not None:
        keys.add("warmup")
    keys.add("architecture")
    return keys


@pytest.mark.parametrize("case_name", BASIC_FORMAT_CASES)
def test_basic_format_metrics_schema_contract(case_name):
    """Basic-format metrics contain EXACTLY the expected keys and no others."""
    fixture = load_fixture(case_name)
    expected = expected_basic_keys(case_name)

    assert fixture["metrics"], f"case '{case_name}' produced no metrics"
    for metrics in fixture["metrics"]:
        all_keys = set(metrics)
        # env_* keys are legitimate reproducibility metadata added by upstream
        # PR #55 (environment_metadata.collect_environment_metadata, flattened
        # by MetricsProcessor into env_-prefixed fields). Exclude them from the
        # exact-set contract; their exact values are already pinned by the
        # parity metrics comparison above. Everything else stays strict.
        non_env = {k for k in all_keys if not k.startswith("env_")}
        assert non_env == expected, (
            f"case '{case_name}' metrics key set changed. "
            f"missing: {sorted(expected - non_env)}, "
            f"unexpected: {sorted(non_env - expected)}"
        )
        leaked = all_keys & TEST_GROUPS_ONLY_KEYS
        assert not leaked, (
            f"test_groups-only keys leaked into basic-format metrics: "
            f"{sorted(leaked)}"
        )


def test_basic_format_locked_key_set_value():
    """Pin the concrete key set for the canonical basic config.

    configs/benchmark-configs.json: duration mode, benchmark-threads set,
    no io-threads. This is the exact schema the dashboards/postgres pipeline
    consumes today.
    """
    fixture = load_fixture("benchmark-configs")
    # env_* reproducibility metadata (upstream #55) is excluded here; its
    # values are pinned by test_golden_parity. The non-env key set stays
    # locked exactly.
    non_env = {k for k in fixture["metrics"][0] if not k.startswith("env_")}
    assert non_env == {
        "timestamp",
        "commit",
        "repository",
        "command",
        "data_size",
        "pipeline",
        "clients",
        "rps",
        "avg_latency_ms",
        "min_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "max_latency_ms",
        "cluster_mode",
        "tls",
        "duration",
        "benchmark_mode",
        "valkey_benchmark_threads",
        "warmup",
        "architecture",
    }


# ---------------------------------------------------------------------------
# Ordering contracts made explicit (also covered by parity, but these
# document WHY the fixtures look the way they do)
# ---------------------------------------------------------------------------


def test_runs2_basic_orders_consecutive_runs_per_combination():
    """runs=2 executes A,A,B,B per combination (run loop inside combo loop)."""
    fixture = load_fixture("benchmark-configs_runs2")
    commands = [m["command"] for m in fixture["metrics"]]
    assert commands[:4] == ["SET", "SET", "GET", "GET"]
    # Every command appears exactly twice, in adjacent positions.
    for i in range(0, len(commands), 2):
        assert commands[i] == commands[i + 1]


def test_runs2_test_groups_repeats_whole_group():
    """runs=2 repeats the entire group: (a, b), (a, b)."""
    fixture = load_fixture("module-test-arm_cluster-false_runs2")
    scenario_order = [m["scenario"] for m in fixture["metrics"]]
    assert scenario_order == ["a", "b", "a", "b"]


def test_unsupported_and_cluster_skips_are_locked():
    """XRANGE (unsupported) and MSET-in-cluster produce no argv or metrics."""
    basic = load_fixture("benchmark-configs")
    assert not any("XRANGE" in a for a in basic["argv"])
    assert not any(m["command"] == "XRANGE" for m in basic["metrics"])

    cluster = load_fixture("benchmark-configs-cluster-tls")
    assert not any("MSET" in a for a in cluster["argv"])
    assert not any(m["command"] == "MSET" for m in cluster["metrics"])
