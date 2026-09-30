#!/usr/bin/env python3
"""Verify the time series emitted by the per-second metrics sampler.

Used by `.github/workflows/sampler-smoke.yml` after a smoke benchmark run, and
importable so the assertions themselves are unit tested. Every scenario in a run
appends to one `timeseries.json` per commit directory, so the checks here are
the ones that catch a sampler that silently clobbered, stalled, or emitted rows
without identity:

  - rows exist for every scenario the config declares (a missing scenario means
    a later write clobbered an earlier one)
  - `elapsed_sec` starts at 0 and strictly increases within each scenario's rows
  - `used_memory` is nonzero on every row (INFO was actually sampled) and
    `ops_per_sec` is nonzero somewhere in each scenario (traffic was sampled)
  - every tiering counter is 0, which is what a stock upstream server reports
  - every row carries the full `info` snapshot and it agrees with the
    `used_memory` column
  - every row carries a `latency` dict, it never reports the sampler's own
    commands, and the benchmarked command has ordered percentiles on some row
  - every disk column is present, checked for presence rather than value
    because the runner has real disk activity of its own
  - every CPU column is present, checked for presence for the same reason
  - the context columns are on every row

The first and last row of each file are printed so a CI log shows the shape of
what was sampled even when the run passes.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

# Tiering counters must sample as 0 on a stock upstream server: the
# External Storage INFO section is absent, so every field reads 0.
TIERING_COLUMNS = (
    "total_num_items_spilled_to_ext_storage",
    "total_num_items_fetched_from_ext_storage",
    "num_items_spilling_to_ext_storage",
    "kbc_fetching_block",
    "completion_read_ok",
    "dram_value_hits",
    "throttle_total_throttled",
    "throttle_queued_clients",
    "throttle_current_rate",
    "throttle_allowed_tps",
    "spill_attempts",
    "spill_submitted_count",
    "spill_serialized_count",
    "mean_spill_ram",
    "inflight_spill_ram_bytes",
    "oom_reject_write_count",
)
# Disk columns are derived from the host's block device, which is busy with
# work unrelated to the benchmark, so these are checked for presence only.
DISK_COLUMNS = (
    "disk_read_iops",
    "disk_write_iops",
    "disk_read_mb",
    "disk_write_mb",
    "disk_read_merges_ps",
    "disk_write_merges_ps",
    "disk_r_await_ms",
    "disk_w_await_ms",
    "disk_aqu_sz",
    "disk_util_pct",
    "disk_in_flight",
    "disk_req_sz_kb",
)
# CPU columns are derived from the host and the server process, whose load is
# not reproducible, so these are checked for presence only. valkey_cpu_* come
# from the main thread CPU seconds INFO reports.
CPU_COLUMNS = (
    "valkey_cpu_user",
    "valkey_cpu_sys",
    "valkey_cpu_total",
    "asio_cpu_pct",
    "cpu_user",
    "cpu_sys",
)
CONTEXT_COLUMNS = ("commit", "scenario", "command", "run")
# Commands the sampler itself issues, which the latency source drops so the
# series describes the benchmark load rather than the measurement of it.
SAMPLER_COMMANDS = frozenset({"info", "hello", "config|get"})
SAMPLER_COMMAND_PREFIX = "latency"
# config_set is an empty dict when the config declares no config_sets,
# so it is checked for presence rather than truthiness.
PRESENT_COLUMNS = ("config_set",)
MIN_ROWS = 8


def expected_scenario_ids(config_path: Path) -> Set[str]:
    """Return the scenario ids declared by the benchmark config at `config_path`."""
    config = json.loads(Path(config_path).read_text())
    expected_scenarios = {
        scenario["id"]
        for entry in config
        for group in entry.get("test_groups", [])
        for scenario in group.get("scenarios", [])
    }
    assert expected_scenarios, "smoke config declares no scenarios"
    return expected_scenarios


def verify_timeseries_file(path: Path, expected_scenarios: Set[str]) -> None:
    """Assert one `timeseries.json` file holds a sane series for every scenario."""
    rows: List[Dict[str, Any]] = json.loads(Path(path).read_text())
    print(f"--- {path}: {len(rows)} rows")
    print(f"first row: {json.dumps(rows[0], indent=2)}")
    print(f"last row: {json.dumps(rows[-1], indent=2)}")

    # Every scenario appends to this one file, so a missing scenario
    # means a later write clobbered an earlier one.
    by_scenario = defaultdict(list)
    for row in rows:
        by_scenario[row.get("scenario")].append(row)
    print(f"scenarios present: {sorted(by_scenario)}")
    missing = expected_scenarios - set(by_scenario)
    assert not missing, f"{path}: no rows for scenario(s) {sorted(missing)}"

    for scenario_id in sorted(expected_scenarios):
        scenario_rows = by_scenario[scenario_id]
        label = f"{path} scenario {scenario_id}"
        assert (
            len(scenario_rows) >= MIN_ROWS
        ), f"{label}: {len(scenario_rows)} rows < {MIN_ROWS}"

        # elapsed_sec restarts per scenario, so it increases within a
        # scenario's rows rather than across the whole file.
        assert scenario_rows[0]["elapsed_sec"] == 0, (
            f"{label}: first elapsed_sec is "
            f"{scenario_rows[0]['elapsed_sec']}, not 0"
        )
        elapsed = [row["elapsed_sec"] for row in scenario_rows]
        assert all(
            later > earlier for earlier, later in zip(elapsed, elapsed[1:])
        ), f"{label}: elapsed_sec not strictly increasing: {elapsed}"

        for index, row in enumerate(scenario_rows):
            assert row["used_memory"] > 0, (
                f"{label} row {index}: used_memory is "
                f"{row['used_memory']}, so INFO was not sampled"
            )
            verify_info_snapshot(row, f"{label} row {index}")
            verify_latency_shape(row, f"{label} row {index}")
            for column in TIERING_COLUMNS:
                assert row[column] == 0, (
                    f"{label} row {index}: {column} is {row[column]}, "
                    "expected 0 on a stock server"
                )
            for column in DISK_COLUMNS:
                assert (
                    column in row
                ), f"{label} row {index}: disk column {column} missing"
            for column in CPU_COLUMNS:
                assert (
                    column in row
                ), f"{label} row {index}: CPU column {column} missing"
            for column in CONTEXT_COLUMNS:
                assert row.get(
                    column
                ), f"{label} row {index}: context column {column} missing"
            for column in PRESENT_COLUMNS:
                assert (
                    column in row
                ), f"{label} row {index}: context column {column} missing"

        assert any(
            row["ops_per_sec"] > 0 for row in scenario_rows
        ), f"{label}: ops_per_sec is 0 on every row, so no traffic was sampled"

        verify_benchmarked_command_latency(scenario_rows, label)


def verify_info_snapshot(row: Dict[str, Any], label: str) -> None:
    """Assert one row carries the full INFO snapshot and it agrees with a column."""
    info = row.get("info")
    assert isinstance(info, dict), f"{label}: info is {type(info)}, not a dict"
    used_memory = info.get("used_memory")
    assert isinstance(
        used_memory, int
    ), f"{label}: info.used_memory is {used_memory!r}, not an int"
    assert used_memory == row["used_memory"], (
        f"{label}: info.used_memory is {used_memory}, "
        f"but the used_memory column is {row['used_memory']}"
    )


def verify_latency_shape(row: Dict[str, Any], label: str) -> None:
    """Assert one row carries a latency dict free of the sampler's own commands."""
    latency = row.get("latency")
    assert isinstance(latency, dict), f"{label}: latency is {type(latency)}, not a dict"
    own = [
        command
        for command in latency
        if command in SAMPLER_COMMANDS or command.startswith(SAMPLER_COMMAND_PREFIX)
    ]
    assert not own, f"{label}: latency reports the sampler's own command(s) {own}"


def verify_benchmarked_command_latency(rows: List[Dict[str, Any]], label: str) -> None:
    """Assert the benchmarked command has a sane latency entry on some row."""
    command = rows[0]["command"].lower()
    measured = [
        row["latency"][command]
        for row in rows
        if row["latency"].get(command, {}).get("calls", 0) > 0
    ]
    assert measured, f"{label}: no row reports latency calls for command {command}"

    entry = measured[0]
    p50 = entry["p50_usec"]
    p99 = entry["p99_usec"]
    p999 = entry["p999_usec"]
    assert (
        p999 >= p99 >= p50 > 0
    ), f"{label}: latency percentiles for {command} are not ordered: {entry}"


def verify(results_dir: Path, config_path: Path) -> None:
    """Assert every `timeseries.json` under `results_dir` is a sane time series."""
    expected_scenarios = expected_scenario_ids(config_path)

    paths = sorted(Path(results_dir).glob("*/timeseries.json"))
    assert paths, f"no timeseries.json written under {results_dir}/"
    print(f"Found {len(paths)} time series file(s)")

    for path in paths:
        verify_timeseries_file(path, expected_scenarios)

    print("Sampler smoke assertions passed")


def main() -> None:
    """Parse arguments and verify the sampled time series."""
    parser = argparse.ArgumentParser(
        description="Verify the time series emitted by the per-second sampler"
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Results directory holding <commit>/timeseries.json files",
    )
    parser.add_argument(
        "config",
        type=Path,
        help="Benchmark config used for the run, read for its scenario ids",
    )
    args = parser.parse_args()
    verify(args.results_dir, args.config)


if __name__ == "__main__":
    main()
