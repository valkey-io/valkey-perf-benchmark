#!/usr/bin/env python3
"""Verify the time series written by the per-second metrics sampler.

Used by `.github/workflows/sampler-smoke.yml` after a smoke benchmark run.
Checks every `<commit>/timeseries.jsonl` under a results dir: every scenario of
the config has rows, `elapsed_sec` increases within each (test_id, run), every
row carries every default source, `valkey_info` is a non-empty dict of string
values, and the other sources are non-empty dicts.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

SOURCE_KEYS = ("valkey_info", "latency_histogram", "process_cpu", "disk")


def fail(message: str) -> None:
    """Print message and exit non-zero."""
    print(f"FAIL: {message}")
    sys.exit(1)


def expected_scenario_ids(config_path: Path) -> Set[str]:
    """Return the scenario ids declared by the benchmark config."""
    config = json.loads(Path(config_path).read_text())
    return {
        scenario["id"]
        for entry in config
        for group in entry.get("test_groups", [])
        for scenario in group.get("scenarios", [])
    }


def check_row(row: Dict[str, Any], label: str) -> None:
    """Fail unless row carries every source as a non-empty dict."""
    for key in SOURCE_KEYS:
        if not isinstance(row.get(key), dict) or not row[key]:
            fail(f"{label}: {key} is missing or empty")
    bad = [k for k, v in row["valkey_info"].items() if not isinstance(v, str)]
    if bad:
        fail(f"{label}: valkey_info values are not strings for {bad[:5]}")


def verify_file(path: Path, expected: Set[str]) -> None:
    """Fail unless one timeseries.jsonl holds a sane series per scenario."""
    rows: List[Dict[str, Any]] = [
        json.loads(line) for line in path.read_text().splitlines() if line
    ]
    print(f"{path}: {len(rows)} rows")

    missing = expected - {row.get("scenario") for row in rows}
    if missing:
        fail(f"{path}: no rows for scenario(s) {sorted(missing)}")

    series = defaultdict(list)
    for index, row in enumerate(rows):
        label = f"{path} row {index}"
        check_row(row, label)
        series[(row.get("test_id"), row.get("run"))].append(row["elapsed_sec"])

    for (test_id, run), elapsed in series.items():
        if any(later <= earlier for earlier, later in zip(elapsed, elapsed[1:])):
            fail(f"{path}: elapsed_sec not increasing for {test_id} run {run}")


def main() -> None:
    """Parse arguments and verify every sampled time series."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("config", type=Path, help="Benchmark config of the run")
    args = parser.parse_args()

    expected = expected_scenario_ids(args.config)
    if not expected:
        fail(f"{args.config} declares no scenarios")
    paths = sorted(args.results_dir.glob("*/timeseries.jsonl"))
    if not paths:
        fail(f"no timeseries.jsonl under {args.results_dir}")
    for path in paths:
        verify_file(path, expected)
    print("Sampler output checks passed")


if __name__ == "__main__":
    main()
