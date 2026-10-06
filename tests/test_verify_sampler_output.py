"""Unit tests for the sampler output verifier."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import verify_sampler_output  # noqa: E402

SOURCES = {
    "valkey_info": {"used_memory": "1048576", "valkey_git_sha1": "00000000"},
    "latency_histogram": {"set": {"calls": 10, "histogram_usec": {"1": 10}}},
    "process_cpu": {"proc_stat": {"cpu": [1, 2, 3]}},
    "disk": {"device": "nvme0n1", "stat": [0] * 17},
}


def rows():
    return [
        {
            "scenario": scenario,
            "test_id": f"1_{scenario}",
            "run": 1,
            "elapsed_sec": elapsed,
            **SOURCES,
        }
        for scenario in ("a", "b")
        for elapsed in (0, 1, 2)
    ]


def write(tmp_path, series):
    path = tmp_path / "abc123" / "timeseries.jsonl"
    path.parent.mkdir()
    path.write_text("".join(json.dumps(row) + "\n" for row in series))
    return path


def test_passes_a_sane_series(tmp_path):
    verify_sampler_output.verify_file(write(tmp_path, rows()), {"a", "b"})


def drop_scenario(series):
    return [row for row in series if row["scenario"] != "b"]


def repeat_elapsed(series):
    series[1]["elapsed_sec"] = 0
    return series


def drop_source(series):
    del series[0]["disk"]
    return series


def empty_info(series):
    series[0]["valkey_info"] = {}
    return series


def typed_info(series):
    series[0]["valkey_info"] = {"used_memory": 1048576}
    return series


@pytest.mark.parametrize(
    "break_series, message",
    [
        (drop_scenario, "no rows for scenario(s) ['b']"),
        (repeat_elapsed, "elapsed_sec not increasing for 1_a run 1"),
        (drop_source, "disk is missing or empty"),
        (empty_info, "valkey_info is missing or empty"),
        (typed_info, "valkey_info values are not strings"),
    ],
)
def test_fails_with_a_short_message(tmp_path, capsys, break_series, message):
    path = write(tmp_path, break_series(rows()))
    with pytest.raises(SystemExit) as exit_info:
        verify_sampler_output.verify_file(path, {"a", "b"})
    assert exit_info.value.code == 1
    assert message in capsys.readouterr().out
