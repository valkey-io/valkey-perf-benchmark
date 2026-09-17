"""Unit tests for the sampler smoke verifier.

The verifier decides whether the smoke workflow passes, so each assertion it
makes is exercised against a fabricated results tree: a passing two-scenario
series, plus one failure per check (clobbered scenario, nonzero tiering counter,
non-increasing elapsed_sec, missing config_set, missing disk column, no time
series file at all).
"""

import json
import sys
from pathlib import Path

import pytest

# Ensure scripts/ is importable
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import verify_sampler_output  # noqa: E402

MIN_ROWS = verify_sampler_output.MIN_ROWS


def _row(scenario, elapsed_sec, **overrides):
    """Build one sampled row that passes every check unless overridden."""
    row = {
        "timestamp": f"2026-01-01T00:00:{elapsed_sec:02d}+00:00",
        "elapsed_sec": elapsed_sec,
        "commit": "abc1234",
        "scenario": scenario,
        "test_id": f"1_{scenario}",
        "command": "SET",
        "config_set": {},
        "used_memory": 1799288,
        "ops_per_sec": 12345.0,
    }
    for column in verify_sampler_output.TIERING_COLUMNS:
        row[column] = 0
    # Disk columns are checked for presence, so these carry the kind of
    # nonzero values a busy runner reports.
    for index, column in enumerate(verify_sampler_output.DISK_COLUMNS):
        row[column] = float(index + 1)
    row.update(overrides)
    return row


def _scenario_rows(scenario, count=MIN_ROWS):
    """Build a passing series of `count` rows for one scenario."""
    return [_row(scenario, elapsed) for elapsed in range(count)]


def _write_results(tmp_path, rows, commit="abc1234"):
    """Write `rows` to <tmp_path>/results/<commit>/timeseries.json."""
    results_dir = tmp_path / "results"
    commit_dir = results_dir / commit
    commit_dir.mkdir(parents=True)
    (commit_dir / "timeseries.json").write_text(json.dumps(rows))
    return results_dir


def _write_config(tmp_path, scenario_ids):
    """Write a benchmark config declaring `scenario_ids` in one test group."""
    config = [
        {
            "test_groups": [
                {
                    "group": 1,
                    "scenarios": [{"id": scenario_id} for scenario_id in scenario_ids],
                }
            ]
        }
    ]
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    return config_path


class TestExpectedScenarioIds:
    """Scenario ids are read from the config the run used."""

    def test_ids_collected_across_groups(self, tmp_path):
        config = [
            {
                "test_groups": [
                    {"group": 1, "scenarios": [{"id": "a"}, {"id": "b"}]},
                    {"group": 2, "scenarios": [{"id": "c"}]},
                ]
            }
        ]
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))

        assert verify_sampler_output.expected_scenario_ids(config_path) == {
            "a",
            "b",
            "c",
        }

    def test_config_without_scenarios_fails(self, tmp_path):
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps([{"test_groups": []}]))

        with pytest.raises(AssertionError, match="declares no scenarios"):
            verify_sampler_output.expected_scenario_ids(config_path)


class TestVerifyPasses:
    """A well-formed two-scenario series passes and prints its shape."""

    def test_two_scenarios_pass(self, tmp_path, capsys):
        rows = _scenario_rows("a") + _scenario_rows("b")
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a", "b"])

        verify_sampler_output.verify(results_dir, config_path)

        output = capsys.readouterr().out
        assert "Found 1 time series file(s)" in output
        assert "scenarios present: ['a', 'b']" in output
        assert "first row:" in output
        assert "last row:" in output
        assert "Sampler smoke assertions passed" in output

    def test_config_set_with_values_passes(self, tmp_path):
        rows = [
            _row("a", elapsed, config_set={"maxmemory": "1gb"})
            for elapsed in range(MIN_ROWS)
        ]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        verify_sampler_output.verify(results_dir, config_path)


class TestVerifyFails:
    """Each failure mode the verifier exists to catch."""

    def test_missing_scenario_detects_clobber(self, tmp_path):
        # Scenario "b" appended over "a" instead of extending the file.
        results_dir = _write_results(tmp_path, _scenario_rows("b"))
        config_path = _write_config(tmp_path, ["a", "b"])

        with pytest.raises(AssertionError, match=r"no rows for scenario\(s\) \['a'\]"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_nonzero_tiering_counter_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[3]["completion_read_ok"] = 17
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="completion_read_ok is 17"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_nonzero_throttle_counter_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[2]["throttle_total_throttled"] = 5
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="throttle_total_throttled is 5"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_nonzero_spill_pipeline_counter_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[5]["inflight_spill_ram_bytes"] = 4096
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="inflight_spill_ram_bytes is 4096"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_missing_disk_column_fails(self, tmp_path):
        rows = _scenario_rows("a")
        del rows[1]["disk_r_await_ms"]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="disk column disk_r_await_ms missing"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_zero_disk_column_passes(self, tmp_path):
        # Disk values are the runner's own activity, so 0 is not a failure.
        rows = [
            _row("a", elapsed, disk_util_pct=0.0, disk_in_flight=0)
            for elapsed in range(MIN_ROWS)
        ]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        verify_sampler_output.verify(results_dir, config_path)

    def test_non_increasing_elapsed_sec_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[4]["elapsed_sec"] = rows[3]["elapsed_sec"]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="not strictly increasing"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_first_elapsed_sec_not_zero_fails(self, tmp_path):
        rows = [_row("a", elapsed) for elapsed in range(1, MIN_ROWS + 1)]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="first elapsed_sec is 1, not 0"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_missing_config_set_key_fails(self, tmp_path):
        rows = _scenario_rows("a")
        del rows[2]["config_set"]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="context column config_set missing"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_missing_commit_context_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[0]["commit"] = ""
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="context column commit missing"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_zero_used_memory_fails(self, tmp_path):
        rows = _scenario_rows("a")
        rows[1]["used_memory"] = 0
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="used_memory is 0"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_zero_ops_per_sec_everywhere_fails(self, tmp_path):
        rows = [_row("a", elapsed, ops_per_sec=0.0) for elapsed in range(MIN_ROWS)]
        results_dir = _write_results(tmp_path, rows)
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="ops_per_sec is 0 on every row"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_too_few_rows_fails(self, tmp_path):
        results_dir = _write_results(tmp_path, _scenario_rows("a", count=MIN_ROWS - 1))
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match=f"rows < {MIN_ROWS}"):
            verify_sampler_output.verify(results_dir, config_path)

    def test_missing_timeseries_file_fails(self, tmp_path):
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        config_path = _write_config(tmp_path, ["a"])

        with pytest.raises(AssertionError, match="no timeseries.json written under"):
            verify_sampler_output.verify(results_dir, config_path)
