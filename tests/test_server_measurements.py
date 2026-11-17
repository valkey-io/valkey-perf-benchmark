"""Tests for server-side window throughput, populate memory, required
commands and the config keys that drive them."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from benchmark import validate_config
from metrics_sampler import summarize_window


def _row(elapsed, usec, calls, errors=0, info_calls=None):
    info = {
        "server_time_usec": str(usec),
        "total_error_replies": str(errors),
        "cmdstat_info": f"calls={info_calls or elapsed},usec=1,usec_per_call=1.00",
    }
    for cmd, n in calls.items():
        info[f"cmdstat_{cmd}"] = f"calls={n},usec=5,usec_per_call=0.50,failed_calls=0"
    return {"elapsed_sec": elapsed, "valkey_info": info}


class TestSummarizeWindow:
    def test_rate_from_calls_and_server_time_excluding_sampler_commands(self):
        rows = [
            _row(0, 0, {"sadd": 0}),
            _row(1, 1_000_000, {"sadd": 100, "srem": 10}, errors=1),
            _row(3, 3_500_000, {"sadd": 600, "srem": 260}, errors=3),
            _row(4, 4_000_000, {"sadd": 700, "srem": 300}),
        ]

        summary = summarize_window(rows, [1, 3])

        assert summary == {
            "server_ops_s": 300.0,
            "server_ops_s_by_command": {"sadd": 200.0, "srem": 100.0},
            "window_sec": [1, 3],
            "window_complete": True,
            "window_error_replies": 2,
        }

    def test_client_stopping_at_window_end_is_incomplete(self):
        rows = [_row(1, 1_000_000, {"get": 1}), _row(3, 3_000_000, {"get": 5})]
        assert summarize_window(rows, [1, 3])["window_complete"] is False

    def test_missing_edge_reading(self):
        rows = [_row(1, 1_000_000, {"get": 1}), {"elapsed_sec": 3}, {"elapsed_sec": 4}]
        assert summarize_window(rows, [1, 3]) == {
            "window_sec": [1, 3],
            "window_complete": False,
        }


class TestMissingCommands:
    def _runner(self, runner, reply):
        client = MagicMock()
        client.execute_command.return_value = reply
        runner._create_client = MagicMock(return_value=client)
        return client

    def test_unknown_commands_are_reported_and_cached(self, minimal_client_runner):
        client = self._runner(minimal_client_runner, [["sadd", -3], None])
        scenario = {
            "requires_commands": ["sexpire", "SADD"],
            "preload": [
                {
                    "type": "set",
                    "key_prefix": "s:",
                    "encoding": "listpack",
                    "members": [{"prefix": "m", "count": 2, "ttl": {"seconds": 9}}],
                }
            ],
        }

        assert minimal_client_runner._missing_commands(scenario) == ["SEXPIRE"]
        client.execute_command.assert_called_once_with(
            "COMMAND", "INFO", "SADD", "SEXPIRE"
        )
        assert minimal_client_runner._missing_commands(scenario) == ["SEXPIRE"]
        assert client.execute_command.call_count == 1

    def test_no_requirements_sends_nothing(self, minimal_client_runner):
        client = self._runner(minimal_client_runner, [])
        assert minimal_client_runner._missing_commands({}) == []
        client.execute_command.assert_not_called()

    def test_scenario_is_skipped_before_server_state_changes(
        self, minimal_client_runner
    ):
        runner = minimal_client_runner
        with (
            patch.object(runner, "_missing_commands", return_value=["SEXPIRE"]),
            patch.object(runner, "_prepare_server_state") as prepare,
        ):
            result = runner._run_single_scenario(
                {"id": "s", "command": "SEXPIRE k 1 MEMBERS 1 m"},
                1,
                None,
                MagicMock(),
                {},
                "default",
            )
        assert result is None
        prepare.assert_not_called()


class TestPopulateMemory:
    def test_memory_and_keys_around_preload(self, minimal_client_runner):
        runner = minimal_client_runner
        readings = iter([(10, 1_000_000), (110, 3_500_000)])
        with (
            patch.object(runner, "_keys_and_used_memory", lambda: next(readings)),
            patch.object(runner, "_run_preload") as run_preload,
        ):
            loaded = runner._populate_scenario_keyspace({"preload": [{}]}, None)

        run_preload.assert_called_once_with([{}])
        assert loaded == {
            "populate_keys": 100,
            "populate_used_memory_mb": 2.5,
            "populate_bytes_per_key": 25000.0,
        }

    def test_unreadable_server_drops_only_the_memory_fields(
        self, minimal_client_runner
    ):
        runner = minimal_client_runner
        with (
            patch.object(
                runner, "_keys_and_used_memory", side_effect=ConnectionError("down")
            ),
            patch.object(runner, "_run_preload") as run_preload,
        ):
            assert runner._populate_scenario_keyspace({"preload": [{}]}, None) == {}
        run_preload.assert_called_once()

    def test_waits_for_lazy_free_before_reading(self, minimal_client_runner):
        runner = minimal_client_runner
        client = MagicMock()
        client.info.side_effect = [
            {"lazyfree_pending_objects": 3, "used_memory": 900},
            {"lazyfree_pending_objects": 0, "used_memory": 500},
        ]
        client.dbsize.return_value = 7
        runner._create_client = MagicMock(return_value=client)
        with patch("valkey_benchmark.time.sleep"):
            assert runner._keys_and_used_memory() == (7, 500)

    def test_rows_get_measurements_only_on_success(self):
        from valkey_benchmark import ClientRunner

        rows = [{"status": "success"}, {"status": "failed"}, None]
        ClientRunner._add_server_measurements(
            rows, {"populate_keys": 1}, {"server_ops_s": 5.0}
        )
        assert rows[0] == {"status": "success", "populate_keys": 1, "server_ops_s": 5.0}
        assert rows[1] == {"status": "failed"}


class TestSamplingWindow:
    def test_sampling_context_yields_window_summary(self, minimal_client_runner):
        runner = minimal_client_runner
        runner.config["per_second_sampling"] = {"sources": {"valkey_info": {}}}
        sampler = MagicMock()
        sampler.rows = [
            _row(1, 1_000_000, {"get": 0}),
            _row(2, 2_000_000, {"get": 50}),
            _row(3, 3_000_000, {"get": 60}),
        ]
        with (
            patch.object(runner, "_start_metrics_sampler", return_value=sampler),
            patch.object(runner, "_stop_metrics_sampler"),
        ):
            with runner._per_second_sampling({"id": "s", "window": [1, 2]}, 1) as m:
                pass
        assert m["server_ops_s"] == 50.0
        assert m["window_complete"] is True


def _cfg(scenario, sampling=True):
    cfg = {"test_groups": [{"group": 1, "scenarios": [scenario]}]}
    if sampling is not None:
        cfg["per_second_sampling"] = sampling
    return cfg


class TestValidation:
    base = {"id": "s", "command": "SCARD k", "duration": 10}

    def test_valid_keys(self):
        validate_config(
            _cfg(
                {
                    **self.base,
                    "window": [2, 8],
                    "requires_commands": ["SEXPIRE"],
                    "check_after_run": False,
                    "preload": [
                        {
                            "type": "set",
                            "key_prefix": "s:",
                            "encoding": "listpack",
                            "members": [{"prefix": "m", "count": 3}],
                        }
                    ],
                }
            )
        )

    @pytest.mark.parametrize(
        "change, sampling, message",
        [
            ({"window": [5, 5]}, True, "start < end"),
            ({"window": [2, 10]}, True, "must be longer"),
            ({"window": [1, 2]}, None, "per_second_sampling"),
            ({"window": [1, 2]}, {"sources": {"disk": {}}}, "valkey_info"),
            ({"requires_commands": "SEXPIRE"}, True, "requires_commands"),
            ({"preload": [{"type": "set"}]}, True, "key_prefix"),
            ({"check_after_run": "yes"}, True, "check_after_run"),
        ],
    )
    def test_invalid(self, change, sampling, message):
        with pytest.raises(ValueError, match=message):
            validate_config(_cfg({**self.base, **change}, sampling))
