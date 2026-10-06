"""Unit tests for the metrics sampler loop and its JSON Lines output."""

import json
import threading
from datetime import datetime
from unittest.mock import patch

import pytest

from metrics_sampler import (
    SOCKET_TIMEOUT_SEC,
    MetricsSampler,
    append_jsonl,
    is_local_address,
)
from samplers import SampleSource


class FakeSource(SampleSource):
    """A source that returns a fixed reading, or raises it when it is one."""

    def __init__(self, name, reading, local_only=False, linux_only=False):
        super().__init__()
        self.name = name
        self.reading = reading
        self.local_only = local_only
        self.linux_only = linux_only

    def sample(self):
        if isinstance(self.reading, Exception):
            raise self.reading
        return self.reading


def sampler_with(*sources, context=None, **kwargs):
    sampler = MetricsSampler(context=context or {}, server_local=True, **kwargs)
    sampler._sources = list(sources)
    return sampler


def one_row(sampler, elapsed_sec=0):
    sampler._sample_once(elapsed_sec)
    return sampler.rows[-1]


class TestRow:
    def test_identity_then_tick_fields_then_one_key_per_source(self):
        context = {"commit": "abc123", "timestamp": "2026-01-01T00:00:00+00:00"}
        sampler = sampler_with(
            FakeSource("valkey_info", {"used_memory": "1"}),
            FakeSource("disk", {"device": "nvme0n1", "stat": [1]}),
            context=context,
        )

        row = one_row(sampler, 2)

        assert list(row) == [
            "commit",
            "timestamp",
            "sample_time",
            "elapsed_sec",
            "valkey_info",
            "disk",
        ]
        assert row["timestamp"] == "2026-01-01T00:00:00+00:00"
        assert row["elapsed_sec"] == 2
        assert row["valkey_info"] == {"used_memory": "1"}
        assert datetime.fromisoformat(row["sample_time"]).utcoffset().seconds == 0

    @pytest.mark.parametrize("failure", [None, RuntimeError("read failed")])
    def test_failed_source_is_absent(self, failure):
        sampler = sampler_with(
            FakeSource("valkey_info", failure), FakeSource("disk", {"stat": [1]})
        )

        row = one_row(sampler)

        assert "valkey_info" not in row
        assert row["disk"] == {"stat": [1]}

    @pytest.mark.parametrize("key", ["valkey_info", "sample_time", "elapsed_sec"])
    def test_identity_colliding_with_a_sample_key_is_rejected(self, key):
        with pytest.raises(ValueError, match=key):
            MetricsSampler(context={key: 1})


class TestAppendJsonl:
    def test_appends_one_compact_line_per_row(self, tmp_path):
        path = tmp_path / "results" / "timeseries.jsonl"

        append_jsonl(path, [{"elapsed_sec": 0, "info": {"a": "1"}}])
        first = path.read_text()
        inode = path.stat().st_ino
        append_jsonl(path, [{"elapsed_sec": 1}])

        content = path.read_text()
        assert first == '{"elapsed_sec":0,"info":{"a":"1"}}\n'
        assert content.startswith(first)
        assert path.stat().st_ino == inode
        assert [json.loads(line) for line in content.splitlines()] == [
            {"elapsed_sec": 0, "info": {"a": "1"}},
            {"elapsed_sec": 1},
        ]

    def test_no_rows_writes_no_file(self, tmp_path):
        append_jsonl(tmp_path / "timeseries.jsonl", [])
        assert not (tmp_path / "timeseries.jsonl").exists()


class TestSourceSelection:
    def test_options_reach_each_source(self):
        sampler = MetricsSampler(
            sources={"valkey_info": {}, "disk": {"path": "/mnt/nvme"}},
            server_local=True,
        )
        with patch("samplers.disk.resolve_block_device", return_value="nvme0n1"):
            sources = sampler._build_sources()

        assert [source.name for source in sources] == ["valkey_info", "disk"]
        assert sources[1].options == {"path": "/mnt/nvme"}
        assert sources[1].device == "nvme0n1"

    def test_source_that_fails_to_start_is_dropped(self):
        sampler = MetricsSampler(
            sources={"valkey_info": {}, "disk": {"path": "/mnt/nvme"}},
            server_local=True,
        )
        with patch("samplers.disk.resolve_block_device", return_value=None):
            assert [s.name for s in sampler._build_sources()] == ["valkey_info"]

    @pytest.mark.parametrize(
        "platform, server_local, expected",
        [
            ("linux", True, ["valkey_info", "process_cpu"]),
            ("linux", False, ["valkey_info"]),
            ("darwin", True, ["valkey_info"]),
        ],
    )
    def test_host_sources_need_linux_and_a_local_server(
        self, platform, server_local, expected
    ):
        sampler = MetricsSampler(
            sources={"valkey_info": {}, "process_cpu": {}}, server_local=server_local
        )
        with patch("metrics_sampler.sys.platform", platform):
            assert [s.name for s in sampler._build_sources()] == expected


@pytest.mark.parametrize(
    "host, local", [("localhost", True), ("127.0.0.1", True), ("192.0.2.1", False)]
)
def test_is_local_address(host, local):
    assert is_local_address(host) is local


class TestThreadPinning:
    def test_pins_to_the_parsed_core_set(self):
        sampler = sampler_with(cpu_range="2-3,8")
        with patch("metrics_sampler.os.sched_setaffinity", create=True) as pin:
            sampler._pin_thread()
        pin.assert_called_once_with(0, {2, 3, 8})

    def test_pin_failure_does_not_raise(self):
        sampler = sampler_with(cpu_range="2-3")
        with patch(
            "metrics_sampler.os.sched_setaffinity", side_effect=OSError, create=True
        ):
            sampler._pin_thread()


class FakeStopEvent:
    """A stop event on a fake clock that sets itself after a number of ticks."""

    def __init__(self, clock, ticks, limit):
        self.clock = clock
        self.ticks = ticks
        self.limit = limit

    def is_set(self):
        return len(self.ticks) >= self.limit

    def wait(self, seconds):
        self.clock[0] += seconds
        return False


class TestSchedule:
    def run_loop(self, durations, interval=1.0):
        """Return (elapsed_sec, clock) per tick, tick k taking durations[k]."""
        clock = [100.0]
        ticks = []

        def sample_once(elapsed_sec):
            ticks.append((elapsed_sec, clock[0]))
            clock[0] += durations[len(ticks) - 1]

        sampler = MetricsSampler(interval=interval, server_local=True)
        sampler._stop_event = FakeStopEvent(clock, ticks, len(durations))
        with (
            patch("metrics_sampler.time.monotonic", side_effect=lambda: clock[0]),
            patch.object(sampler, "_sample_once", side_effect=sample_once),
        ):
            sampler._sample_loop()
        return ticks

    def test_ticks_land_on_multiples_of_the_interval(self):
        ticks = self.run_loop([0.0001] * 9000)
        assert [elapsed for elapsed, _ in ticks] == list(range(9000))
        assert ticks[-1][1] == pytest.approx(100.0 + 8999)

    @pytest.mark.parametrize(
        "durations, expected",
        [
            ([0.1, 1.5, 0.1, 0.1], [0, 1, 3, 4]),
            ([0.1, 2.5, 0.1], [0, 1, 4]),
            ([1.0, 0.1], [0, 2]),
        ],
    )
    def test_overrun_skips_to_the_next_future_slot(self, durations, expected):
        ticks = self.run_loop(durations)
        assert [elapsed for elapsed, _ in ticks] == expected
        assert [clock for _, clock in ticks] == pytest.approx(
            [100.0 + elapsed for elapsed in expected]
        )


class TestClient:
    def test_start_creates_a_bounded_client_with_tls(self):
        tls = {"ssl": True, "ssl_ca_certs": "ca.crt"}
        sampler = MetricsSampler(host="10.0.0.5", port=6380, tls_kwargs=tls)
        with (
            patch("metrics_sampler.valkey.Valkey") as client_cls,
            patch.object(sampler, "_build_sources", return_value=[]),
        ):
            sampler.start()
            sampler.stop()

        client_cls.assert_called_once_with(
            host="10.0.0.5",
            port=6380,
            decode_responses=True,
            socket_timeout=SOCKET_TIMEOUT_SEC,
            socket_connect_timeout=SOCKET_TIMEOUT_SEC,
            **tls,
        )
        client = client_cls.return_value
        client.set_response_callback.assert_called_once()
        assert client.set_response_callback.call_args.args[0] == "INFO"
        client.close.assert_called_once()
        assert sampler._client is None


class TestBackgroundThread:
    def test_start_stop_collects_rows(self):
        ticks = threading.Event()

        class CountingSource(FakeSource):
            def sample(self):
                if len(sampler.rows) >= 2:
                    ticks.set()
                return {"n": "1"}

        sampler = MetricsSampler(interval=0.01, server_local=True)
        with patch.object(
            sampler, "_build_sources", return_value=[CountingSource("valkey_info", {})]
        ):
            sampler.start()
            assert ticks.wait(5)
            sampler.stop()

        assert sampler._sampler_thread is None
        assert all(row["valkey_info"] == {"n": "1"} for row in sampler.rows)

    def test_stop_during_the_start_delay_yields_no_rows(self):
        sampler = MetricsSampler(start_delay=60, server_local=True)
        with patch.object(sampler, "_build_sources", return_value=[]):
            sampler.start()
            sampler.stop()
        assert sampler.rows == []
