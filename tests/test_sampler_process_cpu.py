"""Unit tests for the /proc CPU sample source, against stubbed server data."""

from unittest.mock import MagicMock, patch

import pytest

from samplers import SamplerContext
from samplers.process_cpu import ProcessCpuSource

PROC_STAT = """cpu  1750319 220644 1391872 913251652 219328 0 3001 24711 0 0
cpu0 37882 18635 54709 18972792 10857 0 31 1045 0 0
intr 123456 0 0
ctxt 987654
"""

THREADS = {
    "4242": "valkey-server",
    "4243": "io_thd_1",
    "4244": "a) b",
}


def task_stat(tid, comm, utime, stime):
    return (
        f"{tid} ({comm}) S 1 4242 4242 0 -1 4194560 2000 0 0 0 "
        f"{utime} {stime} 0 0 20 0 4 0 19108672 117420032 195\n"
    )


@pytest.fixture
def proc(tmp_path):
    (tmp_path / "stat").write_text(PROC_STAT)
    for index, (tid, comm) in enumerate(THREADS.items()):
        task = tmp_path / "4242" / "task" / tid
        task.mkdir(parents=True)
        (task / "stat").write_text(task_stat(tid, comm, 100 + index, 10 + index))
    with patch("samplers.process_cpu._PROC_DIR", tmp_path):
        yield tmp_path


def sample(info="# Server\r\nprocess_id:4242\r\n"):
    client = MagicMock()
    if isinstance(info, Exception):
        client.execute_command.side_effect = info
    else:
        client.execute_command.return_value = info
    source = ProcessCpuSource()
    warnings = MagicMock()
    source.start(SamplerContext(client=client, warn_once=warnings))
    return source.sample(), client, warnings


def test_records_cpu_lines_and_per_thread_ticks(proc):
    reading, client, warnings = sample()
    assert reading["proc_stat"] == {
        "cpu": [1750319, 220644, 1391872, 913251652, 219328, 0, 3001, 24711, 0, 0],
        "cpu0": [37882, 18635, 54709, 18972792, 10857, 0, 31, 1045, 0, 0],
    }
    assert reading["threads"] == {
        "4242": {"comm": "valkey-server", "utime": 100, "stime": 10},
        "4243": {"comm": "io_thd_1", "utime": 101, "stime": 11},
        "4244": {"comm": "a) b", "utime": 102, "stime": 12},
    }
    client.execute_command.assert_called_once_with("INFO", "SERVER")
    warnings.assert_not_called()


@pytest.mark.parametrize(
    "info",
    [
        RuntimeError("INFO failed"),
        "# Server\r\n",
        "# Server\r\nprocess_id:not-a-pid\r\n",
        "# Server\r\nprocess_id:0\r\n",
    ],
)
def test_pid_lookup_failure_records_host_cpu_only(proc, info):
    reading, _, warnings = sample(info)
    assert set(reading) == {"proc_stat"}
    warnings.assert_called_once()


def test_unreadable_proc_raises(proc):
    (proc / "stat").unlink()
    with pytest.raises(OSError):
        sample()
