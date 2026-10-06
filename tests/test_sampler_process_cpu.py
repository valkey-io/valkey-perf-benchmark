"""Unit tests for the /proc CPU sample source, against a stubbed /proc."""

from unittest.mock import patch

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


def sample(pid):
    source = ProcessCpuSource()
    source.start(SamplerContext(server_pid=pid))
    return source.sample()


def test_records_cpu_lines_and_per_thread_ticks(proc):
    reading = sample(4242)
    assert reading["proc_stat"] == {
        "cpu": [1750319, 220644, 1391872, 913251652, 219328, 0, 3001, 24711, 0, 0],
        "cpu0": [37882, 18635, 54709, 18972792, 10857, 0, 31, 1045, 0, 0],
    }
    assert reading["threads"] == {
        "4242": {"comm": "valkey-server", "utime": 100, "stime": 10},
        "4243": {"comm": "io_thd_1", "utime": 101, "stime": 11},
        "4244": {"comm": "a) b", "utime": 102, "stime": 12},
    }


def test_no_pid_records_host_cpu_only(proc):
    assert set(sample(None)) == {"proc_stat"}


def test_unreadable_proc_raises(proc):
    (proc / "stat").unlink()
    with pytest.raises(OSError):
        sample(4242)
