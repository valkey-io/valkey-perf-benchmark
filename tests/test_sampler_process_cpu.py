"""Unit tests for the host and process CPU sample source.

Every /proc read is either patched or pointed at a tmp_path tree.
"""

from unittest.mock import patch

from samplers.base import ASIO_THREAD_NAME, SamplerContext
from samplers.process_cpu import (
    ProcessCpuSource,
    read_process_cpu_ticks,
    read_system_cpu_ticks,
    read_thread_cpu_ticks,
)

CPU_COLUMNS = (
    "valkey_cpu_user",
    "valkey_cpu_sys",
    "valkey_cpu_total",
    "asio_cpu_pct",
    "cpu_user",
    "cpu_sys",
)


def make_source(server_pid=None):
    """Build a started ProcessCpuSource with warnings discarded."""
    source = ProcessCpuSource()
    source.start(
        SamplerContext(server_pid=server_pid, warn_once=lambda key, message: None)
    )
    return source


def source_rows(source, count=2, intervals=None):
    """Sample the source `count` times, the first tick having no predecessor."""
    if intervals is None:
        intervals = [None] + [1.0] * (count - 1)
    return [source.sample(interval) for interval in intervals]


class TestSystemCpu:
    def test_parses_proc_stat_first_line(self):
        proc_stat = "cpu  100 10 50 900 5 0 0 0 0 0\ncpu0 1 2 3 4 5 0 0 0 0 0\n"
        with patch("samplers.process_cpu.read_text", return_value=proc_stat):
            # user+nice, system, sum of every bucket
            assert read_system_cpu_ticks() == (110, 50, 1065)

    def test_returns_none_when_unreadable(self):
        with patch("samplers.process_cpu.read_text", return_value=""):
            assert read_system_cpu_ticks() is None

    def test_returns_none_on_garbage(self):
        with patch("samplers.process_cpu.read_text", return_value="cpu  a b c d e\n"):
            assert read_system_cpu_ticks() is None

    def test_percentages_derived_across_two_samples(self):
        with patch(
            "samplers.process_cpu.read_system_cpu_ticks",
            side_effect=[(100, 50, 1000), (200, 100, 2000)],
        ):
            rows = source_rows(make_source())
        assert rows[0]["cpu_user"] == 0.0
        # 100 user ticks of 1000 total ticks, 50 system ticks of 1000
        assert rows[1]["cpu_user"] == 10.0
        assert rows[1]["cpu_sys"] == 5.0

    def test_missing_proc_stat_does_not_raise(self):
        with patch("samplers.process_cpu.read_system_cpu_ticks", return_value=None):
            rows = source_rows(make_source())
        assert rows[1]["cpu_user"] == 0.0
        assert rows[1]["cpu_sys"] == 0.0


class TestProcessAndThreadCpu:
    # utime is field 14 and stime field 15 of /proc/<pid>/stat, counted after
    # the parenthesized comm field.

    def _stat_line(self, comm, utime, stime):
        fields = list(range(3, 30))
        fields[11] = utime  # field 14
        fields[12] = stime  # field 15
        return f"1234 ({comm}) " + " ".join(str(n) for n in fields)

    def _make_task_tree(self, proc_root, threads, pid=1234):
        """Build a fake /proc/<pid>/task tree of (tid, comm, utime, stime)."""
        for tid, comm, utime, stime in threads:
            thread_dir = proc_root / str(pid) / "task" / str(tid)
            thread_dir.mkdir(parents=True)
            (thread_dir / "comm").write_text(f"{comm}\n")
            (thread_dir / "stat").write_text(self._stat_line(comm, utime, stime))

    def test_parses_utime_and_stime(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text(self._stat_line("valkey-server", 500, 250))
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) == (500, 250)

    def test_comm_with_spaces_and_parens_does_not_break_parsing(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text(self._stat_line("valkey server (x)", 700, 300))
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) == (700, 300)

    def test_missing_pid_returns_none(self, tmp_path):
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(9999) is None

    def test_truncated_stat_line_returns_none(self, tmp_path):
        stat_file = tmp_path / "1234"
        stat_file.mkdir()
        (stat_file / "stat").write_text("1234 (valkey-server) S 1 2 3")
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_process_cpu_ticks(1234) is None

    def test_process_cpu_percent_across_two_samples(self):
        with patch("samplers.process_cpu._CLK_TCK", 100):
            with patch(
                "samplers.process_cpu.read_process_cpu_ticks",
                side_effect=[(100, 50), (180, 70)],
            ):
                with patch(
                    "samplers.process_cpu.read_thread_cpu_ticks", return_value=0
                ):
                    rows = source_rows(make_source(server_pid=1234))
        # 80 user ticks in 1s at 100Hz is 80% of one core, 20 sys ticks is 20%
        assert rows[1]["valkey_cpu_user"] == 80.0
        assert rows[1]["valkey_cpu_sys"] == 20.0
        assert rows[1]["valkey_cpu_total"] == 100.0

    def test_asio_thread_ticks_summed_by_name(self, tmp_path):
        self._make_task_tree(
            tmp_path,
            [
                (10, "valkey-server", 1000, 1000),
                (11, ASIO_THREAD_NAME, 40, 10),
                (12, ASIO_THREAD_NAME, 20, 30),
            ],
        )
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            # Only the two fc_io_worker threads count: 40+10 plus 20+30
            assert read_thread_cpu_ticks(1234, ASIO_THREAD_NAME) == 100

    def test_no_matching_thread_returns_zero(self, tmp_path):
        self._make_task_tree(tmp_path, [(10, "valkey-server", 1000, 1000)])
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_thread_cpu_ticks(1234, ASIO_THREAD_NAME) == 0

    def test_missing_task_dir_returns_none(self, tmp_path):
        with patch("samplers.process_cpu._PROC_DIR", tmp_path):
            assert read_thread_cpu_ticks(9999, ASIO_THREAD_NAME) is None

    def test_asio_percent_across_two_samples(self):
        with patch("samplers.process_cpu._CLK_TCK", 100):
            with patch(
                "samplers.process_cpu.read_process_cpu_ticks", return_value=(0, 0)
            ):
                with patch(
                    "samplers.process_cpu.read_thread_cpu_ticks", side_effect=[100, 150]
                ):
                    rows = source_rows(make_source(server_pid=1234))
        # 50 ticks in 1s at 100Hz is half a core
        assert rows[1]["asio_cpu_pct"] == 50.0

    def test_no_pid_yields_zero_process_cpu(self):
        rows = source_rows(make_source(server_pid=None))
        assert rows[1]["valkey_cpu_total"] == 0.0
        assert rows[1]["asio_cpu_pct"] == 0.0

    def test_unreadable_task_dir_does_not_raise(self):
        with patch("samplers.process_cpu.read_process_cpu_ticks", return_value=None):
            with patch("samplers.process_cpu.read_thread_cpu_ticks", return_value=None):
                rows = source_rows(make_source(server_pid=1234))
        assert rows[1]["valkey_cpu_total"] == 0.0
        assert rows[1]["asio_cpu_pct"] == 0.0
