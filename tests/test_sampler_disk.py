"""Unit tests for the block device sample source.

Every /sys read is either patched or pointed at a tmp_path tree.
"""

import os
from unittest.mock import patch

import pytest

from samplers.base import SamplerContext
from samplers.disk import DiskSource, read_disk_counters, resolve_block_device

# Two consecutive stat snapshots, one second apart. The deltas are chosen so
# every derived value comes out different from every other, which is what makes
# a swapped formula fail rather than coincidentally pass.
#
# Deltas: read ios 200, read merges 40, read sectors 8192, read ticks 600,
#         write ios 50, write merges 15, write sectors 3200, write ticks 400,
#         io ticks 250, time in queue 1500.
DISK_SAMPLE_1 = {
    "read_ios": 100,
    "read_merges": 10,
    "read_sectors": 2048,
    "read_ticks": 50,
    "write_ios": 200,
    "write_merges": 20,
    "write_sectors": 4096,
    "write_ticks": 90,
    "in_flight": 3,
    "io_ticks": 1000,
    "time_in_queue": 5000,
}
DISK_SAMPLE_2 = {
    "read_ios": 300,
    "read_merges": 50,
    "read_sectors": 10240,
    "read_ticks": 650,
    "write_ios": 250,
    "write_merges": 35,
    "write_sectors": 7296,
    "write_ticks": 490,
    "in_flight": 7,
    "io_ticks": 1250,
    "time_in_queue": 6500,
}

# Every disk column except disk_in_flight, which is a gauge.
DISK_DERIVED_COLUMNS = (
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
    "disk_req_sz_kb",
)


def make_source(block_device="nvme0n1"):
    """Build a started DiskSource bound to `block_device`, warnings discarded."""
    source = DiskSource()
    source.start(
        SamplerContext(block_device=block_device, warn_once=lambda key, message: None)
    )
    return source


def disk_rows(counters, times=None, block_device="nvme0n1"):
    """Sample once per entry in `counters`, serving each as the stat snapshot."""
    if times is None:
        times = [100.0 + index for index in range(len(counters))]
    source = make_source(block_device)
    with patch("samplers.disk.read_disk_counters", side_effect=counters):
        return [source.sample(now) for now in times]


# /sys/block/<dev>/stat: read ios, read merges, read sectors, read ticks,
# write ios, write merges, write sectors, write ticks, in flight, io ticks,
# time in queue. Sectors are 512-byte units, ticks are milliseconds.
DISK_STAT_LINE = "100 10 2048 50 200 20 4096 90 3 1000 5000"


class StatResult:
    """The one os.stat field resolve_block_device reads."""

    def __init__(self, st_dev):
        self.st_dev = st_dev


def make_sys_tree(tmp_path, devices, entries):
    """Build a fake /sys tree and return the /sys/dev/block directory.

    devices maps a device directory name to the files it holds, and entries
    maps a "major:minor" string to the device directory it points at.
    """
    devices_dir = tmp_path / "devices"
    for name, files in devices.items():
        device_dir = devices_dir / name
        device_dir.mkdir(parents=True)
        for filename, content in files.items():
            (device_dir / filename).write_text(content)

    dev_block = tmp_path / "dev" / "block"
    dev_block.mkdir(parents=True)
    for entry, target in entries.items():
        (dev_block / entry).symlink_to(devices_dir / target)
    return dev_block


class TestResolveBlockDevice:
    @pytest.fixture
    def sys_tree(self, tmp_path):
        """A whole disk, one of its partitions and a device mapper device."""
        return make_sys_tree(
            tmp_path,
            devices={
                "nvme0n1": {"stat": DISK_STAT_LINE},
                "nvme0n1/nvme0n1p1": {"stat": DISK_STAT_LINE, "partition": "1\n"},
                "dm-0": {"stat": DISK_STAT_LINE},
            },
            entries={
                "259:0": "nvme0n1",
                "259:1": "nvme0n1/nvme0n1p1",
                "253:0": "dm-0",
            },
        )

    def _resolve(self, sys_tree, st_dev, path="/data"):
        real_stat = os.stat

        def fake_stat(target, *args, **kwargs):
            if target == path:
                return StatResult(st_dev)
            return real_stat(target, *args, **kwargs)

        with patch("samplers.disk._SYS_DEV_BLOCK_DIR", sys_tree):
            with patch("samplers.disk.os.stat", side_effect=fake_stat):
                return resolve_block_device(path)

    def test_whole_disk_resolves_to_itself(self, sys_tree):
        assert self._resolve(sys_tree, os.makedev(259, 0)) == "nvme0n1"

    def test_partition_resolves_to_its_whole_disk(self, sys_tree):
        assert self._resolve(sys_tree, os.makedev(259, 1)) == "nvme0n1"

    def test_device_mapper_resolves_to_itself(self, sys_tree):
        assert self._resolve(sys_tree, os.makedev(253, 0)) == "dm-0"

    def test_missing_sys_entry_returns_none(self, sys_tree):
        # tmpfs, overlay and NFS have no /sys/dev/block entry.
        assert self._resolve(sys_tree, os.makedev(0, 42)) is None

    def test_device_without_stat_returns_none(self, tmp_path):
        sys_tree = make_sys_tree(
            tmp_path, devices={"nvme0n1": {}}, entries={"259:0": "nvme0n1"}
        )
        assert self._resolve(sys_tree, os.makedev(259, 0)) is None

    def test_unstatable_path_returns_none(self, sys_tree):
        with patch("samplers.disk._SYS_DEV_BLOCK_DIR", sys_tree):
            assert resolve_block_device("/no/such/path/at/all") is None


class TestDeviceSelection:
    def _start(self, **ctx_kwargs):
        source = DiskSource()
        warnings = []
        with patch(
            "samplers.disk.resolve_block_device", side_effect=lambda path: f"dev:{path}"
        ):
            source.start(
                SamplerContext(
                    warn_once=lambda key, message: warnings.append(message),
                    **ctx_kwargs,
                )
            )
        return source, warnings

    def test_explicit_block_device_wins(self):
        source, _ = self._start(
            block_device="sdb", disk_path="/data", ext_storage_path="/ext"
        )
        assert source.device == "sdb"

    def test_disk_path_beats_ext_storage_path(self):
        source, _ = self._start(disk_path="/data", ext_storage_path="/ext")
        assert source.device == "dev:/data"

    def test_ext_storage_path_beats_config_get_dir(self):
        with patch("samplers.disk.run_cli") as cli:
            source, _ = self._start(ext_storage_path="/ext")
        cli.assert_not_called()
        assert source.device == "dev:/ext"

    def test_config_get_dir_is_the_last_resort(self):
        with patch("samplers.disk.run_cli", return_value="dir\n/var/lib/valkey\n"):
            source, _ = self._start()
        assert source.device == "dev:/var/lib/valkey"

    def test_unresolvable_path_warns_once_and_emits_no_columns(self):
        source = DiskSource()
        warnings = []
        with patch("samplers.disk.resolve_block_device", return_value=None):
            source.start(
                SamplerContext(
                    disk_path="/data",
                    warn_once=lambda key, message: warnings.append(message),
                )
            )
        assert source.device is None
        assert len(warnings) == 1
        assert "/data" in warnings[0]
        assert source.sample(100.0) == {}
        assert source.sample(101.0) == {}

    def test_no_path_at_all_emits_no_columns(self):
        source = DiskSource()
        with patch("samplers.disk.run_cli", return_value=None):
            source.start(SamplerContext(warn_once=lambda key, message: None))
        assert source.device is None
        assert source.sample(100.0) == {}


class TestDiskCounters:
    STAT = DISK_STAT_LINE
    COUNTERS = {
        "read_ios": 100,
        "read_merges": 10,
        "read_sectors": 2048,
        "read_ticks": 50,
        "write_ios": 200,
        "write_merges": 20,
        "write_sectors": 4096,
        "write_ticks": 90,
        "in_flight": 3,
        "io_ticks": 1000,
        "time_in_queue": 5000,
    }

    def test_parses_counters(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text(self.STAT)
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") == self.COUNTERS

    def test_trailing_fields_are_ignored(self, tmp_path):
        # Current kernels append discard and flush counters after the eleventh.
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text(self.STAT + " 7 8 9 10 11 12")
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") == self.COUNTERS

    def test_missing_device_returns_none(self, tmp_path):
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme9n9") is None

    def test_short_stat_line_returns_none(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("1 2 3")
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_ten_field_stat_line_returns_none(self, tmp_path):
        # One field short of the eleven the derivations need.
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("100 10 2048 50 200 20 4096 90 3 1000")
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_non_numeric_stat_line_returns_none(self, tmp_path):
        device = tmp_path / "nvme0n1"
        device.mkdir()
        (device / "stat").write_text("a b c d e f g h i j k")
        with patch("samplers.disk._SYS_BLOCK_DIR", tmp_path):
            assert read_disk_counters("nvme0n1") is None

    def test_unreadable_device_stat_does_not_raise(self):
        rows = disk_rows([None, None])
        for column in DISK_DERIVED_COLUMNS:
            assert rows[1][column] == 0.0
        assert rows[1]["disk_in_flight"] == 0


class TestDiskDerivedStats:
    """Each derived disk column, against hand-computed expected values."""

    def test_iops_and_throughput(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 200 read ios and 50 write ios in 1s
        assert row["disk_read_iops"] == 200.0
        assert row["disk_write_iops"] == 50.0
        # 8192 sectors of 512 bytes is exactly 4 MiB, 3200 sectors is 1.5625
        assert row["disk_read_mb"] == 4.0
        assert row["disk_write_mb"] == 1.56

    def test_merge_rates(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 40 read merges and 15 write merges in 1s
        assert row["disk_read_merges_ps"] == 40.0
        assert row["disk_write_merges_ps"] == 15.0

    def test_await_is_ticks_per_io_not_per_second(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 600 read ticks over 200 read ios, 400 write ticks over 50 write ios
        assert row["disk_r_await_ms"] == 3.0
        assert row["disk_w_await_ms"] == 8.0

    def test_queue_depth_is_queued_ms_over_interval_ms(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 1500ms queued over a 1000ms interval
        assert row["disk_aqu_sz"] == 1.5

    def test_util_is_io_ticks_over_interval_ms(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # 250ms busy over a 1000ms interval
        assert row["disk_util_pct"] == 25.0

    def test_in_flight_is_a_gauge(self):
        rows = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])
        # Read directly, so it is populated on the first sample and is not the
        # difference between the two snapshots (which would be 4).
        assert rows[0]["disk_in_flight"] == 3
        assert rows[1]["disk_in_flight"] == 7

    def test_request_size_combines_reads_and_writes(self):
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        # (8192 + 3200) sectors of 512 bytes over (200 + 50) ios, in KB
        assert row["disk_req_sz_kb"] == 22.78

    def test_every_derived_value_is_distinct(self):
        # A formula swap between any two of these would change a value.
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2])[1]
        values = [row[column] for column in DISK_DERIVED_COLUMNS]
        assert len(set(values)) == len(values)

    def test_interval_normalizes_the_rates(self):
        # The same deltas over 2s halve every rate, while the two awaits are
        # per-io and do not move.
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2], [100.0, 102.0])[1]
        assert row["disk_read_iops"] == 100.0
        assert row["disk_read_mb"] == 2.0
        assert row["disk_read_merges_ps"] == 20.0
        assert row["disk_aqu_sz"] == 0.75
        assert row["disk_util_pct"] == 12.5
        assert row["disk_r_await_ms"] == 3.0
        assert row["disk_w_await_ms"] == 8.0
        assert row["disk_req_sz_kb"] == 22.78

    def test_failed_read_widens_the_next_interval(self):
        # The stat read fails at 101.0, so the deltas from 100.0 to 102.0 are
        # divided by the full 2s gap rather than by one tick.
        rows = disk_rows([DISK_SAMPLE_1, None, DISK_SAMPLE_2], [100.0, 101.0, 102.0])
        assert rows[2]["disk_read_iops"] == 100.0
        assert rows[2]["disk_write_iops"] == 25.0
        assert rows[2]["disk_read_mb"] == 2.0

    def test_failed_read_does_not_reset_the_baseline(self):
        rows = disk_rows([DISK_SAMPLE_1, None, DISK_SAMPLE_2], [100.0, 101.0, 102.0])
        # A cleared baseline would report the whole running total as one
        # interval, which is 150 read iops rather than 100.
        assert rows[2]["disk_read_iops"] != DISK_SAMPLE_2["read_ios"] / 2
        assert rows[1]["disk_read_iops"] == 0.0

    def test_first_sample_yields_zero_for_every_derived_stat(self):
        row = disk_rows([DISK_SAMPLE_1])[0]
        for column in DISK_DERIVED_COLUMNS:
            assert row[column] == 0.0, f"{column} is {row[column]} on first sample"

    def test_zero_io_interval_zeroes_await_and_request_size(self):
        # Ticks advance while no io completes: work started in an earlier
        # interval is still in service.
        idle = dict(DISK_SAMPLE_2)
        idle.update(
            {
                "read_ticks": 750,
                "write_ticks": 890,
                "io_ticks": 1400,
                "time_in_queue": 7000,
            }
        )
        row = disk_rows([DISK_SAMPLE_1, DISK_SAMPLE_2, idle])[2]
        assert row["disk_r_await_ms"] == 0.0
        assert row["disk_w_await_ms"] == 0.0
        assert row["disk_req_sz_kb"] == 0.0
        assert row["disk_read_iops"] == 0.0
        assert row["disk_write_iops"] == 0.0
        # The device was still busy, so these are unaffected by the io count.
        assert row["disk_aqu_sz"] == 0.5
        assert row["disk_util_pct"] == 15.0

    def test_util_caps_at_one_hundred(self):
        # io ticks is wall-clock busy time on a concurrent queue, so the raw
        # ratio can exceed 1: 2500ms busy over a 1000ms interval is 250%.
        saturated = dict(DISK_SAMPLE_2)
        saturated["io_ticks"] = DISK_SAMPLE_1["io_ticks"] + 2500
        row = disk_rows([DISK_SAMPLE_1, saturated])[1]
        assert row["disk_util_pct"] == 100.0

    def test_counter_reset_clamps_to_zero(self):
        reset = {name: 0 for name in DISK_SAMPLE_1}
        row = disk_rows([DISK_SAMPLE_2, reset])[1]
        for column in DISK_DERIVED_COLUMNS:
            assert row[column] == 0.0
        assert row["disk_in_flight"] == 0
