"""Unit tests for the block device sample source, against a stubbed /sys."""

import os
from unittest.mock import MagicMock, patch

import pytest

from samplers import SamplerContext
from samplers.disk import DiskSource, resolve_block_device

STAT = (
    " 5664581  1117339 86138367  5868355  4339684  3748278 529318472 10119596"
    "        0  5248410 15987951        0        0        0        0        0"
    "        0\n"
)


@pytest.fixture
def sys_tree(tmp_path):
    """A /sys with nvme0n1 holding partition nvme0n1p1, numbered 259:0 and 259:1."""
    disk = tmp_path / "devices" / "nvme0n1"
    (disk / "nvme0n1p1").mkdir(parents=True)
    (disk / "nvme0n1p1" / "partition").write_text("1\n")
    (disk / "stat").write_text(STAT)
    dev_block = tmp_path / "dev_block"
    dev_block.mkdir()
    os.symlink(disk, dev_block / "259:0")
    os.symlink(disk / "nvme0n1p1", dev_block / "259:1")
    with (
        patch("samplers.disk._SYS_DEV_BLOCK_DIR", dev_block),
        patch("samplers.disk._SYS_BLOCK_DIR", tmp_path / "devices"),
    ):
        yield


REAL_STAT = os.stat


def fake_stat(path, minor):
    """Patch os.stat so path reports device 259:minor."""

    def stat(target, *args, **kwargs):
        if str(target) == path:
            return os.stat_result((0, 0, os.makedev(259, minor), 0, 0, 0, 0, 0, 0, 0))
        return REAL_STAT(target, *args, **kwargs)

    return patch("samplers.disk.os.stat", side_effect=stat)


@pytest.mark.parametrize("minor, expected", [(0, "nvme0n1"), (1, "nvme0n1"), (9, None)])
def test_resolves_the_whole_disk(sys_tree, minor, expected):
    with fake_stat("/data", minor):
        assert resolve_block_device("/data") == expected


def test_records_device_and_raw_counters(sys_tree):
    source = DiskSource({"path": "/mnt/nvme"})
    with fake_stat("/mnt/nvme", 1):
        source.start(SamplerContext())
    assert source.sample() == {
        "device": "nvme0n1",
        "stat": [int(value) for value in STAT.split()],
    }


def config_client(directory):
    client = MagicMock()
    client.config_get.return_value = {"dir": directory}
    return client


def test_path_falls_back_to_config_get_dir(sys_tree):
    source = DiskSource()
    client = config_client("/var/lib/valkey")
    with fake_stat("/var/lib/valkey", 0):
        source.start(SamplerContext(client=client))
    client.config_get.assert_called_once_with("dir")
    assert source.device == "nvme0n1"


def test_start_raises_without_a_device(sys_tree):
    with pytest.raises(RuntimeError, match="No block device backs"):
        DiskSource().start(SamplerContext(client=config_client("/no/such/dir")))


@pytest.mark.parametrize(
    "options, match",
    [
        ([], "must be an object"),
        ({"device": "sda"}, r"does not support key\(s\): \['device'\]"),
        ({"path": ""}, "must be a non-empty string"),
        ({"path": 5}, "must be a non-empty string"),
    ],
)
def test_rejects_bad_options(options, match):
    with pytest.raises(ValueError, match=match):
        DiskSource.validate_options(options)


@pytest.mark.parametrize("options", [{}, {"path": "/mnt/nvme"}])
def test_accepts_good_options(options):
    DiskSource.validate_options(options)
