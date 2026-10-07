"""Unit tests for the tiering storage file reset in ServerLauncher.launch()."""

from unittest.mock import patch

import pytest

from valkey_server import ServerLauncher, ext_storage_capacity_bytes, parse_valkey_size

MB = 1024**2


def _launcher(valkey_path):
    return ServerLauncher(results_dir="results", valkey_path=str(valkey_path))


def _config(**custom):
    return {"custom-server-configs": custom}


def _enabled(path, **extra):
    return _config(
        **{
            "ext-storage-enabled": "yes",
            "ext-storage-path": str(path),
            "ext-storage-capacity": "1mb",
            **extra,
        }
    )


def _launch(launcher, config):
    with patch.object(launcher, "_launch_server"):
        launcher.launch(cluster_mode=False, tls_mode=False, config=config)


def test_existing_file_recreated_empty_at_capacity(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data" * 1000)
    _launch(_launcher(tmp_path), _enabled(storage, **{"ext-storage-enabled": "YES"}))
    assert storage.stat().st_size == MB
    assert storage.read_bytes()[:4000] == bytes(4000)


def test_missing_file_created_at_capacity(tmp_path):
    storage = tmp_path / "flashcache.db"
    _launch(_launcher(tmp_path), _enabled(storage))
    assert storage.stat().st_size == MB


def test_file_blocks_are_allocated(tmp_path):
    storage = tmp_path / "flashcache.db"
    _launch(_launcher(tmp_path), _enabled(storage))
    assert storage.stat().st_blocks * 512 >= MB


@pytest.mark.parametrize(
    "custom",
    [
        {"ext-storage-enabled": "no"},
        {},
        {"ext-storage-enabled": "yes", "ext-storage-path": ""},
    ],
)
def test_file_untouched(tmp_path, custom):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data")
    custom.setdefault("ext-storage-path", str(storage))
    _launch(_launcher(tmp_path), _config(**custom))
    assert storage.read_bytes() == b"data"


def test_file_untouched_without_config(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data")
    _launch(_launcher(tmp_path), None)
    assert storage.read_bytes() == b"data"


def test_directory_raises_and_is_kept(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.mkdir()
    launcher = _launcher(tmp_path)
    with (
        patch.object(launcher, "_launch_server") as launch_server,
        pytest.raises(RuntimeError, match="not a regular file"),
    ):
        launcher.launch(cluster_mode=False, tls_mode=False, config=_enabled(storage))
    assert storage.is_dir()
    launch_server.assert_not_called()


def test_invalid_capacity_raises_before_removing(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data")
    launcher = _launcher(tmp_path)
    with (
        patch.object(launcher, "_launch_server") as launch_server,
        pytest.raises(ValueError, match="invalid size"),
    ):
        launcher.launch(
            cluster_mode=False,
            tls_mode=False,
            config=_enabled(storage, **{"ext-storage-capacity": "lots"}),
        )
    assert storage.read_bytes() == b"data"
    launch_server.assert_not_called()


def test_relative_path_resolved_against_valkey_path(tmp_path):
    _launch(_launcher(tmp_path), _enabled("flashcache.db"))
    assert (tmp_path / "flashcache.db").stat().st_size == MB


def test_relative_path_resolved_against_dir(tmp_path):
    (tmp_path / "data").mkdir()
    _launch(_launcher(tmp_path), _enabled("flashcache.db", dir="data"))
    assert (tmp_path / "data" / "flashcache.db").stat().st_size == MB


def test_reset_on_every_launch(tmp_path):
    storage = tmp_path / "flashcache.db"
    launcher = _launcher(tmp_path)
    for _ in range(2):
        _launch(launcher, _enabled(storage))
        storage.write_bytes(b"data")
    _launch(launcher, _enabled(storage))
    assert storage.read_bytes()[:4] == bytes(4)


def test_reset_before_cluster_nodes_start(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data")
    config = _enabled(storage)
    config.update({"cluster_nodes": 2, "cluster_ports": [7000, 7001]})
    config["server_cpu_ranges"] = ["0", "1"]
    launcher = _launcher(tmp_path)
    seen = []

    def node_started(**kwargs):
        seen.append(storage.stat().st_size)

    with (
        patch.object(launcher, "_launch_cluster_node", side_effect=node_started),
        patch.object(launcher, "_create_multi_node_cluster"),
    ):
        launcher.launch(cluster_mode=True, tls_mode=False, config=config)
    assert seen == [MB, MB]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("8gb", 8 * 1024**3),
        ("8GB", 8 * 1024**3),
        ("512mb", 512 * MB),
        ("2g", 2 * 1000**3),
        ("100kb", 100 * 1024),
        ("1048576", MB),
        (4096, 4096),
    ],
)
def test_parse_valkey_size(value, expected):
    assert parse_valkey_size(value) == expected


@pytest.mark.parametrize("value", ["", "lots", "8tb", "-1gb", "1.5gb"])
def test_parse_valkey_size_rejects(value):
    with pytest.raises(ValueError, match="invalid size"):
        parse_valkey_size(value)


@pytest.mark.parametrize(
    "custom, expected",
    [
        ({"ext-storage-capacity": "8gb"}, 8 * 1024**3),
        ({"ext-storage-capacity-mb": 8192}, 8192 * MB),
        ({}, 1024**3),
    ],
)
def test_ext_storage_capacity_bytes(custom, expected):
    assert ext_storage_capacity_bytes(custom) == expected
