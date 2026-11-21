"""Unit tests for the tiering storage file reset in ServerLauncher.launch()."""

from unittest.mock import patch

import pytest

from valkey_server import FLASHCACHE_DB_FILE, ServerLauncher


def _launcher(valkey_path):
    return ServerLauncher(results_dir="results", valkey_path=str(valkey_path))


def _config(**custom):
    return {"custom-server-configs": custom}


def _enabled(path, **extra):
    return _config(
        **{"ext-storage-enabled": "yes", "ext-storage-path": str(path), **extra}
    )


def _launch(launcher, config):
    with patch.object(launcher, "_launch_server"):
        launcher.launch(cluster_mode=False, tls_mode=False, config=config)


def _db(directory, data=b"data"):
    directory.mkdir(exist_ok=True)
    db_file = directory / FLASHCACHE_DB_FILE
    db_file.write_bytes(data)
    return db_file


def test_existing_file_removed(tmp_path):
    directory = tmp_path / "flashcache"
    db_file = _db(directory)
    other = directory / "keep"
    other.write_bytes(b"x")
    _launch(_launcher(tmp_path), _enabled(directory, **{"ext-storage-enabled": "YES"}))
    assert not db_file.exists()
    assert directory.is_dir()
    assert other.read_bytes() == b"x"


def test_missing_directory_left_for_flashcache(tmp_path):
    directory = tmp_path / "flashcache"
    _launch(_launcher(tmp_path), _enabled(directory))
    assert not directory.exists()


@pytest.mark.parametrize(
    "custom",
    [
        {"ext-storage-enabled": "no"},
        {},
        {"ext-storage-enabled": "yes", "ext-storage-path": ""},
    ],
)
def test_file_untouched(tmp_path, custom):
    directory = tmp_path / "flashcache"
    db_file = _db(directory)
    custom.setdefault("ext-storage-path", str(directory))
    _launch(_launcher(tmp_path), _config(**custom))
    assert db_file.read_bytes() == b"data"


def test_file_untouched_without_config(tmp_path):
    db_file = _db(tmp_path / "flashcache")
    _launch(_launcher(tmp_path), None)
    assert db_file.read_bytes() == b"data"


def test_regular_file_path_raises_and_is_kept(tmp_path):
    storage = tmp_path / "flashcache.db"
    storage.write_bytes(b"data")
    launcher = _launcher(tmp_path)
    with (
        patch.object(launcher, "_launch_server") as launch_server,
        pytest.raises(RuntimeError, match="not a directory"),
    ):
        launcher.launch(cluster_mode=False, tls_mode=False, config=_enabled(storage))
    assert storage.read_bytes() == b"data"
    launch_server.assert_not_called()


def test_relative_path_resolved_against_valkey_path(tmp_path):
    db_file = _db(tmp_path / "flashcache")
    _launch(_launcher(tmp_path), _enabled("flashcache"))
    assert not db_file.exists()


def test_relative_path_resolved_against_dir(tmp_path):
    (tmp_path / "data").mkdir()
    db_file = _db(tmp_path / "data" / "flashcache")
    _launch(_launcher(tmp_path), _enabled("flashcache", dir="data"))
    assert not db_file.exists()


def test_reset_on_every_launch(tmp_path):
    directory = tmp_path / "flashcache"
    launcher = _launcher(tmp_path)
    for _ in range(3):
        db_file = _db(directory)
        _launch(launcher, _enabled(directory))
        assert not db_file.exists()


def test_reset_before_cluster_nodes_start(tmp_path):
    directory = tmp_path / "flashcache"
    db_file = _db(directory)
    config = _enabled(directory)
    config.update({"cluster_nodes": 2, "cluster_ports": [7000, 7001]})
    config["server_cpu_ranges"] = ["0", "1"]
    launcher = _launcher(tmp_path)
    seen = []

    def node_started(**kwargs):
        seen.append(db_file.exists())

    with (
        patch.object(launcher, "_launch_cluster_node", side_effect=node_started),
        patch.object(launcher, "_create_multi_node_cluster"),
    ):
        launcher.launch(cluster_mode=True, tls_mode=False, config=config)
    assert seen == [False, False]
