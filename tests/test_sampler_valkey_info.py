"""Unit tests for the INFO sample source."""

from unittest.mock import MagicMock, patch

from metrics_sampler import create_client
from samplers import SamplerContext
from samplers.valkey_info import ValkeyInfoSource

INFO = """# Server
valkey_version:9.1.0
valkey_git_sha1:00000000
process_id:4242

# Memory
used_memory:1048576
mem_fragmentation_ratio:1.20

# Keyspace
db0:keys=10000,expires=0,avg_ttl=0
"""


def sample_through_client(reply):
    """Sample INFO through create_client with the socket replaced by reply."""
    client = create_client("127.0.0.1", 6379, {})
    connection = MagicMock()
    connection._get_from_local_cache.return_value = None
    connection.read_response.return_value = reply
    connection.retry.call_with_retry.side_effect = lambda do, fail: do()
    with patch.object(
        client.connection_pool, "get_connection", return_value=connection
    ):
        source = ValkeyInfoSource()
        source.start(SamplerContext(client=client))
        reading = source.sample()
    connection.send_command.assert_called_once_with("INFO", "ALL")
    return reading


def test_every_field_is_recorded():
    assert sample_through_client(INFO) == {
        "valkey_version": "9.1.0",
        "valkey_git_sha1": "00000000",
        "process_id": "4242",
        "used_memory": "1048576",
        "mem_fragmentation_ratio": "1.20",
        "db0": "keys=10000,expires=0,avg_ttl=0",
    }


def test_empty_reply_reads_as_none():
    assert sample_through_client("") is None
