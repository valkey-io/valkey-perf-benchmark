"""Unit tests for the INFO sample source."""

from unittest.mock import patch

import pytest

from samplers import SamplerContext
from samplers.valkey_info import ValkeyInfoSource, parse_info

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


def started_source():
    source = ValkeyInfoSource()
    source.start(SamplerContext())
    return source


@pytest.mark.parametrize(
    "field, value",
    [
        ("valkey_git_sha1", "00000000"),
        ("used_memory", "1048576"),
        ("mem_fragmentation_ratio", "1.20"),
        ("db0", "keys=10000,expires=0,avg_ttl=0"),
        ("valkey_version", "9.1.0"),
    ],
)
def test_values_are_raw_strings(field, value):
    with patch("samplers.valkey_info.run_cli", return_value=INFO):
        assert started_source().sample()[field] == value


def test_every_field_is_recorded():
    with patch("samplers.valkey_info.run_cli", return_value=INFO):
        assert started_source().sample() == parse_info(INFO)
    assert len(parse_info(INFO)) == 6


def test_failed_info_reads_as_none():
    with patch("samplers.valkey_info.run_cli", return_value=None):
        assert started_source().sample() is None
