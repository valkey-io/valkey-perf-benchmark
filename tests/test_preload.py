"""Tests for scenario preload validation, loading and checking."""

import pytest

import preload


def _keyspace(**changes):
    keyspace = {
        "type": "set",
        "key_prefix": "s:",
        "sets": 3,
        "encoding": "listpack",
        "members": [{"prefix": "m", "count": 10, "ttl": {"seconds": 5, "count": 2}}],
    }
    keyspace.update(changes)
    return keyspace


class FakeClient:
    """Answers the loader with what it asked to write, and the check with ok."""

    def __init__(self, bad=0, ttls_short=0):
        self.calls = []
        self.bad = bad
        self.ttls_short = ttls_short

    def execute_command(self, *args):
        self.calls.append(args)
        if args[1] == preload.LOAD_SCRIPT:
            first, last, count, ttl_count = args[4], args[5], args[8], args[10]
            return [
                count * (last - first),
                ttl_count * (last - first) - self.ttls_short,
            ]
        return [self.bad, "s:000000000000 hashtable 10" if self.bad else ""]


class TestValidate:
    def test_valid(self):
        preload.validate([_keyspace()], "p")

    @pytest.mark.parametrize(
        "change, message",
        [
            ({"type": "hash"}, "type"),
            ({"key_prefix": ""}, "key_prefix"),
            ({"sets": 0}, "sets"),
            ({"encoding": None}, "encoding"),
            ({"members": []}, "members"),
            ({"extra": 1}, "extra"),
            ({"members": [{"prefix": "m"}]}, "count"),
            ({"members": [{"prefix": "m", "count": 1, "start": -1}]}, "start"),
            (
                {"members": [{"prefix": "m", "count": 1, "ttl": {"count": 1}}]},
                "seconds",
            ),
            (
                {
                    "members": [
                        {
                            "prefix": "m",
                            "count": 1,
                            "ttl": {"seconds": 1, "fraction": 2},
                        }
                    ]
                },
                "fraction",
            ),
        ],
    )
    def test_invalid(self, change, message):
        with pytest.raises(ValueError, match=message):
            preload.validate([_keyspace(**change)], "p")

    def test_duplicate_prefix(self):
        with pytest.raises(ValueError, match="used twice"):
            preload.validate([_keyspace(), _keyspace()], "p")


class TestTtl:
    @pytest.mark.parametrize(
        "ttl, expected",
        [
            (None, 0),
            ({"seconds": 1}, 10),
            ({"seconds": 1, "count": 1}, 1),
            ({"seconds": 1, "count": 50}, 10),
            ({"seconds": 1, "fraction": 0.5}, 5),
            ({"seconds": 1, "fraction": 0.01}, 1),
        ],
    )
    def test_ttl_member_count(self, ttl, expected):
        assert preload.ttl_member_count({"count": 10, "ttl": ttl}) == expected

    def test_required_commands(self):
        assert preload.required_commands([_keyspace()]) == {"SEXPIRE"}
        plain = _keyspace(members=[{"prefix": "m", "count": 1}])
        assert preload.required_commands([plain]) == set()
        assert preload.required_commands(None) == set()


class TestLoad:
    def test_loads_in_batches_then_checks_every_key(self, monkeypatch):
        monkeypatch.setattr(preload, "MEMBERS_PER_EVAL", 20)
        client = FakeClient()

        written = preload.load(client, [_keyspace()])

        loads = [c for c in client.calls if c[1] == preload.LOAD_SCRIPT]
        assert [(c[4], c[5]) for c in loads] == [(0, 2), (2, 3)]
        assert loads[0][3:] == ("s:", 0, 2, "m", 0, 10, 5, 2, 1000)
        checks = [c for c in client.calls if c[1] == preload.CHECK_SCRIPT]
        assert checks and checks[0][3:] == ("s:", 0, 2, "listpack", 10)
        assert written == {"members": 30, "member_ttls": 6}

    def test_short_ttl_count_raises(self):
        with pytest.raises(RuntimeError, match="SEXPIRE set"):
            preload.load(FakeClient(ttls_short=1), [_keyspace()])

    def test_wrong_encoding_raises(self):
        with pytest.raises(RuntimeError, match="not listpack with 10 members"):
            preload.load(FakeClient(bad=1), [_keyspace()])

    def test_verify_without_size(self):
        client = FakeClient()
        preload.verify(client, [_keyspace()], check_size=False)
        assert client.calls[0][-1] == -1

    def test_key_name_matches_valkey_benchmark_placeholder_width(self):
        assert preload.key_name("s:", 7) == "s:000000000007"
