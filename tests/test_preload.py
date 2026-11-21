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


def _expired_keyspace(sets=2):
    return {
        "type": "set",
        "key_prefix": "s:",
        "sets": sets,
        "encoding": "hashtable",
        "interleave": True,
        "members": [
            {"prefix": "m", "count": 6},
            {"prefix": "e", "count": 4, "ttl": {"seconds": 99, "expired": True}},
        ],
    }


class ExpiredClient:
    """Plays a server for the interleaved load, the expire step and the checks."""

    def __init__(self, visible=0, reclaimed=0, debug_error=False):
        self.calls, self.visible, self.reclaimed = [], visible, reclaimed
        self.debug_error, self.stat = debug_error, 7

    def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "DEBUG":
            if self.debug_error:
                raise RuntimeError("DEBUG command not allowed")
            return "OK"
        if args[0] == "INFO":
            value, self.stat = self.stat, self.stat + self.reclaimed
            return f"# Stats\r\nexpired_set_members:{value}\r\n"
        script = args[1]
        if script == preload.INTERLEAVED_LOAD_SCRIPT:
            first, last = args[4], args[5]
            return [10 * (last - first), 4 * (last - first)]
        if script == preload.EXPIRE_SCRIPT:
            first, last, ttl_count = args[4], args[5], args[10]
            return ttl_count * (last - first)
        if script == preload.EXPIRED_CHECK_SCRIPT:
            return self.visible
        return [0, ""]  # CHECK_SCRIPT: every key is fine

    def names(self):
        out = []
        for c in self.calls:
            if c[0] == "EVAL":
                out.append(
                    {
                        preload.INTERLEAVED_LOAD_SCRIPT: "load",
                        preload.EXPIRE_SCRIPT: "expire",
                        preload.EXPIRED_CHECK_SCRIPT: "expired-check",
                        preload.CHECK_SCRIPT: "check",
                    }[c[1]]
                )
            else:
                out.append(" ".join(str(x) for x in c))
        return out


class TestExpired:
    def test_load_expires_after_turning_active_expiry_off(self, monkeypatch):
        monkeypatch.setattr(preload.time, "sleep", lambda s: None)
        client = ExpiredClient()

        written = preload.load(client, [_expired_keyspace()])

        assert client.names() == [
            "load",
            "check",
            "DEBUG SET-ACTIVE-EXPIRE 0",
            "expire",
            "check",
            "expired-check",
            "INFO stats",
        ]
        load = client.calls[0]
        assert load[3:8] == ("s:", 0, 2, 1000, 2)
        assert load[8:] == ("m", 0, 6, 0, 0, "e", 0, 4, 99, 4)
        assert client.calls[3][3:] == ("s:", 0, 2, 1000, "e", 0, 4, 4, 1)
        assert written == {
            "members": 20,
            "member_ttls": 8,
            "expired_members": 8,
            "reclaimed_baseline": 7,
        }

    def test_members_still_visible_fail_and_turn_active_expiry_back_on(
        self, monkeypatch
    ):
        monkeypatch.setattr(preload.time, "sleep", lambda s: None)
        client = ExpiredClient(visible=3)
        with pytest.raises(RuntimeError, match="3 members that should be expired"):
            preload.load(client, [_expired_keyspace()])
        assert client.names()[-1] == "DEBUG SET-ACTIVE-EXPIRE 1"

    def test_debug_refused_names_the_config(self):
        with pytest.raises(RuntimeError, match="enable-debug-command"):
            preload.set_active_expire(ExpiredClient(debug_error=True), False)

    def test_finish_passes_and_turns_active_expiry_on(self):
        client = ExpiredClient()
        assert preload.finish_expired(client, [_expired_keyspace()], 7) == 0
        assert client.names()[-1] == "DEBUG SET-ACTIVE-EXPIRE 1"

    def test_finish_fails_when_the_run_reclaimed_members(self):
        client = ExpiredClient()
        client.stat = 12
        with pytest.raises(RuntimeError, match="reclaimed 5 expired set members"):
            preload.finish_expired(client, [_expired_keyspace()], 7)
        assert client.names()[-1] == "DEBUG SET-ACTIVE-EXPIRE 1"

    def test_check_samples_keys_evenly(self, monkeypatch):
        monkeypatch.setattr(preload, "EXPIRED_CHECK_KEYS", 4)
        client = ExpiredClient()
        preload.verify_expired(client, [_expired_keyspace(sets=10)])
        assert client.calls[0][9:] == (0, 2, 4, 6, 8, 9)

    def test_required_commands_and_has_expired(self):
        assert preload.has_expired([_expired_keyspace()])
        assert preload.required_commands([_expired_keyspace()]) == {
            "SEXPIRE",
            "SPEXPIRE",
            "DEBUG",
        }
        assert not preload.has_expired([_keyspace()])

    @pytest.mark.parametrize(
        "change, message",
        [({"interleave": "yes"}, "interleave"), ("expired", "expired")],
    )
    def test_invalid_flags(self, change, message):
        ks = _expired_keyspace()
        if change == "expired":
            ks["members"][1]["ttl"]["expired"] = 1
        else:
            ks.update(change)
        with pytest.raises(ValueError, match=message):
            preload.validate([ks], "p")
