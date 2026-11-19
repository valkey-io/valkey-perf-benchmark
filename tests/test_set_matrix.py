"""Tests for compiling a set_matrix block into test_groups."""

import copy
import json
from pathlib import Path

import pytest

from benchmark import validate_config
from scripts.resolve_set_matrix import resolve
from set_matrix import build_preload, compile_set_matrix, deep_merge

ROOT = Path(__file__).resolve().parent.parent


def _matrix(**changes):
    matrix = {
        "duration": 8,
        "window": [2, 6],
        "clients": 10,
        "pipelines": [1],
        "ttl_seconds": 1000,
        "shapes": {
            "lp": {
                "members": 16,
                "member_size": 16,
                "encoding": "listpack",
                "encoding_with_ttl": "listpack",
                "toggles": 4,
                "population": 100,
            },
            "int": {
                "members": 512,
                "integer": True,
                "encoding": "intset",
                "encoding_with_ttl": "hashtable",
                "toggles": 16,
                "population": 50,
                "grow_headroom": 64,
            },
        },
        "ttl_mixes": {"none": {}, "half": {"fraction": 0.5}, "one": {"count": 1}},
        "commands": {
            "sismember": {"command": "SISMEMBER {key} {member}"},
            "srem_sadd": {
                "writes": ["SREM {key} {toggle}", "SADD {key} {toggle}"],
                "layout": "toggle",
            },
            "sadd_new": {
                "command": "SADD {rand_key} {new_member}",
                "layout": "population",
                "grows": True,
            },
            "sexpire": {
                "command": "SEXPIRE {key} {ttl} MEMBERS 1 {toggle}",
                "layout": "toggle",
                "toggle_ttl": True,
                "requires": ["SEXPIRE"],
            },
        },
    }
    matrix.update(changes)
    return matrix


def _scenarios(groups):
    return {s["id"]: s for g in groups for s in g["scenarios"]}


class TestCompile:
    def test_one_group_per_shape_and_mix_one_scenario_per_command_and_pipeline(
        self,
    ):
        groups = compile_set_matrix(_matrix(), now=0)

        assert [g["group"] for g in groups] == list(range(1, 7))
        assert groups[0]["description"] == "lp, TTL mix none"
        assert len(_scenarios(groups)) == 2 * 3 * 4

    def test_member_command_targets_one_set_with_members_as_range(self):
        s = _scenarios(compile_set_matrix(_matrix(), now=0))["lp.none.sismember.P1"]

        assert s["command"] == "SISMEMBER set:a:000000000000 mxxx__rand_int__"
        assert s["keyspacelen"] == 16
        assert s["pipeline"] == 1
        assert s["clients"] == 10
        assert s["duration"] == 8
        assert s["window"] == [2, 6]
        assert s["restart_before"] is True
        assert s["preload"] == [
            {
                "type": "set",
                "key_prefix": "set:a:",
                "sets": 1,
                "encoding": "listpack",
                "members": [{"prefix": "mxxx", "count": 16}],
            }
        ]

    def test_integer_shape_uses_digit_prefixes_and_ttl_encoding(self):
        s = _scenarios(compile_set_matrix(_matrix(), now=0))["int.half.sismember.P1"]

        assert s["command"] == "SISMEMBER set:a:000000000000 1__rand_int__"
        assert s["preload"][0]["encoding"] == "hashtable"
        assert s["preload"][0]["members"][0]["ttl"] == {
            "seconds": 1000,
            "fraction": 0.5,
        }

    def test_toggle_pair_is_mixed_and_never_exceeds_members(self):
        s = _scenarios(compile_set_matrix(_matrix(), now=0))["int.none.srem_sadd.P1"]

        assert s["type"] == "mixed"
        assert s["keyspacelen"] == 16
        assert [w["command"] for w in s["writes"]] == [
            "SREM set:a:000000000000 2__rand_int__",
            "SADD set:a:000000000000 2__rand_int__",
        ]
        assert [w["clients"] for w in s["writes"]] == [5, 5]
        groups = s["preload"][0]["members"]
        assert [g["count"] for g in groups] == [496, 16]
        assert s["preload"][0]["encoding"] == "intset"

    def test_population_grows_into_headroom(self):
        s = _scenarios(compile_set_matrix(_matrix(), now=0))["int.none.sadd_new.P1"]

        assert s["command"] == "SADD set:a:__rand_int__ 4__rand_int__"
        assert s["keyspacelen"] == 50
        assert s["preload"][0]["sets"] == 50
        assert s["preload"][0]["members"][0]["count"] == 448

    def test_toggle_ttl_makes_the_set_volatile_and_keeps_requires(self):
        s = _scenarios(compile_set_matrix(_matrix(), now=0))["int.none.sexpire.P1"]

        assert s["command"] == "SEXPIRE set:a:000000000000 1000 MEMBERS 1 2__rand_int__"
        assert s["requires_commands"] == ["SEXPIRE"]
        assert s["preload"][0]["encoding"] == "hashtable"
        assert s["preload"][0]["members"][1]["ttl"] == {"seconds": 1000}

    def test_ttl_adding_command_skips_a_ttl_free_set_that_would_convert(self):
        matrix = _matrix()
        matrix["commands"]["sadd_new"]["adds_ttl"] = True
        ids = _scenarios(compile_set_matrix(matrix))
        assert "int.none.sadd_new.P1" not in ids
        assert "int.half.sadd_new.P1" in ids
        assert "lp.none.sadd_new.P1" in ids

    def test_time_placeholders(self):
        matrix = _matrix(
            commands={
                "at": {"command": "SPEXPIREAT {key} {ttl_at_ms} MEMBERS 1 {member}"}
            }
        )
        s = _scenarios(compile_set_matrix(matrix, now=10))["lp.none.at.P1"]
        assert s["command"].split()[2] == "1010000"

    def test_placeholder_outside_layout_is_rejected(self):
        matrix = _matrix(commands={"bad": {"command": "SISMEMBER {rand_key} {member}"}})
        with pytest.raises(ValueError, match="rand_key"):
            compile_set_matrix(matrix)

    def test_run_lists_select_and_reject_unknown(self):
        matrix = _matrix(
            run_shapes=["lp"], run_ttl_mixes=["one"], run_commands=["sismember"]
        )
        assert list(_scenarios(compile_set_matrix(matrix))) == [
            "lp.one.sismember.P1",
        ]
        with pytest.raises(ValueError, match="unknown commands"):
            compile_set_matrix(_matrix(run_commands=["nope"]))

    @pytest.mark.parametrize(
        "shape_change, message",
        [
            ({"member_size": 12}, "member_size"),
            ({"toggles": 16}, "toggles"),
            ({"population": 0}, "population"),
            ({"colour": "red"}, "colour"),
        ],
    )
    def test_bad_shape(self, shape_change, message):
        matrix = _matrix()
        matrix["shapes"]["lp"].update(shape_change)
        with pytest.raises(ValueError, match=message):
            compile_set_matrix(matrix)

    def test_command_needs_one_of_command_or_writes(self):
        with pytest.raises(ValueError, match="exactly one"):
            compile_set_matrix(_matrix(commands={"x": {"layout": "one"}}))


class TestBuildPreload:
    shape = _matrix()["shapes"]["lp"]

    def test_overlap_starts_b_halfway(self):
        spec = build_preload("overlap", self.shape, {}, {}, "s:", 10)
        assert [k["key_prefix"] for k in spec] == ["s:a:", "s:b:"]
        assert spec[1]["members"] == [{"prefix": "mxxx", "count": 16, "start": 8}]

    def test_two_toggle_keeps_fixed_members_in_both_sets(self):
        spec = build_preload("two_toggle", self.shape, {}, {}, "s:", 10)
        assert [[g["prefix"], g["count"]] for g in spec[0]["members"]] == [
            ["mxxx", 12],
            ["txxx", 4],
        ]
        assert spec[1]["members"] == [{"prefix": "oxxx", "count": 12}]


class TestShippedConfigs:
    @pytest.mark.parametrize(
        "name, count",
        [("set-commands.json", 683), ("set-commands-smoke.json", None)],
    )
    def test_compiles_and_validates(self, name, count):
        configs = json.loads((ROOT / "configs" / name).read_text())
        for cfg in configs:
            validate_config(cfg)
            scenarios = [s for g in cfg["test_groups"] for s in g["scenarios"]]
            assert len({s["id"] for s in scenarios}) == len(scenarios)
            if count is not None:
                assert len(scenarios) == count


class TestOverrides:
    def test_deep_merge_merges_objects_replaces_lists_and_null_deletes(self):
        base = {"a": {"x": 1, "y": 2}, "b": [1, 2], "c": 3}
        merged = deep_merge(base, {"a": {"y": 5}, "b": [9], "c": None})
        assert merged == {"a": {"x": 1, "y": 5}, "b": [9]}
        assert base == {"a": {"x": 1, "y": 2}, "b": [1, 2], "c": 3}

    def test_resolve_applies_overrides_and_command_selection(self):
        cfg = {"set_matrix": _matrix(), "per_second_sampling": True}
        resolved = resolve(
            [copy.deepcopy(cfg)],
            "sismember",
            {"set_matrix": {"shapes": {"lp": {"members": 32}}, "pipelines": [1, 16]}},
        )[0]
        assert resolved["set_matrix"]["run_commands"] == ["sismember"]
        assert resolved["set_matrix"]["shapes"]["lp"]["members"] == 32
        validate_config(resolved)
        ids = _scenarios(resolved["test_groups"])
        assert len(ids) == 2 * 3 * 2  # the override adds pipeline 16
        assert "lp.one.sismember.P16" in ids

    def test_set_matrix_cannot_mix_with_test_groups(self):
        cfg = {"set_matrix": _matrix(), "test_groups": [], "per_second_sampling": True}
        with pytest.raises(ValueError, match="cannot be combined"):
            validate_config(cfg)


def _expired_matrix(**changes):
    matrix = _matrix(
        ttl_mixes={
            "none": {},
            "exp_half": {"expired": True, "fraction": 0.5},
            "exp_all_but_1": {"expired": True, "live": 1, "skip_commands": ["spop"]},
            "exp_one": {"expired": True, "count": 1},
        },
        commands={
            "sismember": {"command": "SISMEMBER {key} {member}", "on_expired": True},
            "sismember_expired": {
                "command": "SISMEMBER {key} {expired_member}",
                "layout": "expired",
                "on_expired": True,
                "expired_only": True,
            },
            "sinter": {
                "command": "SINTER {key} {key2}",
                "layout": "overlap",
                "on_expired": True,
            },
            "spop": {
                "command": "SPOP {rand_key} 1",
                "layout": "population",
                "on_expired": True,
            },
            "srem_sadd": {
                "writes": ["SREM {key} {toggle}", "SADD {key} {toggle}"],
                "layout": "toggle",
            },
        },
    )
    matrix.update(changes)
    return matrix


class TestExpiredMixes:
    def test_live_and_expired_groups_interleaved_with_ranges(self):
        s = _scenarios(compile_set_matrix(_expired_matrix(), now=0))

        live = s["lp.exp_half.sismember.P1"]
        assert live["command"] == "SISMEMBER set:a:000000000000 mxxx__rand_int__"
        assert live["keyspacelen"] == 8
        assert live["preload"] == [
            {
                "type": "set",
                "key_prefix": "set:a:",
                "sets": 1,
                "encoding": "listpack",
                "interleave": True,
                "members": [
                    {"prefix": "mxxx", "count": 8},
                    {
                        "prefix": "exxx",
                        "count": 8,
                        "ttl": {"seconds": 1000, "expired": True},
                    },
                ],
            }
        ]
        gone = s["int.exp_all_but_1.sismember_expired.P1"]
        assert gone["command"] == "SISMEMBER set:a:000000000000 5__rand_int__"
        assert gone["keyspacelen"] == 511
        assert gone["preload"][0]["encoding"] == "hashtable"
        assert s["int.exp_all_but_1.sismember.P1"]["keyspacelen"] == 1
        assert s["lp.exp_one.sismember_expired.P1"]["keyspacelen"] == 1

    def test_overlap_and_population_shapes(self):
        s = _scenarios(compile_set_matrix(_expired_matrix(), now=0))
        a, b = s["lp.exp_half.sinter.P1"]["preload"]
        assert [g.get("start", 0) for g in b["members"]] == [4, 4]
        spop = s["lp.exp_half.spop.P1"]
        assert spop["preload"][0]["sets"] == 100
        assert spop["keyspacelen"] == 100

    def test_command_selection(self):
        ids = _scenarios(compile_set_matrix(_expired_matrix(), now=0))
        assert "lp.none.sismember_expired.P1" not in ids  # expired_only
        assert "lp.exp_half.srem_sadd.P1" not in ids  # not on_expired
        assert "lp.exp_all_but_1.spop.P1" not in ids  # skip_commands
        assert "lp.exp_one.spop.P1" in ids
        assert "lp.none.srem_sadd.P1" in ids

    def test_only_mixes_restricts_a_shape(self):
        matrix = _expired_matrix()
        matrix["shapes"]["int"]["only_mixes"] = ["exp_half"]
        ids = _scenarios(compile_set_matrix(matrix, now=0))
        assert {i.split(".")[1] for i in ids if i.startswith("int.")} == {"exp_half"}

    @pytest.mark.parametrize(
        "mix, members, expected",
        [
            ({"live": 1}, 16, 15),
            ({"count": 1}, 16, 1),
            ({"fraction": 0.1}, 16, 2),
            ({"fraction": 1.0}, 16, 15),
            ({"live": 20}, 16, 1),
        ],
    )
    def test_expired_count_keeps_one_live_and_one_expired(self, mix, members, expected):
        from set_matrix import expired_count

        assert expired_count(mix, members) == expected

    @pytest.mark.parametrize(
        "mixes, commands, message",
        [
            ({"x": {"expired": True}}, None, "needs 'count'"),
            ({"x": {"live": 1}}, None, "'live' is for expired mixes"),
            ({"x": {"expired": True, "count": 1, "fraction": 0.5}}, None, "one of"),
            ({"x": {"colour": 1}}, None, "colour"),
            (
                None,
                {
                    "c": {
                        "command": "SCARD {key}",
                        "layout": "expired",
                        "on_expired": True,
                    }
                },
                "expired_only",
            ),
            (
                None,
                {"c": {"command": "SCARD {key}", "expired_only": True}},
                "on_expired",
            ),
            (
                {"x": {"expired": True, "count": 1}},
                {
                    "c": {
                        "writes": ["SREM {key} {toggle}"],
                        "layout": "toggle",
                        "on_expired": True,
                    }
                },
                "not available in an expired mix",
            ),
        ],
    )
    def test_invalid(self, mixes, commands, message):
        changes = {}
        if mixes:
            changes["ttl_mixes"] = mixes
        if commands:
            changes["commands"] = commands
        with pytest.raises(ValueError, match=message):
            compile_set_matrix(_expired_matrix(**changes), now=0)
