"""Unit tests for valkey_benchmark.py — deep_merge function."""

import copy


from valkey_benchmark import deep_merge

# ---------------------------------------------------------------------------
# Flat dict merge with override precedence
# ---------------------------------------------------------------------------


class TestDeepMergeFlatDicts:
    def test_disjoint_keys_merged(self):
        base = {"a": 1, "b": 2}
        override = {"c": 3, "d": 4}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 2, "c": 3, "d": 4}

    def test_override_takes_precedence(self):
        base = {"a": 1, "b": 2}
        override = {"b": 99}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 99}

    def test_empty_base(self):
        result = deep_merge({}, {"x": 1})
        assert result == {"x": 1}

    def test_empty_override(self):
        result = deep_merge({"x": 1}, {})
        assert result == {"x": 1}

    def test_both_empty(self):
        result = deep_merge({}, {})
        assert result == {}


# ---------------------------------------------------------------------------
# Nested dict recursive merge
# ---------------------------------------------------------------------------


class TestDeepMergeNestedDicts:
    def test_nested_dicts_merged_recursively(self):
        base = {"a": {"x": 1, "y": 2}}
        override = {"a": {"y": 99, "z": 3}}
        result = deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 99, "z": 3}}

    def test_deeply_nested_merge(self):
        base = {"a": {"b": {"c": 1, "d": 2}}}
        override = {"a": {"b": {"d": 99}}}
        result = deep_merge(base, override)
        assert result == {"a": {"b": {"c": 1, "d": 99}}}

    def test_nested_with_new_key(self):
        base = {"a": {"x": 1}}
        override = {"a": {"y": 2}, "b": {"z": 3}}
        result = deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 2}, "b": {"z": 3}}


# ---------------------------------------------------------------------------
# Non-dict override values replacing dicts
# ---------------------------------------------------------------------------


class TestDeepMergeNonDictOverride:
    def test_string_replaces_dict(self):
        base = {"a": {"x": 1}}
        override = {"a": "replaced"}
        result = deep_merge(base, override)
        assert result == {"a": "replaced"}

    def test_list_replaces_dict(self):
        base = {"a": {"x": 1}}
        override = {"a": [1, 2, 3]}
        result = deep_merge(base, override)
        assert result == {"a": [1, 2, 3]}

    def test_none_replaces_dict(self):
        base = {"a": {"x": 1}}
        override = {"a": None}
        result = deep_merge(base, override)
        assert result == {"a": None}

    def test_int_replaces_dict(self):
        base = {"a": {"nested": True}}
        override = {"a": 42}
        result = deep_merge(base, override)
        assert result == {"a": 42}


# ---------------------------------------------------------------------------
# Originals not modified
# ---------------------------------------------------------------------------


class TestDeepMergeImmutability:
    def test_base_not_modified(self):
        base = {"a": 1, "b": {"c": 2}}
        base_copy = copy.deepcopy(base)
        deep_merge(base, {"b": {"c": 99}})
        assert base == base_copy

    def test_override_not_modified(self):
        override = {"a": {"x": 1}}
        override_copy = copy.deepcopy(override)
        deep_merge({"a": {"y": 2}}, override)
        assert override == override_copy
