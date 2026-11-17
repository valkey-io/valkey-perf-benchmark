"""Compile a ``set_matrix`` config block into ordinary ``test_groups``.

The block names set shapes (size, encoding, member size, number of sets), TTL
mixes and command templates. Every shape x TTL mix becomes one test group, and
every command x pipeline in it one scenario with its own ``preload``, so the
runner, the filters (``--groups``, ``--scenarios``) and the result rows work
exactly as for a hand-written config.

Placeholders in a command template:

- ``{key}``, ``{key2}``: the first set of keyspace A and B; ``{dest}``: a
  destination key for the *STORE commands.
- ``{rand_key}``: a random set of the population (layout ``population``).
- ``{member}``: a random existing member; ``{toggle}``: a random toggled
  member; ``{new_member}``: a random member the sets do not hold.
- ``{ttl}``, ``{ttl_ms}``: ``ttl_seconds`` in seconds and milliseconds;
  ``{ttl_at}``, ``{ttl_at_ms}``: that far in the future as a Unix time.

valkey-benchmark draws every ``__rand_int__`` from one ``-r`` range, so each
layout fixes which placeholders a template may use and what ``-r`` is.
"""

import copy
import time
from typing import Any, Dict, List

# Layout -> (placeholders the template may use, what -r counts).
LAYOUTS = {
    # One set; reads and writes address its members.
    "one": ({"key", "member"}, "members"),
    # Two sets of equal size; B's members start halfway through A's, so the
    # sets overlap by half.
    "overlap": ({"key", "key2", "dest", "member"}, "members"),
    # One set of members - toggles fixed members plus `toggles` members that
    # the command removes and re-adds (or changes), so the set never holds
    # more than `members`.
    "toggle": ({"key", "toggle"}, "toggles"),
    # Sets A and B of members - toggles fixed members each; the `toggles`
    # toggled members start in A and move between the two.
    "two_toggle": ({"key", "key2", "toggle"}, "toggles"),
    # `population` sets, addressed at random. -r is the population, so a
    # random member is drawn from that range and only {new_member} fits.
    "population": ({"rand_key", "new_member"}, "population"),
}

TIME_PLACEHOLDERS = {"ttl", "ttl_ms", "ttl_at", "ttl_at_ms"}

# Member prefixes per role. String members are the letter padded with "x" to
# member_size; integer members use a digit, so the set can be an intset.
ROLE_LETTERS = {"member": "m", "toggle": "t", "other": "o", "new_member": "n"}
ROLE_DIGITS = {"member": "1", "toggle": "2", "other": "3", "new_member": "4"}

INDEX_DIGITS = 12

SHAPE_KEYS = {
    "members",
    "member_size",
    "integer",
    "encoding",
    "encoding_with_ttl",
    "toggles",
    "population",
    "grow_headroom",
}
COMMAND_KEYS = {
    "command",
    "writes",
    "layout",
    "requires",
    "toggle_ttl",
    "grows",
    "adds_ttl",
}
MATRIX_KEYS = {
    "duration",
    "window",
    "clients",
    "pipelines",
    "ttl_seconds",
    "key_prefix",
    "shapes",
    "ttl_mixes",
    "commands",
    "run_shapes",
    "run_ttl_mixes",
    "run_commands",
}


def deep_merge(base: Any, override: Any) -> Any:
    """Return base with override merged in. Dicts merge, null deletes a key."""
    if not isinstance(base, dict) or not isinstance(override, dict):
        return copy.deepcopy(override)
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if value is None:
            merged.pop(key, None)
        elif key in merged:
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _member_prefix(shape: Dict[str, Any], role: str) -> str:
    if shape.get("integer"):
        return ROLE_DIGITS[role]
    return ROLE_LETTERS[role].ljust(shape["member_size"] - INDEX_DIGITS, "x")


def _check_shape(name: str, shape: Dict[str, Any]) -> None:
    where = f"set_matrix.shapes.{name}"
    unknown = sorted(set(shape) - SHAPE_KEYS)
    if unknown:
        raise ValueError(f"{where} does not support key(s): {unknown}")
    for key in ("members", "population"):
        value = shape.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{where}.{key} must be a positive integer")
    for key in ("encoding", "encoding_with_ttl"):
        if not isinstance(shape.get(key), str):
            raise ValueError(f"{where}.{key} must name an encoding")
    if shape.get("integer"):
        if shape.get("member_size", INDEX_DIGITS + 1) != INDEX_DIGITS + 1:
            raise ValueError(
                f"{where}: integer members are always {INDEX_DIGITS + 1} digits"
            )
    elif (
        not isinstance(shape.get("member_size"), int)
        or shape["member_size"] <= INDEX_DIGITS
    ):
        raise ValueError(
            f"{where}.member_size must be an integer above {INDEX_DIGITS} "
            "(a prefix plus the 12-digit index valkey-benchmark writes)"
        )
    toggles = shape.get("toggles", 0)
    if not isinstance(toggles, int) or not 0 <= toggles < shape["members"]:
        raise ValueError(f"{where}.toggles must be below members")
    headroom = shape.get("grow_headroom", 0)
    if not isinstance(headroom, int) or not 0 <= headroom < shape["members"]:
        raise ValueError(f"{where}.grow_headroom must be below members")


def _ttl(mix: Dict[str, Any], seconds: int) -> Dict[str, Any]:
    """Return a preload member-group ttl for a TTL mix, or {} for none."""
    if mix.get("count"):
        return {"seconds": seconds, "count": mix["count"]}
    if mix.get("fraction"):
        return {"seconds": seconds, "fraction": mix["fraction"]}
    return {}


def _group(prefix: str, count: int, start: int = 0, ttl=None) -> Dict[str, Any]:
    group = {"prefix": prefix, "count": count}
    if start:
        group["start"] = start
    if ttl:
        group["ttl"] = ttl
    return group


def _keyspace(prefix: str, sets: int, groups: List[dict], shape: dict) -> dict:
    volatile = any("ttl" in group for group in groups)
    return {
        "type": "set",
        "key_prefix": prefix,
        "sets": sets,
        "encoding": shape["encoding_with_ttl"] if volatile else shape["encoding"],
        "members": groups,
    }


def build_preload(
    layout: str,
    shape: Dict[str, Any],
    ttl: Dict[str, Any],
    command: Dict[str, Any],
    key_prefix: str,
    ttl_seconds: int,
) -> List[Dict[str, Any]]:
    """Return the preload keyspaces for one layout, shape and TTL mix."""
    members, toggles = shape["members"], shape.get("toggles", 0)
    member = _member_prefix(shape, "member")
    a, b = f"{key_prefix}a:", f"{key_prefix}b:"
    toggle_ttl = {"seconds": ttl_seconds} if command.get("toggle_ttl") else None
    if layout == "one":
        return [_keyspace(a, 1, [_group(member, members, ttl=ttl)], shape)]
    if layout == "overlap":
        return [
            _keyspace(a, 1, [_group(member, members, ttl=ttl)], shape),
            _keyspace(
                b, 1, [_group(member, members, start=members // 2, ttl=ttl)], shape
            ),
        ]
    fixed = members - toggles
    toggled = _group(_member_prefix(shape, "toggle"), toggles, ttl=toggle_ttl)
    if layout == "toggle":
        return [_keyspace(a, 1, [_group(member, fixed, ttl=ttl), toggled], shape)]
    if layout == "two_toggle":
        other = _member_prefix(shape, "other")
        return [
            _keyspace(a, 1, [_group(member, fixed, ttl=ttl), toggled], shape),
            _keyspace(b, 1, [_group(other, fixed, ttl=ttl)], shape),
        ]
    count = members - (shape.get("grow_headroom", 0) if command.get("grows") else 0)
    return [_keyspace(a, shape["population"], [_group(member, count, ttl=ttl)], shape)]


def _placeholders(shape: dict, key_prefix: str, ttl_seconds: int, now: int) -> dict:
    first = "0" * INDEX_DIGITS
    rand = "__rand_int__"
    return {
        "key": f"{key_prefix}a:{first}",
        "key2": f"{key_prefix}b:{first}",
        "dest": f"{key_prefix}dest",
        "rand_key": f"{key_prefix}a:{rand}",
        "member": _member_prefix(shape, "member") + rand,
        "toggle": _member_prefix(shape, "toggle") + rand,
        "new_member": _member_prefix(shape, "new_member") + rand,
        "ttl": str(ttl_seconds),
        "ttl_ms": str(ttl_seconds * 1000),
        "ttl_at": str(now + ttl_seconds),
        "ttl_at_ms": str((now + ttl_seconds) * 1000),
    }


def _render(template: str, values: dict, allowed: set, where: str) -> str:
    """Fill a command template, rejecting placeholders its layout does not set."""
    usable = {k: v for k, v in values.items() if k in allowed | TIME_PLACEHOLDERS}
    try:
        return template.format_map(usable)
    except KeyError as e:
        raise ValueError(
            f"{where}: placeholder {{{e.args[0]}}} is not available in this "
            f"layout; it allows {sorted(allowed | TIME_PLACEHOLDERS)}"
        ) from None


def _selected(matrix: dict, section: str, run_key: str) -> List[str]:
    names = list(matrix[section])
    wanted = matrix.get(run_key)
    if wanted is None:
        return names
    missing = [n for n in wanted if n not in matrix[section]]
    if missing:
        raise ValueError(f"set_matrix.{run_key} names unknown {section}: {missing}")
    return [n for n in names if n in wanted]


def compile_set_matrix(matrix: Dict[str, Any], now: int = None) -> List[dict]:
    """Return the test_groups a set_matrix block describes."""
    if not isinstance(matrix, dict):
        raise ValueError("'set_matrix' must be an object")
    unknown = sorted(set(matrix) - MATRIX_KEYS)
    if unknown:
        raise ValueError(f"set_matrix does not support key(s): {unknown}")
    for section in ("shapes", "ttl_mixes", "commands"):
        if not isinstance(matrix.get(section), dict) or not matrix[section]:
            raise ValueError(f"set_matrix.{section} must be a non-empty object")
    now = int(time.time()) if now is None else now
    duration = matrix.get("duration", 16)
    clients = matrix.get("clients", 50)
    pipelines = matrix.get("pipelines", [1])
    ttl_seconds = matrix.get("ttl_seconds", 100000000)
    key_prefix = matrix.get("key_prefix", "set:")

    for name, cmd in matrix["commands"].items():
        unknown = sorted(set(cmd) - COMMAND_KEYS)
        if unknown:
            raise ValueError(f"set_matrix.commands.{name} does not support {unknown}")
        if ("command" in cmd) == ("writes" in cmd):
            raise ValueError(
                f"set_matrix.commands.{name} needs exactly one of 'command' "
                "or 'writes'"
            )
        if cmd.get("layout", "one") not in LAYOUTS:
            raise ValueError(
                f"set_matrix.commands.{name}.layout must be one of {list(LAYOUTS)}"
            )

    groups = []
    for shape_name in _selected(matrix, "shapes", "run_shapes"):
        shape = matrix["shapes"][shape_name]
        _check_shape(shape_name, shape)
        values = _placeholders(shape, key_prefix, ttl_seconds, now)
        for mix_name in _selected(matrix, "ttl_mixes", "run_ttl_mixes"):
            ttl = _ttl(matrix["ttl_mixes"][mix_name], ttl_seconds)
            scenarios = []
            for cmd_name in _selected(matrix, "commands", "run_commands"):
                cmd = matrix["commands"][cmd_name]
                if (
                    cmd.get("adds_ttl")
                    and not ttl
                    and shape["encoding"] != shape["encoding_with_ttl"]
                ):
                    # The first member TTL converts such a set (an intset
                    # becomes a hashtable), so the run would measure the
                    # conversion; the TTL mixes cover this command instead.
                    continue
                layout = cmd.get("layout", "one")
                allowed, range_of = LAYOUTS[layout]
                where = f"set_matrix.commands.{cmd_name}"
                if range_of == "toggles" and not shape.get("toggles"):
                    raise ValueError(
                        f"{where} toggles members, so shape {shape_name} "
                        "needs 'toggles'"
                    )
                keyspacelen = shape[range_of]
                preload_spec = build_preload(
                    layout, shape, ttl, cmd, key_prefix, ttl_seconds
                )
                for pipeline in pipelines:
                    scenario = {
                        "id": f"{shape_name}.{mix_name}.{cmd_name}.P{pipeline}",
                        "description": (
                            f"{cmd_name} on {shape_name}, TTL mix {mix_name}, "
                            f"pipeline {pipeline}"
                        ),
                        "restart_before": True,
                        "preload": copy.deepcopy(preload_spec),
                        "keyspacelen": keyspacelen,
                        "pipeline": pipeline,
                        "duration": duration,
                    }
                    if "window" in matrix:
                        scenario["window"] = list(matrix["window"])
                    if cmd.get("requires"):
                        scenario["requires_commands"] = list(cmd["requires"])
                    if "command" in cmd:
                        scenario["command"] = _render(
                            cmd["command"], values, allowed, where
                        )
                        scenario["clients"] = clients
                    else:
                        scenario["type"] = "mixed"
                        scenario["writes"] = [
                            {
                                "id": f"{i + 1}-{template.split()[0].lower()}",
                                "command": _render(template, values, allowed, where),
                                "clients": max(1, clients // len(cmd["writes"])),
                            }
                            for i, template in enumerate(cmd["writes"])
                        ]
                    scenarios.append(scenario)
            if scenarios:
                groups.append(
                    {
                        "group": len(groups) + 1,
                        "description": f"{shape_name}, TTL mix {mix_name}",
                        "scenarios": scenarios,
                    }
                )
    if not groups:
        raise ValueError("set_matrix selects no scenarios")
    return groups
