"""Server-side data preload for scenarios, with an encoding check.

A scenario's ``preload`` is a list of keyspaces. Each keyspace is ``sets``
keys named ``<key_prefix><12-digit index>``, the shape valkey-benchmark gives
``<key_prefix>__rand_int__``, so a measured command can address them with a
placeholder. Members are built the same way: ``<prefix><12-digit index>`` for
indices ``start .. start + count - 1``. A member group may give a TTL to some
of its members through SEXPIRE, picked evenly across the group.

The data is written by a Lua script on the server, in batches, so large
populations load without a client round trip per member. After the load every
key is checked for its member count and its OBJECT ENCODING. ``verify`` can be
run again after the measured run to show the run did not convert or empty a
key.
"""

import time
from typing import Any, Dict, List, Optional, Set

# Members are written into the same 12-digit slot valkey-benchmark fills for
# __rand_int__, so "m" + __rand_int__ in a command hits "m%012d" members.
INDEX_DIGITS = 12

PRELOAD_TYPES = ("set",)

# Upper bound on members written by one EVAL, so no single script call blocks
# the server for long.
MEMBERS_PER_EVAL = 200000

# SADD and SEXPIRE take at most this many members per call inside the script,
# which keeps Lua's unpack() within its stack limit.
MEMBERS_PER_CALL = 1000

KEYSPACE_KEYS = {"type", "key_prefix", "sets", "encoding", "members", "interleave"}
GROUP_KEYS = {"prefix", "count", "start", "ttl"}
TTL_KEYS = {"seconds", "count", "fraction", "expired"}

# An expired group is loaded with its long TTL, then given this TTL in
# milliseconds once active expiry is off, so its members are expired but stay
# stored for the measured run.
EXPIRE_MS = 1

# How many keys of a keyspace the expired-member check reads, spread evenly.
EXPIRED_CHECK_KEYS = 100

LOAD_SCRIPT = r"""
local key_prefix, first, last = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local prefix, start, count = ARGV[4], tonumber(ARGV[5]), tonumber(ARGV[6])
local ttl_seconds, ttl_count = tonumber(ARGV[7]), tonumber(ARGV[8])
local per_call = tonumber(ARGV[9])
local members = {}
for i = 0, count - 1 do
  members[#members + 1] = prefix .. string.format('%012d', start + i)
end
local volatile = {}
for i = 0, ttl_count - 1 do
  volatile[#volatile + 1] = members[math.floor(i * count / ttl_count) + 1]
end
local added, expired = 0, 0
for k = first, last - 1 do
  local key = key_prefix .. string.format('%012d', k)
  for s = 1, #members, per_call do
    added = added + redis.call('SADD', key, unpack(members, s, math.min(s + per_call - 1, #members)))
  end
  for s = 1, #volatile, per_call do
    local e = math.min(s + per_call - 1, #volatile)
    local reply = redis.call('SEXPIRE', key, ttl_seconds, 'MEMBERS', e - s + 1, unpack(volatile, s, e))
    for _, r in ipairs(reply) do
      if r == 1 then expired = expired + 1 end
    end
  end
end
return {added, expired}
"""

# Loads all groups of a key in one pass, interleaved in proportion to their
# sizes, so a listpack holds the groups spread through it instead of one group
# after another. ARGV: key_prefix, first, last, per_call, ngroups, then per
# group prefix, start, count, ttl_seconds, ttl_count.
INTERLEAVED_LOAD_SCRIPT = r"""
local key_prefix, first, last = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local per_call, ngroups = tonumber(ARGV[4]), tonumber(ARGV[5])
local order, volatile = {}, {}
for g = 0, ngroups - 1 do
  local b = 6 + g * 5
  local prefix, start, count = ARGV[b], tonumber(ARGV[b + 1]), tonumber(ARGV[b + 2])
  local ttl_seconds, ttl_count = tonumber(ARGV[b + 3]), tonumber(ARGV[b + 4])
  local members = {}
  for i = 0, count - 1 do
    local m = prefix .. string.format('%012d', start + i)
    members[#members + 1] = m
    order[#order + 1] = {(i + 0.5) / count, g, m}
  end
  local v = {}
  for i = 0, ttl_count - 1 do v[#v + 1] = members[math.floor(i * count / ttl_count) + 1] end
  volatile[#volatile + 1] = {ttl_seconds, v}
end
table.sort(order, function(x, y) if x[1] ~= y[1] then return x[1] < y[1] end return x[2] < y[2] end)
local names = {}
for i, e in ipairs(order) do names[i] = e[3] end
local added, expired = 0, 0
for k = first, last - 1 do
  local key = key_prefix .. string.format('%012d', k)
  for s = 1, #names, per_call do
    added = added + redis.call('SADD', key, unpack(names, s, math.min(s + per_call - 1, #names)))
  end
  for _, g in ipairs(volatile) do
    local v = g[2]
    for s = 1, #v, per_call do
      local e = math.min(s + per_call - 1, #v)
      local reply = redis.call('SEXPIRE', key, g[1], 'MEMBERS', e - s + 1, unpack(v, s, e))
      for _, r in ipairs(reply) do
        if r == 1 then expired = expired + 1 end
      end
    end
  end
end
return {added, expired}
"""

# Gives a group's TTL members a TTL of ARGV[9] ms, so they expire right after
# the script. ARGV: key_prefix, first, last, per_call, prefix, start, count,
# ttl_count, ms. Returns how many members took the TTL.
EXPIRE_SCRIPT = r"""
local key_prefix, first, last = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local per_call, prefix = tonumber(ARGV[4]), ARGV[5]
local start, count, ttl_count, ms = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8]), ARGV[9]
local v = {}
for i = 0, ttl_count - 1 do
  v[#v + 1] = prefix .. string.format('%012d', start + math.floor(i * count / ttl_count))
end
local set = 0
for k = first, last - 1 do
  local key = key_prefix .. string.format('%012d', k)
  for s = 1, #v, per_call do
    local e = math.min(s + per_call - 1, #v)
    local reply = redis.call('SPEXPIRE', key, ms, 'MEMBERS', e - s + 1, unpack(v, s, e))
    for _, r in ipairs(reply) do
      if r == 1 then set = set + 1 end
    end
  end
end
return set
"""

# Counts the expired group's members that a read still sees as present in the
# listed keys; 0 means every one of them is expired. ARGV: key_prefix, per_call,
# prefix, start, count, ttl_count, then key indices.
EXPIRED_CHECK_SCRIPT = r"""
local key_prefix, per_call, prefix = ARGV[1], tonumber(ARGV[2]), ARGV[3]
local start, count, ttl_count = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local v = {}
for i = 0, ttl_count - 1 do
  v[#v + 1] = prefix .. string.format('%012d', start + math.floor(i * count / ttl_count))
end
local visible = 0
for a = 7, #ARGV do
  local key = key_prefix .. string.format('%012d', tonumber(ARGV[a]))
  for s = 1, #v, per_call do
    for _, r in ipairs(redis.call('SMISMEMBER', key, unpack(v, s, math.min(s + per_call - 1, #v)))) do
      visible = visible + r
    end
  end
end
return visible
"""

CHECK_SCRIPT = r"""
local key_prefix, first, last = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local encoding, card = ARGV[4], tonumber(ARGV[5])
local bad, example = 0, ''
for k = first, last - 1 do
  local key = key_prefix .. string.format('%012d', k)
  local n = redis.call('SCARD', key)
  local e = n > 0 and redis.call('OBJECT', 'ENCODING', key) or 'missing'
  if e ~= encoding or (card >= 0 and n ~= card) then
    bad = bad + 1
    if example == '' then example = key .. ' ' .. e .. ' ' .. n end
  end
end
return {bad, example}
"""


def key_name(key_prefix: str, index: int) -> str:
    """Return the name of key ``index`` of a keyspace."""
    return f"{key_prefix}{index:0{INDEX_DIGITS}d}"


def ttl_member_count(group: Dict[str, Any]) -> int:
    """Return how many members of a group get a TTL."""
    ttl = group.get("ttl")
    if not ttl:
        return 0
    count = group["count"]
    if "count" in ttl:
        return min(ttl["count"], count)
    fraction = ttl.get("fraction", 1.0)
    if fraction <= 0:
        return 0
    return max(1, min(count, round(count * fraction)))


def has_expired(preload: Optional[List[Dict[str, Any]]]) -> bool:
    """Return whether a preload leaves expired members stored for the run."""
    return any(
        (group.get("ttl") or {}).get("expired")
        for keyspace in preload or []
        for group in keyspace["members"]
    )


def required_commands(preload: Optional[List[Dict[str, Any]]]) -> Set[str]:
    """Return the server commands a preload needs beyond SADD."""
    needed = set()
    for keyspace in preload or []:
        if any(ttl_member_count(group) for group in keyspace["members"]):
            needed.add("SEXPIRE")
    if has_expired(preload):
        needed |= {"SPEXPIRE", "DEBUG"}
    return needed


def expired_set_members(client) -> int:
    """Return the server's count of reclaimed expired set members (INFO stats)."""
    info = client.execute_command("INFO", "stats")
    if isinstance(info, dict):
        return int(info.get("expired_set_members", 0))
    for line in info.splitlines():
        if line.startswith("expired_set_members:"):
            return int(line.split(":", 1)[1])
    return 0


def set_active_expire(client, enabled: bool) -> None:
    """Turn the server's active expiry on or off with DEBUG SET-ACTIVE-EXPIRE."""
    try:
        client.execute_command("DEBUG", "SET-ACTIVE-EXPIRE", 1 if enabled else 0)
    except Exception as e:
        raise RuntimeError(
            "expired-member scenarios turn active expiry off with DEBUG "
            "SET-ACTIVE-EXPIRE, which this server refused; set "
            "custom-server-configs enable-debug-command to 'local' (client on "
            f"the server host) or 'yes': {e}"
        ) from e


def _check_keys(obj: Any, allowed: Set[str], location: str) -> None:
    if not isinstance(obj, dict):
        raise ValueError(f"{location} must be an object")
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise ValueError(f"{location} does not support key(s): {unknown}")


def _positive_int(value: Any, location: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{location} must be a positive integer")


def validate(preload: Any, location: str) -> None:
    """Raise ValueError when a scenario's ``preload`` list is malformed."""
    if not isinstance(preload, list) or not preload:
        raise ValueError(f"{location} must be a non-empty list")
    prefixes = set()
    for i, keyspace in enumerate(preload):
        where = f"{location}[{i}]"
        _check_keys(keyspace, KEYSPACE_KEYS, where)
        if keyspace.get("type") not in PRELOAD_TYPES:
            raise ValueError(f"{where}.type must be one of {list(PRELOAD_TYPES)}")
        prefix = keyspace.get("key_prefix")
        if not isinstance(prefix, str) or not prefix:
            raise ValueError(f"{where}.key_prefix must be a non-empty string")
        if prefix in prefixes:
            raise ValueError(f"{where}.key_prefix {prefix!r} is used twice")
        prefixes.add(prefix)
        _positive_int(keyspace.get("sets", 1), f"{where}.sets")
        if not isinstance(keyspace.get("interleave", False), bool):
            raise ValueError(f"{where}.interleave must be true or false")
        if not isinstance(keyspace.get("encoding"), str):
            raise ValueError(f"{where}.encoding must name the expected encoding")
        groups = keyspace.get("members")
        if not isinstance(groups, list) or not groups:
            raise ValueError(f"{where}.members must be a non-empty list")
        for j, group in enumerate(groups):
            gwhere = f"{where}.members[{j}]"
            _check_keys(group, GROUP_KEYS, gwhere)
            if not isinstance(group.get("prefix"), str):
                raise ValueError(f"{gwhere}.prefix must be a string")
            _positive_int(group.get("count"), f"{gwhere}.count")
            start = group.get("start", 0)
            if isinstance(start, bool) or not isinstance(start, int) or start < 0:
                raise ValueError(f"{gwhere}.start must be a non-negative integer")
            ttl = group.get("ttl")
            if ttl is None:
                continue
            _check_keys(ttl, TTL_KEYS, f"{gwhere}.ttl")
            _positive_int(ttl.get("seconds"), f"{gwhere}.ttl.seconds")
            if "count" in ttl and "fraction" in ttl:
                raise ValueError(f"{gwhere}.ttl takes 'count' or 'fraction', not both")
            if "count" in ttl:
                _positive_int(ttl["count"], f"{gwhere}.ttl.count")
            if not isinstance(ttl.get("expired", False), bool):
                raise ValueError(f"{gwhere}.ttl.expired must be true or false")
            fraction = ttl.get("fraction", 1.0)
            if (
                isinstance(fraction, bool)
                or not isinstance(fraction, (int, float))
                or not 0 < fraction <= 1
            ):
                raise ValueError(f"{gwhere}.ttl.fraction must be in (0, 1]")


def _load_interleaved(client, keyspace: Dict[str, Any]) -> List[int]:
    """Load all groups of each key of a keyspace in one interleaved pass."""
    sets, groups = keyspace.get("sets", 1), keyspace["members"]
    args = []
    for group in groups:
        args += [
            group["prefix"],
            group.get("start", 0),
            group["count"],
            (group.get("ttl") or {}).get("seconds", 0),
            ttl_member_count(group),
        ]
    batch = max(1, MEMBERS_PER_EVAL // sum(group["count"] for group in groups))
    added = ttls = 0
    for first in range(0, sets, batch):
        last = min(first + batch, sets)
        reply = client.execute_command(
            "EVAL",
            INTERLEAVED_LOAD_SCRIPT,
            0,
            keyspace["key_prefix"],
            first,
            last,
            MEMBERS_PER_CALL,
            len(groups),
            *args,
        )
        added += int(reply[0])
        ttls += int(reply[1])
    want = sets * sum(ttl_member_count(group) for group in groups)
    if ttls != want:
        raise RuntimeError(
            f"preload {keyspace['key_prefix']!r}: SEXPIRE set {ttls} member "
            f"TTLs, expected {want}"
        )
    return [added, ttls]


def _expired_groups(preload: List[Dict[str, Any]]):
    for keyspace in preload:
        for group in keyspace["members"]:
            if (group.get("ttl") or {}).get("expired"):
                yield keyspace, group


def _expire(client, preload: List[Dict[str, Any]]) -> int:
    """Turn active expiry off, then expire the expired groups' TTL members.

    Returns how many members are expired. Active expiry stays off until
    ``finish_expired`` turns it back on, so the members stay stored.
    """
    set_active_expire(client, False)
    total = 0
    for keyspace, group in _expired_groups(preload):
        sets, ttl_count = keyspace.get("sets", 1), ttl_member_count(group)
        batch = max(1, MEMBERS_PER_EVAL // max(1, ttl_count))
        done = 0
        for first in range(0, sets, batch):
            last = min(first + batch, sets)
            done += int(
                client.execute_command(
                    "EVAL",
                    EXPIRE_SCRIPT,
                    0,
                    keyspace["key_prefix"],
                    first,
                    last,
                    MEMBERS_PER_CALL,
                    group["prefix"],
                    group.get("start", 0),
                    group["count"],
                    ttl_count,
                    EXPIRE_MS,
                )
            )
        if done != sets * ttl_count:
            raise RuntimeError(
                f"preload {keyspace['key_prefix']!r}: SPEXPIRE set {done} "
                f"member TTLs, expected {sets * ttl_count}"
            )
        total += done
    time.sleep(0.05)  # well past EXPIRE_MS
    return total


def verify_expired(client, preload: List[Dict[str, Any]]) -> None:
    """Raise unless the expired groups' members read as absent.

    Checks up to EXPIRED_CHECK_KEYS keys per keyspace, spread evenly; the size
    check in ``verify`` already shows every key still stores them.
    """
    for keyspace, group in _expired_groups(preload):
        sets = keyspace.get("sets", 1)
        step = max(1, sets // EXPIRED_CHECK_KEYS)
        keys = sorted({*range(0, sets, step), sets - 1})
        visible = int(
            client.execute_command(
                "EVAL",
                EXPIRED_CHECK_SCRIPT,
                0,
                keyspace["key_prefix"],
                MEMBERS_PER_CALL,
                group["prefix"],
                group.get("start", 0),
                group["count"],
                ttl_member_count(group),
                *keys,
            )
        )
        if visible:
            raise RuntimeError(
                f"preload {keyspace['key_prefix']!r}: {visible} members that "
                f"should be expired still read as present in {len(keys)} keys"
            )


def finish_expired(client, preload: List[Dict[str, Any]], baseline: int) -> int:
    """After the run: check nothing was reclaimed, then turn active expiry on.

    Returns how many expired members the server reclaimed since ``baseline``.
    """
    try:
        reclaimed = expired_set_members(client) - baseline
        if reclaimed:
            raise RuntimeError(
                f"the run reclaimed {reclaimed} expired set members, so the "
                "expired fraction changed during the measurement"
            )
        verify_expired(client, preload)
        return reclaimed
    finally:
        set_active_expire(client, True)


def load(client, preload: List[Dict[str, Any]]) -> Dict[str, int]:
    """Write every keyspace, then verify it. Returns members and TTLs written.

    Groups whose ttl has ``expired`` are then expired (see ``_expire``), and
    the result also carries ``expired_members`` and ``reclaimed_baseline``,
    the INFO count ``finish_expired`` compares against after the run.
    """
    added = expired = 0
    for keyspace in preload:
        if keyspace.get("interleave"):
            a, e = _load_interleaved(client, keyspace)
            added, expired = added + a, expired + e
            continue
        sets = keyspace.get("sets", 1)
        for group in keyspace["members"]:
            ttl_count = ttl_member_count(group)
            batch = max(1, MEMBERS_PER_EVAL // group["count"])
            for first in range(0, sets, batch):
                last = min(first + batch, sets)
                reply = client.execute_command(
                    "EVAL",
                    LOAD_SCRIPT,
                    0,
                    keyspace["key_prefix"],
                    first,
                    last,
                    group["prefix"],
                    group.get("start", 0),
                    group["count"],
                    (group.get("ttl") or {}).get("seconds", 0),
                    ttl_count,
                    MEMBERS_PER_CALL,
                )
                added += int(reply[0])
                expired += int(reply[1])
                want = ttl_count * (last - first)
                if int(reply[1]) != want:
                    raise RuntimeError(
                        f"preload {keyspace['key_prefix']!r}: SEXPIRE set "
                        f"{reply[1]} member TTLs, expected {want}"
                    )
    verify(client, preload, check_size=True)
    written = {"members": added, "member_ttls": expired}
    if has_expired(preload):
        try:
            written["expired_members"] = _expire(client, preload)
            verify(client, preload, check_size=True)  # still stored: none reclaimed
            verify_expired(client, preload)
            written["reclaimed_baseline"] = expired_set_members(client)
        except Exception:
            set_active_expire(client, True)
            raise
    return written


def verify(client, preload: List[Dict[str, Any]], check_size: bool) -> None:
    """Raise RuntimeError unless every key exists with the expected encoding.

    With check_size every key must also hold exactly the members its groups
    wrote. Groups with overlapping member names are not supported by the size
    check.
    """
    for keyspace in preload:
        sets = keyspace.get("sets", 1)
        card = sum(group["count"] for group in keyspace["members"])
        batch = max(1, MEMBERS_PER_EVAL // max(1, card))
        batch = min(batch, 100000)
        for first in range(0, sets, batch):
            last = min(first + batch, sets)
            bad, example = client.execute_command(
                "EVAL",
                CHECK_SCRIPT,
                0,
                keyspace["key_prefix"],
                first,
                last,
                keyspace["encoding"],
                card if check_size else -1,
            )
            if int(bad):
                expected = keyspace["encoding"] + (
                    f" with {card} members" if check_size else ""
                )
                raise RuntimeError(
                    f"preload check: {bad} of {keyspace['key_prefix']!r} keys "
                    f"{first}..{last - 1} are not {expected}; first is "
                    f"'{example}' (key, encoding, members)"
                )
