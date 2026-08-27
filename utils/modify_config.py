"""Modify a benchmark config from named parameters supplied via a workflow form.

Output is validated through the framework's ``validate_config`` before writing.
CLI flags mirror the workflow input names. Three modes:

* Predefined-command mode overrides ``commands``, ``data_sizes``,
  ``pipelines``, ``io-threads``, ``cluster_mode`` and ``benchmark-threads`` on
  the first config entry.
* Arbitrary-command mode replaces those predefined fields with a generated
  ``test_groups`` block: one scenario per ``pipeline x data_size``, each
  optionally carrying a ``populate_with`` write command.
* Mixed mode (``--write-command`` + ``--read-command``) generates ``type:
  "mixed"`` scenarios (one per ``pipeline x data_size``) whose write and read
  sides run concurrently, split by ``--write-ratio``, and derives a
  ``cpu_allocation`` partitioning the client CPU pool across the two processes.
  Each side accepts EITHER a predefined command name (a bare token in
  READ/WRITE_COMMANDS, emitted as ``-t NAME``) or an arbitrary command string
  (emitted after ``--``); the two forms can be mixed across sides.

Flag classification (see ``build_config``): ``--commands``, ``--data-size``,
``--pipelines``, ``--arbitrary-command`` and ``--populate-command`` are
predefined-only (require a commands-format base; in arbitrary/mixed mode
``--data-size``/``--pipelines`` are the scenario sweep). ``--cluster-mode``,
``--io-threads`` and ``--benchmark-threads`` are entry-level and apply to a
scenario-format base too.
"""

import argparse
import copy
import json
import re
import shlex
import sys
from pathlib import Path
from typing import List, Optional

# Allow ``python utils/modify_config.py`` to import the framework modules that
# live at the repository root (mirrors tests/conftest.py path bootstrap).
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark import validate_config
from utils.cpu_utils import parse_core_range
from valkey_benchmark import READ_COMMANDS, WRITE_COMMANDS

# ---------- Framework/protocol bounds (not machine-specific) -----------------
MAX_COMMAND_LENGTH = 512
DATA_SIZE_MIN = 1
DATA_SIZE_MAX = 1048576  # 1 MiB payload ceiling
PIPELINE_MIN = 1
PIPELINE_MAX = 1000

# A mixed scenario runs its write and read sides as two concurrent processes,
# so the client CPU pool is split two ways when deriving cores_per_client.
MIXED_CONCURRENT_PROCESSES = 2

# ---------- Machine-specific bounds (CLI-parameterised, with defaults) -------
# Defaults reproduce the on-demand workflow's runner (20-CPU NUMA node, 96-CPU
# client pool); they are CLI parameters because they describe the runner host.
DEFAULT_SERVER_CPU_CEILING = 19  # max index for the derived server_cpu_range
DEFAULT_MAX_IO_THREADS = 20  # CPUs available on the server NUMA node
DEFAULT_MAX_BENCHMARK_THREADS = 96  # size of the client CPU pool
DEFAULT_MAX_SWEEP_SCENARIOS = 64  # ceiling on generated pipelines x data_sizes


class ConfigModificationError(ValueError):
    """Raised when inputs are invalid or the generated config is rejected."""


# ---------- Input parsing / validation ---------------------------------------


def _reject_duplicates(values: list, name: str) -> None:
    """Reject a list that repeats a value (a duplicate is almost always a typo)."""
    if len(set(values)) != len(values):
        dupes = sorted({v for v in values if values.count(v) > 1}, key=values.index)
        raise ConfigModificationError(
            f"{name} contains duplicate values {dupes}; each value must be unique."
        )


def _to_bounded_int(token: str, name: str, low: int, high: int) -> int:
    """Convert a digit string to an int in ``[low, high]``.

    Wraps Python's integer string-conversion digit-limit ValueError (raised for
    absurdly long digit strings) in the script's own error type.
    """
    if not re.fullmatch(r"[0-9]+", token):
        raise ConfigModificationError(
            f"{name} values must be integers between {low} and {high}. Got: {token}"
        )
    try:
        number = int(token)
    except ValueError as exc:
        raise ConfigModificationError(f"{name} value {token[:12]}… is unusable: {exc}")
    if not (low <= number <= high):
        raise ConfigModificationError(
            f"{name} values must be integers between {low} and {high}. Got: {token}"
        )
    return number


def _parse_csv_list(raw: str, name: str, convert) -> Optional[list]:
    """Parse a comma list, rejecting empty components and duplicates.

    ``convert(token)`` maps each stripped token to its stored value (a bounded
    int or an upper-cased command name) or raises. Returns ``None`` for an
    empty/absent input (the workflow's "empty means use the config default").
    """
    if raw is None or not raw.strip():
        return None
    values = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            raise ConfigModificationError(
                f"{name} has an empty component; remove stray commas. Got: {raw!r}"
            )
        values.append(convert(token))
    _reject_duplicates(values, name)
    return values


def _parse_int_list(raw: str, name: str, low: int, high: int) -> Optional[List[int]]:
    """Parse a comma-separated integer list, bounds-checking each value."""
    return _parse_csv_list(
        raw, name, lambda token: _to_bounded_int(token, name, low, high)
    )


def _parse_benchmark_threads(raw: str, high: int) -> Optional[int]:
    """Parse the single benchmark-threads value, bounded by the client pool."""
    if raw is None or not raw.strip():
        return None
    return _to_bounded_int(raw.strip(), "benchmark_threads", 1, high)


def _parse_commands(raw: str) -> Optional[List[str]]:
    """Parse comma-separated predefined commands, upper-cased and allowlisted
    against the framework's own READ_COMMANDS/WRITE_COMMANDS."""
    commands = _parse_csv_list(raw, "commands", str.upper)
    if commands is None:
        return None
    supported = READ_COMMANDS + WRITE_COMMANDS
    for command in commands:
        if command not in supported:
            raise ConfigModificationError(
                f"Unsupported command: {command}. Supported commands: "
                f"{', '.join(supported)}"
            )
    return commands


def _validate_command_string(command: str, label: str) -> None:
    """Reject a malformed arbitrary/populate command at script time.

    Rejects multi-line and over-length strings, and parses the command with
    ``shlex.split`` so unmatched quotes, trailing backslashes and empty commands
    fail here rather than only after the benchmark process starts.
    """
    if "\n" in command:
        raise ConfigModificationError(f"{label} must be a single line.")
    if len(command) > MAX_COMMAND_LENGTH:
        raise ConfigModificationError(
            f"{label} must be at most {MAX_COMMAND_LENGTH} characters."
        )
    try:
        tokens = shlex.split(command)
    except ValueError as exc:
        raise ConfigModificationError(
            f"{label} is not a parseable command ({exc}): {command!r}"
        )
    # ``all(...)`` catches both an empty list and a list of only empty tokens:
    # shlex.split("''") yields [''], which is not caught by ``not tokens``.
    if all(not token for token in tokens):
        raise ConfigModificationError(
            f"{label} must contain at least one token. Got: {command!r}"
        )
    # The command NAME is the first token; an empty one (e.g. "'' GET") is
    # unrunnable even when later argument tokens are present.
    if not tokens[0]:
        raise ConfigModificationError(
            f"{label} command name (first token) must be non-empty. Got: {command!r}"
        )


def _classify_mixed_workload(value: str, *, side: str) -> tuple:
    """Classify a mixed write/read side, returning ``("test", NAME)`` or
    ``("command", value)``.

    A single bare token that is a framework predefined command becomes a
    ``test:`` workload (``-t NAME``, upper-cased like --commands); anything else
    is an arbitrary ``command:`` string (run after ``--``). A predefined name is
    rejected on the wrong side (a READ on the write side, or vice versa) because
    the write side seeds the keyspace the read side queries; an arbitrary string
    is opaque and only held to the command-string parseability rules.
    """
    predefined = READ_COMMANDS + WRITE_COMMANDS
    side_allowlist = WRITE_COMMANDS if side == "write" else READ_COMMANDS
    label = f"{side}_command"

    try:
        tokens = shlex.split(value)
    except ValueError:
        tokens = None  # malformed -> arbitrary; _validate_command_string reports it

    if tokens is not None and len(tokens) == 1 and tokens[0].upper() in predefined:
        name = tokens[0].upper()
        if name not in side_allowlist:
            other = "read" if side == "write" else "write"
            raise ConfigModificationError(
                f"--{side}-command {value!r} is a predefined {other.upper()} "
                f"command, but the mixed {side} side must be a {side.upper()} "
                f"workload. Predefined {side} commands: "
                f"{', '.join(side_allowlist)}. (Supply an arbitrary command "
                "string to run this command on the "
                f"{side} side regardless.)"
            )
        return "test", name

    # Arbitrary command string: held to the same parseability rules as
    # --arbitrary-command (single line, length, shlex-parseable, non-empty).
    _validate_command_string(value, label)
    return "command", value


def _derive_server_cpu_range(io_threads: List[int], ceiling: int) -> str:
    """Derive ``0-(max_io_threads - 1)`` capped at the runner CPU ceiling."""
    end = max(io_threads) - 1
    if end > ceiling:
        end = ceiling
    return f"0-{end}"


# ---------- Base-config shape / mode validation ------------------------------

# List-valued base fields the script reads by index or iterates; shape is
# validated up front so a bad field raises ConfigModificationError, not a raw
# AttributeError/TypeError deep in generation.
_LIST_FIELDS = (
    "commands",
    "data_sizes",
    "pipelines",
    "io-threads",
    "clients",
    "keyspacelen",
    "requests",
)

# Flags that patch entry-level keys and therefore apply to ANY base, including
# one already in scenario (test_groups) format. Everything else is
# predefined-only: it edits or replaces commands/data_sizes/pipelines, which a
# scenario-format base does not have.
UNIVERSAL_FLAGS = ("--cluster-mode", "--io-threads", "--benchmark-threads")


def _validate_base_shape(base_configs) -> None:
    """Validate every base entry's shape before any field is read.

    Guarantees callers see one exception type: a non-empty list of dicts whose
    list-valued fields are actually lists. ``io-threads`` may also be a scalar
    int, which the framework normalizes. Every entry is checked (not just entry
    0) so a malformed later entry raises ConfigModificationError.
    """
    if not isinstance(base_configs, list) or not base_configs:
        raise ConfigModificationError("Config must be a non-empty JSON array.")
    for index, entry in enumerate(base_configs):
        if not isinstance(entry, dict):
            raise ConfigModificationError(
                f"Config entry [{index}] must be a JSON object."
            )
        for key in _LIST_FIELDS:
            value = entry.get(key)
            if value is None:
                continue
            # A scalar io-threads is valid; the framework normalizes it.
            if (
                key == "io-threads"
                and isinstance(value, int)
                and not isinstance(value, bool)
            ):
                continue
            if not isinstance(value, list):
                raise ConfigModificationError(
                    f"base config entry [{index}] field {key!r} must be a list."
                )


def _validate_run_bound_exclusive(entry: dict) -> None:
    """Reject a base entry that sets BOTH ``duration`` and ``requests``.

    ``validate_config`` forbids this only in its ``has_commands`` branch, but the
    arbitrary/mixed conversion deletes ``commands`` before validating, so the
    conflict would otherwise escape. Checked before conversion.
    """
    if entry.get("duration") is not None and entry.get("requests") is not None:
        raise ConfigModificationError(
            "base config entry sets both 'duration' and 'requests'; they are "
            "mutually exclusive. Supply exactly one."
        )


def _is_scenario_format(entry: dict) -> bool:
    """Return True when entry 0 is already in scenario (test_groups) format."""
    return "test_groups" in entry or "scenarios" in entry


def _effective_cpu_cores(entry: dict) -> tuple:
    """Return the effective ``(server_cores, client_cores)`` sets for an entry.

    Reads ``cpu_allocation.servers``/``clients`` (lists of range strings) when a
    cpu_allocation block is present, otherwise ``server_cpu_range``/
    ``client_cpu_range``, so the overlap check sees the same cores the runner
    pins even after mixed conversion folds the explicit ranges into
    cpu_allocation.
    """
    alloc = entry.get("cpu_allocation")
    if alloc is not None:
        server_ranges = alloc.get("servers", [])
        client_ranges = alloc.get("clients", [])
    else:
        server_ranges = (
            [entry["server_cpu_range"]] if "server_cpu_range" in entry else []
        )
        client_ranges = (
            [entry["client_cpu_range"]] if "client_cpu_range" in entry else []
        )

    def _flatten(ranges: list) -> set:
        cores: set = set()
        for range_str in ranges:
            cores.update(parse_core_range(range_str))
        return cores

    return _flatten(server_ranges), _flatten(client_ranges)


def _validate_cpu_config(entry: dict, index: int) -> None:
    """Reject CPU configuration conflicts detectable from the config alone.

    Catches two conflicts that pass ``validate_config`` but fail at benchmark
    startup: ``cpu_allocation`` coexisting with explicit ``server_cpu_range``/
    ``client_cpu_range`` (which an io-threads override can itself create), and an
    overlap between the effective server and client cores — resolved via
    ``_effective_cpu_cores`` so mixed conversion (which folds the ranges into
    cpu_allocation) cannot bypass it. It deliberately does NOT check total cores
    against the host CPU count: this script runs on a different machine than the
    benchmark, so that check would reject valid configs.
    """
    has_alloc = "cpu_allocation" in entry
    has_server = "server_cpu_range" in entry
    has_client = "client_cpu_range" in entry

    if has_alloc and (has_server or has_client):
        raise ConfigModificationError(
            f"generated config entry [{index}] has cpu_allocation together with "
            "server_cpu_range/client_cpu_range; they are mutually exclusive. "
            "(An --io-threads override derives server_cpu_range, which conflicts "
            "with a base that already defines cpu_allocation.)"
        )

    try:
        server_cores, client_cores = _effective_cpu_cores(entry)
    except ValueError as exc:
        raise ConfigModificationError(
            f"generated config entry [{index}] has an invalid CPU range: {exc}"
        )
    overlap = server_cores & client_cores
    if overlap:
        raise ConfigModificationError(
            f"generated config entry [{index}] server and client CPU ranges "
            f"overlap on cores: {sorted(overlap)}"
        )


# ---------- Scenario generation ----------------------------------------------

# Base fields the generated scenarios collapse to a single value: the script
# reads clients/requests via [0], and the framework reads keyspacelen[0] for a
# generated scenario. A multi-element list here almost certainly means the
# operator expected a sweep the generator does not perform.
_SINGLE_VALUE_FIELDS = ("clients", "requests", "keyspacelen")


def _reject_multi_value_fields(entry: dict) -> None:
    """Reject multi-element lists in fields collapsed to one value."""
    for field in _SINGLE_VALUE_FIELDS:
        value = entry.get(field)
        if isinstance(value, list) and len(value) > 1:
            raise ConfigModificationError(
                f"base config entry {field!r} has {len(value)} values {value}; "
                f"generated scenarios use a single {field} value, so a "
                "multi-element list would silently use only the first. Supply "
                "exactly one."
            )


def _check_sweep_size(pipelines: List[int], data_sizes: List[int], limit: int) -> None:
    """Cap the generated ``pipelines x data_sizes`` sweep."""
    count = len(pipelines) * len(data_sizes)
    if count > limit:
        raise ConfigModificationError(
            f"generated sweep would create {count} scenarios ({len(pipelines)} "
            f"pipelines x {len(data_sizes)} data sizes), exceeding the limit of "
            f"{limit}. Reduce --pipelines/--data-size or raise "
            "--max-sweep-scenarios."
        )


def _run_bound(base: dict) -> dict:
    """Return the single run bound (``duration`` OR ``requests``) for a generated
    scenario, preserving the framework's duration-over-requests precedence.

    A base with neither is rejected: the runner would otherwise silently fall
    back to a 60-second run and discard the configured request count.
    """
    if base.get("duration") is not None:
        return {"duration": base["duration"]}
    if base.get("requests"):
        return {"requests": base["requests"][0]}
    raise ConfigModificationError(
        "base config entry must define 'duration' or 'requests' for generated "
        "scenarios; neither is set and the runner would silently fall back to a "
        "60-second run."
    )


def _iter_sweep(prefix: str, pipelines: List[int], data_sizes: List[int]):
    """Yield ``(scenario_id, pipeline, data_size)`` per pipeline x data_size.

    Shared id construction for the arbitrary and mixed builders.
    """
    for data_size in data_sizes:
        for pipeline in pipelines:
            yield f"{prefix}_p{pipeline}_d{data_size}", pipeline, data_size


def _resolve_sweep(entry: dict, pipeline_list, data_sizes, max_sweep: int):
    """Resolve and validate the pipelines x data_sizes sweep for a generated mode.

    Sweep comes from --pipelines/--data-size or the base's own values (read
    before the predefined fields are removed). Applies the single-value and
    sweep-size guards, then removes the predefined fields the generated
    ``test_groups`` replaces. Shared by arbitrary and mixed.
    """
    scenario_pipelines = (
        pipeline_list if pipeline_list is not None else entry.get("pipelines")
    )
    scenario_sizes = data_sizes if data_sizes is not None else entry.get("data_sizes")
    if not scenario_pipelines or not scenario_sizes:
        raise ConfigModificationError(
            "generated scenarios need pipelines and data_sizes, from "
            "--pipelines/--data-size or the base config entry."
        )
    if not entry.get("clients"):
        raise ConfigModificationError(
            "base config entry must define 'clients' for scenario generation."
        )
    _reject_multi_value_fields(entry)
    _check_sweep_size(scenario_pipelines, scenario_sizes, max_sweep)
    for key in ("commands", "data_sizes", "pipelines"):
        entry.pop(key, None)
    return scenario_pipelines, scenario_sizes


def _build_arbitrary_group(
    base: dict,
    command: str,
    populate_command: str,
    pipelines: List[int],
    data_sizes: List[int],
) -> List[dict]:
    """Build the ``test_groups`` block for an arbitrary command (one scenario per
    pipeline x data_size).

    A populate command is attached to each scenario as ``populate_with`` (not a
    separate write scenario) so the framework threads ONE seed through both the
    populate pass and the main run and they hit the same keys.
    """
    clients = base["clients"][0]
    run_bound = _run_bound(base)

    scenarios = []
    for scenario_id, pipeline, data_size in _iter_sweep("cmd", pipelines, data_sizes):
        scenario = {
            "id": scenario_id,
            "type": "arbitrary",
            "command": command,
            "clients": clients,
            "pipeline": pipeline,
            "data_size": data_size,
            "warmup": base.get("warmup", 0),
            **run_bound,
        }
        if populate_command:
            scenario["populate_with"] = populate_command
        scenarios.append(scenario)
    return [{"group": 1, "description": "arbitrary command", "scenarios": scenarios}]


def _validate_mixed_inputs(
    write_command: str,
    read_command: str,
    write_ratio: int,
    commands: str,
    arbitrary_command: str,
    populate_command: str,
    entry: dict,
) -> None:
    """Reject conflicting or out-of-range inputs for mixed mode.

    Mixed mode (BOTH write and read commands) is mutually exclusive with the
    predefined/arbitrary/populate flags, requires a 1..99 write ratio, and needs
    a base without its own cpu_allocation (it derives one). See the individual
    checks for the exact rules.
    """
    if not (write_command and read_command):
        raise ConfigModificationError(
            "mixed mode needs BOTH --write-command and --read-command; supplying "
            "only one is ambiguous. For a single command use --arbitrary-command."
        )
    if commands:
        raise ConfigModificationError(
            "--commands cannot be combined with mixed mode (--write-command/"
            "--read-command); mixed generates its own scenarios. Supply only one."
        )
    if arbitrary_command:
        raise ConfigModificationError(
            "--arbitrary-command cannot be combined with mixed mode "
            "(--write-command/--read-command); supply only one."
        )
    if populate_command:
        raise ConfigModificationError(
            "--populate-command cannot be combined with mixed mode; the mixed "
            "write side seeds the keyspace, and the framework rejects "
            "'populate_with' on a mixed scenario."
        )
    if not (1 <= write_ratio <= 99):
        raise ConfigModificationError(
            f"--write-ratio must be between 1 and 99 (got {write_ratio}); 0 or "
            "100 is not a mixed load — use --arbitrary-command for a single "
            "command."
        )
    if "cpu_allocation" in entry:
        raise ConfigModificationError(
            "mixed mode derives cpu_allocation from the base's client_cpu_range "
            "to split the client CPU pool between its concurrent write and read "
            "processes, but the base already defines cpu_allocation. Supply a "
            "base that uses server_cpu_range/client_cpu_range, or remove "
            "cpu_allocation."
        )
    # Classify each side now so bad side-placement fails early with an
    # actionable error; the result is recomputed in _build_mixed_group.
    _classify_mixed_workload(write_command, side="write")
    _classify_mixed_workload(read_command, side="read")


def _split_clients(total: int, write_ratio: int) -> tuple:
    """Split ``total`` client connections into ``(write, read)`` by percent.

    The write side is floored and the read side takes the remainder, so the two
    always sum to ``total`` EXACTLY — flooring both would silently drop
    connections. Both sides must receive at least one client; a ratio/total that
    would starve either side is rejected rather than emitting a zero-client
    sub-scenario the runner cannot execute.
    """
    write_clients = total * write_ratio // 100
    read_clients = total - write_clients  # reads absorb the rounding remainder
    if write_clients < 1 or read_clients < 1:
        raise ConfigModificationError(
            f"--write-ratio {write_ratio} splits {total} client(s) into "
            f"{write_clients} write / {read_clients} read; each side needs at "
            "least one client. Raise the base 'clients' or move the ratio "
            "toward 50."
        )
    return write_clients, read_clients


def _apply_mixed_cpu_allocation(entry: dict) -> None:
    """Repartition the base's client CPU pool across the mixed processes.

    Emits the *manual* (servers/clients arrays) cpu_allocation form, not the
    auto form: the auto form collapses the pool to a single
    ``cores_per_client``-wide block that the second mixed process overruns,
    whereas the manual form preserves the operator's exact core placement while
    ``cores_per_client = pool // processes`` hands each process its own slice
    (e.g. 96-191 -> 96-143 write, 144-191 read). The explicit
    ``server_cpu_range``/``client_cpu_range`` are folded into the arrays and
    removed so the mutually-exclusive-CPU invariant still holds.
    """
    client_range = entry.get("client_cpu_range")
    if not client_range:
        # No client pool to partition; leave pinning to the runner defaults.
        return

    pool_size = len(parse_core_range(client_range))
    cores_per_client = max(1, pool_size // MIXED_CONCURRENT_PROCESSES)
    needed = MIXED_CONCURRENT_PROCESSES * cores_per_client
    if pool_size < needed:
        raise ConfigModificationError(
            f"client_cpu_range {client_range!r} has only {pool_size} core(s), but "
            f"a mixed load runs {MIXED_CONCURRENT_PROCESSES} concurrent processes "
            f"needing {needed} ({cores_per_client} per process). Widen "
            "client_cpu_range."
        )

    cpu_allocation = {
        "cores_per_client": cores_per_client,
        "clients": [client_range],
    }
    server_range = entry.get("server_cpu_range")
    if server_range:
        cpu_allocation["cores_per_server"] = len(parse_core_range(server_range))
        cpu_allocation["servers"] = [server_range]
    else:
        cpu_allocation["cores_per_server"] = 1

    entry["cpu_allocation"] = cpu_allocation
    entry.pop("server_cpu_range", None)
    entry.pop("client_cpu_range", None)


def _build_mixed_group(
    base: dict,
    write_command: str,
    read_command: str,
    write_ratio: int,
    pipelines: List[int],
    data_sizes: List[int],
) -> List[dict]:
    """Build the ``test_groups`` block for a simultaneous read+write workload.

    One ``type: "mixed"`` scenario per ``pipeline x data_size`` (id
    ``mix_p{p}_d{d}``), each carrying the base's run bound (``duration`` OR
    ``requests``), ``warmup``, and ``warmup_writes_only: True`` (only the write
    side seeds the keyspace during warmup). The write/read ``clients`` split
    comes from base ``clients[0]`` and ``write_ratio`` and sums to it exactly;
    both are set explicitly because the framework does not inherit ``clients``
    onto mixed sub-scenarios. Each side carries ``test:`` or ``command:`` per
    ``_classify_mixed_workload``.
    """
    total_clients = base["clients"][0]
    write_clients, read_clients = _split_clients(total_clients, write_ratio)

    write_key, write_value = _classify_mixed_workload(write_command, side="write")
    read_key, read_value = _classify_mixed_workload(read_command, side="read")
    run_bound = _run_bound(base)

    scenarios = []
    for scenario_id, pipeline, data_size in _iter_sweep("mix", pipelines, data_sizes):
        scenarios.append(
            {
                "id": scenario_id,
                "type": "mixed",
                "pipeline": pipeline,
                "data_size": data_size,
                "warmup": base.get("warmup", 0),
                "warmup_writes_only": True,
                # One process per sub-scenario PER active port: without this a
                # multi-node cluster would double the process/client/CPU count.
                # The script cannot know the node count, so it forces single.
                "cluster_execution": "single",
                **run_bound,
                "writes": [
                    {"id": "w", write_key: write_value, "clients": write_clients}
                ],
                "reads": [{"id": "r", read_key: read_value, "clients": read_clients}],
            }
        )
    return [{"group": 1, "description": "mixed read+write", "scenarios": scenarios}]


# ---------- Config building --------------------------------------------------


def build_config(
    base_configs: List[dict],
    *,
    commands: str = "",
    arbitrary_command: str = "",
    populate_command: str = "",
    write_command: str = "",
    read_command: str = "",
    write_ratio: int = 50,
    data_size: str = "",
    io_threads: str = "",
    benchmark_threads: str = "",
    pipelines: str = "",
    cluster_mode: Optional[bool] = None,
    server_cpu_ceiling: int = DEFAULT_SERVER_CPU_CEILING,
    max_io_threads: int = DEFAULT_MAX_IO_THREADS,
    max_benchmark_threads: int = DEFAULT_MAX_BENCHMARK_THREADS,
    max_sweep_scenarios: int = DEFAULT_MAX_SWEEP_SCENARIOS,
) -> List[dict]:
    """Return a modified copy of ``base_configs`` with overrides applied.

    Parameters mirror the workflow input names and accept the same string forms
    (an empty string means "leave the base config value untouched").
    ``cluster_mode`` is tri-state: ``True`` enables, ``False`` disables, ``None``
    leaves the base alone. Only the first config entry is modified.

    Supplying ``arbitrary_command`` (or a write+read pair) CONVERTS entry [0] to
    scenario format: its ``commands``/``data_sizes``/``pipelines`` are removed
    and replaced by a generated ``test_groups`` block. The predefined-only flags
    are rejected against an already-scenario-format base; only
    ``--cluster-mode``/``--io-threads``/``--benchmark-threads`` apply to it.

    The result is validated through ``validate_config`` (plus static CPU-conflict
    checks) before returning; a failure is raised as ``ConfigModificationError``.
    """
    _validate_base_shape(base_configs)

    configs = copy.deepcopy(base_configs)
    entry = configs[0]

    # Reject a base that sets both duration and requests before conversion, so
    # the arbitrary/mixed paths (which delete 'commands') cannot slip it past
    # the framework's commands-only check.
    _validate_run_bound_exclusive(entry)

    mixed_mode = bool(write_command or read_command)
    if mixed_mode:
        _validate_mixed_inputs(
            write_command,
            read_command,
            write_ratio,
            commands,
            arbitrary_command,
            populate_command,
            entry,
        )

    # --commands and --arbitrary-command are mutually exclusive: arbitrary mode
    # would otherwise silently win and discard --commands.
    if commands and arbitrary_command:
        raise ConfigModificationError(
            "--commands cannot be combined with --arbitrary-command; arbitrary "
            "mode replaces the predefined commands entirely. Supply only one."
        )

    # A scenario-format base has no commands/data_sizes/pipelines to edit or
    # replace, so predefined-only flags cannot affect what runs.
    if _is_scenario_format(entry):
        offending = [
            flag
            for flag, supplied in (
                ("--commands", commands),
                ("--data-size", data_size),
                ("--pipelines", pipelines),
                ("--arbitrary-command", arbitrary_command),
                ("--populate-command", populate_command),
                ("--write-command", write_command),
                ("--read-command", read_command),
            )
            if supplied
        ]
        if offending:
            raise ConfigModificationError(
                "base config entry [0] is already in scenario (test_groups) "
                f"format; {', '.join(offending)} cannot be applied to it. Only "
                f"{', '.join(UNIVERSAL_FLAGS)} apply to a scenario-format base."
            )

    # Parse and bounds-check the shared numeric inputs up front.
    data_sizes = _parse_int_list(data_size, "data_size", DATA_SIZE_MIN, DATA_SIZE_MAX)
    io_thread_list = _parse_int_list(io_threads, "io_threads", 1, max_io_threads)
    pipeline_list = _parse_int_list(pipelines, "pipelines", PIPELINE_MIN, PIPELINE_MAX)
    benchmark_thread_count = _parse_benchmark_threads(
        benchmark_threads, max_benchmark_threads
    )

    if arbitrary_command:
        _validate_command_string(arbitrary_command, "arbitrary_command")
    if populate_command:
        if not arbitrary_command:
            raise ConfigModificationError(
                "populate_command requires arbitrary_command."
            )
        _validate_command_string(populate_command, "populate_command")

    def _apply_shared_overrides() -> None:
        """Apply overrides valid in both modes (cluster, io-threads, threads)."""
        if cluster_mode is not None:
            entry["cluster_mode"] = cluster_mode
        if io_thread_list is not None:
            entry["io-threads"] = io_thread_list
            entry["server_cpu_range"] = _derive_server_cpu_range(
                io_thread_list, server_cpu_ceiling
            )
        if benchmark_thread_count is not None:
            entry["benchmark-threads"] = benchmark_thread_count

    if mixed_mode:
        scenario_pipelines, scenario_sizes = _resolve_sweep(
            entry, pipeline_list, data_sizes, max_sweep_scenarios
        )
        entry["test_groups"] = _build_mixed_group(
            entry,
            write_command,
            read_command,
            write_ratio,
            scenario_pipelines,
            scenario_sizes,
        )
        # Shared overrides first (an --io-threads override may rewrite
        # server_cpu_range), then fold the CPU ranges into a cpu_allocation.
        _apply_shared_overrides()
        _apply_mixed_cpu_allocation(entry)
    elif arbitrary_command:
        scenario_pipelines, scenario_sizes = _resolve_sweep(
            entry, pipeline_list, data_sizes, max_sweep_scenarios
        )
        entry["test_groups"] = _build_arbitrary_group(
            entry,
            arbitrary_command,
            populate_command,
            scenario_pipelines,
            scenario_sizes,
        )
        _apply_shared_overrides()
    else:
        parsed_commands = _parse_commands(commands)
        if parsed_commands is not None:
            entry["commands"] = parsed_commands
        if data_sizes is not None:
            entry["data_sizes"] = data_sizes
        if pipeline_list is not None:
            entry["pipelines"] = pipeline_list
        _apply_shared_overrides()

    # Final gate: never emit something the runner would reject. validate_config
    # mutates its argument (bool coercion, test_groups compilation), so validate
    # a copy and keep the emitted config exactly as built.
    for index, cfg in enumerate(configs):
        _validate_cpu_config(cfg, index)
        try:
            validate_config(copy.deepcopy(cfg))
        except ValueError as exc:
            raise ConfigModificationError(
                f"generated config entry [{index}] failed framework validation: {exc}"
            )

    return configs


# ---------- CLI --------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments. Flags mirror the workflow input names."""
    parser = argparse.ArgumentParser(
        description=(
            "Modify a benchmark config from named parameters, replacing the "
            "jq/bash config surgery in on-demand benchmark workflows."
        ),
        allow_abbrev=False,
    )
    parser.add_argument("--config", required=True, help="Path to the base config JSON.")
    parser.add_argument(
        "--output",
        default=None,
        help="Where to write the modified config (default: overwrite --config).",
    )
    parser.add_argument(
        "--commands",
        default="",
        help="Comma-separated predefined commands to benchmark (e.g. SET,GET).",
    )
    parser.add_argument(
        "--arbitrary-command",
        default="",
        help="Arbitrary command to benchmark. CONVERTS entry [0] to scenario "
        "format: its commands/data_sizes/pipelines are removed and replaced by "
        "a generated test_groups block (one scenario per pipeline x data_size). "
        "Cannot be combined with --commands.",
    )
    parser.add_argument(
        "--populate-command",
        default="",
        help="Write command attached to each generated read scenario as "
        "populate_with so populate and read share one seed. Requires "
        "--arbitrary-command.",
    )
    parser.add_argument(
        "--write-command",
        default="",
        help="Write side of a simultaneous mixed load. Accepts EITHER a "
        "predefined command name (e.g. SET -> run as '-t SET') OR an arbitrary "
        "command string (e.g. 'SET key:__rand_int__ __data__' -> run after "
        "'--'). Supplying BOTH --write-command and --read-command CONVERTS "
        "entry [0] to scenario format: its commands/data_sizes/pipelines are "
        "replaced by a generated 'mixed' scenario (per pipeline x data_size) "
        "whose write and read sides run concurrently. A predefined write value "
        "must be a WRITE command. Mutually exclusive with --commands/"
        "--arbitrary-command/--populate-command.",
    )
    parser.add_argument(
        "--read-command",
        default="",
        help="Read side of a simultaneous mixed load. Accepts EITHER a "
        "predefined command name (e.g. GET -> run as '-t GET') OR an arbitrary "
        "command string (e.g. 'GET key:__rand_int__' -> run after '--'). A "
        "predefined read value must be a READ command. See --write-command; "
        "both must be supplied together.",
    )
    parser.add_argument(
        "--write-ratio",
        type=int,
        default=50,
        help="Percent of client connections given to the write side of a mixed "
        "load (1-99, default 50); the read side takes the rest. 0 or 100 is not "
        "a mixed load — use --arbitrary-command instead.",
    )
    parser.add_argument(
        "--data-size",
        default="",
        help=f"Comma-separated data sizes in bytes "
        f"(range: {DATA_SIZE_MIN}-{DATA_SIZE_MAX}).",
    )
    parser.add_argument(
        "--io-threads",
        default="",
        help="Comma-separated io-threads values (range: 1-<max-io-threads>).",
    )
    parser.add_argument(
        "--benchmark-threads",
        default="",
        help="Client thread count for valkey-benchmark "
        "(range: 1-<max-benchmark-threads>).",
    )
    parser.add_argument(
        "--pipelines",
        default="",
        help=f"Comma-separated pipeline values (range: {PIPELINE_MIN}-{PIPELINE_MAX}).",
    )
    parser.add_argument(
        "--cluster-mode",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Tri-state: --cluster-mode enables, --no-cluster-mode disables, "
        "omitted leaves the base config value unchanged.",
    )
    parser.add_argument(
        "--server-cpu-ceiling",
        type=int,
        default=DEFAULT_SERVER_CPU_CEILING,
        help="Max index for the derived server_cpu_range (runner-specific, "
        f"default: {DEFAULT_SERVER_CPU_CEILING}).",
    )
    parser.add_argument(
        "--max-io-threads",
        type=int,
        default=DEFAULT_MAX_IO_THREADS,
        help="Upper bound for io-threads values (server NUMA-node CPUs, "
        f"default: {DEFAULT_MAX_IO_THREADS}).",
    )
    parser.add_argument(
        "--max-benchmark-threads",
        type=int,
        default=DEFAULT_MAX_BENCHMARK_THREADS,
        help="Upper bound for benchmark-threads (client CPU pool size, "
        f"default: {DEFAULT_MAX_BENCHMARK_THREADS}).",
    )
    parser.add_argument(
        "--max-sweep-scenarios",
        type=int,
        default=DEFAULT_MAX_SWEEP_SCENARIOS,
        help="Ceiling on generated pipelines x data_sizes scenarios "
        f"(default: {DEFAULT_MAX_SWEEP_SCENARIOS}).",
    )
    parser.add_argument(
        "--print",
        dest="print_config",
        action="store_true",
        help="Print the resulting config to stdout.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point. Returns a non-zero exit code on any validation error."""
    args = parse_args(argv)

    config_path = Path(args.config)
    try:
        with open(config_path, "r") as fp:
            base_configs = json.load(fp)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Error: could not read config {config_path}: {exc}", file=sys.stderr)
        return 1

    try:
        result = build_config(
            base_configs,
            commands=args.commands,
            arbitrary_command=args.arbitrary_command,
            populate_command=args.populate_command,
            write_command=args.write_command,
            read_command=args.read_command,
            write_ratio=args.write_ratio,
            data_size=args.data_size,
            io_threads=args.io_threads,
            benchmark_threads=args.benchmark_threads,
            pipelines=args.pipelines,
            cluster_mode=args.cluster_mode,
            server_cpu_ceiling=args.server_cpu_ceiling,
            max_io_threads=args.max_io_threads,
            max_benchmark_threads=args.max_benchmark_threads,
            max_sweep_scenarios=args.max_sweep_scenarios,
        )
    except ConfigModificationError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    # Make the mode explicit so a user is not surprised that the predefined
    # fields vanished when an arbitrary command converts the entry.
    if args.write_command or args.read_command:
        print(
            "Mode: mixed — entry [0] converted to scenario (test_groups) format; "
            "its commands/data_sizes/pipelines were replaced by a generated "
            "'mixed' scenario whose write and read sides run concurrently.",
            file=sys.stderr,
        )
    elif args.arbitrary_command:
        print(
            "Mode: arbitrary-command — entry [0] converted to scenario "
            "(test_groups) format; its commands/data_sizes/pipelines were "
            "replaced by generated scenarios.",
            file=sys.stderr,
        )
    else:
        print("Mode: predefined — overrides applied in place.", file=sys.stderr)

    output_path = Path(args.output) if args.output else config_path
    try:
        with open(output_path, "w") as fp:
            json.dump(result, fp, indent=2)
            fp.write("\n")
    except OSError as exc:
        print(
            f"Error: could not write output config {output_path}: {exc}",
            file=sys.stderr,
        )
        return 1

    if args.print_config:
        print(json.dumps(result, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
