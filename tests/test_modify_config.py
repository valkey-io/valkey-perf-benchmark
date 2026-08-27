"""Unit tests for utils/modify_config.py.

Cover predefined-command overrides, arbitrary-command scenario generation,
populate ordering, the derived server_cpu_range ceiling, every validation
rejection, and that generated configs pass the framework's validate_config.
"""

import copy
import json
from pathlib import Path

import pytest

from benchmark import validate_config
from utils.modify_config import (
    ConfigModificationError,
    build_config,
    main,
)


@pytest.fixture
def base_config():
    """Single-entry base config in the shape workflows start from."""
    return [
        {
            "duration": 180,
            "keyspacelen": [3000000],
            "data_sizes": [16, 128, 2048],
            "pipelines": [1, 10],
            "clients": [1600],
            "commands": ["SET", "GET"],
            "cluster_mode": False,
            "tls_mode": False,
            "warmup": 30,
            "io-threads": [1, 9],
            "benchmark-threads": 90,
            "server_cpu_range": "0-8",
            "client_cpu_range": "96-191",
        }
    ]


# ---------------------------------------------------------------------------
# Predefined-command mode
# ---------------------------------------------------------------------------


class TestPredefinedOverrides:
    """WHEN predefined parameters are supplied, they SHALL land on entry [0]."""

    def test_overrides_land_on_entry(self, base_config):
        result = build_config(
            base_config,
            commands="set,get,hset",
            data_size="32,64",
            pipelines="1,20",
            io_threads="1,4",
            benchmark_threads="48",
            cluster_mode=True,
        )
        entry = result[0]
        assert entry["commands"] == ["SET", "GET", "HSET"]  # upper-cased
        assert entry["data_sizes"] == [32, 64]
        assert entry["pipelines"] == [1, 20]
        assert entry["io-threads"] == [1, 4]
        assert entry["benchmark-threads"] == 48
        assert entry["cluster_mode"] is True
        # Untouched fields are preserved.
        assert entry["keyspacelen"] == [3000000]
        assert entry["client_cpu_range"] == "96-191"
        assert "test_groups" not in entry

    def test_omitted_params_leave_base_untouched(self, base_config):
        original = copy.deepcopy(base_config)
        result = build_config(base_config)
        assert result == original

    def test_input_not_mutated(self, base_config):
        original = copy.deepcopy(base_config)
        build_config(base_config, commands="SET", data_size="64")
        assert base_config == original


# ---------------------------------------------------------------------------
# Derived server_cpu_range (ceiling cap)
# ---------------------------------------------------------------------------


class TestServerCpuRange:
    """WHEN io-threads are supplied, server_cpu_range SHALL be
    0-(max_io_threads - 1), capped at the configurable ceiling."""

    def test_derived_from_max_io_thread(self, base_config):
        result = build_config(base_config, io_threads="1,4,8")
        assert result[0]["server_cpu_range"] == "0-7"

    def test_capped_at_ceiling(self, base_config):
        # max io-thread 20 -> 19, already at the default ceiling.
        result = build_config(base_config, io_threads="1,20")
        assert result[0]["server_cpu_range"] == "0-19"

    def test_ceiling_is_a_parameter(self, base_config):
        result = build_config(
            base_config, io_threads="1,16", server_cpu_ceiling=7, max_io_threads=32
        )
        # 16 - 1 = 15, capped at ceiling 7.
        assert result[0]["server_cpu_range"] == "0-7"


# ---------------------------------------------------------------------------
# Arbitrary-command mode
# ---------------------------------------------------------------------------


class TestArbitraryMode:
    """WHEN an arbitrary command is supplied, a test_groups block SHALL be
    generated (one scenario per pipeline x data_size) and the predefined
    fields removed."""

    def test_generates_pipeline_x_datasize_scenarios(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GETRANGE key:__rand_int__ 0 100",
            data_size="16,64",
            pipelines="1,10",
        )
        entry = result[0]
        # Predefined fields removed, scenario fields generated.
        for removed in ("commands", "data_sizes", "pipelines"):
            assert removed not in entry
        scenarios = entry["test_groups"][0]["scenarios"]
        ids = [s["id"] for s in scenarios]
        assert ids == [
            "cmd_p1_d16",
            "cmd_p10_d16",
            "cmd_p1_d64",
            "cmd_p10_d64",
        ]
        first = scenarios[0]
        assert first["command"] == "GETRANGE key:__rand_int__ 0 100"
        assert first["type"] == "arbitrary"
        assert first["clients"] == 1600  # base clients[0]
        assert first["pipeline"] == 1
        assert first["data_size"] == 16
        assert first["duration"] == 180  # base duration
        assert first["warmup"] == 30  # base warmup

    def test_preserves_non_predefined_fields(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="SET key:__rand_int__ __data__",
            io_threads="1,4",
            benchmark_threads="50",
            cluster_mode=True,
        )
        entry = result[0]
        assert entry["keyspacelen"] == [3000000]
        assert entry["tls_mode"] is False
        assert entry["client_cpu_range"] == "96-191"
        # Shared overrides still apply in arbitrary mode.
        assert entry["io-threads"] == [1, 4]
        assert entry["server_cpu_range"] == "0-3"
        assert entry["benchmark-threads"] == 50
        assert entry["cluster_mode"] is True

    def test_falls_back_to_base_sweep(self, base_config):
        # No --data-size / --pipelines: sweep comes from the base config.
        result = build_config(base_config, arbitrary_command="GET key:__rand_int__")
        ids = [s["id"] for s in result[0]["test_groups"][0]["scenarios"]]
        # base data_sizes [16,128,2048] x pipelines [1,10]
        assert ids == [
            "cmd_p1_d16",
            "cmd_p10_d16",
            "cmd_p1_d128",
            "cmd_p10_d128",
            "cmd_p1_d2048",
            "cmd_p10_d2048",
        ]

    def test_commands_with_arbitrary_command_rejected(self, base_config):
        with pytest.raises(ConfigModificationError, match="cannot be combined"):
            build_config(
                base_config,
                commands="SET,GET",
                arbitrary_command="GET key:__rand_int__",
            )


# ---------------------------------------------------------------------------
# Populate mode
# ---------------------------------------------------------------------------


class TestPopulateMode:
    """WHEN a populate command is supplied, each read scenario SHALL carry it
    as ``populate_with`` so the framework shares one seed between the populate
    pass and the main run; NO separate populate scenario is emitted."""

    def test_populate_with_on_each_read_no_separate_scenario(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            populate_command="SET key:__rand_int__ __data__",
            data_size="16,64",
            pipelines="1,10",
        )
        scenarios = result[0]["test_groups"][0]["scenarios"]
        ids = [s["id"] for s in scenarios]
        # One scenario per pipeline x data_size; NO populate_d* scenarios.
        assert ids == [
            "cmd_p1_d16",
            "cmd_p10_d16",
            "cmd_p1_d64",
            "cmd_p10_d64",
        ]
        assert all(
            s.get("populate_with") == "SET key:__rand_int__ __data__" for s in scenarios
        )
        assert all(s["type"] == "arbitrary" for s in scenarios)

    def test_no_populate_with_when_populate_absent(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        scenarios = result[0]["test_groups"][0]["scenarios"]
        assert all("populate_with" not in s for s in scenarios)

    def test_finding1_populate_and_main_share_seed(self, base_config):
        """End-to-end: the populate pass and the main run get the SAME --seed.

        Builds both argv through ClientRunner with ``random.randint`` patched;
        both invocations must carry the one drawn seed.
        """
        from unittest.mock import patch

        from valkey_benchmark import ClientRunner

        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            populate_command="SET key:__rand_int__ __data__",
            data_size="16",
            pipelines="1",
        )
        entry = result[0]
        scenario = entry["test_groups"][0]["scenarios"][0]

        runner = ClientRunner(
            commit_id="abc123",
            config=entry,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )

        captured = []

        def _capture(*args, **kwargs):
            captured.append(list(kwargs.get("command", args[0] if args else [])))
            return None

        with patch("valkey_benchmark.random.randint", return_value=424242):
            seed_val = runner._draw_scenario_seed(scenario, origin_simple=False)
            with patch.object(runner, "_run", side_effect=_capture):
                runner._populate_scenario_keyspace(scenario, seed_val)
                runner._execute_benchmark_run(scenario, seed_val)

        assert len(captured) == 2, "expected a populate argv and a main argv"

        def _seed_of(argv):
            return argv[argv.index("--seed") + 1]

        populate_seed = _seed_of(captured[0])
        main_seed = _seed_of(captured[1])
        assert populate_seed == main_seed == "424242"


# ---------------------------------------------------------------------------
# Validation rejections
# ---------------------------------------------------------------------------


class TestValidationRejections:
    """WHEN an input is out of range or malformed, build_config SHALL raise
    ConfigModificationError."""

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            ({"data_size": "0"}, "data_size"),
            ({"data_size": "1048577"}, "data_size"),
            ({"io_threads": "21"}, "io_threads"),
            ({"pipelines": "1001"}, "pipelines"),
            ({"benchmark_threads": "97"}, "benchmark_threads"),
            ({"data_size": "16,abc"}, "data_size"),
            ({"arbitrary_command": "GET a\nSET b c"}, "single line"),
            ({"arbitrary_command": "GET " + "x" * 512}, "at most 512"),
            (
                {
                    "arbitrary_command": "GET key:__rand_int__",
                    "populate_command": "SET a b\nSET c d",
                },
                "single line",
            ),
            (
                {"populate_command": "SET key:__rand_int__ __data__"},
                "requires arbitrary_command",
            ),
            ({"commands": "SET,FLUSHALL"}, "Unsupported command"),
        ],
    )
    def test_invalid_input_rejected(self, base_config, kwargs, match):
        with pytest.raises(ConfigModificationError, match=match):
            build_config(base_config, **kwargs)

    def test_empty_config(self):
        with pytest.raises(ConfigModificationError, match="non-empty JSON array"):
            build_config([])


# ---------------------------------------------------------------------------
# Framework validation of generated output
# ---------------------------------------------------------------------------


class TestGeneratedConfigPassesValidation:
    """The generated config SHALL pass the framework's validate_config."""

    def test_predefined_passes(self, base_config):
        result = build_config(
            base_config, commands="SET,GET", data_size="64", pipelines="10"
        )
        validate_config(copy.deepcopy(result[0]))  # should not raise

    def test_arbitrary_passes(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GETRANGE key:__rand_int__ 0 100",
            data_size="16,64",
            pipelines="1,10",
        )
        validate_config(copy.deepcopy(result[0]))  # should not raise

    def test_arbitrary_with_populate_passes(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            populate_command="SET key:__rand_int__ __data__",
        )
        validate_config(copy.deepcopy(result[0]))  # should not raise


# ---------------------------------------------------------------------------
# CLI / file IO
# ---------------------------------------------------------------------------


class TestCli:
    """WHEN invoked as a CLI, main SHALL read, modify, and write the config."""

    def _write(self, tmp_path, config):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(config))
        return path

    def test_output_defaults_to_overwrite(self, tmp_path, base_config):
        path = self._write(tmp_path, base_config)
        rc = main(["--config", str(path), "--commands", "hset"])
        assert rc == 0
        written = json.loads(path.read_text())
        assert written[0]["commands"] == ["HSET"]

    def test_writes_to_separate_output(self, tmp_path, base_config):
        path = self._write(tmp_path, base_config)
        out = tmp_path / "out.json"
        rc = main(
            [
                "--config",
                str(path),
                "--output",
                str(out),
                "--arbitrary-command",
                "GET key:__rand_int__",
            ]
        )
        assert rc == 0
        # Input untouched, output holds the generated test_groups.
        assert json.loads(path.read_text())[0]["commands"] == ["SET", "GET"]
        assert "test_groups" in json.loads(out.read_text())[0]

    def test_invalid_input_returns_nonzero(self, tmp_path, base_config):
        path = self._write(tmp_path, base_config)
        rc = main(["--config", str(path), "--data-size", "0"])


# ---------------------------------------------------------------------------
# Request-bound / scenario-format bases
# ---------------------------------------------------------------------------


@pytest.fixture
def request_based_config():
    """Base whose entry is request-bound (no duration)."""
    return [
        {
            "requests": [1000000],
            "keyspacelen": [3000000],
            "data_sizes": [16],
            "pipelines": [1],
            "clients": [1600],
            "commands": ["SET", "GET"],
            "cluster_mode": False,
            "tls_mode": False,
            "warmup": 0,
        }
    ]


@pytest.fixture
def scenario_format_config():
    """Base entry [0] already in scenario (test_groups) format."""
    return [
        {
            "cluster_mode": False,
            "tls_mode": False,
            "test_groups": [
                {
                    "group": 1,
                    "scenarios": [
                        {"id": "s1", "command": "SET foo bar", "type": "write"}
                    ],
                }
            ],
        }
    ]


class TestFinding2RunBound:
    """A request-based base SHALL carry 'requests' onto scenarios, not silently
    become a 60-second duration run; a base with neither is rejected."""

    def test_requests_carried_not_dropped(self, request_based_config):
        result = build_config(
            request_based_config,
            arbitrary_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        scenario = result[0]["test_groups"][0]["scenarios"][0]
        assert scenario.get("requests") == 1000000
        assert "duration" not in scenario  # never both, never a silent fallback

    def test_duration_carried_when_present(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        scenario = result[0]["test_groups"][0]["scenarios"][0]
        assert scenario.get("duration") == 180
        assert "requests" not in scenario

    def test_neither_duration_nor_requests_rejected(self, base_config):
        broken = copy.deepcopy(base_config)
        del broken[0]["duration"]
        broken[0].pop("requests", None)
        with pytest.raises(ConfigModificationError, match="duration.*or.*requests"):
            build_config(broken, arbitrary_command="GET key:__rand_int__")


class TestFinding3CommandParsing:
    """Malformed arbitrary/populate commands SHALL be rejected at script time via
    shlex parsing, not after the benchmark starts."""

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            ({"arbitrary_command": "GET 'unterminated"}, "arbitrary_command"),
            ({"arbitrary_command": "GET key\\"}, "arbitrary_command"),
            ({"arbitrary_command": "   "}, "at least one token"),
            (
                {
                    "arbitrary_command": "GET key:__rand_int__",
                    "populate_command": "SET 'unterminated",
                },
                "populate_command",
            ),
        ],
    )
    def test_malformed_command_rejected(self, base_config, kwargs, match):
        with pytest.raises(ConfigModificationError, match=match):
            build_config(base_config, **kwargs)


class TestFinding4ScenarioFormatBase:
    """Predefined-only overrides SHALL be rejected against a base that is already
    in scenario format; universal entry-level flags apply."""

    def test_predefined_override_rejected(self, scenario_format_config):
        with pytest.raises(ConfigModificationError, match="scenario"):
            build_config(scenario_format_config, commands="SET,GET")

    def test_arbitrary_rejected_against_scenario_base(self, scenario_format_config):
        with pytest.raises(ConfigModificationError, match="scenario"):
            build_config(
                scenario_format_config, arbitrary_command="GET key:__rand_int__"
            )

    def test_universal_flags_apply_to_scenario_base(self, scenario_format_config):
        result = build_config(
            scenario_format_config,
            io_threads="1,4",
            benchmark_threads="50",
            cluster_mode=True,
        )
        entry = result[0]
        assert entry["io-threads"] == [1, 4]
        assert entry["benchmark-threads"] == 50
        assert entry["cluster_mode"] is True
        # The base scenarios are left intact.
        assert entry["test_groups"][0]["scenarios"][0]["id"] == "s1"


class TestFinding5CpuConflicts:
    """CPU conflicts detectable from the config alone SHALL be rejected at
    script time."""

    def test_io_threads_range_conflicts_with_cpu_allocation(self, base_config):
        # Base uses cpu_allocation; deriving server_cpu_range from --io-threads
        # creates the mutually-exclusive conflict.
        cfg = copy.deepcopy(base_config)
        del cfg[0]["server_cpu_range"]
        del cfg[0]["client_cpu_range"]
        cfg[0]["cpu_allocation"] = {"cores_per_server": 8, "cores_per_client": 8}
        with pytest.raises(ConfigModificationError, match="cpu_allocation"):
            build_config(cfg, commands="SET", io_threads="1,4")

    def test_overlapping_server_client_ranges_rejected(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg[0]["server_cpu_range"] = "0-10"
        cfg[0]["client_cpu_range"] = "8-20"  # overlaps 8,9,10
        with pytest.raises(ConfigModificationError, match="overlap"):
            build_config(cfg, commands="SET")


class TestFinding6ClusterTriState:
    """Cluster mode SHALL be a tri-state — enable, disable, or leave the base
    alone."""

    def test_disable_against_true_base(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg[0]["cluster_mode"] = True
        result = build_config(cfg, commands="SET", cluster_mode=False)
        assert result[0]["cluster_mode"] is False

    def test_enable_against_false_base(self, base_config):
        result = build_config(base_config, commands="SET", cluster_mode=True)
        assert result[0]["cluster_mode"] is True

    def test_omitted_leaves_base_alone(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg[0]["cluster_mode"] = True
        result = build_config(cfg, commands="SET")  # cluster_mode defaults to None
        assert result[0]["cluster_mode"] is True


class TestFinding7Duplicates:
    """Duplicate list values SHALL be rejected (a duplicate is almost certainly
    a typo)."""

    @pytest.mark.parametrize(
        "kwargs",
        [{"data_size": "16,16"}, {"pipelines": "1,1"}, {"commands": "SET,SET"}],
    )
    def test_duplicate_rejected(self, base_config, kwargs):
        with pytest.raises(ConfigModificationError, match="duplicate"):
            build_config(base_config, **kwargs)


class TestFinding8MalformedLists:
    """Empty components in comma-separated lists SHALL be rejected rather than
    silently normalized."""

    @pytest.mark.parametrize(
        "kwargs",
        [{"data_size": "1,,10,"}, {"data_size": ",,,"}, {"commands": "SET,,GET"}],
    )
    def test_empty_component_rejected(self, base_config, kwargs):
        with pytest.raises(ConfigModificationError, match="empty component"):
            build_config(base_config, **kwargs)


class TestFinding9WriteFailure:
    """An output write failure SHALL be reported as a clean error, not an
    uncaught traceback."""

    def test_missing_parent_dir_returns_nonzero(self, tmp_path, base_config):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(base_config))
        missing = tmp_path / "does_not_exist" / "out.json"
        rc = main(
            ["--config", str(path), "--output", str(missing), "--commands", "SET"]
        )
        assert rc == 1


class TestFinding10BaseShape:
    """Malformed base configs SHALL surface as ConfigModificationError, not
    AttributeError/TypeError."""

    def test_entry_not_a_dict(self):
        with pytest.raises(ConfigModificationError, match="entry \\[0\\]"):
            build_config(["not a dict"], commands="SET")

    @pytest.mark.parametrize(
        "field, bad_value", [("data_sizes", 16), ("clients", 1600)]
    )
    def test_scalar_list_field_rejected(self, field, bad_value):
        # A scalar where a list is expected would blow up when read/iterated.
        broken = [
            {
                "duration": 180,
                "keyspacelen": [3000000],
                "data_sizes": [16],
                "pipelines": [1],
                "clients": [1600],
                "commands": ["SET"],
                "cluster_mode": False,
                "tls_mode": False,
                "warmup": 0,
            }
        ]
        broken[0][field] = bad_value
        with pytest.raises(ConfigModificationError, match="must be a list"):
            build_config(broken, arbitrary_command="GET key:__rand_int__")


# ---------------------------------------------------------------------------
# Mixed mode (simultaneous read + write)
# ---------------------------------------------------------------------------


class TestMixedMode:
    """WHEN both --write-command and --read-command are supplied, entry [0]
    SHALL be converted to scenario format with a 'mixed' scenario per
    pipeline x data_size, its client pool split by --write-ratio."""

    def test_generates_mixed_scenarios_shape_and_ids(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            write_ratio=30,
            data_size="16,64",
            pipelines="1,10",
        )
        entry = result[0]
        for removed in ("commands", "data_sizes", "pipelines"):
            assert removed not in entry
        scenarios = entry["test_groups"][0]["scenarios"]
        assert [s["id"] for s in scenarios] == [
            "mix_p1_d16",
            "mix_p10_d16",
            "mix_p1_d64",
            "mix_p10_d64",
        ]
        s = scenarios[0]
        assert s["type"] == "mixed"
        assert s["pipeline"] == 1
        assert s["data_size"] == 16
        assert s["warmup"] == 30  # base warmup
        assert s["warmup_writes_only"] is True
        assert s["duration"] == 180  # base duration
        assert "requests" not in s  # never both
        assert s["writes"] == [
            {"id": "w", "command": "SET key:__rand_int__ __data__", "clients": 480}
        ]
        assert s["reads"] == [
            {"id": "r", "command": "GET key:__rand_int__", "clients": 1120}
        ]

    def test_falls_back_to_base_sweep(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
        )
        ids = [s["id"] for s in result[0]["test_groups"][0]["scenarios"]]
        # base data_sizes [16,128,2048] x pipelines [1,10]
        assert ids == [
            "mix_p1_d16",
            "mix_p10_d16",
            "mix_p1_d128",
            "mix_p10_d128",
            "mix_p1_d2048",
            "mix_p10_d2048",
        ]

    def test_default_ratio_is_fifty(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"][0]["clients"] == 800
        assert s["reads"][0]["clients"] == 800

    @pytest.mark.parametrize("ratio, expected_write", [(30, 480), (50, 800), (33, 528)])
    def test_client_split_sums_exactly_across_ratios(
        self, base_config, ratio, expected_write
    ):
        # CRITICAL: the two sides must sum to the base clients EXACTLY;
        # flooring both would silently drop connections.
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            write_ratio=ratio,
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        write_clients = s["writes"][0]["clients"]
        read_clients = s["reads"][0]["clients"]
        assert write_clients == expected_write
        assert write_clients + read_clients == 1600  # base clients[0], exact

    @pytest.mark.parametrize(
        "total, ratio", [(101, 50), (7, 33), (1599, 33), (3, 50), (1600, 33)]
    )
    def test_split_helper_sums_on_odd_totals(self, total, ratio):
        from utils.modify_config import _split_clients

        write_clients, read_clients = _split_clients(total, ratio)
        assert write_clients + read_clients == total  # exact
        assert write_clients >= 1 and read_clients >= 1
        # write side floored; read side deliberately absorbs the remainder.
        assert write_clients == total * ratio // 100
        assert read_clients == total - write_clients

    def test_cpu_allocation_emitted_and_ranges_removed(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        entry = result[0]
        # Ranges folded into cpu_allocation and removed (mutually exclusive).
        assert "server_cpu_range" not in entry
        assert "client_cpu_range" not in entry
        alloc = entry["cpu_allocation"]
        # 96-191 is 96 cores / 2 concurrent processes = 48 cores each.
        assert alloc["cores_per_client"] == 48
        assert alloc["clients"] == ["96-191"]
        assert alloc["cores_per_server"] == 9  # 0-8
        assert alloc["servers"] == ["0-8"]

    def test_request_based_base_carries_requests(self, request_based_config):
        result = build_config(
            request_based_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s.get("requests") == 1000000
        assert "duration" not in s  # never both, never a silent fallback

    def test_generated_mixed_passes_validation(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16,64",
            pipelines="1,10",
        )
        # Final gate: the emitted config must satisfy the framework validator.
        validate_config(copy.deepcopy(result[0]))  # should not raise

    def test_input_not_mutated(self, base_config):
        original = copy.deepcopy(base_config)
        build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
        )
        assert base_config == original


class TestMixedPredefinedWorkloads:
    """WHEN a mixed side names a predefined command it SHALL emit a ``test:``
    sub-scenario; an arbitrary string SHALL still emit ``command:``; the two
    forms mix and match across sides."""

    def test_predefined_pair_generates_test_subscenarios(self, base_config):
        result = build_config(
            base_config,
            write_command="SET",
            read_command="GET",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"] == [{"id": "w", "test": "SET", "clients": 800}]
        assert s["reads"] == [{"id": "r", "test": "GET", "clients": 800}]
        # A predefined side must NOT also carry an arbitrary command key.
        assert "command" not in s["writes"][0]
        assert "command" not in s["reads"][0]

    def test_predefined_names_are_canonicalized_uppercase(self, base_config):
        # Matches _parse_commands: a bare predefined token is upper-cased.
        result = build_config(
            base_config,
            write_command="set",
            read_command="get",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"][0]["test"] == "SET"
        assert s["reads"][0]["test"] == "GET"

    def test_arbitrary_pair_still_generates_command_subscenarios(self, base_config):
        # Regression: multi-token strings remain arbitrary (command:) workloads.
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"] == [
            {"id": "w", "command": "SET key:__rand_int__ __data__", "clients": 800}
        ]
        assert s["reads"] == [
            {"id": "r", "command": "GET key:__rand_int__", "clients": 800}
        ]
        assert "test" not in s["writes"][0]
        assert "test" not in s["reads"][0]

    def test_predefined_write_arbitrary_read(self, base_config):
        result = build_config(
            base_config,
            write_command="SET",
            read_command="GET key:__rand_int__",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"][0] == {"id": "w", "test": "SET", "clients": 800}
        assert s["reads"][0] == {
            "id": "r",
            "command": "GET key:__rand_int__",
            "clients": 800,
        }

    def test_arbitrary_write_predefined_read(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET",
            data_size="16",
            pipelines="1",
        )
        s = result[0]["test_groups"][0]["scenarios"][0]
        assert s["writes"][0] == {
            "id": "w",
            "command": "SET key:__rand_int__ __data__",
            "clients": 800,
        }
        assert s["reads"][0] == {"id": "r", "test": "GET", "clients": 800}

    def test_generated_predefined_mixed_passes_validation(self, base_config):
        result = build_config(
            base_config,
            write_command="SET",
            read_command="GET",
            data_size="16,64",
            pipelines="1,10",
        )
        # Final gate: a predefined-mixed config must satisfy the framework
        # validator exactly as the arbitrary form does.
        validate_config(copy.deepcopy(result[0]))  # should not raise


class TestMixedRejections:
    """WHEN mixed inputs conflict or are out of range, build_config SHALL raise
    ConfigModificationError."""

    _W = "SET key:__rand_int__ __data__"
    _R = "GET key:__rand_int__"

    @pytest.mark.parametrize(
        "kwargs, match",
        [
            ({"write_command": _W}, "BOTH"),
            ({"read_command": _R}, "BOTH"),
            (
                {"write_command": _W, "read_command": _R, "write_ratio": 0},
                "between 1 and 99",
            ),
            (
                {"write_command": _W, "read_command": _R, "write_ratio": 100},
                "between 1 and 99",
            ),
            (
                {"write_command": "GET", "read_command": "GET"},
                "predefined READ command",
            ),
            (
                {"write_command": "SET", "read_command": "SET"},
                "predefined WRITE command",
            ),
            (
                {"write_command": _W, "read_command": _R, "commands": "SET,GET"},
                "cannot be combined with mixed",
            ),
            (
                {"write_command": _W, "read_command": _R, "arbitrary_command": _R},
                "cannot be combined with mixed",
            ),
            (
                {"write_command": _W, "read_command": _R, "populate_command": _W},
                "populate",
            ),
            (
                {"write_command": "SET 'unterminated", "read_command": _R},
                "write_command",
            ),
            (
                {"write_command": _W, "read_command": "GET 'unterminated"},
                "read_command",
            ),
        ],
    )
    def test_mixed_input_rejected(self, base_config, kwargs, match):
        with pytest.raises(ConfigModificationError, match=match):
            build_config(base_config, **kwargs)

    def test_base_with_cpu_allocation_rejected(self, base_config):
        cfg = copy.deepcopy(base_config)
        del cfg[0]["server_cpu_range"]
        del cfg[0]["client_cpu_range"]
        cfg[0]["cpu_allocation"] = {"cores_per_server": 8, "cores_per_client": 8}
        with pytest.raises(
            ConfigModificationError, match="already defines cpu_allocation"
        ):
            build_config(
                cfg,
                write_command="SET key:__rand_int__ __data__",
                read_command="GET key:__rand_int__",
            )

    def test_split_starving_a_side_rejected(self):
        # A client pool too small to give both sides at least one connection.
        tiny = [
            {
                "duration": 180,
                "keyspacelen": [100],
                "data_sizes": [16],
                "pipelines": [1],
                "clients": [1],
                "commands": ["SET"],
                "cluster_mode": False,
                "tls_mode": False,
                "warmup": 0,
            }
        ]
        with pytest.raises(ConfigModificationError, match="at least one client"):
            build_config(
                tiny,
                write_command="SET key:__rand_int__ __data__",
                read_command="GET key:__rand_int__",
                write_ratio=50,
            )

    def test_one_core_client_pool_rejected(self, base_config):
        """Finding 2: a 1-core client pool cannot feed the two concurrent mixed
        processes, so the script must reject it rather than clamp
        cores_per_client to 1 and emit a config that raises at runtime."""
        cfg = copy.deepcopy(base_config)
        cfg[0]["client_cpu_range"] = "30"
        with pytest.raises(ConfigModificationError, match="concurrent process"):
            build_config(
                cfg,
                write_command="SET key:__rand_int__ __data__",
                read_command="GET key:__rand_int__",
            )

    def test_mixed_against_scenario_format_base_rejected(self, scenario_format_config):
        with pytest.raises(ConfigModificationError, match="scenario"):
            build_config(
                scenario_format_config,
                write_command="SET key:__rand_int__ __data__",
                read_command="GET key:__rand_int__",
            )

    def test_base_overlap_rejected_in_mixed(self, base_config):
        """Case A: an overlap already in the base (server 0-3, client 3-6 share
        core 3) SHALL be rejected in mixed mode too. The conversion folds the
        ranges into cpu_allocation, and the effective-core check still fires."""
        cfg = copy.deepcopy(base_config)
        cfg[0]["server_cpu_range"] = "0-3"
        cfg[0]["client_cpu_range"] = "3-6"
        with pytest.raises(ConfigModificationError, match=r"overlap on cores: \[3\]"):
            build_config(cfg, write_command=self._W, read_command=self._R)

    def test_io_threads_widened_overlap_rejected_in_mixed(self, base_config):
        """Case B: an overlap INTRODUCED during generation by --io-threads
        widening the server range (base server 0-1, client 4-9; io-threads 8 ->
        server 0-7) SHALL be rejected in mixed mode, naming all four shared
        cores."""
        cfg = copy.deepcopy(base_config)
        cfg[0]["server_cpu_range"] = "0-1"
        cfg[0]["client_cpu_range"] = "4-9"
        with pytest.raises(
            ConfigModificationError, match=r"overlap on cores: \[4, 5, 6, 7\]"
        ):
            build_config(
                cfg, write_command=self._W, read_command=self._R, io_threads="8"
            )

    def test_non_overlapping_mixed_still_accepted(self, base_config):
        """Guard against over-rejection: a normal mixed run with disjoint server
        and client ranges (0-8 / 96-191, as in configs/benchmark-config-arm.json)
        SHALL still be accepted after conversion."""
        result = build_config(base_config, write_command=self._W, read_command=self._R)
        assert result[0]["cpu_allocation"]["servers"] == ["0-8"]
        assert result[0]["cpu_allocation"]["clients"] == ["96-191"]


class TestMixedCli:
    """WHEN invoked as a CLI, mixed flags SHALL produce a written mixed config."""

    def test_mixed_cli_end_to_end(self, tmp_path, base_config):
        path = tmp_path / "config.json"
        path.write_text(json.dumps(base_config))
        out = tmp_path / "out.json"
        rc = main(
            [
                "--config",
                str(path),
                "--output",
                str(out),
                "--write-command",
                "SET key:__rand_int__ __data__",
                "--read-command",
                "GET key:__rand_int__",
                "--write-ratio",
                "30",
                "--data-size",
                "64",
                "--pipelines",
                "1",
            ]
        )
        assert rc == 0
        written = json.loads(out.read_text())[0]
        s = written["test_groups"][0]["scenarios"][0]
        assert s["type"] == "mixed"
        assert s["warmup_writes_only"] is True
        assert s["writes"][0]["clients"] + s["reads"][0]["clients"] == 1600
        assert written["cpu_allocation"]["cores_per_client"] == 48


# ---------------------------------------------------------------------------
# Mixed argv propagation and generated-scenario invariants
# ---------------------------------------------------------------------------


class TestFinding1MixedRequestArgv:
    """A request-bounded mixed scenario produces children whose argv carries the
    request count (-n) — for BOTH an arbitrary (command:) child and a predefined
    (test:) child. Argv is built through ClientRunner."""

    def test_both_children_carry_request_count_in_argv(self, request_based_config):
        from valkey_benchmark import ClientRunner

        result = build_config(
            request_based_config,
            write_command="SET key:__rand_int__ __data__",  # arbitrary -> command:
            read_command="GET",  # predefined -> test:
            data_size="16",
            pipelines="1",
        )
        entry = result[0]
        scenario = entry["test_groups"][0]["scenarios"][0]

        runner = ClientRunner(
            commit_id="abc123",
            config=entry,
            cluster_mode=False,
            tls_mode=False,
            target_ip="127.0.0.1",
            results_dir=Path("/tmp/test_results"),
            valkey_path="/tmp/valkey",
            valkey_benchmark_path="src/valkey-benchmark",
        )

        writes, reads = runner._normalize_mixed_configs(scenario)
        write_argv = runner._build_benchmark_command(writes[0], seed_val=1)
        read_argv = runner._build_benchmark_command(reads[0], seed_val=1)

        # Arbitrary (command:) child carries -n <requests>, never a duration.
        assert "command" in writes[0]
        assert "-n" in write_argv
        assert write_argv[write_argv.index("-n") + 1] == "1000000"
        assert "--duration" not in write_argv
        # Predefined (test:) child carries -n <requests> too.
        assert "test" in reads[0]
        assert "-n" in read_argv
        assert read_argv[read_argv.index("-n") + 1] == "1000000"
        assert "--duration" not in read_argv


class TestFinding4MixedClusterExecution:
    """Generated mixed scenarios SHALL set cluster_execution single so
    _launch_mixed_processes spawns one process per sub-scenario regardless of
    node count."""

    def test_generated_mixed_sets_cluster_execution_single(self, base_config):
        result = build_config(
            base_config,
            write_command="SET key:__rand_int__ __data__",
            read_command="GET key:__rand_int__",
            data_size="16,64",
            pipelines="1,10",
        )
        scenarios = result[0]["test_groups"][0]["scenarios"]
        assert scenarios
        assert all(s["cluster_execution"] == "single" for s in scenarios)


class TestFinding5MultiValueCollapsed:
    """A multi-element list in a field the generator collapses to one value
    (clients/requests/keyspacelen) SHALL be rejected, not silently [0]."""

    def test_multi_element_requests_rejected(self, request_based_config):
        cfg = copy.deepcopy(request_based_config)
        cfg[0]["requests"] = [111, 222]
        with pytest.raises(ConfigModificationError, match="requests"):
            build_config(
                cfg,
                arbitrary_command="GET key:__rand_int__",
                data_size="16",
                pipelines="1",
            )

    @pytest.mark.parametrize("field", ["keyspacelen", "clients"])
    def test_multi_element_field_rejected(self, base_config, field):
        cfg = copy.deepcopy(base_config)
        cfg[0][field] = [100, 200]
        with pytest.raises(ConfigModificationError, match=field):
            build_config(
                cfg,
                arbitrary_command="GET key:__rand_int__",
                data_size="16",
                pipelines="1",
            )


class TestFinding6ScalarIoThreads:
    """A scalar io-threads in the base SHALL be accepted (the framework
    normalizes it); lists still work."""

    def test_scalar_io_threads_accepted(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg[0]["io-threads"] = 1  # scalar, not a list
        result = build_config(cfg, commands="SET")
        assert result[0]["io-threads"] == 1


class TestFinding7EmptyQuotedCommand:
    """A command parsing to only empty tokens (e.g. '') SHALL be rejected across
    arbitrary, populate, write and read commands."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"arbitrary_command": "''"},
            {"arbitrary_command": "GET key:__rand_int__", "populate_command": "''"},
            {"write_command": "''", "read_command": "GET key:__rand_int__"},
            {"write_command": "SET key:__rand_int__ __data__", "read_command": "''"},
        ],
    )
    def test_empty_quoted_rejected(self, base_config, kwargs):
        with pytest.raises(ConfigModificationError, match="at least one token"):
            build_config(base_config, **kwargs)


class TestFinding8AllEntriesValidated:
    """A malformed LATER entry SHALL raise ConfigModificationError, not a raw
    TypeError from the final validation gate."""

    def test_non_dict_later_entry_rejected(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg.append(42)  # entry [1] is not a dict
        with pytest.raises(ConfigModificationError, match="entry \\[1\\]"):
            build_config(cfg, commands="SET")


class TestFinding9IntDigitLimit:
    """An absurdly long digit string SHALL raise the script's error type, not
    leak Python's integer string-conversion ValueError."""

    def test_huge_data_size_rejected_cleanly(self, base_config):
        with pytest.raises(ConfigModificationError):
            build_config(base_config, data_size="9" * 5000)


class TestFinding10DurationAndRequests:
    """A base with BOTH duration and requests SHALL be rejected before
    conversion, naming both fields (the arbitrary/mixed path deletes commands so
    the framework's own commands-only check never runs)."""

    def test_both_rejected_in_arbitrary_mode(self, base_config):
        cfg = copy.deepcopy(base_config)
        cfg[0]["requests"] = [1000000]  # base_config already sets duration=180
        with pytest.raises(ConfigModificationError, match="duration.*requests"):
            build_config(
                cfg,
                arbitrary_command="GET key:__rand_int__",
                data_size="16",
                pipelines="1",
            )


class TestFinding11SweepCap:
    """The generated pipelines x data_sizes sweep SHALL be capped with a clear
    error naming the limit; the cap is a parameter."""

    def test_sweep_over_default_cap_rejected(self, base_config):
        # 9 data sizes x 8 pipelines = 72 > default cap of 64.
        with pytest.raises(ConfigModificationError, match="exceeding the limit"):
            build_config(
                base_config,
                arbitrary_command="GET key:__rand_int__",
                data_size="1,2,3,4,5,6,7,8,9",
                pipelines="1,2,3,4,5,6,7,8",
            )

    def test_cap_is_a_parameter(self, base_config):
        with pytest.raises(ConfigModificationError, match="limit of 2"):
            build_config(
                base_config,
                arbitrary_command="GET key:__rand_int__",
                data_size="16,64",
                pipelines="1,10",
                max_sweep_scenarios=2,
            )

    def test_within_cap_passes(self, base_config):
        result = build_config(
            base_config,
            arbitrary_command="GET key:__rand_int__",
            data_size="16,64",
            pipelines="1,10",
            max_sweep_scenarios=4,
        )
        assert len(result[0]["test_groups"][0]["scenarios"]) == 4


class TestFinding12EmptyCommandName:
    """The command NAME (first token) SHALL be non-empty; a later empty token is
    a legitimate argument and stays allowed."""

    @pytest.mark.parametrize("command", ["'' GET", '"" SET key value'])
    def test_empty_command_name_rejected(self, base_config, command):
        with pytest.raises(ConfigModificationError, match="command name"):
            build_config(
                base_config, arbitrary_command=command, data_size="16", pipelines="1"
            )

    def test_empty_trailing_argument_allowed(self, base_config):
        # SET key '' — the empty token is an argument, not the command name.
        result = build_config(
            base_config,
            arbitrary_command="SET key ''",
            data_size="16",
            pipelines="1",
        )
        scenario = result[0]["test_groups"][0]["scenarios"][0]
        assert scenario["command"] == "SET key ''"
