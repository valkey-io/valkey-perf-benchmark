"""Unit tests for utils/push_timeseries_to_postgres.py.

Tests cover run grouping, run start times, JSON Lines reading, row building,
the missing table and missing column exits, dry-run behavior, CLI argument
rules and identifier validation. No database is needed.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from psycopg2.extras import Json

from utils.push_timeseries_to_postgres import (
    COLUMNS,
    DEFAULT_TABLE,
    IDENTITY_COLUMNS,
    JSON_COLUMNS,
    build_parser,
    build_row,
    collect_rows,
    get_table_columns,
    group_key,
    group_runs,
    insert_rows,
    load_timeseries,
    main,
    process_file,
    run_started_at,
    validate_table_name,
)


def sample_time(second):
    """Return the ISO sample_time the sampler writes for a second of a run."""
    return f"2026-10-06T01:26:{second:02d}.000+00:00"


def sample_row(**overrides):
    """Build a minimal sampler row."""
    row = {
        "timestamp": "2026-09-11T17:30:07-04:00",
        "commit": "a" * 40,
        "repository": "valkey-io/valkey",
        "command": "SET",
        "data_size": 64,
        "pipeline": 1,
        "clients": 4,
        "cluster_mode": False,
        "tls": False,
        "test_id": "1_a",
        "group": 1,
        "scenario": "a",
        "config_set": {},
        "run": 1,
        "profiling_set": {"enabled": False},
        "sample_time": sample_time(10),
        "elapsed_sec": 0,
        "valkey_info": {"used_memory": "1024"},
        "latency_histogram": {"set": {"calls": 3, "histogram_usec": {"1": 3}}},
        "process_cpu": {"proc_stat": {"cpu": [1, 2, 3, 4, 5, 6, 7, 8, 0, 0]}},
        "disk": {"device": "nvme0n1", "stat": [1, 2, 3, 4, 5]},
    }
    row.update(overrides)
    return row


def as_dict(values):
    """Map a built row tuple back to its column names."""
    return dict(zip(COLUMNS, values))


# ---------------------------------------------------------------------------
# group_key and group_runs
# ---------------------------------------------------------------------------


class TestGrouping:
    def test_same_run_groups_together(self):
        rows = [sample_row(elapsed_sec=0), sample_row(elapsed_sec=1)]
        assert len(group_runs(rows)) == 1

    def test_two_runs_split(self):
        rows = [sample_row(run=1), sample_row(run=2)]
        assert len(group_runs(rows)) == 2

    def test_two_config_sets_split(self):
        rows = [
            sample_row(config_set={"maxmemory": "1gb"}),
            sample_row(config_set={"maxmemory": "2gb"}),
        ]
        assert len(group_runs(rows)) == 2

    def test_config_set_key_order_does_not_split(self):
        rows = [
            sample_row(config_set={"a": 1, "b": 2}),
            sample_row(config_set={"b": 2, "a": 1}),
        ]
        assert len(group_runs(rows)) == 1

    def test_io_threads_split(self):
        rows = [sample_row(io_threads=1), sample_row(io_threads=4)]
        assert len(group_runs(rows)) == 2

    def test_different_commits_split(self):
        rows = [sample_row(commit="a" * 40), sample_row(commit="b" * 40)]
        assert len(group_runs(rows)) == 2

    def test_different_test_ids_split(self):
        rows = [sample_row(test_id="1_a"), sample_row(test_id="1_b")]
        assert len(group_runs(rows)) == 2

    def test_group_key_serializes_config_set_sorted(self):
        key = group_key(sample_row(config_set={"b": 2, "a": 1}))
        assert key[3] == json.dumps({"a": 1, "b": 2}, sort_keys=True)


# ---------------------------------------------------------------------------
# run_started_at
# ---------------------------------------------------------------------------


class TestRunStartedAt:
    def test_earliest_sample_time_wins(self):
        rows = [
            sample_row(sample_time=sample_time(12), elapsed_sec=2),
            sample_row(sample_time=sample_time(10), elapsed_sec=0),
            sample_row(sample_time=sample_time(11), elapsed_sec=1),
        ]
        assert run_started_at(rows) == datetime(
            2026, 10, 6, 1, 26, 10, tzinfo=timezone.utc
        )

    def test_no_sample_times_returns_none(self):
        assert run_started_at([{"run": 1}]) is None

    def test_per_group_values_differ(self):
        rows = [
            sample_row(run=1, sample_time=sample_time(10)),
            sample_row(run=1, sample_time=sample_time(11), elapsed_sec=1),
            sample_row(run=2, sample_time=sample_time(40)),
        ]
        starts = {
            key[2]: run_started_at(group) for key, group in group_runs(rows).items()
        }
        assert starts[1].second == 10
        assert starts[2].second == 40


# ---------------------------------------------------------------------------
# build_row and collect_rows
# ---------------------------------------------------------------------------


class TestBuildRow:
    def test_tuple_follows_columns(self):
        assert len(build_row(sample_row(), "cfg", None)) == len(COLUMNS)

    def test_config_name_and_run_started_at_injected(self):
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        values = as_dict(build_row(sample_row(), "cfg", started))
        assert values["config_name"] == "cfg"
        assert values["run_started_at"] == started

    def test_cli_config_name_wins_over_row(self):
        values = as_dict(build_row(sample_row(config_name="row"), "cfg", None))
        assert values["config_name"] == "cfg"

    def test_identity_columns_passed_as_is(self):
        row = sample_row(io_threads=4, elapsed_sec=7)
        values = as_dict(build_row(row, "cfg", None))
        for column in IDENTITY_COLUMNS:
            assert values[column] == row.get(column)

    def test_raw_sources_wrapped_unchanged(self):
        row = sample_row()
        values = as_dict(build_row(row, "cfg", None))
        for column in JSON_COLUMNS:
            assert isinstance(values[column], Json)
            assert values[column].adapted == row[column]

    def test_absent_source_is_null(self):
        row = sample_row()
        del row["disk"]
        assert as_dict(build_row(row, "cfg", None))["disk"] is None

    def test_absent_identity_field_is_null(self):
        assert as_dict(build_row(sample_row(), "cfg", None))["module_commit"] is None

    def test_unmapped_row_keys_ignored(self):
        values = as_dict(build_row(sample_row(env_kernel="6.1"), "cfg", None))
        assert "env_kernel" not in values


class TestCollectRows:
    def test_each_group_gets_its_own_start(self):
        rows = [
            sample_row(run=1, sample_time=sample_time(10)),
            sample_row(run=2, sample_time=sample_time(50)),
        ]
        starts = sorted(
            (values["run"], values["run_started_at"].second)
            for values in map(as_dict, collect_rows(rows, "cfg"))
        )
        assert starts == [(1, 10), (2, 50)]

    def test_row_count_preserved(self):
        rows = [sample_row(elapsed_sec=i) for i in range(5)]
        assert len(collect_rows(rows, "cfg")) == 5


# ---------------------------------------------------------------------------
# load_timeseries
# ---------------------------------------------------------------------------


def write_timeseries(tmp_path, commit, rows):
    """Write a timeseries.jsonl into a commit directory."""
    commit_dir = tmp_path / commit
    commit_dir.mkdir()
    path = commit_dir / "timeseries.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return path


class TestLoadTimeseries:
    def test_reads_one_object_per_line(self, tmp_path):
        path = write_timeseries(tmp_path, "c", [sample_row(), sample_row(run=2)])
        assert [row["run"] for row in load_timeseries(path)] == [1, 2]

    def test_blank_lines_skipped(self, tmp_path):
        path = tmp_path / "timeseries.jsonl"
        path.write_text('\n{"run": 1}\n   \n{"run": 2}\n\n')
        assert len(load_timeseries(path)) == 2

    def test_malformed_line_names_line_number(self, tmp_path):
        path = tmp_path / "timeseries.jsonl"
        path.write_text('{"run": 1}\n\n{"run": \n')
        with pytest.raises(ValueError, match="line 3"):
            load_timeseries(path)

    def test_non_object_line_rejected(self, tmp_path):
        path = tmp_path / "timeseries.jsonl"
        path.write_text('{"run": 1}\n[1, 2]\n')
        with pytest.raises(ValueError, match="line 2"):
            load_timeseries(path)


# ---------------------------------------------------------------------------
# validate_table_name
# ---------------------------------------------------------------------------


class TestValidateTableName:
    def test_default_table_accepted(self):
        assert validate_table_name(DEFAULT_TABLE) == DEFAULT_TABLE

    def test_underscore_prefix_accepted(self):
        assert validate_table_name("_tmp_table") == "_tmp_table"

    @pytest.mark.parametrize(
        "name",
        [
            "Bad-Name",
            "drop table x",
            "table;drop",
            "",
            "1table",
            'quoted"name',
            "a" * 64,
        ],
    )
    def test_invalid_names_rejected(self, name):
        with pytest.raises(ValueError):
            validate_table_name(name)


# ---------------------------------------------------------------------------
# get_table_columns and insert_rows against a mock connection
# ---------------------------------------------------------------------------


def mock_conn(fetchall=None, rowcount=0):
    """Build a connection mock whose cursor is a context manager."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall or []
    cursor.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


class TestGetTableColumns:
    def test_returns_column_names(self):
        conn, _ = mock_conn(fetchall=[("commit",), ("run",)])
        assert get_table_columns(conn, "t") == {"commit", "run"}

    def test_empty_for_missing_table(self):
        conn, _ = mock_conn(fetchall=[])
        assert get_table_columns(conn, "t") == set()


class TestInsertRows:
    def test_commits_once(self):
        conn, _ = mock_conn(rowcount=2)
        with patch("utils.push_timeseries_to_postgres.execute_values") as execute:
            insert_rows(conn, "t", [(1,), (2,)])
        assert execute.call_count == 1
        conn.commit.assert_called_once()

    def test_batches_large_input(self):
        conn, _ = mock_conn(rowcount=500)
        rows = [(i,) for i in range(1100)]
        with patch("utils.push_timeseries_to_postgres.execute_values") as execute:
            insert_rows(conn, "t", rows)
        assert execute.call_count == 3

    def test_returns_inserted_count(self):
        conn, _ = mock_conn(rowcount=7)
        with patch("utils.push_timeseries_to_postgres.execute_values"):
            assert insert_rows(conn, "t", [(1,)]) == 7

    def test_statement_names_every_column_and_ignores_conflicts(self):
        conn, _ = mock_conn(rowcount=1)
        with patch("utils.push_timeseries_to_postgres.execute_values") as execute:
            insert_rows(conn, "t", [(1,)])
        statement = repr(execute.call_args[0][1])
        assert "ON CONFLICT DO NOTHING" in statement
        for column in COLUMNS:
            assert f"Identifier('{column}')" in statement


# ---------------------------------------------------------------------------
# process_file
# ---------------------------------------------------------------------------


class TestProcessFile:
    def test_reports_inserted_and_skipped(self, tmp_path, capsys):
        rows = [sample_row(elapsed_sec=i) for i in range(4)]
        path = write_timeseries(tmp_path, "a" * 40, rows)
        conn, _ = mock_conn(rowcount=3)
        with patch("utils.push_timeseries_to_postgres.execute_values"):
            inserted, skipped = process_file(path, conn, "t", "cfg", False)
        assert (inserted, skipped) == (3, 1)
        assert "Inserted 3 rows, skipped 1" in capsys.readouterr().out

    def test_dry_run_does_not_insert(self, tmp_path, capsys):
        path = write_timeseries(tmp_path, "a" * 40, [sample_row()])
        with patch("utils.push_timeseries_to_postgres.execute_values") as execute:
            inserted, skipped = process_file(path, None, "t", "cfg", True)
        execute.assert_not_called()
        assert (inserted, skipped) == (0, 0)
        assert "would insert 1 rows" in capsys.readouterr().out

    def test_empty_file_is_skipped(self, tmp_path):
        path = write_timeseries(tmp_path, "a" * 40, [])
        assert process_file(path, None, "t", "cfg", True) == (0, 0)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def push_argv(tmp_path, *extra):
    """Return a push command line against tmp_path with connection flags."""
    return [
        "push_timeseries_to_postgres.py",
        "--results-dir",
        str(tmp_path),
        "--config-name",
        "cfg",
        "--host",
        "h",
        "--database",
        "d",
        "--username",
        "u",
        "--password",
        "p",
        *extra,
    ]


class TestMain:
    def run_main(self, argv, conn):
        with (
            patch("sys.argv", argv),
            patch(
                "utils.push_timeseries_to_postgres.psycopg2.connect", return_value=conn
            ),
        ):
            with pytest.raises(SystemExit) as exit_info:
                main()
        return exit_info.value.code

    def test_missing_table_exits_1(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        conn, _ = mock_conn(fetchall=[])
        assert self.run_main(push_argv(tmp_path), conn) == 1
        assert "does not exist" in capsys.readouterr().err

    def test_table_without_raw_columns_exits_1(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        columns = [(column,) for column in COLUMNS if column != "valkey_info"]
        conn, _ = mock_conn(fetchall=columns)
        assert self.run_main(push_argv(tmp_path), conn) == 1
        err = capsys.readouterr().err
        assert "valkey_info" in err
        assert "dashboards/schema.sql" in err

    def test_malformed_file_exits_1(self, tmp_path, capsys):
        commit_dir = tmp_path / ("a" * 40)
        commit_dir.mkdir()
        (commit_dir / "timeseries.jsonl").write_text("not json\n")
        conn, _ = mock_conn(fetchall=[(column,) for column in COLUMNS])
        assert self.run_main(push_argv(tmp_path), conn) == 1
        assert "line 1" in capsys.readouterr().err

    def test_pushes_every_file(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        write_timeseries(tmp_path, "b" * 40, [sample_row(commit="b" * 40)])
        conn, _ = mock_conn(fetchall=[(column,) for column in COLUMNS], rowcount=1)
        with (
            patch("sys.argv", push_argv(tmp_path)),
            patch(
                "utils.push_timeseries_to_postgres.psycopg2.connect", return_value=conn
            ),
            patch("utils.push_timeseries_to_postgres.execute_values") as execute,
        ):
            main()
        assert execute.call_count == 2
        assert "Inserted 2 rows, skipped 0" in capsys.readouterr().out

    def test_dry_run_never_connects(self, tmp_path):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        argv = [
            "push_timeseries_to_postgres.py",
            "--results-dir",
            str(tmp_path),
            "--config-name",
            "cfg",
            "--dry-run",
        ]
        with (
            patch("sys.argv", argv),
            patch("utils.push_timeseries_to_postgres.psycopg2.connect") as connect,
        ):
            main()
        connect.assert_not_called()

    def test_missing_results_dir_exits_1(self, tmp_path):
        argv = [
            "push_timeseries_to_postgres.py",
            "--results-dir",
            str(tmp_path / "nope"),
            "--config-name",
            "cfg",
            "--dry-run",
        ]
        with patch("sys.argv", argv):
            with pytest.raises(SystemExit) as exit_info:
                main()
        assert exit_info.value.code == 1

    def test_invalid_table_name_rejected(self, tmp_path):
        argv = [
            "push_timeseries_to_postgres.py",
            "--results-dir",
            str(tmp_path),
            "--config-name",
            "cfg",
            "--table",
            "bad name",
            "--dry-run",
        ]
        with patch("sys.argv", argv):
            with pytest.raises(SystemExit) as exit_info:
                main()
        assert exit_info.value.code == 2


# ---------------------------------------------------------------------------
# CLI argument rules
# ---------------------------------------------------------------------------


class TestCliArguments:
    def test_results_dir_and_config_name_required(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_config_name_required(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--results-dir", "/tmp"])

    def test_defaults(self):
        args = build_parser().parse_args(
            ["--results-dir", "/tmp", "--config-name", "cfg"]
        )
        assert args.port == 5432
        assert args.sslmode == "require"
        assert args.table == DEFAULT_TABLE
        assert args.dry_run is False

    def test_sslmode_override(self):
        args = build_parser().parse_args(
            ["--results-dir", "/tmp", "--config-name", "cfg", "--sslmode", "disable"]
        )
        assert args.sslmode == "disable"

    def test_connection_flags_required_without_dry_run(self, tmp_path):
        argv = [
            "push_timeseries_to_postgres.py",
            "--results-dir",
            str(tmp_path),
            "--config-name",
            "cfg",
        ]
        with patch("sys.argv", argv):
            with pytest.raises(SystemExit) as exit_info:
                main()
        assert exit_info.value.code == 2

    def test_password_required_without_dry_run(self, tmp_path):
        argv = push_argv(tmp_path)[:-2]
        with patch("sys.argv", argv):
            with pytest.raises(SystemExit) as exit_info:
                main()
        assert exit_info.value.code == 2
