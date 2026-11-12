"""Unit tests for the timeseries.jsonl push in utils/push_to_postgres.py.

Tests cover run grouping, run start times, JSON Lines reading, row building,
table name resolution, the missing table and missing column exits, pushing
metrics.json and timeseries.jsonl together, dry-run behavior and CLI argument
rules. No database is needed.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from psycopg2.extras import Json

from utils.push_to_postgres import (
    TIMESERIES_COLUMNS as COLUMNS,
    TIMESERIES_IDENTITY_COLUMNS as IDENTITY_COLUMNS,
    TIMESERIES_JSON_COLUMNS as JSON_COLUMNS,
    build_parser,
    build_timeseries_row as build_row,
    collect_timeseries_rows as collect_rows,
    group_key,
    group_runs,
    insert_timeseries_rows as insert_rows,
    load_timeseries,
    main,
    process_commit_timeseries,
    resolve_table_name,
    resolve_timeseries_table_name,
    run_started_at,
    timeseries_table_error,
)

MODULE = "utils.push_to_postgres"


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
# table name resolution
# ---------------------------------------------------------------------------


class TestResolveTimeseriesTableName:
    def test_core(self):
        assert resolve_timeseries_table_name("core") == "benchmark_timeseries"

    def test_tag(self):
        assert resolve_timeseries_table_name("tag") == "benchmark_tags_timeseries"

    def test_other_identifier(self):
        assert (
            resolve_timeseries_table_name("tiering") == "benchmark_timeseries_tiering"
        )

    def test_follows_metrics_table(self):
        assert resolve_table_name("tiering") == "benchmark_metrics_tiering"

    @pytest.mark.parametrize(
        "table_id", ["Bad-Name", "drop table x", "table;drop", "", "1table", "a" * 32]
    )
    def test_invalid_identifier_rejected(self, table_id):
        with pytest.raises(ValueError):
            resolve_timeseries_table_name(table_id)


# ---------------------------------------------------------------------------
# timeseries_table_error and insert_rows against a mock connection
# ---------------------------------------------------------------------------


def mock_conn(fetchall=None, rowcount=0):
    """Build a connection mock whose cursor is a context manager."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall or []
    cursor.rowcount = rowcount
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


def all_columns():
    """Return an information_schema result naming every timeseries column."""
    return [(column,) for column in COLUMNS]


class TestTimeseriesTableError:
    def test_ready_table(self):
        conn, _ = mock_conn(fetchall=all_columns())
        assert timeseries_table_error(conn, "t") is None

    def test_missing_table(self):
        conn, _ = mock_conn(fetchall=[])
        assert "does not exist" in timeseries_table_error(conn, "t")

    def test_missing_columns(self):
        conn, _ = mock_conn(fetchall=[c for c in all_columns() if c[0] != "disk"])
        error = timeseries_table_error(conn, "t")
        assert "disk" in error
        assert "dashboards/schema.sql" in error


class TestInsertRows:
    def test_commits_once(self):
        conn, _ = mock_conn(rowcount=2)
        with patch(f"{MODULE}.execute_values") as execute:
            insert_rows(conn, "t", [(1,), (2,)])
        assert execute.call_count == 1
        conn.commit.assert_called_once()

    def test_batches_large_input(self):
        conn, _ = mock_conn(rowcount=500)
        rows = [(i,) for i in range(1100)]
        with patch(f"{MODULE}.execute_values") as execute:
            insert_rows(conn, "t", rows)
        assert execute.call_count == 3

    def test_returns_inserted_count(self):
        conn, _ = mock_conn(rowcount=7)
        with patch(f"{MODULE}.execute_values"):
            assert insert_rows(conn, "t", [(1,)]) == 7

    def test_statement_names_every_column_and_ignores_conflicts(self):
        conn, _ = mock_conn(rowcount=1)
        with patch(f"{MODULE}.execute_values") as execute:
            insert_rows(conn, "t", [(1,)])
        statement = repr(execute.call_args[0][1])
        assert "ON CONFLICT DO NOTHING" in statement
        for column in COLUMNS:
            assert f"Identifier('{column}')" in statement


# ---------------------------------------------------------------------------
# process_commit_timeseries
# ---------------------------------------------------------------------------


class TestProcessCommitTimeseries:
    def test_reports_inserted_and_skipped(self, tmp_path, capsys):
        rows = [sample_row(elapsed_sec=i) for i in range(4)]
        path = write_timeseries(tmp_path, "a" * 40, rows)
        conn, _ = mock_conn(rowcount=3)
        with patch(f"{MODULE}.execute_values"):
            result = process_commit_timeseries(path.parent, conn, "t", "cfg", False)
        assert result == (3, 1)
        assert "Inserted 3 rows into t, skipped 1" in capsys.readouterr().out

    def test_dry_run_does_not_insert(self, tmp_path, capsys):
        path = write_timeseries(tmp_path, "a" * 40, [sample_row()])
        with patch(f"{MODULE}.execute_values") as execute:
            result = process_commit_timeseries(path.parent, None, "t", "cfg", True)
        execute.assert_not_called()
        assert result == (0, 0)
        assert "would insert 1 rows" in capsys.readouterr().out

    def test_empty_file_is_skipped(self, tmp_path):
        path = write_timeseries(tmp_path, "a" * 40, [])
        assert process_commit_timeseries(path.parent, None, "t", "cfg", True) == (0, 0)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def write_metrics(tmp_path, commit):
    """Write a one-row metrics.json into a commit directory."""
    commit_dir = tmp_path / commit
    commit_dir.mkdir(exist_ok=True)
    metric = {"timestamp": "2026-09-11T17:30:07-04:00", "commit": commit}
    (commit_dir / "metrics.json").write_text(json.dumps([metric]))
    return commit_dir


def push_argv(tmp_path, *extra):
    """Return a push command line against tmp_path with connection flags."""
    return [
        "push_to_postgres.py",
        "--results-dir",
        str(tmp_path),
        "--table",
        "tiering",
        "--test-type",
        "tiering-mock",
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


def dry_run_argv(tmp_path, *extra):
    """Return a dry-run command line against tmp_path."""
    return ["push_to_postgres.py", "--results-dir", str(tmp_path), "--dry-run", *extra]


def run_main(argv, conn=None):
    """Run main with metrics and timeseries inserts mocked."""
    with (
        patch("sys.argv", argv),
        patch(f"{MODULE}.psycopg2.connect", return_value=conn) as connect,
        patch(f"{MODULE}.push_to_postgres", return_value=1) as push_metrics,
        patch(f"{MODULE}.execute_values") as execute,
    ):
        main()
    return connect, push_metrics, execute


def run_main_exit(argv, conn=None):
    """Run main and return its exit code."""
    with pytest.raises(SystemExit) as exit_info:
        run_main(argv, conn)
    return exit_info.value.code


def inserted_timeseries(execute):
    """Map every row passed to execute_values back to its column names."""
    return [as_dict(row) for call in execute.call_args_list for row in call[0][2]]


class TestMain:
    def test_dir_with_both_files_pushes_both(self, tmp_path):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        write_metrics(tmp_path, "a" * 40)
        conn, _ = mock_conn(fetchall=all_columns(), rowcount=1)
        _, push_metrics, execute = run_main(push_argv(tmp_path), conn)

        metrics, _, metrics_table, _ = push_metrics.call_args[0]
        assert metrics_table == "benchmark_metrics_tiering"
        assert metrics[0]["test_type"] == "tiering-mock"
        assert "benchmark_timeseries_tiering" in repr(execute.call_args[0][1])
        assert len(inserted_timeseries(execute)) == 1

    def test_config_name_taken_from_test_type(self, tmp_path):
        write_timeseries(tmp_path, "a" * 40, [sample_row(config_name="row")])
        conn, _ = mock_conn(fetchall=all_columns(), rowcount=1)
        _, _, execute = run_main(push_argv(tmp_path), conn)
        assert [row["config_name"] for row in inserted_timeseries(execute)] == [
            "tiering-mock"
        ]

    def test_dir_without_timeseries_is_unchanged(self, tmp_path, capsys):
        write_metrics(tmp_path, "a" * 40)
        conn, cursor = mock_conn()
        _, push_metrics, execute = run_main(push_argv(tmp_path), conn)
        push_metrics.assert_called_once()
        execute.assert_not_called()
        cursor.execute.assert_not_called()
        out = capsys.readouterr().out
        assert "timeseries rows" not in out
        assert "timeseries files" not in out

    def test_timeseries_only_dir_is_pushed(self, tmp_path):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        conn, _ = mock_conn(fetchall=all_columns(), rowcount=1)
        _, push_metrics, execute = run_main(push_argv(tmp_path), conn)
        push_metrics.assert_not_called()
        assert execute.call_count == 1

    def test_pushes_every_file(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        write_timeseries(tmp_path, "b" * 40, [sample_row(commit="b" * 40)])
        conn, _ = mock_conn(fetchall=all_columns(), rowcount=1)
        _, _, execute = run_main(push_argv(tmp_path), conn)
        assert execute.call_count == 2
        assert "Inserted 2 timeseries rows, skipped 0" in capsys.readouterr().out

    def test_missing_table_exits_1_before_metrics_push(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        write_metrics(tmp_path, "a" * 40)
        conn, _ = mock_conn(fetchall=[])
        with (
            patch("sys.argv", push_argv(tmp_path)),
            patch(f"{MODULE}.psycopg2.connect", return_value=conn),
            patch(f"{MODULE}.push_to_postgres") as push_metrics,
        ):
            with pytest.raises(SystemExit) as exit_info:
                main()
        assert exit_info.value.code == 1
        push_metrics.assert_not_called()
        err = capsys.readouterr().err
        assert "'benchmark_timeseries_tiering' does not exist" in err

    def test_table_without_raw_columns_exits_1(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        columns = [c for c in all_columns() if c[0] != "valkey_info"]
        conn, _ = mock_conn(fetchall=columns)
        assert run_main_exit(push_argv(tmp_path), conn) == 1
        err = capsys.readouterr().err
        assert "valkey_info" in err
        assert "dashboards/schema.sql" in err

    def test_malformed_file_exits_1(self, tmp_path, capsys):
        commit_dir = tmp_path / ("a" * 40)
        commit_dir.mkdir()
        (commit_dir / "timeseries.jsonl").write_text("not json\n")
        conn, _ = mock_conn(fetchall=all_columns())
        assert run_main_exit(push_argv(tmp_path), conn) == 1
        assert "line 1" in capsys.readouterr().err

    def test_dry_run_never_connects(self, tmp_path, capsys):
        write_timeseries(tmp_path, "a" * 40, [sample_row()])
        write_metrics(tmp_path, "a" * 40)
        connect, push_metrics, execute = run_main(dry_run_argv(tmp_path))
        connect.assert_not_called()
        execute.assert_not_called()
        assert push_metrics.call_args[0][3] is True
        out = capsys.readouterr().out
        assert "would insert 1 rows into benchmark_timeseries" in out

    def test_missing_results_dir_exits_1(self, tmp_path):
        assert run_main_exit(dry_run_argv(tmp_path / "nope")) == 1

    def test_invalid_table_identifier_rejected(self, tmp_path):
        with pytest.raises(ValueError):
            run_main(dry_run_argv(tmp_path, "--table", "bad name"))

    def test_sslmode_passed_to_connect(self, tmp_path):
        write_metrics(tmp_path, "a" * 40)
        conn, _ = mock_conn()
        connect, _, _ = run_main(push_argv(tmp_path, "--sslmode", "disable"), conn)
        assert connect.call_args.kwargs["sslmode"] == "disable"


# ---------------------------------------------------------------------------
# CLI argument rules
# ---------------------------------------------------------------------------


class TestCliArguments:
    def test_results_dir_required(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_defaults(self):
        args = build_parser().parse_args(["--results-dir", "/tmp"])
        assert args.port == 5432
        assert args.sslmode == "require"
        assert args.table == "core"
        assert args.test_type == "core"
        assert args.dry_run is False

    def test_config_name_option_removed(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--results-dir", "/tmp", "--config-name", "x"])

    def test_sslmode_override(self):
        args = build_parser().parse_args(
            ["--results-dir", "/tmp", "--sslmode", "disable"]
        )
        assert args.sslmode == "disable"

    def test_connection_flags_required_without_dry_run(self, tmp_path):
        assert (
            run_main_exit(["push_to_postgres.py", "--results-dir", str(tmp_path)]) == 2
        )

    def test_password_required_without_dry_run(self, tmp_path):
        assert run_main_exit(push_argv(tmp_path)[:-2]) == 2
