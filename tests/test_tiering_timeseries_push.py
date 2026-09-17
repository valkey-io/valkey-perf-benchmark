"""Tests for pushing a per-second benchmark time series through push_to_postgres.

The data tiering benchmark emits one row per (commit, scenario, elapsed_sec)
instead of one row per benchmark configuration. Run identity fields (timestamp,
commit, command, data_size, pipeline, clients) are denormalized onto every
per-second row so that create_indexes(), which hardcodes those column names,
runs unmodified.

These tests cover the three stages that a per-second row set has to survive:
schema inference (analyze_metrics_schema), table creation with the hardcoded
indexes (create_or_update_table plus create_indexes), and the bulk insert
conversion (convert_metrics_to_rows, push_to_postgres). psycopg2 is mocked, so
no database is involved.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from utils.push_to_postgres import (
    analyze_metrics_schema,
    convert_metrics_to_rows,
    create_or_update_table,
    push_to_postgres,
    resolve_table_name,
)

# Development placeholder identifier, resolved through resolve_table_name.
TIERING_TABLE_ID = "tiering_ts"

# Columns create_indexes() indexes unconditionally. A table missing any of these
# fails at the index step, so every per-second row must carry all of them.
HARDCODED_INDEX_COLUMNS = [
    "commit",
    "timestamp",
    "command",
    "data_size",
    "pipeline",
    "clients",
]

SAMPLE_COUNT = 60


def per_second_metrics(
    commit="a1b2c3d4e5f6", scenario="zipfian-80-20", count=SAMPLE_COUNT
):
    """Build a per-second metric row set sharing one commit and scenario.

    Args:
        commit: Commit sha carried on every row.
        scenario: Scenario id carried on every row.
        count: Number of one second samples to generate.

    Returns:
        List of metric dicts differing only in elapsed_sec, timestamp and values.
    """
    start = datetime(2026, 3, 1, 12, 0, 0)
    rows = []
    for elapsed in range(count):
        rows.append(
            {
                # Denormalized run identity, identical on every row
                "timestamp": (start + timedelta(seconds=elapsed)).isoformat(),
                "commit": commit,
                "command": "SET",
                "data_size": 400,
                "pipeline": 1,
                "clients": 200,
                "architecture": "aarch64",
                "scenario": scenario,
                # Time series axis
                "elapsed_sec": elapsed,
                # Per-second metrics
                "ops_per_sec": 120000 + elapsed,
                "used_memory": 34_359_738_368 + elapsed,
                "keyspace_hits": 1000 * elapsed,
                "keyspace_misses": 7 * elapsed,
                "total_num_items_spilled_to_ext_storage": 50 * elapsed,
                "total_num_items_fetched_from_ext_storage": 25 * elapsed,
                "disk_hit_pct": 12.5,
                "valkey_cpu_total": 98.4,
            }
        )
    return rows


def executed_statements(cursor):
    """Return repr strings of every statement passed to cursor.execute.

    psycopg2 sql.Composed objects need a live connection for as_string, so the
    repr (which names every Identifier) is used for assertions instead.
    """
    return [repr(call.args[0]) for call in cursor.execute.call_args_list]


@pytest.fixture
def mock_new_table_conn():
    """Mock connection whose target table does not exist yet."""
    conn = MagicMock()
    cursor = MagicMock()
    # create_or_update_table checks table existence via fetchone()[0]
    cursor.fetchone.return_value = (False,)
    cursor.fetchall.return_value = []
    cursor.rowcount = SAMPLE_COUNT
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# Placeholder table name
# ---------------------------------------------------------------------------


class TestTieringTableName:
    def test_placeholder_id_resolves_without_code_change(self):
        assert resolve_table_name(TIERING_TABLE_ID) == "benchmark_metrics_tiering_ts"


# ---------------------------------------------------------------------------
# Schema inference over many per-second rows
# ---------------------------------------------------------------------------


class TestPerSecondSchemaInference:
    def test_infers_schema_from_sixty_rows(self):
        schema = analyze_metrics_schema(per_second_metrics())
        assert schema["id"] == "SERIAL PRIMARY KEY"
        assert schema["created_at"] == "TIMESTAMPTZ DEFAULT NOW()"

    def test_elapsed_sec_is_integer(self):
        schema = analyze_metrics_schema(per_second_metrics())
        assert schema["elapsed_sec"] == "INTEGER"

    def test_scenario_is_varchar(self):
        schema = analyze_metrics_schema(per_second_metrics())
        assert schema["scenario"] == "VARCHAR(50)"

    def test_all_hardcoded_index_columns_present(self):
        schema = analyze_metrics_schema(per_second_metrics())
        for column in HARDCODED_INDEX_COLUMNS:
            assert column in schema, f"{column} missing, create_indexes would fail"

    def test_identity_columns_keep_not_null_types(self):
        schema = analyze_metrics_schema(per_second_metrics())
        assert schema["timestamp"] == "TIMESTAMPTZ NOT NULL"
        assert schema["commit"] == "VARCHAR(255) NOT NULL"
        assert schema["command"] == "TEXT NOT NULL"

    def test_column_present_even_when_every_value_is_none(self):
        # A field is registered from its key, before the None check, so an
        # always-null identity field still yields a column for create_indexes.
        rows = per_second_metrics(count=3)
        for row in rows:
            row["data_size"] = None
        schema = analyze_metrics_schema(rows)
        assert "data_size" in schema

    def test_absent_identity_field_yields_no_column(self):
        rows = per_second_metrics(count=3)
        for row in rows:
            del row["data_size"]
        schema = analyze_metrics_schema(rows)
        assert "data_size" not in schema


# ---------------------------------------------------------------------------
# Table creation plus the hardcoded indexes
# ---------------------------------------------------------------------------


class TestPerSecondTableCreation:
    def test_creates_table_with_elapsed_sec_and_scenario(self, mock_new_table_conn):
        conn, cursor = mock_new_table_conn
        schema = analyze_metrics_schema(per_second_metrics())
        create_or_update_table(conn, schema, "benchmark_metrics_tiering_ts")

        create_stmt = next(
            stmt for stmt in executed_statements(cursor) if "CREATE TABLE" in stmt
        )
        assert "Identifier('elapsed_sec')" in create_stmt
        assert "Identifier('scenario')" in create_stmt
        assert "Identifier('benchmark_metrics_tiering_ts')" in create_stmt

    def test_creates_all_four_hardcoded_indexes(self, mock_new_table_conn):
        conn, cursor = mock_new_table_conn
        schema = analyze_metrics_schema(per_second_metrics())
        create_or_update_table(conn, schema, "benchmark_metrics_tiering_ts")

        index_stmts = [
            stmt for stmt in executed_statements(cursor) if "CREATE INDEX" in stmt
        ]
        assert len(index_stmts) == 4
        joined = " ".join(index_stmts)
        for suffix in ["commit", "timestamp", "command", "config"]:
            assert f"idx_benchmark_metrics_tiering_ts_{suffix}" in joined

    def test_composite_index_columns_all_exist_in_created_table(
        self, mock_new_table_conn
    ):
        conn, cursor = mock_new_table_conn
        schema = analyze_metrics_schema(per_second_metrics())
        create_or_update_table(conn, schema, "benchmark_metrics_tiering_ts")

        statements = executed_statements(cursor)
        create_stmt = next(stmt for stmt in statements if "CREATE TABLE" in stmt)
        config_stmt = next(
            stmt
            for stmt in statements
            if "idx_benchmark_metrics_tiering_ts_config" in stmt
        )
        for column in ["commit", "command", "data_size", "pipeline", "clients"]:
            assert f"Identifier('{column}')" in config_stmt
            assert f"Identifier('{column}')" in create_stmt

    def test_missing_identity_field_leaves_index_referencing_absent_column(
        self, mock_new_table_conn
    ):
        # Mechanical proof that the failure lands at the index step and not at
        # insert time: create_indexes still names data_size even though the
        # CREATE TABLE statement no longer declares it.
        conn, cursor = mock_new_table_conn
        rows = per_second_metrics(count=5)
        for row in rows:
            del row["data_size"]
        schema = analyze_metrics_schema(rows)
        create_or_update_table(conn, schema, "benchmark_metrics_tiering_ts")

        statements = executed_statements(cursor)
        create_stmt = next(stmt for stmt in statements if "CREATE TABLE" in stmt)
        config_stmt = next(
            stmt
            for stmt in statements
            if "idx_benchmark_metrics_tiering_ts_config" in stmt
        )
        assert "Identifier('data_size')" not in create_stmt
        assert "Identifier('data_size')" in config_stmt


# ---------------------------------------------------------------------------
# Bulk insert conversion
# ---------------------------------------------------------------------------


class TestPerSecondRowConversion:
    def test_all_sixty_rows_convert(self):
        metrics = per_second_metrics()
        schema = analyze_metrics_schema(metrics)
        columns = [c for c in schema if c not in ("id", "created_at")]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == SAMPLE_COUNT
        assert skipped == 0

    def test_elapsed_sec_values_preserved_in_order(self):
        metrics = per_second_metrics()
        schema = analyze_metrics_schema(metrics)
        columns = [c for c in schema if c not in ("id", "created_at")]
        rows, _ = convert_metrics_to_rows(metrics, columns)
        elapsed_index = columns.index("elapsed_sec")
        assert [row[elapsed_index] for row in rows] == list(range(SAMPLE_COUNT))

    def test_timestamps_parsed_and_distinct(self):
        metrics = per_second_metrics()
        schema = analyze_metrics_schema(metrics)
        columns = [c for c in schema if c not in ("id", "created_at")]
        rows, _ = convert_metrics_to_rows(metrics, columns)
        timestamp_index = columns.index("timestamp")
        timestamps = [row[timestamp_index] for row in rows]
        assert all(isinstance(value, datetime) for value in timestamps)
        assert len(set(timestamps)) == SAMPLE_COUNT

    def test_shared_identity_repeated_on_every_row(self):
        metrics = per_second_metrics()
        schema = analyze_metrics_schema(metrics)
        columns = [c for c in schema if c not in ("id", "created_at")]
        rows, _ = convert_metrics_to_rows(metrics, columns)
        commit_index = columns.index("commit")
        scenario_index = columns.index("scenario")
        assert {row[commit_index] for row in rows} == {"a1b2c3d4e5f6"}
        assert {row[scenario_index] for row in rows} == {"zipfian-80-20"}

    def test_row_missing_timestamp_is_skipped_others_survive(self):
        metrics = per_second_metrics()
        del metrics[10]["timestamp"]
        schema = analyze_metrics_schema(metrics)
        columns = [c for c in schema if c not in ("id", "created_at")]
        rows, skipped = convert_metrics_to_rows(metrics, columns)
        assert len(rows) == SAMPLE_COUNT - 1
        assert skipped == 1


# ---------------------------------------------------------------------------
# Full push path
# ---------------------------------------------------------------------------


class TestPerSecondPushPath:
    def test_push_inserts_every_sample(self, mock_new_table_conn):
        conn, cursor = mock_new_table_conn
        with patch("utils.push_to_postgres.execute_values") as mock_insert:
            count = push_to_postgres(
                per_second_metrics(), conn, "benchmark_metrics_tiering_ts"
            )

        assert count == SAMPLE_COUNT
        inserted_rows = mock_insert.call_args.args[2]
        assert len(inserted_rows) == SAMPLE_COUNT
        conn.commit.assert_called()

    def test_push_targets_placeholder_table(self, mock_new_table_conn):
        conn, cursor = mock_new_table_conn
        table_name = resolve_table_name(TIERING_TABLE_ID)
        with patch("utils.push_to_postgres.execute_values") as mock_insert:
            push_to_postgres(per_second_metrics(), conn, table_name)

        insert_stmt = repr(mock_insert.call_args.args[1])
        assert "Identifier('benchmark_metrics_tiering_ts')" in insert_stmt
        assert "Identifier('elapsed_sec')" in insert_stmt

    def test_dry_run_needs_no_connection(self):
        count = push_to_postgres(
            per_second_metrics(), None, "benchmark_metrics_tiering_ts", dry_run=True
        )
        assert count == SAMPLE_COUNT
