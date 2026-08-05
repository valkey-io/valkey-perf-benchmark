"""Integration tests for push_to_postgres.py against a real PostgreSQL instance.

Setup:
    These tests expect a PostgreSQL instance running at localhost:5433.
    Currently using Docker:

        docker run -d --name test-postgres -p 5433:5432 \
            -e POSTGRES_USER=testuser \
            -e POSTGRES_PASSWORD=valkey-search \
            -e POSTGRES_DB=testdb \
            postgres:15-alpine

    If Postgres is not available, all tests are skipped gracefully via pytest.skip().
"""

import json
import os
from pathlib import Path

import pytest

from .conftest import requires_postgres

from utils.push_to_postgres import (
    push_to_postgres,
    process_commit_metrics,
    get_existing_columns,
    create_or_update_table,
    analyze_metrics_schema,
)


@pytest.fixture
def metrics_table(pg_conn):
    """Create a unique metrics table name and drop it after the test."""
    table_name = f"test_metrics_{os.getpid()}"
    yield table_name, pg_conn
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_conn.commit()


def _sample_metrics(commit="abc123", command="GET", rps=150000.0):
    return {
        "timestamp": "2024-06-01T12:00:00Z",
        "commit": commit,
        "command": command,
        "data_size": 64,
        "pipeline": 1,
        "clients": 50,
        "rps": rps,
        "avg_latency_ms": 0.45,
        "p99_latency_ms": 1.2,
    }


@requires_postgres
class TestPushToPostgres:
    def test_basic_push_and_query(self, metrics_table):
        table, conn = metrics_table
        metrics = [_sample_metrics(), _sample_metrics(command="SET", rps=120000.0)]

        count = push_to_postgres(metrics, conn, table)
        assert count == 2

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 2

    def test_schema_evolution_adds_column(self, metrics_table):
        table, conn = metrics_table

        # First push: no p50 field
        metrics_v1 = [_sample_metrics()]
        push_to_postgres(metrics_v1, conn, table)

        columns_before = get_existing_columns(conn, table)
        assert "p50_latency_ms" not in columns_before
        # Verify all expected columns from _sample_metrics
        for col in ["id", "created_at", "timestamp", "commit", "command",
                    "data_size", "pipeline", "clients", "rps",
                    "avg_latency_ms", "p99_latency_ms"]:
            assert col in columns_before, f"Expected column '{col}' missing after first push"

        # Second push: adds p50_latency_ms
        metrics_v2 = [
            {**_sample_metrics(commit="def456"), "p50_latency_ms": 0.3}
        ]
        push_to_postgres(metrics_v2, conn, table)

        columns_after = get_existing_columns(conn, table)
        assert "p50_latency_ms" in columns_after

        # Old row should have NULL for new column
        with conn.cursor() as cur:
            cur.execute(f"SELECT p50_latency_ms FROM {table} WHERE commit = 'abc123'")
            assert cur.fetchone()[0] is None

    def test_dry_run_no_side_effects(self, metrics_table):
        table, conn = metrics_table
        metrics = [_sample_metrics()]

        count = push_to_postgres(metrics, conn, table, dry_run=True)
        assert count == 1

        # Table should not exist
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = %s)",
                (table,),
            )
            assert cur.fetchone()[0] is False

    def test_correct_column_types(self, metrics_table):
        table, conn = metrics_table
        metrics = [_sample_metrics()]
        push_to_postgres(metrics, conn, table)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = %s",
                (table,),
            )
            col_types = {row[0]: row[1] for row in cur.fetchall()}

        assert col_types["rps"] == "numeric"  # DECIMAL maps to numeric
        assert col_types["pipeline"] == "integer"
        assert "timestamp" in col_types["timestamp"].lower()


@requires_postgres
class TestCreateOrUpdateTable:
    def test_creates_table_and_adds_missing_columns(self, metrics_table):
        table, conn = metrics_table

        # Create with initial schema
        schema_v1 = analyze_metrics_schema([_sample_metrics()])
        create_or_update_table(conn, schema_v1, table)

        columns = get_existing_columns(conn, table)
        assert "commit" in columns
        assert "timestamp" in columns
        assert "rps" in columns
        assert "id" in columns
        assert "created_at" in columns

        # Check indexes were created
        with conn.cursor() as cur:
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename = %s",
                (table,),
            )
            indexes = [r[0] for r in cur.fetchall()]
        assert f"idx_{table}_commit" in indexes
        assert f"idx_{table}_timestamp" in indexes
        assert f"idx_{table}_command" in indexes
        assert f"idx_{table}_config" in indexes

        # Update with extended schema — adds new column, keeps existing
        assert "new_field" not in columns
        schema_v2 = {**schema_v1, "new_field": "INTEGER"}
        create_or_update_table(conn, schema_v2, table)

        columns_after = get_existing_columns(conn, table)
        assert "new_field" in columns_after
        assert "rps" in columns_after
        assert "commit" in columns_after


@requires_postgres
class TestProcessCommitMetrics:
    def test_with_test_type(self, metrics_table, tmp_path):
        table, conn = metrics_table
        commit_dir = tmp_path / "abc123"
        commit_dir.mkdir()
        (commit_dir / "metrics.json").write_text(json.dumps([_sample_metrics()]))

        count, skipped = process_commit_metrics(commit_dir, conn, table, test_type="fts")
        assert count == 1
        assert skipped is False

        # Verify test_type was added
        with conn.cursor() as cur:
            cur.execute(f"SELECT test_type FROM {table}")
            assert cur.fetchone()[0] == "fts"

    def test_skips_missing_file(self, metrics_table, tmp_path):
        """process_commit_metrics skips gracefully when no metrics.json exists."""
        table, conn = metrics_table
        commit_dir = tmp_path / "nofile"
        commit_dir.mkdir()

        count, skipped = process_commit_metrics(commit_dir, conn, table)
        assert count == 0
        assert skipped is True
