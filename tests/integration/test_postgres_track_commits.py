"""Integration tests for postgres_track_commits.py against a real PostgreSQL instance.

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

import os

import pytest

from .conftest import requires_postgres, GitRepoFixture

from utils.postgres_track_commits import (
    create_tables,
    mark_commits,
    cleanup_incomplete_commits,
    determine_commits_to_benchmark,
    get_commits_by_config,
    get_unique_configs,
    _resolve_module_table_name,
    CORE_TABLE_NAME,
)


@pytest.fixture
def track_table(pg_conn):
    """Create a unique tracking table and drop it after the test."""
    table_name = f"test_track_{os.getpid()}"
    create_tables(pg_conn, table_name, table_id="core")
    yield table_name, pg_conn
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_conn.commit()


@pytest.fixture
def repo_with_commits(tmp_path) -> GitRepoFixture:
    """Git repo with 5 commits on main branch."""
    repo = GitRepoFixture(tmp_path / "repo")
    for i in range(4):  # initial commit + 4 more = 5 total
        repo.create_commit(f"Commit {i+2}")
    return repo


@requires_postgres
class TestCreateTables:
    def test_core_table_columns_indexes_constraints(self, pg_conn):
        try:
            create_tables(pg_conn, table_id="core")

            # Check columns
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (CORE_TABLE_NAME,),
                )
                columns = [r[0] for r in cur.fetchall()]
            assert "id" in columns
            assert "sha" in columns
            assert "timestamp" in columns
            assert "status" in columns
            assert "config" in columns
            assert "architecture" in columns
            assert "created_at" in columns
            assert "updated_at" in columns

            # Check indexes (core uses prefix "_")
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = %s",
                    (CORE_TABLE_NAME,),
                )
                indexes = [r[0] for r in cur.fetchall()]
            assert "idx_commits_sha" in indexes
            assert "idx_commits_status" in indexes
            assert "idx_commits_timestamp" in indexes
            assert "idx_commits_config" in indexes
            assert "idx_commits_sha_status" in indexes

            # Check unique constraint
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_name = %s AND constraint_type = 'UNIQUE'",
                    (CORE_TABLE_NAME,),
                )
                constraints = [r[0] for r in cur.fetchall()]
            assert "unique_sha_config_arch" in constraints

        finally:
            with pg_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {CORE_TABLE_NAME}")
            pg_conn.commit()

    def test_module_table_columns_indexes_constraints(self, pg_conn):
        table = _resolve_module_table_name("search")
        try:
            create_tables(pg_conn, table, table_id="search")

            # Check columns (same schema as core)
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                columns = [r[0] for r in cur.fetchall()]
            assert "sha" in columns
            assert "config" in columns
            assert "architecture" in columns

            # Check indexes (module uses prefix "_search_")
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = %s",
                    (table,),
                )
                indexes = [r[0] for r in cur.fetchall()]
            assert "idx_search_commits_sha" in indexes
            assert "idx_search_commits_status" in indexes
            assert "idx_search_commits_timestamp" in indexes
            assert "idx_search_commits_config" in indexes
            assert "idx_search_commits_sha_status" in indexes

            # Check unique constraint with module prefix
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_name = %s AND constraint_type = 'UNIQUE'",
                    (table,),
                )
                constraints = [r[0] for r in cur.fetchall()]
            assert "unique_search_sha_config_arch" in constraints

        finally:
            with pg_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            pg_conn.commit()

    def test_idempotent(self, pg_conn):
        table = f"test_idem_{os.getpid()}"
        try:
            create_tables(pg_conn, table, table_id="core")
            create_tables(pg_conn, table, table_id="core")  # no error
        finally:
            with pg_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            pg_conn.commit()


@requires_postgres
class TestMarkAndQuery:
    def test_mark_upsert_and_architecture_isolation(
        self, track_table, repo_with_commits
    ):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()
        config = [{"data_sizes": [16, 64], "io-threads": 1}]

        # Mark as in_progress, verify
        mark_commits(conn, repo.path, [sha], "in_progress", "x86_64", config, table)
        results = get_commits_by_config(conn, "x86_64", config, table)
        assert len(results) == 1
        assert results[0]["sha"] == sha
        assert results[0]["status"] == "in_progress"

        # Upsert to complete, verify only 1 row and status updated
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config, table)
        results = get_commits_by_config(conn, "x86_64", config, table)
        assert len(results) == 1
        assert results[0]["status"] == "complete"

        # arm64 should see nothing
        results = get_commits_by_config(conn, "arm64", config, table)
        assert len(results) == 0

    def test_head_resolves_to_current_sha(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        current_sha = repo.get_current_commit()
        config = [{"data_sizes": [16]}]

        mark_commits(conn, repo.path, ["HEAD"], "complete", "x86_64", config, table)

        results = get_commits_by_config(conn, "x86_64", config, table)
        assert len(results) == 1
        assert results[0]["sha"] == current_sha  # resolved, not "HEAD"

    def test_mark_multiple_shas_in_one_call(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        config = [{"data_sizes": [16]}]

        # All 5 commit SHAs from the repo
        shas = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=config,
            table_name=table,
        )
        assert len(shas) == 5

        mark_commits(conn, repo.path, shas, "complete", "x86_64", config, table)

        results = get_commits_by_config(conn, "x86_64", config, table)
        assert len(results) == 5
        assert {r["sha"] for r in results} == set(shas)

    def test_get_commits_by_config_none_returns_all(
        self, track_table, repo_with_commits
    ):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        config_a = [{"data_sizes": [16]}]
        config_b = [{"data_sizes": [64]}]
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config_a, table)
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config_b, table)

        # config=None -> no config filter, both rows returned
        results = get_commits_by_config(conn, "x86_64", config=None, table_name=table)
        assert len(results) == 2


@requires_postgres
class TestCleanup:
    def test_removes_in_progress(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()
        config = [{"data_sizes": [16]}]

        mark_commits(conn, repo.path, [sha], "in_progress", "x86_64", config, table)
        removed = cleanup_incomplete_commits(
            conn, table, config=config, architecture="x86_64"
        )
        assert removed == 1

        results = get_commits_by_config(conn, "x86_64", config, table)
        assert len(results) == 0

    def test_does_not_remove_complete(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()
        config = [{"data_sizes": [16]}]

        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config, table)
        removed = cleanup_incomplete_commits(
            conn, table, config=config, architecture="x86_64"
        )
        assert removed == 0


@requires_postgres
class TestDetermineCommits:
    def test_determine_returns_skips_and_respects_max(
        self, track_table, repo_with_commits
    ):
        table, conn = track_table
        repo = repo_with_commits
        config = [{"data_sizes": [16]}]

        # All 5 commits unbenchmarked
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=config,
            table_name=table,
        )
        assert len(commits) == 5

        # Mark one as complete, should return 4
        sha = repo.get_current_commit()
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config, table)
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=config,
            table_name=table,
        )
        assert sha not in commits
        assert len(commits) == 4

        # Respects max_commits
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=2,
            architecture="x86_64",
            config=config,
            table_name=table,
        )
        assert len(commits) == 2


@requires_postgres
class TestSubsetDetection:
    def test_list_config_subset_skipped(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        superset_config = [{"data_sizes": [16, 64, 256], "io-threads": 1}]
        mark_commits(
            conn, repo.path, [sha], "complete", "x86_64", superset_config, table
        )

        subset_config = [{"data_sizes": [16, 64], "io-threads": 1}]
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=subset_config,
            table_name=table,
        )
        assert sha not in commits

    def test_dict_config_subset_skipped(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        superset_config = {"data_sizes": [16, 64, 256], "io-threads": 1, "tls": True}
        mark_commits(
            conn, repo.path, [sha], "complete", "x86_64", superset_config, table
        )

        subset_config = {"data_sizes": [16, 64], "io-threads": 1}
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=subset_config,
            table_name=table,
        )
        assert sha not in commits

    def test_non_subset_not_skipped(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        stored_config = [{"data_sizes": [16], "io-threads": 1}]
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", stored_config, table)

        different_config = [{"data_sizes": [64, 128], "io-threads": 2}]
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=different_config,
            table_name=table,
        )
        assert sha in commits

    def test_disable_subset_detection_does_not_skip(
        self, track_table, repo_with_commits
    ):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        superset_config = [{"data_sizes": [16, 64, 256], "io-threads": 1}]
        mark_commits(
            conn, repo.path, [sha], "complete", "x86_64", superset_config, table
        )

        subset_config = [{"data_sizes": [16, 64], "io-threads": 1}]
        commits = determine_commits_to_benchmark(
            conn,
            repo.path,
            "HEAD",
            max_commits=10,
            architecture="x86_64",
            config=subset_config,
            enable_subset_detection=False,
            table_name=table,
        )
        # Detection is off, so the subset is still returned for benchmarking
        assert sha in commits

    def test_subset_detection_with_module_table(self, pg_conn, repo_with_commits):
        core_table = CORE_TABLE_NAME
        module_table = _resolve_module_table_name("search")
        try:
            create_tables(pg_conn, core_table, table_id="core")
            create_tables(pg_conn, module_table, table_id="search")
            repo = repo_with_commits
            sha = repo.get_current_commit()

            # Store superset in core table
            superset_config = [{"data_sizes": [16, 64, 256]}]
            mark_commits(
                pg_conn,
                repo.path,
                [sha],
                "complete",
                "x86_64",
                superset_config,
                core_table,
            )

            # Query module table with subset — should NOT be skipped (different table)
            subset_config = [{"data_sizes": [16]}]
            commits = determine_commits_to_benchmark(
                pg_conn,
                repo.path,
                "HEAD",
                max_commits=10,
                architecture="x86_64",
                config=subset_config,
                table_name=module_table,
            )
            assert sha in commits

            # Now store superset in module table too — should be skipped
            mark_commits(
                pg_conn,
                repo.path,
                [sha],
                "complete",
                "x86_64",
                superset_config,
                module_table,
            )
            commits = determine_commits_to_benchmark(
                pg_conn,
                repo.path,
                "HEAD",
                max_commits=10,
                architecture="x86_64",
                config=subset_config,
                table_name=module_table,
            )
            assert sha not in commits
        finally:
            with pg_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {core_table}")
                cur.execute(f"DROP TABLE IF EXISTS {module_table}")
            pg_conn.commit()


@requires_postgres
class TestGetUniqueConfigs:
    def test_returns_distinct_configs(self, track_table, repo_with_commits):
        table, conn = track_table
        repo = repo_with_commits
        sha = repo.get_current_commit()

        config_a = [{"data_sizes": [16], "io-threads": 1}]
        config_b = [{"data_sizes": [64], "io-threads": 2}]

        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config_a, table)
        mark_commits(conn, repo.path, [sha], "complete", "x86_64", config_b, table)

        configs = get_unique_configs(conn, table)
        assert len(configs) == 2
