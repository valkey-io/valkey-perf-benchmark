"""Integration tests for module_postgres_track_commits.py using a real PostgreSQL database.

Setup:
    These tests expect a PostgreSQL instance running at localhost:5433.
    Currently using Docker:
        docker run -d --name test-postgres -p 5433:5432 \
            -e POSTGRES_USER=testuser \
            -e POSTGRES_PASSWORD=valkey-search \
            -e POSTGRES_DB=testdb \
            postgres:15-alpine

    If Postgres is not available, all tests are skipped gracefully via pytest.skip().

What's tested:
    - _create_module_table
    - populate_module_commits
    - fetch_next_module_commits
    - mark_module_commits
    - cleanup_module_commits
    - _assign_priority_in_memory
    - Subset detection (_mark_subset_pairs_in_memory)
    - Full lifecycle (populate → fetch → mark → re-populate)
    - Config+arch isolation
    - Large cartesian product (10×10 scale)

"""

import pytest
from unittest.mock import patch
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

from utils.module_postgres_track_commits import (
    CommitPair,
    _assign_priority_in_memory,
    _create_module_table,
    _module_table_name,
    _parse_timestamp,
    cleanup_module_commits,
    fetch_next_module_commits,
    mark_module_commits,
    populate_module_commits,
)

# ---------------------------------------------------------------------------
# Connection config — matches the Docker container setup
# ---------------------------------------------------------------------------

TEST_DB_HOST = "localhost"
TEST_DB_PORT = 5433
TEST_DB_NAME = "testdb"
TEST_DB_USER = "testuser"
TEST_DB_PASS = "valkey-search"

# Test constants used across all tests
MODULE_NAME = "test_search"
CONFIG_NAME = "test-config.json"
ARCHITECTURE = "aarch64"
CONFIG_SETS = [
    {"io-threads": 8, "search.reader-threads": 1, "search.writer-threads": 1}
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """Create a real PostgreSQL connection for testing.

    Skips the entire test if Postgres is not available (e.g., Docker not running).
    """
    try:
        connection = psycopg2.connect(
            host=TEST_DB_HOST,
            port=TEST_DB_PORT,
            database=TEST_DB_NAME,
            user=TEST_DB_USER,
            password=TEST_DB_PASS,
            connect_timeout=5,
        )
        yield connection
        connection.close()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not available for integration tests")


@pytest.fixture(autouse=True)
def clean_table(conn):

    table = _module_table_name(MODULE_NAME)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    _create_module_table(conn, MODULE_NAME)
    yield
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


@pytest.fixture
def mock_git():
    with patch(
        "utils.module_postgres_track_commits.git_rev_list_with_timestamps"
    ) as mock_batch:
        yield mock_batch


# ---------------------------------------------------------------------------
# populate_module_commits
# ---------------------------------------------------------------------------


class TestPopulateIntegration:

    def test_inserts_cartesian_product(self, conn, mock_git):
        mock_git.side_effect = [
            {
                "core1": "2026-06-04T10:00:00+00:00",
                "core2": "2026-06-03T10:00:00+00:00",
            },
            {"mod1": "2026-06-02T10:00:00+00:00", "mod2": "2026-06-01T10:00:00+00:00"},
        ]

        result = populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        assert result == 4

        # Verify all 4 expected pairs exist with correct status, config, and timestamps
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, module_sha, status, config_name, architecture, priority, "
                f"core_timestamp, module_timestamp, max_commit_timestamp, min_commit_timestamp "
                f"FROM {table} ORDER BY sha, module_sha"
            )
            rows = cur.fetchall()

        assert len(rows) == 4
        expected = [
            # (sha, mod, expected_core_ts, expected_mod_ts, expected_max_ts, expected_min_ts)
            (
                "core1",
                "mod1",
                "2026-06-04T10:00:00+00:00",
                "2026-06-02T10:00:00+00:00",
                "2026-06-04T10:00:00+00:00",
                "2026-06-02T10:00:00+00:00",
            ),
            (
                "core1",
                "mod2",
                "2026-06-04T10:00:00+00:00",
                "2026-06-01T10:00:00+00:00",
                "2026-06-04T10:00:00+00:00",
                "2026-06-01T10:00:00+00:00",
            ),
            (
                "core2",
                "mod1",
                "2026-06-03T10:00:00+00:00",
                "2026-06-02T10:00:00+00:00",
                "2026-06-03T10:00:00+00:00",
                "2026-06-02T10:00:00+00:00",
            ),
            (
                "core2",
                "mod2",
                "2026-06-03T10:00:00+00:00",
                "2026-06-01T10:00:00+00:00",
                "2026-06-03T10:00:00+00:00",
                "2026-06-01T10:00:00+00:00",
            ),
        ]
        for row, (
            exp_sha,
            exp_mod,
            exp_core_ts,
            exp_mod_ts,
            exp_max_ts,
            exp_min_ts,
        ) in zip(rows, expected):
            (
                sha,
                module_sha,
                status,
                config_name,
                architecture,
                priority,
                core_ts,
                mod_ts,
                max_ts,
                min_ts,
            ) = row
            assert sha == exp_sha
            assert module_sha == exp_mod
            assert status == "pending"
            assert config_name == CONFIG_NAME
            assert architecture == ARCHITECTURE
            assert priority == 1  # no pointer yet → all forward
            assert core_ts.isoformat() == exp_core_ts
            assert mod_ts.isoformat() == exp_mod_ts
            assert max_ts.isoformat() == exp_max_ts
            assert min_ts.isoformat() == exp_min_ts

    def test_new_config_gets_separate_rows(self, conn, mock_git):
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS,
        )

        # Same pair, different config — should insert a new row
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        result = populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-B.json",
            CONFIG_SETS,
        )

        assert result == 1

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 2


# ---------------------------------------------------------------------------
# fetch_next_module_commits
# ---------------------------------------------------------------------------


class TestFetchNextIntegration:

    def _populate(self, conn, mock_git, core_shas, mod_shas):
        """Helper to populate the queue with given SHAs."""
        mock_git.side_effect = [
            {sha: "2026-06-01T10:00:00+00:00" for sha in core_shas},
            {sha: "2026-06-01T10:00:00+00:00" for sha in mod_shas},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

    def test_fetches_pending_pairs(self, conn, mock_git):
        self._populate(conn, mock_git, ["core1"], ["mod1"])

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        assert pairs == ["core1:mod1"]

    def test_marks_fetched_as_in_progress(self, conn, mock_git):
        self._populate(conn, mock_git, ["core1"], ["mod1"])
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} "
                f"WHERE sha = 'core1' AND module_sha = 'mod1' "
                f"AND config_name = %s AND architecture = %s",
                (CONFIG_NAME, ARCHITECTURE),
            )
            assert cur.fetchone()[0] == "in_progress"

    def test_does_not_fetch_in_progress(self, conn, mock_git):
        self._populate(conn, mock_git, ["core1", "core2"], ["mod1"])

        # First fetch takes one pair
        first, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )
        # Second fetch should return the OTHER pair
        second, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        assert len(second) == 1
        assert second[0] != first[0]

    def test_returns_empty_when_all_in_progress(self, conn, mock_git):
        self._populate(conn, mock_git, ["core1"], ["mod1"])
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        assert pairs == []

    def test_respects_max_pairs(self, conn, mock_git):
        self._populate(conn, mock_git, ["core1", "core2", "core3"], ["mod1"])

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=2
        )

        assert len(pairs) == 2

        # Verify exactly 2 rows are in_progress, 1 still pending (for this config+arch)
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE status = 'in_progress' AND config_name = %s AND architecture = %s",
                (CONFIG_NAME, ARCHITECTURE),
            )
            assert cur.fetchone()[0] == 2
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE status = 'pending' AND config_name = %s AND architecture = %s",
                (CONFIG_NAME, ARCHITECTURE),
            )
            assert cur.fetchone()[0] == 1

    def test_does_not_fetch_other_config(self, conn, mock_git):
        # Populate with config-A
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS,
        )

        # Populate same SHAs with config-B
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-B.json",
            CONFIG_SETS,
        )

        # Fetch for config-A — should get config-A's pair
        pairs_a, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS,
            ARCHITECTURE,
            max_pairs=1,
        )
        assert pairs_a == ["core1:mod1"]

        # Verify config-A's row is now in_progress
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} "
                f"WHERE sha = 'core1' AND module_sha = 'mod1' AND config_name = %s",
                ("config-A.json",),
            )
            assert cur.fetchone()[0] == "in_progress"

            # Verify config-B's row is still pending (untouched)
            cur.execute(
                f"SELECT status FROM {table} "
                f"WHERE sha = 'core1' AND module_sha = 'mod1' AND config_name = %s",
                ("config-B.json",),
            )
            assert cur.fetchone()[0] == "pending"


# ---------------------------------------------------------------------------
# mark_module_commits
# ---------------------------------------------------------------------------


class TestMarkModuleCommitsIntegration:
    """Verify marking pairs as complete works with config/arch checks."""

    def _setup_in_progress(self, conn, mock_git):
        """Helper: populate one pair and fetch it (→ in_progress).
        Simulates the state right after fetch_next but before benchmark completes.
        """
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

    def test_marks_complete(self, conn, mock_git):
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        assert result == 1

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status, updated_at, created_at FROM {table} "
                f"WHERE sha = 'core1' AND module_sha = 'mod1'"
            )
            row = cur.fetchone()
            assert row[0] == "completed"
            # updated_at should be >= created_at (it was modified after creation)
            assert row[1] >= row[2]

    def test_wrong_config_does_not_match(self, conn, mock_git):
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            "wrong-config.json",
            CONFIG_SETS,
            ARCHITECTURE,
        )

        assert result == 0

        # Verify row is still in_progress (unchanged)
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} WHERE sha = 'core1' AND module_sha = 'mod1'"
            )
            assert cur.fetchone()[0] == "in_progress"

    def test_wrong_arch_does_not_match(self, conn, mock_git):
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn, MODULE_NAME, ["core1:mod1"], CONFIG_NAME, CONFIG_SETS, "x86_64"
        )

        assert result == 0

        # Verify row is still in_progress (unchanged)
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} WHERE sha = 'core1' AND module_sha = 'mod1'"
            )
            assert cur.fetchone()[0] == "in_progress"

    def test_completed_pair_not_fetched_again(self, conn, mock_git):
        self._setup_in_progress(conn, mock_git)
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        assert pairs == []


# ---------------------------------------------------------------------------
# cleanup_module_commits
# ---------------------------------------------------------------------------


class TestCleanupIntegration:
    """Verify cleanup resets in_progress → pending, scoped to config/arch."""

    def test_resets_in_progress_to_pending(self, conn, mock_git):
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        result = cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        assert result == 1

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE sha = 'core1'")
            assert cur.fetchone()[0] == "pending"

    def test_does_not_affect_other_config(self, conn, mock_git):
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        # Cleanup with DIFFERENT config — should not affect our row
        result = cleanup_module_commits(
            conn, MODULE_NAME, "other-config.json", CONFIG_SETS, ARCHITECTURE
        )

        assert result == 0

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE sha = 'core1'")
            assert cur.fetchone()[0] == "in_progress"  # unchanged

    def test_cleanup_makes_pair_fetchable_again(self, conn, mock_git):
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )
        cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        # Should be fetchable again
        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )

        assert pairs == ["core1:mod1"]


# ---------------------------------------------------------------------------
# Priority classification
# ---------------------------------------------------------------------------


class TestPriorityIntegration:
    """Verify forward/fallback priority assignment based on pointers.

    Priority logic:
    - Pointer = the newest completed pair's (core_timestamp, module_timestamp)
    - Forward (1): BOTH timestamps >= pointer (newer code on both sides)
    - Fallback (2): at least one timestamp < pointer (backfill work)
    - No pointer yet (first run): everything is forward (1)

    Forward pairs are fetched before fallback pairs (priority ASC in sort order).
    """

    def test_after_complete_new_pairs_get_classified(self, conn, mock_git):
        # First populate with old timestamps → sets the pointer baseline
        mock_git.side_effect = [
            {"core_old": "2026-01-01T10:00:00+00:00"},
            {"mod_old": "2026-01-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Mark as complete — pointer is now at 2026-01-01
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core_old:mod_old"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        # Populate new SHAs with mixed timestamps
        # core_new and mod_new are NEWER than pointer → forward
        # core_old is at pointer, mod_new is newer → still forward (>=)
        # core_old + mod_old already exists (skip)
        # core_new + mod_old: core is newer but mod is OLDER → fallback
        mock_git.side_effect = [
            {
                "core_old": "2025-06-01T10:00:00+00:00",
                "core_new": "2026-06-01T10:00:00+00:00",
            },
            {
                "mod_old": "2025-06-01T10:00:00+00:00",
                "mod_new": "2026-06-01T10:00:00+00:00",
            },
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            # core_new:mod_new — both newer than pointer → forward (1)
            cur.execute(
                f"SELECT priority FROM {table} WHERE sha = 'core_new' AND module_sha = 'mod_new'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1

            # core_old:mod_new — core is older than pointer → fallback (2)
            cur.execute(
                f"SELECT priority FROM {table} WHERE sha = 'core_old' AND module_sha = 'mod_new'"
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2


# ---------------------------------------------------------------------------
# Fetch ordering
# ---------------------------------------------------------------------------


class TestFetchOrderIntegration:

    def test_forward_fetched_before_fallback(self, conn, mock_git):
        """Forward (priority=1) pairs should be fetched before fallback (priority=2)."""
        # First: populate and complete one pair to set the pointer
        mock_git.side_effect = [
            {"core_old": "2026-01-01T10:00:00+00:00"},
            {"mod_old": "2026-01-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=1
        )
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core_old:mod_old"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        # Now populate with mixed timestamps
        mock_git.side_effect = [
            {
                "core_old": "2025-06-01T10:00:00+00:00",
                "core_new": "2026-06-01T10:00:00+00:00",
            },
            {
                "mod_old": "2025-06-01T10:00:00+00:00",
                "mod_new": "2026-06-01T10:00:00+00:00",
            },
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Verify priority assignment before fetching
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, module_sha, priority FROM {table} "
                f"WHERE status = 'pending' AND config_name = %s AND architecture = %s "
                f"ORDER BY priority, sha, module_sha",
                (CONFIG_NAME, ARCHITECTURE),
            )
            pending_rows = cur.fetchall()

        # Should have 3 pending: 1 forward + 2 fallback
        assert len(pending_rows) == 3
        assert pending_rows[0] == ("core_new", "mod_new", 1)  # forward
        assert pending_rows[1][2] == 2  # fallback
        assert pending_rows[2][2] == 2  # fallback

        # Fetch all — forward should come first in results
        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=10
        )

        assert len(pairs) == 3
        # First pair must be the forward one
        assert pairs[0] == "core_new:mod_new"
        # Remaining are fallback (order between them depends on timestamp)
        fallback_pairs = set(pairs[1:])
        assert "core_new:mod_old" in fallback_pairs
        assert "core_old:mod_new" in fallback_pairs

    def test_newer_max_timestamp_fetched_first_within_same_priority(
        self, conn, mock_git
    ):
        """Within the same priority, pairs with newer max_commit_timestamp come first."""
        mock_git.side_effect = [
            {
                "core_new": "2026-06-05T10:00:00+00:00",
                "core_mid": "2026-06-03T10:00:00+00:00",
                "core_old": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-02T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Fetch all — should be ordered by max_commit_timestamp DESC
        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=10
        )

        assert len(pairs) == 3
        assert pairs[0] == "core_new:mod1"  # max_ts = 06-05
        assert pairs[1] == "core_mid:mod1"  # max_ts = 06-03
        assert pairs[2] == "core_old:mod1"  # max_ts = 06-02


# ---------------------------------------------------------------------------
# Multiple marks
# ---------------------------------------------------------------------------


class TestMultipleMarksIntegration:

    def test_mark_multiple_pairs_complete(self, conn, mock_git):
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-01T10:00:00+00:00",
                "core3": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Fetch all 3
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=3
        )

        # Mark all 3 complete
        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1", "core2:mod1", "core3:mod1"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        assert result == 3

        # Verify all are complete
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, status FROM {table} "
                f"WHERE config_name = %s AND architecture = %s ORDER BY sha",
                (CONFIG_NAME, ARCHITECTURE),
            )
            rows = cur.fetchall()

        assert len(rows) == 3
        for sha, status in rows:
            assert status == "completed"


# ---------------------------------------------------------------------------
# Cleanup doesn't affect complete
# ---------------------------------------------------------------------------


class TestCleanupDoesNotAffectComplete:

    def test_complete_rows_unaffected_by_cleanup(self, conn, mock_git):
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Fetch both
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=2
        )

        # Mark only core1:mod1 as complete, leave core2:mod1 as in_progress
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        # Run cleanup
        cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        # Verify: core1:mod1 still complete, core2:mod1 reset to pending
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, status FROM {table} "
                f"WHERE config_name = %s AND architecture = %s ORDER BY sha",
                (CONFIG_NAME, ARCHITECTURE),
            )
            rows = dict(cur.fetchall())

        assert rows["core1"] == "completed"
        assert rows["core2"] == "pending"


# ---------------------------------------------------------------------------
# Full lifecycle
# ---------------------------------------------------------------------------


class TestFullLifecycleIntegration:

    def test_populate_fetch_mark_populate_fetch(self, conn, mock_git):
        """Simulate two cron runs: first processes pairs, second picks up new ones.

        Run 1: populate (core1 × mod1) → fetch all → mark complete
        Run 2: populate (core1,core2 × mod1,mod2 = 4 total, 1 already done)
                → fetch all → should get 3 new pairs, not core1:mod1 again
        """
        # === Run 1 ===
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=10
        )
        assert pairs == ["core1:mod1"]

        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS,
            ARCHITECTURE,
        )

        # === Run 2 (new commits on both repos) ===
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00", "mod2": "2026-06-03T10:00:00+00:00"},
        ]

        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE, max_pairs=10
        )

        # Should get 3 new pairs (core1:mod1 already complete, not returned)
        assert len(pairs) == 3
        assert "core1:mod1" not in pairs
        # All 3 new combos should be present
        assert set(pairs) == {"core1:mod2", "core2:mod1", "core2:mod2"}

        # Verify final state
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, module_sha, status FROM {table} "
                f"WHERE config_name = %s AND architecture = %s ORDER BY sha, module_sha",
                (CONFIG_NAME, ARCHITECTURE),
            )
            rows = cur.fetchall()

        assert len(rows) == 4
        statuses = {(row[0], row[1]): row[2] for row in rows}
        assert statuses[("core1", "mod1")] == "completed"
        assert statuses[("core1", "mod2")] == "in_progress"
        assert statuses[("core2", "mod1")] == "in_progress"
        assert statuses[("core2", "mod2")] == "in_progress"


# ---------------------------------------------------------------------------
# Large cartesian product
# ---------------------------------------------------------------------------


class TestLargeCartesianProduct:

    def test_10x10_cartesian(self, conn, mock_git):
        core_shas = [f"core{i:02d}" for i in range(10)]
        mod_shas = [f"mod{i:02d}" for i in range(10)]
        mock_git.side_effect = [
            {sha: "2026-06-01T10:00:00+00:00" for sha in core_shas},
            {sha: "2026-06-01T10:00:00+00:00" for sha in mod_shas},
        ]

        result = populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        assert result == 100

        # Verify all 100 rows exist and are pending with priority assigned
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE status = 'pending'")
            assert cur.fetchone()[0] == 100

            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE priority IS NULL")
            assert cur.fetchone()[0] == 0

    def test_repopulate_large_set_inserts_zero(self, conn, mock_git):
        core_shas = [f"core{i:02d}" for i in range(10)]
        mod_shas = [f"mod{i:02d}" for i in range(10)]

        mock_git.side_effect = [
            {sha: "2026-06-01T10:00:00+00:00" for sha in core_shas},
            {sha: "2026-06-01T10:00:00+00:00" for sha in mod_shas},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        # Second populate — same SHAs
        mock_git.side_effect = [
            {sha: "2026-06-01T10:00:00+00:00" for sha in core_shas},
            {sha: "2026-06-01T10:00:00+00:00" for sha in mod_shas},
        ]
        result = populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
        )

        assert result == 0

    def test_max_commits_limits_populate(self, conn, mock_git):
        # git_rev_list_with_timestamps will be called with max_count=3, so mock returns only 3
        mock_git.side_effect = [
            {f"core{i:02d}": "2026-06-01T10:00:00+00:00" for i in range(3)},
            {f"mod{i:02d}": "2026-06-01T10:00:00+00:00" for i in range(3)},
        ]

        result = populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS,
            max_core_commits=3,
            max_module_commits=3,
        )

        assert result == 9

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 9


# ---------------------------------------------------------------------------
# Concurrent config populations
# ---------------------------------------------------------------------------


class TestConcurrentConfigPopulations:

    def test_two_configs_independent_queues(self, conn, mock_git):
        # Populate config-A
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS,
        )

        # Populate config-B (same SHAs, different config)
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            "config-B.json",
            CONFIG_SETS,
        )

        # Table should have 4 rows total (2 per config)
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 4

        # Fetch for config-A — should get 2 pairs
        pairs_a, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert len(pairs_a) == 2

        # Config-B pairs should still be pending (not fetched)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE config_name = 'config-B.json' AND status = 'pending'"
            )
            assert cur.fetchone()[0] == 2

        # Mark config-A pairs complete
        mark_module_commits(
            conn, MODULE_NAME, pairs_a, "config-A.json", CONFIG_SETS, ARCHITECTURE
        )

        # Config-B should still be untouched (all pending)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE config_name = 'config-B.json' AND status = 'pending'"
            )
            assert cur.fetchone()[0] == 2

        # Config-A should be all complete
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE config_name = 'config-A.json' AND status = 'completed'"
            )
            assert cur.fetchone()[0] == 2


# ---------------------------------------------------------------------------
# Subset detection (_mark_subset_pairs_in_memory via _assign_priority_in_memory)
# ---------------------------------------------------------------------------


CONFIG_SETS_SMALL = [{"io-threads": 8, "search.reader-threads": 1}]

CONFIG_SETS_LARGE = [
    {"io-threads": 8, "search.reader-threads": 1},
    {"io-threads": 8, "search.reader-threads": 8},
]


class TestSubsetDetectionIntegration:
    """Verify that subset config_sets get marked 'completed_as_subset' when a superset exists."""

    def _populate_and_complete(self, conn, mock_git, config_sets):
        """Helper: populate pairs with given config_sets and mark them completed."""
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            config_sets,
        )
        # Fetch and mark complete
        pairs, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, config_sets, ARCHITECTURE, max_pairs=10
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs, CONFIG_NAME, config_sets, ARCHITECTURE
        )

    def test_subset_marked_completed_as_subset(self, conn, mock_git):
        """Two distinct supersets completed — subset pairs matched via IN() clause."""
        superset_a = [
            {"io-threads": 8, "search.reader-threads": 1},
            {"io-threads": 8, "search.reader-threads": 8},
        ]
        superset_b = [
            {"io-threads": 8, "search.reader-threads": 1},
            {"io-threads": 4, "search.reader-threads": 4},
        ]

        # Step 1: Complete superset A for core1:mod1
        mock_git.side_effect = [
            {"core1": "2026-06-01T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            superset_a,
        )
        pairs_a, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, superset_a, ARCHITECTURE, max_pairs=10
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs_a, CONFIG_NAME, superset_a, ARCHITECTURE
        )

        # Step 2: Complete superset B for core2:mod1
        mock_git.side_effect = [
            {"core2": "2026-06-02T10:00:00+00:00"},
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            superset_b,
        )
        pairs_b, _ = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, superset_b, ARCHITECTURE, max_pairs=10
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs_b, CONFIG_NAME, superset_b, ARCHITECTURE
        )

        # Step 3: Populate subset — both pairs should be completed_as_subset
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL,
        )

        # Step 4: Verify both subset rows were marked
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} " f"WHERE config_sets = %s ORDER BY sha",
                (Json(CONFIG_SETS_SMALL),),
            )
            statuses = [row[0] for row in cur.fetchall()]

        assert all(s == "completed_as_subset" for s in statuses)
        assert len(statuses) == 2

    def test_subset_not_fetched(self, conn, mock_git):
        self._populate_and_complete(conn, mock_git, CONFIG_SETS_LARGE)

        # Populate subset
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL,
        )

        # Fetch for subset config — should be empty (all completed_as_subset)
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert pairs == []

    def test_non_subset_still_pending(self, conn, mock_git):
        # Complete config with reader-threads=1 only
        self._populate_and_complete(conn, mock_git, CONFIG_SETS_SMALL)

        # Populate with a DIFFERENT config (reader-threads=8 only, not subset of small)
        different_config = [{"io-threads": 8, "search.reader-threads": 8}]

        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            different_config,
        )

        # Should be fetchable (not marked as subset)
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            different_config,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert len(pairs) == 2

    def test_superset_not_marked_as_subset(self, conn, mock_git):
        # Complete the smaller config
        self._populate_and_complete(conn, mock_git, CONFIG_SETS_SMALL)

        # Populate with the larger config
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
        )

        # LARGE should be fetchable (it's a superset, not a subset)
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert len(pairs) == 2

    def test_exact_match_is_subset(self, conn, mock_git):
        self._populate_and_complete(conn, mock_git, CONFIG_SETS_LARGE)

        # Populate same config again
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
        )

        # Should not be fetchable (all marked as subset since exact match was completed)
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert pairs == []

    def test_partial_overlap_subset(self, conn, mock_git):
        """Only pairs that exist in the superset's completed rows get marked as subset.

        Scenario:
          1. Complete LARGE config for (core1, mod1) and (core2, mod1)
          2. Populate SMALL config (subset) for (core1, mod1), (core2, mod1), (core3, mod1)
             — core3 is a new commit that wasn't in the superset run
        """
        # Step 1: Complete superset with core1, core2
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
        )
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE,
            ARCHITECTURE,
            max_pairs=10,
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs, CONFIG_NAME, CONFIG_SETS_LARGE, ARCHITECTURE
        )

        # Step 2: Populate subset with core1, core2, core3 (core3 is new)
        mock_git.side_effect = [
            {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
                "core3": "2026-06-01T10:00:00+00:00",
            },
            {"mod1": "2026-06-01T10:00:00+00:00"},
        ]
        populate_module_commits(
            conn,
            Path("/fake"),
            "unstable",
            Path("/fake-mod"),
            "main",
            ARCHITECTURE,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL,
        )

        # Step 3: Verify statuses
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, module_sha, status FROM {table} "
                f"WHERE config_sets = %s ORDER BY sha",
                (Json(CONFIG_SETS_SMALL),),
            )
            rows = cur.fetchall()

        statuses = {(row[0], row[1]): row[2] for row in rows}
        assert statuses[("core1", "mod1")] == "completed_as_subset"
        assert statuses[("core2", "mod1")] == "completed_as_subset"
        assert statuses[("core3", "mod1")] == "pending"

        # Only core3:mod1 should be fetchable
        pairs, _ = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert pairs == ["core3:mod1"]


# ---------------------------------------------------------------------------
# _assign_priority_in_memory
# ---------------------------------------------------------------------------


class TestAssignPriorityInMemory:

    def _make_pair(self, core_ts, module_ts):
        """Helper to build a CommitPair with specific timestamps."""
        core_dt = _parse_timestamp(core_ts)
        module_dt = _parse_timestamp(module_ts)
        return CommitPair(
            core_sha="core_" + core_ts[:10],
            module_sha="mod_" + module_ts[:10],
            core_timestamp=core_dt,
            module_timestamp=module_dt,
            max_commit_timestamp=max(core_dt, module_dt),
            min_commit_timestamp=min(core_dt, module_dt),
            config_name=CONFIG_NAME,
            config_sets=CONFIG_SETS,
            architecture=ARCHITECTURE,
        )

    def test_no_pointer_all_forward(self, conn):

        table = _module_table_name(MODULE_NAME)

        pairs = [
            self._make_pair("2026-06-01T10:00:00+00:00", "2026-06-02T10:00:00+00:00"),
            self._make_pair("2026-06-03T10:00:00+00:00", "2026-06-04T10:00:00+00:00"),
        ]

        _assign_priority_in_memory(
            conn, pairs, table, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        assert pairs[0].priority == 1
        assert pairs[1].priority == 1

    def test_forward_and_fallback_classification(self, conn):
        """4x4 grid test mimicking the priority diagram.

        Core commits: A(oldest), B, C, D(newest)
        Module commits: a(oldest), b, c, d(newest)

        Already completed: Aa, Ab, Ba, Bb (everything at or behind the pointer)
        Pointer = newest completed pair (B, b).

        New pairs to classify:
          Forward (>=): core >= B AND module >= b
            Bc, Bd, Cb, Cc, Cd, Db, Dc, Dd
          Fallback: core < B OR module < b
            Ac, Ad, Ca, Da, Ba (wait — Ba is completed)
            Actually only: Ac, Ad, Ca, Da
        """

        table = _module_table_name(MODULE_NAME)

        core_times = {
            "A": "2026-06-01",
            "B": "2026-06-02",
            "C": "2026-06-03",
            "D": "2026-06-04",
        }
        mod_times = {
            "a": "2026-06-01",
            "b": "2026-06-02",
            "c": "2026-06-03",
            "d": "2026-06-04",
        }

        # Already completed pairs: Aa, Ab, Ba, Bb
        completed_pairs = [("A", "a"), ("A", "b"), ("B", "a"), ("B", "b")]
        with conn.cursor() as cur:
            for core, mod in completed_pairs:
                core_t = core_times[core]
                mod_t = mod_times[mod]
                cur.execute(
                    f"""
                    INSERT INTO {table} (sha, module_sha, core_timestamp, module_timestamp,
                                         max_commit_timestamp, min_commit_timestamp,
                                         status, priority, config_name, config_sets, architecture)
                    VALUES (%s, %s, %s, %s, %s, %s, 'completed', 1, %s, %s, %s)
                """,
                    (
                        core,
                        mod,
                        f"{core_t}T00:00:00+00:00",
                        f"{mod_t}T00:00:00+00:00",
                        f"{max(core_t, mod_t)}T00:00:00+00:00",
                        f"{min(core_t, mod_t)}T00:00:00+00:00",
                        CONFIG_NAME,
                        Json(CONFIG_SETS),
                        ARCHITECTURE,
                    ),
                )
        conn.commit()

        # Build new pairs (everything not already completed)
        completed_set = set(completed_pairs)
        pairs = []
        for core, core_t in core_times.items():
            for mod, mod_t in mod_times.items():
                if (core, mod) in completed_set:
                    continue
                pairs.append(
                    self._make_pair(
                        f"{core_t}T00:00:00+00:00", f"{mod_t}T00:00:00+00:00"
                    )
                )
                pairs[-1].core_sha = core
                pairs[-1].module_sha = mod

        _assign_priority_in_memory(
            conn, pairs, table, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        # Build lookup: (core, mod) -> priority
        result = {(p.core_sha, p.module_sha): p.priority for p in pairs}

        # Forward (priority=1): core >= B AND module >= b
        forward_expected = [
            ("B", "c"),
            ("B", "d"),
            ("C", "b"),
            ("C", "c"),
            ("C", "d"),
            ("D", "b"),
            ("D", "c"),
            ("D", "d"),
        ]
        for pair in forward_expected:
            assert result[pair] == 1, f"{pair} should be forward(1), got {result[pair]}"

        # Fallback (priority=2): core < B OR module < b
        fallback_expected = [
            ("A", "c"),
            ("A", "d"),
            ("C", "a"),
            ("D", "a"),
        ]
        for pair in fallback_expected:
            assert (
                result[pair] == 2
            ), f"{pair} should be fallback(2), got {result[pair]}"

    def test_skips_already_assigned_pairs(self, conn):

        table = _module_table_name(MODULE_NAME)

        pair = self._make_pair("2026-06-05T10:00:00+00:00", "2026-06-04T10:00:00+00:00")
        pair.priority = 99
        pair.status = "completed_as_subset"

        _assign_priority_in_memory(
            conn, [pair], table, CONFIG_NAME, CONFIG_SETS, ARCHITECTURE
        )

        assert pair.priority == 99
