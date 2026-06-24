"""Integration tests for module_postgres_track_commits.py using a real PostgreSQL database.

Unlike other integration tests in this directory (which mock git repos and benchmark
binaries), these tests require a real PostgreSQL instance to verify the full queue
lifecycle: populate → fetch → benchmark → mark complete.

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
    - Table creation (idempotent, correct schema)
    - Populate (cartesian product insertion, skip existing, config isolation)
    - Priority classification (all-forward on first run, forward/fallback after pointer)
    - Fetch next (sort order, config/arch filter, marks in_progress, respects max_pairs)
    - Mark complete (config/arch check, warns on mismatch)
    - Cleanup (resets in_progress → pending, scoped to config/arch)
    - Config+arch isolation (different configs don't interfere with each other)

Run with:
    pytest tests/integration/test_module_tracking_integration.py -v
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
    check_incomplete_rows,
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
CONFIG_SETS_JSON = Json(CONFIG_SETS)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """Create a real PostgreSQL connection for testing.

    Skips the entire test if Postgres is not available (e.g., Docker not running).
    Each test gets its own connection that's closed after the test.
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
    """Drop and recreate the test table before each test.

    This ensures each test starts with a clean slate — no leftover rows
    from previous tests that could affect results.
    """
    table = _module_table_name(MODULE_NAME)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    yield
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


@pytest.fixture(autouse=True)
def mock_git():
    """Mock git helpers so tests don't need real git repos.

    What patch() does:
        patch("utils.module_postgres_track_commits.git_rev_list") temporarily replaces
        the REAL git_rev_list function (which runs `git rev-list` in a subprocess)
        with a fake MagicMock object. During the test, any call to git_rev_list()
        hits the fake instead of running actual git commands.

        Same for git_commit_time (which runs `git show --format=%cI`).

    Why we mock these:
        Our functions need git SHAs and timestamps as input. In production, these
        come from real git repos. In tests, we don't have valkey/valkey-search repos,
        so we tell the mock what to return:

        mock_rev_list.side_effect = [["sha1", "sha2"], ["mod1", "mod2"]]
        → First call returns ["sha1", "sha2"] (core repo)
        → Second call returns ["mod1", "mod2"] (module repo)

        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
        → Every call returns this timestamp regardless of which SHA is asked about
    """
    with (
        patch("utils.module_postgres_track_commits.git_rev_list") as mock_rev_list,
        patch(
            "utils.module_postgres_track_commits.git_commit_time"
        ) as mock_commit_time,
    ):
        yield mock_rev_list, mock_commit_time


# ---------------------------------------------------------------------------
# _create_module_table
# ---------------------------------------------------------------------------


class TestCreateModuleTable:
    """Verify table creation works correctly."""

    def test_creates_table(self, conn):
        """Table should exist after _create_module_table is called."""
        _create_module_table(conn, MODULE_NAME)

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                (table,),
            )
            assert cur.fetchone()[0] is True

    def test_idempotent(self, conn):
        """Calling _create_module_table twice should not error."""
        _create_module_table(conn, MODULE_NAME)
        _create_module_table(conn, MODULE_NAME)


# ---------------------------------------------------------------------------
# populate_module_commits
# ---------------------------------------------------------------------------


class TestPopulateIntegration:
    """Verify populate inserts the correct cartesian product of pairs.

    populate_module_commits() does:
    1. Calls cleanup (reset stale in_progress → pending)
    2. Gets git histories from both repos (mocked here)
    3. Queries DB for existing pairs (to skip duplicates)
    4. Inserts new pairs with ON CONFLICT DO NOTHING
    5. Calls _assign_priority_in_memory to classify new rows
    """

    def test_inserts_cartesian_product(self, conn, mock_git):
        """2 core SHAs × 2 module SHAs = 4 pairs inserted.

        Scenario: fresh table, two repos with 2 commits each, each with distinct timestamps.
        Expected: all 4 combinations get inserted as 'pending' with correct fields,
                  correct timestamps, and correct max/min computed from the pair.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [
            ["core1", "core2"],
            ["mod1", "mod2"],
        ]

        # Each SHA gets a distinct timestamp
        def commit_time_by_sha(repo, sha):
            timestamps = {
                "core1": "2026-06-04T10:00:00+00:00",
                "core2": "2026-06-03T10:00:00+00:00",
                "mod1": "2026-06-02T10:00:00+00:00",
                "mod2": "2026-06-01T10:00:00+00:00",
            }
            return timestamps[sha]

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
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

    def test_skips_existing_pairs(self, conn, mock_git):
        """Running populate twice with same SHAs should not duplicate rows.

        Scenario: populate once, then populate again with identical git history.
        Expected: second call returns 0 (ON CONFLICT DO NOTHING skips all).
        This simulates a cron run where no new commits have landed.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [
            ["core1", "core2"],
            ["mod1"],
        ]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        # Second populate with same SHAs — nothing new
        mock_rev_list.side_effect = [
            ["core1", "core2"],
            ["mod1"],
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
            CONFIG_SETS_JSON,
        )

        assert result == 0

    def test_new_config_gets_separate_rows(self, conn, mock_git):
        """Same (sha, module_sha) pair with different config = separate row.

        Scenario: populate with config-A, then populate same SHAs with config-B.
        Expected: both rows exist (unique constraint is per config+arch).
        This ensures changing configs triggers a full re-benchmark.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        # Same pair, different config — should insert a new row
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
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
            CONFIG_SETS_JSON,
        )

        assert result == 1

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 2

    def test_all_rows_get_priority_assigned(self, conn, mock_git):
        """After populate with no prior completions, all rows get priority=1 (forward).

        Scenario: fresh table (no pointer), populate inserts rows and calls
                  _assign_priority_in_memory which sees no completed pairs.
        Expected: every row has priority=1 (forward), no NULLs.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1", "mod2"]]

        def commit_time_by_sha(repo, sha):
            timestamps = {
                "core1": "2026-06-04T10:00:00+00:00",
                "core2": "2026-06-03T10:00:00+00:00",
                "mod1": "2026-06-02T10:00:00+00:00",
                "mod2": "2026-06-01T10:00:00+00:00",
            }
            return timestamps[sha]

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
        )

        # No NULL priorities — all rows should have priority assigned
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE priority IS NULL")
            assert cur.fetchone()[0] == 0

        # All rows should be priority=1 (forward) since no pointer exists
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT priority FROM {table}")
            priorities = {row[0] for row in cur.fetchall()}
        assert priorities == {1}


# ---------------------------------------------------------------------------
# fetch_next_module_commits
# ---------------------------------------------------------------------------


class TestFetchNextIntegration:
    """Verify fetch returns correct pairs and marks them in_progress.

    fetch_next_module_commits() does:
    1. SELECT pending pairs WHERE config+arch match, sorted by priority order
    2. LIMIT to max_pairs
    3. UPDATE those rows to status='in_progress'
    4. Return the pairs as "core_sha:module_sha" strings
    """

    def _populate(self, conn, mock_git, core_shas, mod_shas):
        """Helper to populate the queue with given SHAs."""
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [core_shas, mod_shas]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

    def test_fetches_pending_pairs(self, conn, mock_git):
        """Should return a pending pair."""
        self._populate(conn, mock_git, ["core1"], ["mod1"])

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        assert pairs == ["core1:mod1"]

    def test_marks_fetched_as_in_progress(self, conn, mock_git):
        """Fetched pair should have status='in_progress' in the DB."""
        self._populate(conn, mock_git, ["core1"], ["mod1"])
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
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
        """A pair that's already in_progress should not be fetched again.

        Scenario: two pairs exist. Fetch one (→ in_progress). Fetch again.
        Expected: second fetch returns the OTHER pair, not the same one.
        This prevents double-running the same benchmark.
        """
        self._populate(conn, mock_git, ["core1", "core2"], ["mod1"])

        # First fetch takes one pair
        first = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )
        # Second fetch should return the OTHER pair
        second = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        assert len(second) == 1
        assert second[0] != first[0]

    def test_returns_empty_when_all_in_progress(self, conn, mock_git):
        """If all pairs are in_progress, fetch returns empty."""
        self._populate(conn, mock_git, ["core1"], ["mod1"])
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        assert pairs == []

    def test_respects_max_pairs(self, conn, mock_git):
        """Should not return more pairs than max_pairs."""
        self._populate(conn, mock_git, ["core1", "core2", "core3"], ["mod1"])

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=2
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
        """Pairs for a different config should not be returned.

        Scenario: populate same SHAs with config-A AND config-B.
                  Fetch with config-A should only get config-A's pair.
                  config-B's pair should remain pending and untouched.
        """
        mock_rev_list, mock_commit_time = mock_git

        # Populate with config-A
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Populate same SHAs with config-B
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Fetch for config-A — should get config-A's pair
        pairs_a = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS_JSON,
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
    """Verify marking pairs as complete works with config/arch checks.

    mark_module_commits() does:
    1. UPDATE status='completed' WHERE sha + module_sha + config_name + config_sets + architecture all match
    2. If no row matches (wrong config/arch), prints WARNING and returns 0
    3. This is the final step in the workflow — confirms benchmark succeeded
    """

    def _setup_in_progress(self, conn, mock_git):
        """Helper: populate one pair and fetch it (→ in_progress).
        Simulates the state right after fetch_next but before benchmark completes.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

    def test_marks_complete(self, conn, mock_git):
        """Should update status to 'completed' and set updated_at."""
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
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
        """Marking with wrong config should not update any row."""
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            "wrong-config.json",
            CONFIG_SETS_JSON,
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
        """Marking with wrong architecture should not update any row."""
        self._setup_in_progress(conn, mock_git)

        result = mark_module_commits(
            conn, MODULE_NAME, ["core1:mod1"], CONFIG_NAME, CONFIG_SETS_JSON, "x86_64"
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
        """Once complete, a pair should never be returned by fetch_next.

        This is the key invariant: completed work is never re-done.
        Simulates the full lifecycle: populate → fetch → mark → fetch again.
        """
        self._setup_in_progress(conn, mock_git)
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
            ARCHITECTURE,
        )

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        assert pairs == []


# ---------------------------------------------------------------------------
# cleanup_module_commits
# ---------------------------------------------------------------------------


class TestCleanupIntegration:
    """Verify cleanup resets in_progress → pending, scoped to config/arch.

    cleanup_module_commits() does:
    1. UPDATE status='pending' WHERE status='in_progress' AND config+arch match
    2. Returns count of rows reset

    Purpose: if a workflow crashes after fetch (pair is in_progress) but before
    mark_complete, the next run's cleanup makes that pair fetchable again.
    Unlike core (which DELETEs), we keep the row and just reset its status.
    """

    def test_resets_in_progress_to_pending(self, conn, mock_git):
        """in_progress pairs should become pending after cleanup.

        Simulates: workflow crashed after fetch, before mark_complete.
        Next run calls cleanup → pair is retryable.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        result = cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )

        assert result == 1

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE sha = 'core1'")
            assert cur.fetchone()[0] == "pending"

    def test_does_not_affect_other_config(self, conn, mock_git):
        """Cleanup for config-B should not reset config-A's in_progress pairs.

        Scenario: pair is in_progress for config-A. Cleanup runs for config-B.
        Expected: pair remains in_progress. Different configs are isolated.
        This protects against one runner accidentally resetting another's work.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )

        # Cleanup with DIFFERENT config — should not affect our row
        result = cleanup_module_commits(
            conn, MODULE_NAME, "other-config.json", CONFIG_SETS_JSON, ARCHITECTURE
        )

        assert result == 0

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE sha = 'core1'")
            assert cur.fetchone()[0] == "in_progress"  # unchanged

    def test_cleanup_makes_pair_fetchable_again(self, conn, mock_git):
        """After cleanup, previously in_progress pairs can be fetched again.

        Full cycle: populate → fetch (in_progress) → cleanup (pending) → fetch again.
        Verifies the retry mechanism actually works end-to-end.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )
        cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )

        # Should be fetchable again
        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
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

    def test_first_run_all_forward(self, conn, mock_git):
        """With no completed pairs (first run), all pairs get priority=1 (forward).

        Scenario: empty table, first populate ever.
        Expected: no pointer exists → _assign_priority_in_memory sets all to 1.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1", "mod2"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT priority FROM {table}")
            priorities = {row[0] for row in cur.fetchall()}

        assert priorities == {1}

    def test_after_complete_new_pairs_get_classified(self, conn, mock_git):
        """After a pair is completed, new pairs are classified as forward or fallback.

        Forward: both core_timestamp and module_timestamp >= the pointer
        Fallback: at least one timestamp < the pointer
        """
        mock_rev_list, mock_commit_time = mock_git

        # First populate with old timestamps → sets the pointer baseline
        mock_rev_list.side_effect = [["core_old"], ["mod_old"]]
        mock_commit_time.return_value = "2026-01-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Mark as complete — pointer is now at 2026-01-01
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core_old:mod_old"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
            ARCHITECTURE,
        )

        # Populate new SHAs with mixed timestamps
        # core_new and mod_new are NEWER than pointer → forward
        # core_old is at pointer, mod_new is newer → still forward (>=)
        # core_old + mod_old already exists (skip)
        # core_new + mod_old: core is newer but mod is OLDER → fallback
        mock_rev_list.side_effect = [["core_old", "core_new"], ["mod_old", "mod_new"]]

        def commit_time_by_sha(repo, sha):
            if "new" in sha:
                return "2026-06-01T10:00:00+00:00"  # newer than pointer
            return "2025-06-01T10:00:00+00:00"  # older than pointer

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
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
    """Verify fetch returns pairs in the correct priority and timestamp order."""

    def test_forward_fetched_before_fallback(self, conn, mock_git):
        """Forward (priority=1) pairs should be fetched before fallback (priority=2).

        Scenario: populate pairs, mark one complete to create a pointer,
                  then populate new pairs so some are forward and some fallback.
                  Fetch all — forward pairs should appear before fallback in results.

        Expected pairs after second populate:
          - core_new:mod_new → forward (both newer than pointer)
          - core_new:mod_old → fallback (mod is older than pointer)
          - core_old:mod_new → fallback (core is older than pointer)
          Total: 1 forward, 2 fallback (core_old:mod_old already complete)
        """
        mock_rev_list, mock_commit_time = mock_git

        # First: populate and complete one pair to set the pointer
        mock_rev_list.side_effect = [["core_old"], ["mod_old"]]
        mock_commit_time.return_value = "2026-01-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=1
        )
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core_old:mod_old"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
            ARCHITECTURE,
        )

        # Now populate with mixed timestamps
        mock_rev_list.side_effect = [["core_old", "core_new"], ["mod_old", "mod_new"]]

        def commit_time_by_sha(repo, sha):
            if "new" in sha:
                return "2026-06-01T10:00:00+00:00"
            return "2025-06-01T10:00:00+00:00"

        mock_commit_time.side_effect = commit_time_by_sha
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
            CONFIG_SETS_JSON,
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
        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=10
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
        """Within the same priority, pairs with newer max_commit_timestamp come first.

        Scenario: all pairs are forward (priority=1) but have different timestamps.
                  Fetch all — should return in order of max_commit_timestamp DESC.

        Timestamps:
          core_new = 2026-06-05, core_mid = 2026-06-03, core_old = 2026-06-01
          mod1 = 2026-06-02

        Expected max_commit_timestamps:
          core_new:mod1 → max(06-05, 06-02) = 06-05
          core_mid:mod1 → max(06-03, 06-02) = 06-03
          core_old:mod1 → max(06-01, 06-02) = 06-02

        Expected fetch order: core_new:mod1, core_mid:mod1, core_old:mod1
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [
            ["core_new", "core_mid", "core_old"],
            ["mod1"],
        ]

        def commit_time_by_sha(repo, sha):
            timestamps = {
                "core_new": "2026-06-05T10:00:00+00:00",
                "core_mid": "2026-06-03T10:00:00+00:00",
                "core_old": "2026-06-01T10:00:00+00:00",
                "mod1": "2026-06-02T10:00:00+00:00",
            }
            return timestamps[sha]

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
        )

        # Fetch all — should be ordered by max_commit_timestamp DESC
        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=10
        )

        assert len(pairs) == 3
        assert pairs[0] == "core_new:mod1"  # max_ts = 06-05
        assert pairs[1] == "core_mid:mod1"  # max_ts = 06-03
        assert pairs[2] == "core_old:mod1"  # max_ts = 06-02


# ---------------------------------------------------------------------------
# Multiple marks
# ---------------------------------------------------------------------------


class TestMultipleMarksIntegration:
    """Verify marking multiple pairs works correctly."""

    def test_mark_multiple_pairs_complete(self, conn, mock_git):
        """Mark several pairs complete in one call, verify all statuses."""
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2", "core3"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        # Fetch all 3
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=3
        )

        # Mark all 3 complete
        result = mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1", "core2:mod1", "core3:mod1"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
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
    """Verify cleanup only resets in_progress, never touches complete."""

    def test_complete_rows_unaffected_by_cleanup(self, conn, mock_git):
        """Cleanup should not change status of completed pairs.

        Scenario: one pair complete, one pair in_progress. Run cleanup.
        Expected: complete stays complete, in_progress resets to pending.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        # Fetch both
        fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=2
        )

        # Mark only core1:mod1 as complete, leave core2:mod1 as in_progress
        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
            ARCHITECTURE,
        )

        # Run cleanup
        cleanup_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
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
    """End-to-end test simulating multiple cron runs."""

    def test_populate_fetch_mark_populate_fetch(self, conn, mock_git):
        """Simulate two cron runs: first processes pairs, second picks up new ones.

        Run 1: populate (core1 × mod1) → fetch all → mark complete
        Run 2: populate (core1,core2 × mod1,mod2 = 4 total, 1 already done)
                → fetch all → should get 3 new pairs, not core1:mod1 again
        """
        mock_rev_list, mock_commit_time = mock_git

        # === Run 1 ===
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=10
        )
        assert pairs == ["core1:mod1"]

        mark_module_commits(
            conn,
            MODULE_NAME,
            ["core1:mod1"],
            CONFIG_NAME,
            CONFIG_SETS_JSON,
            ARCHITECTURE,
        )

        # === Run 2 (new commits on both repos) ===
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1", "mod2"]]

        def commit_time_run2(repo, sha):
            timestamps = {
                "core1": "2026-06-01T10:00:00+00:00",
                "core2": "2026-06-02T10:00:00+00:00",
                "mod1": "2026-06-01T10:00:00+00:00",
                "mod2": "2026-06-03T10:00:00+00:00",
            }
            return timestamps[sha]

        mock_commit_time.side_effect = commit_time_run2

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
            CONFIG_SETS_JSON,
        )

        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE, max_pairs=10
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
# Timestamp computation
# ---------------------------------------------------------------------------


class TestTimestampComputation:
    """Verify max_commit_timestamp and min_commit_timestamp are computed correctly."""

    def test_max_min_timestamps_computed(self, conn, mock_git):
        """max_commit_timestamp = max(core_ts, module_ts), min = min of both.

        Scenario: core_ts = June 5, module_ts = June 1.
        Expected: max = June 5, min = June 1.
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]

        def commit_time_by_sha(repo, sha):
            if sha == "core1":
                return "2026-06-05T10:00:00+00:00"
            return "2026-06-01T10:00:00+00:00"

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
        )

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT core_timestamp, module_timestamp, max_commit_timestamp, min_commit_timestamp "
                f"FROM {table} WHERE sha = 'core1' AND module_sha = 'mod1'"
            )
            row = cur.fetchone()

        core_ts, mod_ts, max_ts, min_ts = row
        assert core_ts.isoformat() == "2026-06-05T10:00:00+00:00"
        assert mod_ts.isoformat() == "2026-06-01T10:00:00+00:00"
        assert max_ts.isoformat() == "2026-06-05T10:00:00+00:00"
        assert min_ts.isoformat() == "2026-06-01T10:00:00+00:00"

    def test_max_min_when_module_is_newer(self, conn, mock_git):
        """When module timestamp is newer than core.

        Scenario: core_ts = June 1, module_ts = June 5.
        Expected: max = June 5 (module), min = June 1 (core).
        """
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]

        def commit_time_by_sha(repo, sha):
            if sha == "core1":
                return "2026-06-01T10:00:00+00:00"
            return "2026-06-05T10:00:00+00:00"

        mock_commit_time.side_effect = commit_time_by_sha

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
            CONFIG_SETS_JSON,
        )

        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT max_commit_timestamp, min_commit_timestamp "
                f"FROM {table} WHERE sha = 'core1' AND module_sha = 'mod1'"
            )
            row = cur.fetchone()

        max_ts, min_ts = row
        assert max_ts.isoformat() == "2026-06-05T10:00:00+00:00"
        assert min_ts.isoformat() == "2026-06-01T10:00:00+00:00"


# ---------------------------------------------------------------------------
# Large cartesian product
# ---------------------------------------------------------------------------


class TestLargeCartesianProduct:
    """Verify populate works correctly with larger sets."""

    def test_10x10_cartesian(self, conn, mock_git):
        """10 core SHAs × 10 module SHAs = 100 pairs.

        Verifies correctness at scale — no duplicates, all inserted.
        """
        mock_rev_list, mock_commit_time = mock_git
        core_shas = [f"core{i:02d}" for i in range(10)]
        mod_shas = [f"mod{i:02d}" for i in range(10)]
        mock_rev_list.side_effect = [core_shas, mod_shas]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
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
        """Running populate twice on 10×10 should insert 0 the second time."""
        mock_rev_list, mock_commit_time = mock_git
        core_shas = [f"core{i:02d}" for i in range(10)]
        mod_shas = [f"mod{i:02d}" for i in range(10)]

        mock_rev_list.side_effect = [core_shas, mod_shas]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Second populate — same SHAs
        mock_rev_list.side_effect = [core_shas, mod_shas]
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
            CONFIG_SETS_JSON,
        )

        assert result == 0

    def test_max_commits_limits_populate(self, conn, mock_git):
        """max_core_commits and max_module_commits limit how many SHAs are scanned.

        Scenario: git has 10 core and 10 module commits, but max is set to 3 each.
        Expected: only 3×3 = 9 pairs inserted (not 100).
        """
        mock_rev_list, mock_commit_time = mock_git
        # git_rev_list will be called with max_count=3, so mock returns only 3
        mock_rev_list.side_effect = [
            [f"core{i:02d}" for i in range(3)],
            [f"mod{i:02d}" for i in range(3)],
        ]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
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
    """Verify two configs sharing the same table don't interfere."""

    def test_two_configs_independent_queues(self, conn, mock_git):
        """Populate same SHAs for config-A and config-B.
        Fetch for config-A should only return config-A pairs.
        Mark config-A complete should not affect config-B.
        """
        mock_rev_list, mock_commit_time = mock_git

        # Populate config-A
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Populate config-B (same SHAs, different config)
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_JSON,
        )

        # Table should have 4 rows total (2 per config)
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            assert cur.fetchone()[0] == 4

        # Fetch for config-A — should get 2 pairs
        pairs_a = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            "config-A.json",
            CONFIG_SETS_JSON,
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
            conn, MODULE_NAME, pairs_a, "config-A.json", CONFIG_SETS_JSON, ARCHITECTURE
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
CONFIG_SETS_SMALL_JSON = Json(CONFIG_SETS_SMALL)

CONFIG_SETS_LARGE = [
    {"io-threads": 8, "search.reader-threads": 1},
    {"io-threads": 8, "search.reader-threads": 8},
]
CONFIG_SETS_LARGE_JSON = Json(CONFIG_SETS_LARGE)


class TestSubsetDetectionIntegration:
    """Verify that subset config_sets get marked 'completed_as_subset' when a superset exists."""

    def _populate_and_complete(self, conn, mock_git, config_sets, config_sets_json):
        """Helper: populate pairs with given config_sets and mark them completed."""
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            config_sets_json,
        )
        # Fetch and mark complete
        pairs = fetch_next_module_commits(
            conn, MODULE_NAME, CONFIG_NAME, config_sets_json, ARCHITECTURE, max_pairs=10
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs, CONFIG_NAME, config_sets_json, ARCHITECTURE
        )

    def test_subset_marked_completed_as_subset(self, conn, mock_git):
        """If superset config_sets is already completed, subset rows get marked.

        Scenario:
          1. Populate with LARGE config_sets (2 elements), mark completed
          2. Populate with SMALL config_sets (1 element, subset of LARGE)
          3. _assign_priority_in_memory detects subset, marks new rows 'completed_as_subset'
        """
        # Step 1: Complete the superset
        self._populate_and_complete(
            conn, mock_git, CONFIG_SETS_LARGE, CONFIG_SETS_LARGE_JSON
        )

        # Step 2: Populate with subset config_sets
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_SMALL_JSON,
        )

        # Step 3: Verify subset rows were marked
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT status FROM {table} " f"WHERE config_sets = %s ORDER BY sha",
                (CONFIG_SETS_SMALL_JSON,),
            )
            statuses = [row[0] for row in cur.fetchall()]

        assert all(s == "completed_as_subset" for s in statuses)
        assert len(statuses) == 2

    def test_subset_not_fetched(self, conn, mock_git):
        """Rows marked 'completed_as_subset' should never be returned by fetch_next.

        Scenario: after subset detection, fetch should return empty for subset config.
        """
        self._populate_and_complete(
            conn, mock_git, CONFIG_SETS_LARGE, CONFIG_SETS_LARGE_JSON
        )

        # Populate subset
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_SMALL_JSON,
        )

        # Fetch for subset config — should be empty (all completed_as_subset)
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL_JSON,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert pairs == []

    def test_non_subset_still_pending(self, conn, mock_git):
        """Config_sets that are NOT a subset should remain pending normally.

        Scenario: complete config [A], then populate config [B] (not a subset of [A]).
        Expected: [B] rows get priority assigned and remain fetchable.
        """
        # Complete config with reader-threads=1 only
        self._populate_and_complete(
            conn, mock_git, CONFIG_SETS_SMALL, CONFIG_SETS_SMALL_JSON
        )

        # Populate with a DIFFERENT config (reader-threads=8 only, not subset of small)
        different_config = [{"io-threads": 8, "search.reader-threads": 8}]
        different_config_json = Json(different_config)

        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            different_config_json,
        )

        # Should be fetchable (not marked as subset)
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            different_config_json,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert len(pairs) == 2

    def test_superset_not_marked_as_subset(self, conn, mock_git):
        """If we complete a SMALL config first, then populate LARGE, LARGE should NOT be subset.

        Scenario: complete [reader-threads=1], then populate [reader-threads=1, reader-threads=8].
        Expected: LARGE is NOT a subset of SMALL, so it stays pending.
        """
        # Complete the smaller config
        self._populate_and_complete(
            conn, mock_git, CONFIG_SETS_SMALL, CONFIG_SETS_SMALL_JSON
        )

        # Populate with the larger config
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_LARGE_JSON,
        )

        # LARGE should be fetchable (it's a superset, not a subset)
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE_JSON,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert len(pairs) == 2

    def test_exact_match_is_subset(self, conn, mock_git):
        """Same config_sets completed twice should mark second as subset.

        Scenario: complete config [A, B], then populate same [A, B] again.
        Expected: second populate's rows are 'completed_as_subset' (exact match = subset).
        """
        self._populate_and_complete(
            conn, mock_git, CONFIG_SETS_LARGE, CONFIG_SETS_LARGE_JSON
        )

        # Populate same config again
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_LARGE_JSON,
        )

        # Should not be fetchable (all marked as subset since exact match was completed)
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE_JSON,
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
        Expected:
          - core1:mod1 → completed_as_subset (pair exists in superset's completed)
          - core2:mod1 → completed_as_subset (pair exists in superset's completed)
          - core3:mod1 → pending (no completed pair for core3 in superset)
        """
        # Step 1: Complete superset with core1, core2
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1", "core2"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_LARGE_JSON,
        )
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_LARGE_JSON,
            ARCHITECTURE,
            max_pairs=10,
        )
        mark_module_commits(
            conn, MODULE_NAME, pairs, CONFIG_NAME, CONFIG_SETS_LARGE_JSON, ARCHITECTURE
        )

        # Step 2: Populate subset with core1, core2, core3 (core3 is new)
        mock_rev_list.side_effect = [["core1", "core2", "core3"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"
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
            CONFIG_SETS_SMALL_JSON,
        )

        # Step 3: Verify statuses
        table = _module_table_name(MODULE_NAME)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT sha, module_sha, status FROM {table} "
                f"WHERE config_sets = %s ORDER BY sha",
                (CONFIG_SETS_SMALL_JSON,),
            )
            rows = cur.fetchall()

        statuses = {(row[0], row[1]): row[2] for row in rows}
        assert statuses[("core1", "mod1")] == "completed_as_subset"
        assert statuses[("core2", "mod1")] == "completed_as_subset"
        assert statuses[("core3", "mod1")] == "pending"

        # Only core3:mod1 should be fetchable
        pairs = fetch_next_module_commits(
            conn,
            MODULE_NAME,
            CONFIG_NAME,
            CONFIG_SETS_SMALL_JSON,
            ARCHITECTURE,
            max_pairs=10,
        )
        assert pairs == ["core3:mod1"]


# ---------------------------------------------------------------------------
# check_incomplete_rows
# ---------------------------------------------------------------------------


class TestCheckIncompleteRows:
    """Verify check_incomplete_rows detects NULL values in required fields."""

    def test_passes_when_all_rows_complete(self, conn, mock_git):
        """Normal populate produces complete rows — check should pass (return 0)."""
        mock_rev_list, mock_commit_time = mock_git
        mock_rev_list.side_effect = [["core1"], ["mod1"]]
        mock_commit_time.return_value = "2026-06-01T10:00:00+00:00"

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
            CONFIG_SETS_JSON,
        )

        # Should pass — all rows have priority and status
        count = check_incomplete_rows(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )
        assert count == 0

    def test_exits_when_null_priority_found(self, conn, mock_git):
        """Manually insert a row with NULL priority — check should exit with error."""
        mock_rev_list, mock_commit_time = mock_git
        _create_module_table(conn, MODULE_NAME)
        table = _module_table_name(MODULE_NAME)

        # Insert a row with NULL priority (simulating corruption)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (sha, module_sha, core_timestamp, module_timestamp,
                                     max_commit_timestamp, min_commit_timestamp,
                                     status, config_name, config_sets, architecture)
                VALUES ('bad1', 'mod1', '2026-06-01T10:00:00+00:00', '2026-06-01T10:00:00+00:00',
                        '2026-06-01T10:00:00+00:00', '2026-06-01T10:00:00+00:00',
                        'pending', %s, %s, %s)
            """,
                (CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE),
            )
        conn.commit()

        # Should detect the NULL priority and raise
        with pytest.raises(ValueError):
            check_incomplete_rows(
                conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
            )

    def test_passes_on_empty_table(self, conn, mock_git):
        """Empty table should pass (nothing to check)."""
        mock_rev_list, mock_commit_time = mock_git
        _create_module_table(conn, MODULE_NAME)

        count = check_incomplete_rows(
            conn, MODULE_NAME, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )
        assert count == 0


# ---------------------------------------------------------------------------
# _assign_priority_in_memory
# ---------------------------------------------------------------------------


class TestAssignPriorityInMemory:
    """Verify in-memory priority assignment based on pointer."""

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

    def test_no_pointer_all_forward(self, conn, mock_git):
        """When no completed pairs exist, all get priority=1 (forward)."""

        _create_module_table(conn, MODULE_NAME)
        table = _module_table_name(MODULE_NAME)

        pairs = [
            self._make_pair("2026-06-01T10:00:00+00:00", "2026-06-02T10:00:00+00:00"),
            self._make_pair("2026-06-03T10:00:00+00:00", "2026-06-04T10:00:00+00:00"),
        ]

        _assign_priority_in_memory(
            conn, pairs, table, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )

        assert pairs[0].priority == 1
        assert pairs[1].priority == 1

    def test_forward_and_fallback_classification(self, conn, mock_git):
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

        _create_module_table(conn, MODULE_NAME)
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
                        CONFIG_SETS_JSON,
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
            conn, pairs, table, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
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

    def test_skips_already_assigned_pairs(self, conn, mock_git):
        """Pairs with priority already set (e.g., subset=99) should not be changed."""

        _create_module_table(conn, MODULE_NAME)
        table = _module_table_name(MODULE_NAME)

        pair = self._make_pair("2026-06-05T10:00:00+00:00", "2026-06-04T10:00:00+00:00")
        pair.priority = 99
        pair.status = "completed_as_subset"

        _assign_priority_in_memory(
            conn, [pair], table, CONFIG_NAME, CONFIG_SETS_JSON, ARCHITECTURE
        )

        assert pair.priority == 99
