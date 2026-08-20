"""Tests for ServerLauncher shutdown logic.

Unit tests for helpers + a real-process test that validates SIGKILL escalation
by spawning a process that ignores SIGTERM (simulating a stuck valkey-server).
"""

import os
import signal
import subprocess
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

from valkey_server import ServerLauncher, VALKEY_SERVER


def _valkey_server_already_running() -> bool:
    """Return True if a real valkey-server matching our pattern is already running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", VALKEY_SERVER],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


# Skip real-process tests when a valkey-server is already running on the host
# to avoid killing unrelated servers (e.g., on benchmark machines).
_skip_if_server_running = pytest.mark.skipif(
    _valkey_server_already_running(),
    reason=(
        f"Real valkey-server matching '{VALKEY_SERVER}' already running. "
        "Skipping to avoid killing it."
    ),
)


@pytest.fixture
def launcher():
    """Create a minimal ServerLauncher instance."""
    sl = ServerLauncher(
        results_dir="/tmp/test_results",
        valkey_path="/tmp/valkey",
    )
    sl.config = None
    return sl


# ---------------------------------------------------------------------------
# Unit tests for simple helpers (no time mocking needed)
# ---------------------------------------------------------------------------


class TestValkeyProcessesRunning:
    """Test _valkey_processes_running helper."""

    @patch("valkey_server.subprocess.run")
    def test_returns_true_when_process_found(self, mock_run, launcher):
        mock_run.return_value = MagicMock(returncode=0, stdout="12345\n")
        assert launcher._valkey_processes_running() is True

    @patch("valkey_server.subprocess.run")
    def test_returns_false_when_no_process(self, mock_run, launcher):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert launcher._valkey_processes_running() is False

    @patch("valkey_server.subprocess.run")
    def test_returns_false_on_exception(self, mock_run, launcher):
        mock_run.side_effect = Exception("pgrep not found")
        assert launcher._valkey_processes_running() is False


class TestGetValkeyPids:
    """Test _get_valkey_pids helper."""

    @patch("valkey_server.subprocess.run")
    def test_returns_pids(self, mock_run, launcher):
        mock_run.return_value = MagicMock(returncode=0, stdout="12345\n67890\n")
        assert launcher._get_valkey_pids() == ["12345", "67890"]

    @patch("valkey_server.subprocess.run")
    def test_returns_empty_when_no_process(self, mock_run, launcher):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert launcher._get_valkey_pids() == []


class TestWaitForPortAvailable:
    """Test _wait_for_port_available helper."""

    @patch("valkey_server.subprocess.run")
    def test_returns_immediately_when_port_free(self, mock_run, launcher):
        """Port is free (no output lines) — should return quickly."""
        mock_run.return_value = MagicMock(
            returncode=0, stdout="State  Recv-Q Send-Q Local Address:Port\n"
        )
        start = time.time()
        launcher._wait_for_port_available(port=6379, timeout=10)
        elapsed = time.time() - start
        assert (
            elapsed < 2.0
        ), f"Should return immediately when port free, took {elapsed:.1f}s"

    @patch("valkey_server.subprocess.run")
    def test_waits_until_port_free(self, mock_run, launcher):
        """Port is busy first, then becomes free after 2 calls."""
        busy_result = MagicMock(
            returncode=0,
            stdout="State  Recv-Q Send-Q Local Address:Port\nLISTEN 0      511    *:6379\n",
        )
        free_result = MagicMock(
            returncode=0, stdout="State  Recv-Q Send-Q Local Address:Port\n"
        )
        mock_run.side_effect = [busy_result, busy_result, free_result]

        start = time.time()
        launcher._wait_for_port_available(port=6379, timeout=10)
        elapsed = time.time() - start
        # Should take ~2s (2 sleep(1) iterations before free)
        assert 1.5 < elapsed < 4.0, f"Expected ~2s wait, got {elapsed:.1f}s"

    @patch("valkey_server.subprocess.run")
    def test_proceeds_after_timeout(self, mock_run, launcher):
        """Port never frees — should warn and proceed after timeout."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="State  Recv-Q Send-Q Local Address:Port\nLISTEN 0      511    *:6379\n",
        )
        start = time.time()
        # Use short timeout so test doesn't take long
        launcher._wait_for_port_available(port=6379, timeout=3)
        elapsed = time.time() - start
        assert 2.5 < elapsed < 5.0, f"Should wait ~3s then proceed, took {elapsed:.1f}s"

    @patch("valkey_server.subprocess.run")
    def test_handles_ss_failure(self, mock_run, launcher):
        """If ss command fails, should still proceed after timeout."""
        mock_run.side_effect = Exception("ss not found")
        start = time.time()
        launcher._wait_for_port_available(port=6379, timeout=3)
        elapsed = time.time() - start
        assert 2.5 < elapsed < 5.0, f"Should wait ~3s then proceed, took {elapsed:.1f}s"


# ---------------------------------------------------------------------------
# Real-process test: validates shutdown + SIGKILL escalation
# ---------------------------------------------------------------------------

# Python script that ignores SIGTERM and keeps running.
# Contains "src/valkey-server" in its command line so pgrep/pkill -f can find it.
_STUBBORN_SCRIPT = """
import signal
import time
import sys

# Ignore SIGTERM completely (simulates stuck valkey-server)
signal.signal(signal.SIGTERM, signal.SIG_IGN)

# Stay alive until killed with SIGKILL
while True:
    time.sleep(0.1)
"""


@pytest.mark.slow
@_skip_if_server_running
class TestShutdownWithRealProcess:
    """Spawn a real process that ignores SIGTERM, verify SIGKILL escalation.

    These tests are marked slow and skipped when a real valkey-server is
    already running on the host to avoid killing unrelated servers.
    """

    @pytest.fixture
    def stubborn_process(self, tmp_path):
        """Spawn a Python process whose command line matches the VALKEY_SERVER
        pattern (pgrep -f 'src/valkey-server') and ignores SIGTERM."""
        # Write script to a file path containing "src/valkey-server" so
        # pgrep -f "src/valkey-server" matches the cmdline.
        script_dir = tmp_path / "src"
        script_dir.mkdir()
        script_path = script_dir / "valkey-server"
        script_path.write_text(_STUBBORN_SCRIPT)

        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Give it a moment to set up signal handler
        time.sleep(0.3)
        yield proc
        # Cleanup: ensure it's dead no matter what
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass

    def test_sigkill_escalation_kills_stubborn_process(
        self, launcher, stubborn_process
    ):
        """_wait_for_process_shutdown should SIGKILL a SIGTERM-resistant process."""
        pid = stubborn_process.pid

        # Verify process is actually running
        assert stubborn_process.poll() is None, "Stubborn process should be alive"

        # Verify pgrep can find it (same pattern the code uses)
        check = subprocess.run(
            ["pgrep", "-f", VALKEY_SERVER], capture_output=True, text=True
        )
        assert (
            str(pid) in check.stdout
        ), f"pgrep -f '{VALKEY_SERVER}' should find PID {pid}, got: {check.stdout}"

        # Use a short timeout (2s) so test doesn't take 30s
        launcher._wait_for_process_shutdown(timeout=2)

        # Reap the child process (it's a zombie until parent calls wait)
        # and verify it was killed by a signal
        exit_code = stubborn_process.wait(timeout=10)
        # Negative exit code means killed by signal; -9 = SIGKILL
        assert (
            exit_code == -signal.SIGKILL
        ), f"Process should have been killed by SIGKILL (exit -9), got: {exit_code}"

    def test_graceful_shutdown_no_sigkill_needed(self, launcher):
        """When no process is running, _wait_for_process_shutdown returns quickly."""
        start = time.time()
        launcher._wait_for_process_shutdown(timeout=30)
        elapsed = time.time() - start
        # Should return almost immediately (< 2s) since no process exists
        assert elapsed < 2.0, f"Should return fast when no process, took {elapsed:.1f}s"
