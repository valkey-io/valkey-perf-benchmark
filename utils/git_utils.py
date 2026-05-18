"""Git utilities for resolving refs and fetching commits."""

import logging
import subprocess
from pathlib import Path
from typing import Optional


def resolve_ref(ref: str, repo_path: Path) -> str:
    """Resolve a git ref (branch, tag, or SHA) to a full commit SHA.

    Handles shallow clones by fetching the ref if not found locally.

    Args:
        ref: Git reference (branch name, tag, or commit SHA)
        repo_path: Path to the git repository

    Returns:
        Full 40-character commit SHA

    Raises:
        RuntimeError: If ref cannot be resolved after fetch attempts
    """
    # Try resolving locally first
    sha = _try_resolve_local(ref, repo_path)
    if sha:
        return sha

    # Fetch and retry
    logging.info(f"Ref '{ref}' not found locally, fetching from origin...")
    _fetch_ref(ref, repo_path)

    # Try origin/ref for branches
    sha = _try_resolve_local(f"origin/{ref}", repo_path)
    if sha:
        return sha

    # Try FETCH_HEAD (for direct SHA fetches)
    sha = _try_resolve_local("FETCH_HEAD", repo_path)
    if sha:
        return sha

    # Final attempt with original ref
    sha = _try_resolve_local(ref, repo_path)
    if sha:
        return sha

    raise RuntimeError(f"Could not resolve ref '{ref}' in {repo_path}")


def _try_resolve_local(ref: str, repo_path: Path) -> Optional[str]:
    """Try to resolve ref locally without network access."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def _fetch_ref(ref: str, repo_path: Path) -> None:
    """Fetch a specific ref from origin."""
    # Try fetching as branch/tag first
    result = subprocess.run(
        ["git", "fetch", "origin", ref],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return

    # If that fails, try fetching as a SHA (requires uploadpack.allowReachableSHA1InWant)
    subprocess.run(
        ["git", "fetch", "origin", ref, "--depth=1"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )


def get_commit_timestamp(sha: str, repo_path: Path) -> str:
    """Get ISO8601 timestamp for a commit SHA.

    Args:
        sha: Commit SHA (should be resolved, not a branch name)
        repo_path: Path to the git repository

    Returns:
        ISO8601 formatted commit timestamp

    Raises:
        RuntimeError: If timestamp cannot be retrieved
    """
    try:
        result = subprocess.run(
            ["git", "show", "-s", "--format=%cI", sha],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to get timestamp for {sha}: {e.stderr}") from e
