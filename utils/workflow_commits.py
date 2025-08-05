import argparse
import json
import subprocess
import logging
from pathlib import Path
from typing import List, Dict


def load_commits(path: Path) -> List[Dict[str, str]]:
    """Return the list of commit entries stored in ``path``."""
    if path.exists() and path.stat().st_size > 0:
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return []
    return []


def save_commits(path: Path, commits: List[Dict[str, str]]) -> None:
    """Write commit entries to ``path`` with pretty formatting."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(commits, f, indent=2)


def completed_shas(path: Path) -> List[str]:
    """Return a list of commit SHAs present in ``path``."""
    return [c.get("sha", "") for c in load_commits(path)]


def _git_rev_list(repo: Path, branch: str) -> List[str]:
    proc = subprocess.run(
        ["git", "rev-list", branch],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip().splitlines()


def _git_commit_time(repo: Path, sha: str) -> str:
    proc = subprocess.run(
        ["git", "show", "-s", "--format=%cI", sha],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def determine_commits_to_benchmark(
    repo: Path, branch: str, completed_file: Path, max_commits: int
) -> List[str]:
    """Return up to ``max_commits`` SHAs not present in ``completed_file``.

    This will also automatically clean up any dangling 'in_progress' commits
    before determining which commits need to be benchmarked. This ensures that
    commits left in 'in_progress' state by failed benchmark processes are
    properly reset and can be re-processed.

    Args:
        repo: Path to the git repository
        branch: Git branch to examine
        completed_file: Path to completed_commits.json tracking file
        max_commits: Maximum number of commits to return

    Returns:
        List of commit SHAs that need to be benchmarked
    """
    cleaned_count = cleanup_in_progress_commits(completed_file)
    if cleaned_count > 0:
        logging.info(
            f"Cleaned up {cleaned_count} in_progress commits before determining commits to benchmark"
        )

    seen = set(completed_shas(completed_file))
    commits = []
    for sha in _git_rev_list(repo, branch):
        if sha not in seen:
            commits.append(sha)
        if len(commits) >= max_commits:
            break
    return commits


def mark_commits(
    completed_file: Path, repo: Path, shas: List[str], status: str
) -> None:
    """Update ``completed_file`` marking ``shas`` with the given ``status``."""
    commits = load_commits(completed_file)
    for sha in shas:
        # Resolve HEAD to actual commit SHA
        if sha == "HEAD":
            sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=repo, text=True
            ).strip()
        ts = _git_commit_time(repo, sha)
        found = False
        for entry in commits:
            if entry.get("sha") == sha:
                entry["status"] = status
                entry.setdefault("timestamp", ts)
                found = True
                break
        if not found:
            commits.append({"sha": sha, "timestamp": ts, "status": status})
        print(f"Marked {sha} as {status} with timestamp {ts}")
    save_commits(completed_file, commits)


def cleanup_in_progress_commits(completed_file: Path) -> int:
    """Remove all 'in_progress' entries from completed_commits.json.
    This return the number of "in_progress" entries that were cleaned up.
    """
    try:
        # Load existing commits
        commits = load_commits(completed_file)

        # Count in_progress entries before cleanup
        in_progress_count = sum(
            1 for commit in commits if commit.get("status") == "in_progress"
        )

        if in_progress_count == 0:
            logging.info("No in_progress commits found to clean up")
            return 0

        # Filter out in_progress entries, keeping only complete ones
        cleaned_commits = [
            commit for commit in commits if commit.get("status") != "in_progress"
        ]

        # Save the cleaned commits back to file
        save_commits(completed_file, cleaned_commits)

        logging.info(
            f"Cleaned up {in_progress_count} in_progress commits from {completed_file}"
        )
        return in_progress_count

    except FileNotFoundError:
        logging.warning(f"Completed commits file not found: {completed_file}")
        return 0
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON from {completed_file}: {e}")
        return 0
    except PermissionError as e:
        logging.error(f"Permission denied accessing {completed_file}: {e}")
        return 0
    except Exception as e:
        logging.error(f"Unexpected error during cleanup of {completed_file}: {e}")
        return 0


def completed_from_results(results_root: Path) -> List[Dict[str, str]]:
    """Generate completed commit entries from ``results_root`` directory."""
    entries = []
    if not results_root.exists():
        return entries
    for commit_dir in results_root.iterdir():
        metrics_file = commit_dir / "metrics.json"
        if metrics_file.exists():
            timestamp = ""
            try:
                with metrics_file.open("r", encoding="utf-8") as f:
                    metrics = json.load(f)
                    if metrics:
                        timestamp = metrics[0].get("timestamp", "")
            except Exception:
                pass
            entries.append(
                {"sha": commit_dir.name, "timestamp": timestamp, "status": "complete"}
            )
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Commit tracking helpers")
    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("determine", help="List commits to benchmark")
    d.add_argument("--repo", type=Path, required=True)
    d.add_argument("--branch", default="unstable")
    d.add_argument("--completed-file", type=Path, required=True)
    d.add_argument("--max-commits", type=int, default=3)

    m = sub.add_parser("mark", help="Mark commits with a status")
    m.add_argument("--repo", type=Path, required=True)
    m.add_argument("--completed-file", type=Path, required=True)
    m.add_argument("--status", choices=["in_progress", "complete"], required=True)
    m.add_argument("shas", nargs="+")

    f = sub.add_parser(
        "from-results", help="Generate completed_commits.json from results"
    )
    f.add_argument("--results-dir", type=Path, required=True)
    f.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()

    if args.cmd == "determine":
        commits = determine_commits_to_benchmark(
            repo=args.repo,
            branch=args.branch,
            completed_file=args.completed_file,
            max_commits=args.max_commits,
        )
        print(" ".join(commits))
    elif args.cmd == "mark":
        mark_commits(
            completed_file=args.completed_file,
            repo=args.repo,
            shas=args.shas,
            status=args.status,
        )
    elif args.cmd == "from-results":
        entries = completed_from_results(Path(args.results_dir))
        save_commits(Path(args.output), entries)


if __name__ == "__main__":
    main()
