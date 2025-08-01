import argparse
import json
import subprocess
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
    """Return up to ``max_commits`` SHAs not present in ``completed_file``."""
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
