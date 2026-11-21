#!/usr/bin/env python3
"""Write a set_matrix config with command selection and JSON overrides applied.

    python scripts/resolve_set_matrix.py --config configs/set-commands.json \
        --commands sismember,srem_sadd \
        --overrides '{"set_matrix": {"duration": 6, "window": [1, 4]}}' \
        --out results/resolved-config.json

Overrides are merged into every config entry: objects merge key by key, other
values replace, and null deletes a key. The written file is what benchmark.py
runs, so it can be kept next to the results it produced.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmark import validate_config  # noqa: E402
from set_matrix import deep_merge  # noqa: E402


def resolve(configs: list, commands: str, overrides: dict) -> list:
    """Return configs with overrides merged and run_commands set."""
    resolved = []
    for cfg in configs:
        cfg = deep_merge(cfg, overrides)
        if commands and commands != "all":
            if "set_matrix" not in cfg:
                raise ValueError("--commands needs a config with a set_matrix block")
            cfg["set_matrix"]["run_commands"] = [
                c.strip() for c in commands.split(",") if c.strip()
            ]
        resolved.append(cfg)
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--commands", default="all", help="comma-separated command ids, or all"
    )
    parser.add_argument("--overrides", default="{}", help="JSON object")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    overrides = json.loads(args.overrides or "{}")
    if not isinstance(overrides, dict):
        raise SystemExit("--overrides must be a JSON object")
    with open(args.config) as f:
        resolved = resolve(json.load(f), args.commands, overrides)

    scenarios = 0
    for cfg in resolved:
        check = json.loads(json.dumps(cfg))
        validate_config(check)
        scenarios += sum(len(g["scenarios"]) for g in check["test_groups"])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(resolved, indent=2) + "\n")
    print(f"{out}: {scenarios} scenarios")


if __name__ == "__main__":
    main()
