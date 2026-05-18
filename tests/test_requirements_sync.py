"""Verify requirements.txt is in sync with requirements.in."""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_requirements_txt_contains_all_packages_from_requirements_in():
    """Every package declared in requirements.in must appear in requirements.txt."""
    req_in = (PROJECT_ROOT / "requirements.in").read_text()
    req_txt = (PROJECT_ROOT / "requirements.txt").read_text().lower()

    missing = []
    for line in req_in.splitlines():
        line = re.sub(r"#.*", "", line).strip()
        if not line:
            continue
        # Extract package name before any version specifier
        name = re.split(r"[><=!;\[]", line)[0].strip().lower()
        if not name:
            continue
        # Normalize underscores/hyphens (pip treats them as equivalent)
        normalized = re.sub(r"[-_]+", "[-_]", name)
        pattern = re.escape(normalized).replace(r"\[", "[").replace(r"\]", "]")
        if not re.search(rf"^{pattern}==", req_txt, re.MULTILINE):
            missing.append(name)

    assert not missing, (
        f"requirements.txt is missing packages from requirements.in: {missing}\n"
        "Run: uv pip compile --generate-hashes --python-version 3.10 "
        "requirements.in -o requirements.txt"
    )
