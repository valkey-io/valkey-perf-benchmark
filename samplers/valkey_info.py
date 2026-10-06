"""Valkey INFO source for the per-second metrics sampler.

Reads `INFO ALL` once per tick and records every field as the string the server
reported, so typing is left to ingest.
"""

from typing import Dict, Optional

from .base import SampleSource, run_cli


def parse_info(text: str) -> Dict[str, str]:
    """Parse INFO output into a flat field to value dict, skipping headers.

    Sections are flattened because field names are unique across INFO sections.
    """
    fields: Dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip()
    return fields


class ValkeyInfoSource(SampleSource):
    """Every `INFO ALL` field as a raw string."""

    name = "valkey_info"

    def sample(self) -> Optional[Dict[str, str]]:
        """Return the parsed INFO ALL fields, or None when INFO failed."""
        output = run_cli(self.ctx, "INFO", "ALL")
        if output is None:
            return None
        return parse_info(output) or None
