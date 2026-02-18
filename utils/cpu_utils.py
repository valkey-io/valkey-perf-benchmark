"""CPU core range parsing and allocation utilities."""

import os
from typing import List


def calculate_cpu_ranges(
    cluster_nodes: int, cores_per_unit: int, offset: int = 0
) -> List[str]:
    """Calculate CPU ranges for servers or clients."""
    ranges = []
    for i in range(cluster_nodes):
        start = offset + i * cores_per_unit
        end = start + cores_per_unit - 1
        ranges.append(f"{start}-{end}")
    return ranges


def calculate_and_validate_cpu_ranges(
    cfg: dict,
    manual_key: str,
    auto_cores_key: str,
    use_offset: bool = False,
):
    """Calculate CPU ranges with validation (DRY helper)."""
    if "cpu_allocation" not in cfg:
        return None

    cpu_alloc = cfg["cpu_allocation"]

    if manual_key in cpu_alloc:
        ranges = cpu_alloc[manual_key]
    else:
        # Use actual cluster_mode, not config value (respects --cluster-mode-filter)
        cluster_nodes = (
            1 if not cfg.get("cluster_mode") else cfg.get("cluster_nodes", 1)
        )
        offset = 0
        if use_offset:
            offset = cluster_nodes * cpu_alloc["cores_per_server"]
        ranges = calculate_cpu_ranges(cluster_nodes, cpu_alloc[auto_cores_key], offset)

    for range_str in ranges:
        parse_core_range(range_str)

    return ranges


def calculate_server_cpu_ranges(cfg: dict):
    """Calculate server CPU ranges from config."""
    return calculate_and_validate_cpu_ranges(
        cfg, "servers", "cores_per_server", use_offset=False
    )


def calculate_client_cpu_ranges(cfg: dict):
    """Calculate client CPU ranges from config."""
    return calculate_and_validate_cpu_ranges(
        cfg, "clients", "cores_per_client", use_offset=True
    )


def validate_explicit_cpu_ranges(server_range: str, client_range: str) -> None:
    """Validate explicit server + client CPU ranges for overlap and total."""
    server_cores = set(parse_core_range(server_range))
    client_cores = set(parse_core_range(client_range))

    overlap = server_cores & client_cores
    if overlap:
        raise ValueError(
            f"server_cpu_range and client_cpu_range overlap on cores: {sorted(overlap)}"
        )

    total_cores = server_cores | client_cores
    max_cores = os.cpu_count()
    if max_cores and len(total_cores) > max_cores:
        raise ValueError(
            f"Total CPU allocation ({len(total_cores)} cores) exceeds system cores ({max_cores})"
        )


def parse_core_range(range_str: str) -> List[int]:
    """Parse CPU core range string to list of core IDs.

    Supports:
    - Simple range: "0-3" → [0, 1, 2, 3]
    - Comma separated: "0,2,4" → [0, 2, 4]
    - Multiple ranges: "0-3,8-11" → [0, 1, 2, 3, 8, 9, 10, 11]

    Returns:
        List of core IDs

    Raises:
        ValueError: If format is invalid
    """
    if not range_str or not isinstance(range_str, str):
        raise ValueError("Core range must be a non-empty string")

    if range_str.startswith(",") or range_str.endswith(","):
        raise ValueError("Core range cannot start or end with comma")

    if ",," in range_str:
        raise ValueError("Core range cannot contain consecutive commas")

    cores = []

    try:
        parts = [part.strip() for part in range_str.split(",")]
        if not parts or any(not part for part in parts):
            raise ValueError("Core range must contain at least one core or range")

        for part in parts:
            if "-" in part:
                # Handle range format like "0-3" or "144-191"
                range_parts = part.split("-")
                if len(range_parts) != 2:
                    raise ValueError(f"Range format should be 'start-end', got: {part}")
                start, end = int(range_parts[0]), int(range_parts[1])
                if start < 0 or end < 0 or start > end:
                    raise ValueError(f"Invalid core range values in: {part}")
                cores.extend(range(start, end + 1))
            else:
                # Handle individual core number
                core = int(part)
                if core < 0:
                    raise ValueError(f"Core numbers must be non-negative, got: {core}")
                cores.append(core)
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(f"Invalid core range format: {range_str}")
        raise

    return cores
