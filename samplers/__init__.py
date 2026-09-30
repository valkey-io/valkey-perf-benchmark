"""Sample sources a per-second metrics run can select between.

A config names sources by the keys of `SOURCES`, and a run that names none gets
`DEFAULT_SOURCES`.
"""

from .base import SampleSource, SamplerContext
from .disk import DiskSource
from .latency_histogram import LatencyHistogramSource
from .process_cpu import ProcessCpuSource
from .valkey_info import ValkeyInfoSource

SOURCES = {
    cls.name: cls
    for cls in (
        ValkeyInfoSource,
        LatencyHistogramSource,
        ProcessCpuSource,
        DiskSource,
    )
}

DEFAULT_SOURCES = ("valkey_info", "latency_histogram", "process_cpu", "disk")
