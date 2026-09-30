"""Stub inputs that produce tests/data/sampler_golden_rows.json.

One place holds the canned INFO text, the /proc tick values and the block
device counter snapshots, so the stored golden rows and the test that replays
them are driven by the same inputs. Three ticks are served, with the measured
intervals 1.0s and 2.0s between them, which is what makes a rate column differ
from a raw delta column.
"""

# time.monotonic values served to the three ticks: elapsed_sec 0, 1 and 3.
GOLDEN_MONOTONIC = [100.0, 101.0, 103.0]
GOLDEN_START_MONOTONIC = 100.0
GOLDEN_WALL_CLOCK = 1782868117

GOLDEN_HOST = "127.0.0.1"
GOLDEN_PORT = 6379
GOLDEN_CLI_PATH = "valkey-cli"
GOLDEN_SERVER_PID = 1234
GOLDEN_BLOCK_DEVICE = "nvme0n1"
GOLDEN_INTERVAL = 1.0
GOLDEN_CLK_TCK = 100
GOLDEN_CONTEXT = {
    "commit": "abc1234",
    "scenario": "zipfian-80-20",
    "test_id": "1_zipfian-80-20",
    "command": "SET",
    "data_size": 64,
    "pipeline": 1,
    "clients": 4,
    "config_set": {},
    "io_threads": 8,
    "architecture": "arm64",
}

# A tiering-enabled INFO ALL snapshot. Carries a version string, a Commandstats
# line, a Keyspace line and float-formatted fields alongside the counters, so
# every value shape the sampler can meet is present.
INFO_TEMPLATE = """# Server
valkey_version:8.0.1
process_id:1234
executable:/usr/local/bin/valkey-server

# Clients
connected_clients:5
blocked_clients:{blocked_clients}

# Memory
used_memory:{used_memory}
used_memory_rss:6959104
maxmemory:1073741824
mem_fragmentation_ratio:{mem_frag_ratio}
mem_allocator:jemalloc-5.3.0

# Stats
total_commands_processed:{total_commands}
instantaneous_ops_per_sec:12345
keyspace_hits:{keyspace_hits}
keyspace_misses:{keyspace_misses}

# Commandstats
cmdstat_get:calls={get_calls},usec=1250,usec_per_call=2.50,rejected_calls=0,failed_calls=0
cmdstat_set:calls={set_calls},usec=4000,usec_per_call=4.00,rejected_calls=0,failed_calls=0

# External Storage
total_num_items_spilled_to_ext_storage:{spilled}
total_num_items_fetched_from_ext_storage:{fetched}
num_items_spilling_to_ext_storage:{spilling}
kbc_fetching_block:{kbc_fetching_block}
completion_read_ok:{completion_read_ok}
oom_reject_write_count:{oom_reject_write_count}
spill_attempts:{spill_attempts}
spill_submitted:99
dram_value_hits:{dram_value_hits}
throttle_total_throttled:{throttle_total_throttled}
throttle_queued_clients:{throttle_queued_clients}
throttle_current_rate:{throttle_current_rate}
throttle_allowed_tps:{throttle_allowed_tps}
spill_submitted_count:{spill_submitted_count}
spill_serialized_count:{spill_serialized_count}
mean_spill_ram:{mean_spill_ram}
inflight_spill_ram_bytes:{inflight_spill_ram_bytes}

# Keyspace
db0:keys={keys},expires=0,avg_ttl=0
"""

INFO_VALUES = [
    {
        "blocked_clients": 2,
        "used_memory": 1799288,
        "mem_frag_ratio": "3.96",
        "total_commands": 1000,
        "keyspace_hits": 800,
        "keyspace_misses": 200,
        "get_calls": 500,
        "set_calls": 1000,
        "spilled": 5000,
        "fetched": 1200,
        "spilling": 7,
        "kbc_fetching_block": 4,
        "completion_read_ok": 250,
        "oom_reject_write_count": 11,
        "spill_attempts": 6100,
        "dram_value_hits": 1000,
        "throttle_total_throttled": 900,
        "throttle_queued_clients": 13,
        "throttle_current_rate": "0.8125",
        "throttle_allowed_tps": "45000.5",
        "spill_submitted_count": 5900,
        "spill_serialized_count": 5850,
        "mean_spill_ram": 2048,
        "inflight_spill_ram_bytes": 102400,
        "keys": 10000,
    },
    {
        "blocked_clients": 3,
        "used_memory": 2100000,
        "mem_frag_ratio": "3.50",
        "total_commands": 3000,
        "keyspace_hits": 1600,
        "keyspace_misses": 400,
        "get_calls": 1500,
        "set_calls": 2200,
        "spilled": 7000,
        "fetched": 2000,
        "spilling": 9,
        "kbc_fetching_block": 5,
        "completion_read_ok": 1250,
        "oom_reject_write_count": 12,
        "spill_attempts": 7100,
        "dram_value_hits": 2000,
        "throttle_total_throttled": 950,
        "throttle_queued_clients": 5,
        "throttle_current_rate": "0.7500",
        "throttle_allowed_tps": "44000.0",
        "spill_submitted_count": 6900,
        "spill_serialized_count": 6850,
        "mean_spill_ram": 3072,
        "inflight_spill_ram_bytes": 204800,
        "keys": 20000,
    },
    {
        "blocked_clients": 0,
        "used_memory": 2500000,
        "mem_frag_ratio": "3.10",
        "total_commands": 7000,
        "keyspace_hits": 3000,
        "keyspace_misses": 900,
        "get_calls": 3500,
        "set_calls": 4400,
        "spilled": 9000,
        "fetched": 2600,
        "spilling": 2,
        "kbc_fetching_block": 1,
        "completion_read_ok": 1250,
        "oom_reject_write_count": 13,
        "spill_attempts": 8100,
        "dram_value_hits": 3000,
        "throttle_total_throttled": 980,
        "throttle_queued_clients": 0,
        "throttle_current_rate": "0.5000",
        "throttle_allowed_tps": "43000.0",
        "spill_submitted_count": 7900,
        "spill_serialized_count": 7900,
        "mean_spill_ram": 4096,
        "inflight_spill_ram_bytes": 0,
        "keys": 30000,
    },
]

GOLDEN_INFO_TEXTS = [INFO_TEMPLATE.format(**values) for values in INFO_VALUES]

# (user+nice, system, total) tick triples from /proc/stat.
GOLDEN_SYSTEM_CPU = [(100, 50, 1000), (200, 100, 2000), (500, 200, 4000)]

# (utime, stime) tick pairs from /proc/<pid>/stat.
GOLDEN_PROCESS_CPU = [(100, 50), (180, 70), (400, 130)]

# Summed user+system ticks of the async IO worker threads.
GOLDEN_ASIO_TICKS = [100, 150, 400]

# /sys/block/<dev>/stat snapshots.
GOLDEN_DISK_COUNTERS = [
    {
        "read_ios": 100,
        "read_merges": 10,
        "read_sectors": 2048,
        "read_ticks": 50,
        "write_ios": 200,
        "write_merges": 20,
        "write_sectors": 4096,
        "write_ticks": 90,
        "in_flight": 3,
        "io_ticks": 1000,
        "time_in_queue": 5000,
    },
    {
        "read_ios": 300,
        "read_merges": 50,
        "read_sectors": 10240,
        "read_ticks": 650,
        "write_ios": 250,
        "write_merges": 35,
        "write_sectors": 7296,
        "write_ticks": 490,
        "in_flight": 7,
        "io_ticks": 1250,
        "time_in_queue": 6500,
    },
    {
        "read_ios": 700,
        "read_merges": 130,
        "read_sectors": 26624,
        "read_ticks": 1850,
        "write_ios": 450,
        "write_merges": 75,
        "write_sectors": 23680,
        "write_ticks": 1290,
        "in_flight": 11,
        "io_ticks": 2250,
        "time_in_queue": 12500,
    },
]
