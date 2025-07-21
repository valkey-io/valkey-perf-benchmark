# Valkey Performance Benchmark

A comprehensive benchmarking tool for [Valkey](https://github.com/valkey-io/valkey), an in-memory data store. This tool allows you to measure performance across different commits, configurations, including TLS and cluster modes.

## Features

- Benchmark Valkey server with various commands (SET, GET, RPUSH, etc.)
- Test with different data sizes and pipeline configurations
- Support for TLS and cluster mode testing
- Automatic server setup and teardown
- Detailed performance metrics collection and reporting
- Compare performance between different Valkey versions/commits
- Optional CPU pinning via `taskset` (separate ranges for server and benchmark)

## Prerequisites

- Git
- Python 3.6+
- Linux environment (for taskset CPU pinning)
- Build tools required by Valkey (gcc, make, etc.)

## Project Structure

```
valkey-perf-benchmark/
├── configs/                  # Benchmark configuration files
│   └── benchmark-configs.json
├── results/                  # Benchmark results stored here
├── benchmark.py              # Main entry point
├── valkey_build.py           # Handles building Valkey from source
├── valkey_server.py          # Manages Valkey server instances
├── valkey_benchmark.py       # Runs benchmark tests
├── process_metrics.py        # Processes and formats benchmark results
└── logger.py                 # Logging utilities
```

## Usage

### Basic Usage

```bash
# Run both server and client benchmarks with default configuration
python benchmark.py

# Run only the server component
python benchmark.py --mode server

# Run only the client component against a specific server
python benchmark.py --mode client --target-ip 192.168.1.100

# Use a specific configuration file
python benchmark.py --config ./configs/my-custom-config.json

# Benchmark a specific commit
python benchmark.py --commits 1a2b3c4d

# Use a pre-existing Valkey dir
python benchmark.py --valkey-path /path/to/valkey

# Use a pre-running Valkey Server
python benchmark.py --valkey-path /path/to/valkey --use-running-server

### Comparison Mode

# Compare with baseline
python benchmark.py --commits HEAD --baseline unstable
```

### Advanced Options

```bash
# Use an already running Valkey server (client mode only with `--valkey-path`)
python benchmark.py --mode client --valkey-path /path/to/valkey --use-running-server

# Specify custom results directory
python benchmark.py --results-dir ./my-results

# Set logging level
python benchmark.py --log-level DEBUG
# Pin server and benchmark to separate CPU cores
python benchmark.py --cpu-range 0-1,2-3
```

## Configuration

Create benchmark configurations in JSON format. Example:

```json
[
  {
    "requests": [10000000],
    "keyspacelen": [10000000],
    "data_sizes": [16, 64, 256],
    "pipelines": [1, 10, 100],
    "commands": ["SET", "GET"],
    "cluster_mode": "yes",
    "tls_mode": "yes",
    "warmup": 10
  }
]
```

### Configuration Parameters

| Parameter | Description | Data Type | Multiple Values |
|-----------|-------------|-----------|----------------|
| `requests` | Number of requests to perform | Integer | Yes |
| `keyspacelen` | Key space size (number of distinct keys) | Integer | Yes |
| `data_sizes` | Size of data in bytes | Integer | Yes |
| `pipelines` | Number of commands to pipeline | Integer | Yes |
| `clients` | Number of concurrent client connections | Integer | Yes |
| `commands` | Valkey commands to benchmark | String | Yes |
| `cluster_mode` | Whether to enable cluster mode | String ("yes"/"no") | No |
| `tls_mode` | Whether to enable TLS | String ("yes"/"no") | No |
| `warmup` | Warmup time in seconds before benchmarking | Integer | No |

Supported commands:
```
"SET", "GET", "RPUSH", "LPUSH", "LPOP", "SADD", "SPOP", "HSET", "GET", "MGET", "LRANGE", "SPOP", "ZPOPMIN"
```

## Results

Benchmark results are stored in the `results/` directory, organized by commit ID:

```
results/
└── <commit-id>/
    ├── logs.txt                         # Benchmark logs
    ├── metrics.json                     # Performance metrics in JSON format
    └── valkey_log_cluster_disabled.log  # Valkey server logs
```

Sample metrics.json

```
[
    {
        "timestamp": "2025-05-28T01:29:42+02:00",
        "commit": "ff7135836b5d9ccaa19d5dbaf2a0b0325755c8b4",
        "command": "SET",
        "data_size": 16,
        "pipeline": 10,
        "clients": 10000000,
        "requests": 10000000,
        "rps": 556204.44,
        "avg_latency_ms": 0.813,
        "min_latency_ms": 0.28,
        "p50_latency_ms": 0.775,
        "p95_latency_ms": 1.159,
        "p99_latency_ms": 1.407,
        "max_latency_ms": 2.463,
        "cluster_mode": false,
        "tls": false
    }
]

## Dashboard Hosted on S3

The `dashboard/` directory contains a small JavaScript application for visualizing
benchmark metrics. Changes to this directory trigger the `dashboard_sync.yml`
workflow which uploads the files to an Amazon S3 bucket configured for static
website hosting. Metrics files (`completed_commits.json` and the `results/`
folder) are stored in the same bucket so the dashboard can fetch them directly.
`completed_commits.json` now stores objects containing the commit SHA, the
original commit timestamp, and the benchmark status. The dashboard ignores
entries with the status `in_progress`.

```json
[ { "sha": "abcdef123", "timestamp": "2024-01-02T15:04:05Z", "status": "complete" } ]
```

Open `dashboard/index.html` from your bucket to view the latest benchmark
results. See `dashboard/README.md` for more details.
## License

This project is licensed under the same license as Valkey.
