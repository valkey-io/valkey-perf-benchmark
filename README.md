# Valkey Performance Benchmark

A comprehensive benchmarking tool for [Valkey](https://github.com/valkey-io/valkey), an in-memory data structure store. This tool allows you to measure performance across different configurations, including TLS and cluster modes.

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
    "warmup": [10]
  }
]
```

### Configuration Parameters

- `requests`: Number of requests to perform
- `keyspacelen`: Key space size (number of distinct keys)
- `data_sizes`: Size of data in bytes
- `pipelines`: Number of commands to pipeline
- `commands`: Redis commands to benchmark
- `cluster_mode`: Whether to enable cluster mode ("yes" or "no")
- `tls_mode`: Whether to enable TLS ("yes" or "no")
- `warmup`: Warmup time in seconds before benchmarking

## Results

Benchmark results are stored in the `results/` directory, organized by commit ID:

```
results/
└── <commit-id>/
    ├── logs.txt                         # Benchmark logs
    ├── metrics.json                     # Performance metrics in JSON format
    # each row includes the commit timestamp
    └── valkey_log_cluster_disabled.log  # Valkey server logs
```

## Dashboard Hosted on S3

The `dashboard/` directory contains a small React application for visualizing
benchmark metrics. Changes to this directory trigger the `dashboard_sync.yml`
workflow which uploads the files to an Amazon S3 bucket configured for static
website hosting. Metrics files (`completed_commits.json` and the `results/`
folder) are stored in the same bucket so the dashboard can fetch them directly.
`completed_commits.json` now stores objects containing the commit SHA, the
original commit timestamp, and the benchmark status. The dashboard ignores
entries with the status `in_progress`:

```json
[ { "sha": "abcdef123", "timestamp": "2024-01-02T15:04:05Z", "status": "complete" } ]
```

Open `dashboard/index.html` from your bucket to view the latest benchmark
results. See `dashboard/README.md` for more details.
## License

This project is licensed under the same license as Valkey.
