# Valkey Performance Benchmark

A comprehensive benchmarking tool for [Valkey](https://github.com/valkey-io/valkey), an in-memory data store. This tool allows you to measure performance across different commits, configurations, including TLS and cluster modes.

## Features

- Benchmark Valkey server with various commands (SET, GET, RPUSH, etc.)
- Test with different data sizes and pipeline configurations
- Support for TLS and cluster mode testing
- Automatic server setup and teardown
- Detailed performance metrics collection and reporting
- Compare performance between different Valkey versions/commits
- Optional CPU pinning via `taskset` using `--server-cpu-range` and `--client-cpu-range`
- Continuous benchmarking via GitHub Actions workflow
- Automated commit tracking and progress management
- S3 integration for storing results and hosting dashboard

## Prerequisites

- Git
- Python 3.6+
- [valkey-py](https://github.com/valkey-io/valkey-py) Python client (install via `pip install valkey`)
- Linux environment (for taskset CPU pinning)
- Build tools required by Valkey (gcc, make, etc.)

## Project Structure

```
valkey-perf-benchmark/
├── .github/workflows/        # GitHub Actions workflows
│   ├── valkey_benchmark.yml  # Continuous benchmarking workflow
│   ├── dashboard_sync.yml    # Dashboard deployment to S3
│   ├── basic.yml            # Basic validation tests
│   ├── check_format.yml     # Code formatting checks
│   └── cluster_tls.yml      # Cluster and TLS specific tests
├── configs/                  # Benchmark configuration files
│   ├── benchmark-configs.json
│   └── benchmark-configs-cluster-tls.json
├── dashboard/               # Web dashboard for visualizing results
│   ├── index.html
│   ├── app.js
│   └── README.md
├── results/                 # Benchmark results stored here
├── utils/                   # Utility scripts
│   ├── workflow_commits.py  # Commit tracking and management
│   └── compare_benchmark_results.py  # Result comparison utilities
├── benchmark.py             # Main entry point
├── valkey_build.py          # Handles building Valkey from source
├── valkey_server.py         # Manages Valkey server instances
├── valkey_benchmark.py      # Runs benchmark tests
├── process_metrics.py       # Processes and formats benchmark results
└── requirements.txt         # Python dependencies
```

Each benchmark run clones a fresh copy of the Valkey repository for the
target commit. If ``--valkey-path`` is omitted, the repository is cloned into
``valkey_<commit>`` and removed after the run to keep builds isolated and
repeatable.

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

# Without --valkey-path a directory named valkey_<commit> is cloned and later removed

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

# Pin server and client processes to different CPUs
python benchmark.py --server-cpu-range 0-1 --client-cpu-range 2-3

# Specify custom completed commits file location
python benchmark.py --completed-file ./my-completed-commits.json
```

## Configuration

Create benchmark configurations in JSON format. Each object represents a single
set of options and configurations are **not** automatically cross-multiplied.
Example:

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

When `warmup` is provided for read commands, the benchmark performs three
stages:
1. A data injection pass using the corresponding write command with
   `--sequential` to seed the keyspace.
2. A warmup run of the read command (without `--sequential`) for the specified
   duration.
3. The main benchmark run of the read command.

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

```json
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
```

## Continuous Benchmarking & CI/CD

### GitHub Actions Workflows

The project includes several GitHub Actions workflows for automated testing and deployment:

- **`valkey_benchmark.yml`**: Continuous benchmarking workflow that runs on self-hosted EC2 runners
  - Automatically benchmarks new commits from the Valkey unstable branch
  - Manages commit tracking via `completed_commits.json`
  - Uploads results to S3 for dashboard consumption
  - Supports manual triggering with configurable commit limits

- **`dashboard_sync.yml`**: Automatically deploys dashboard changes to S3
- **`basic.yml`**: Basic validation and testing
- **`check_format.yml`**: Code formatting validation
- **`cluster_tls.yml`**: Specialized tests for cluster and TLS configurations

### Commit Tracking

The system uses `completed_commits.json` to track benchmarking progress. This file is automatically create

```json
[
  {
    "sha": "abcdef123456",
    "timestamp": "2024-01-02T15:04:05Z",
    "status": "complete"
  },
  {
    "sha": "789xyz456def",
    "timestamp": "2024-01-02T16:30:22Z", 
    "status": "in_progress"
  }
]
```

Status values:
- `complete`: Benchmark finished successfully
- `in_progress`: Currently being benchmarked
- Failed benchmarks are cleaned up automatically

### Dashboard

The `dashboard/` directory contains a JavaScript application for visualizing benchmark metrics. The dashboard:
- Fetches data from local results directory when run locally, or from S3 bucket when hosted on S3
- Ignores commits with `in_progress` status
- Provides interactive charts and performance comparisons
- Updates automatically when new results are uploaded
- Can be used for visualizing local benchmarking results during Valkey development

**Deployment Options:**
- **S3 Hosted**: Access via your S3 bucket's static website URL (automatically deployed via `dashboard_sync.yml`)
- **Local Development**: Serve the dashboard locally using any HTTP server to visualize your local Valkey benchmarking results generated by this project:
  ```bash
  # Using Python's built-in server
  cd dashboard
  python -m http.server 8000
  # Then open http://localhost:8000
  
  # Or using Node.js
  npx serve .
  ```

## Utilities

### Commit Management
- `utils/workflow_commits.py`: Manages commit tracking, status updates, and cleanup operations
- `utils/compare_benchmark_results.py`: Utilities for comparing benchmark results across commits

### Configuration Files
- `configs/benchmark-configs.json`: Standard benchmark configurations
- `configs/benchmark-configs-cluster-tls.json`: Specialized configurations for cluster and TLS testing

## Development

### Local Development
The tool automatically creates `completed_commits.json` if it doesn't exist, making local development straightforward. Simply run:

```bash
python benchmark.py --commits HEAD
```

### Adding New Configurations
Create new JSON configuration files in the `configs/` directory following the existing format. Each configuration object represents a complete benchmark scenario.

## License

Please see the [LICENSE.md](./LICENSE.md)