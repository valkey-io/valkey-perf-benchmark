# Valkey Performance Benchmark

A benchmarking tool for [Valkey](https://github.com/valkey-io/valkey), an in-memory data store. Measures performance across different commits and configurations, including TLS and cluster modes.

## Features

- Benchmarks Valkey server with various commands (SET, GET, RPUSH, etc.)
- Tests with different data sizes and pipeline configurations
- Supports TLS and cluster mode testing
- Handles automatic server setup and teardown
- Collects detailed performance metrics and generates reports
- Compares performance between different Valkey versions/commits
- Provides CPU pinning via `taskset` using configuration file settings
- Runs continuous benchmarking via GitHub Actions workflow
- Tracks commits and manages progress automatically
- Includes Grafana dashboards for visualizing performance metrics

## Prerequisites

- Git
- Python 3.6+
- [valkey-py](https://github.com/valkey-io/valkey-py) Python client (install via `pip install valkey`)
- Linux environment (for taskset CPU pinning)
- Build tools required by Valkey. (gcc, make, etc.)
- Install python modules required for this project: `pip install -r requirements.txt`.

## Project Structure

```
valkey-perf-benchmark/
├── .github/workflows/        # GitHub Actions workflows
│   ├── valkey_benchmark.yml  # Continuous benchmarking workflow
│   ├── basic.yml            # Basic validation tests
│   ├── check_format.yml     # Code formatting checks
│   └── cluster_tls.yml      # Cluster and TLS specific tests
├── configs/                  # Benchmark configuration files
│   ├── benchmark-configs.json
│   └── benchmark-configs-cluster-tls.json
├── dashboards/              # Grafana dashboards and deployment configs
│   ├── *.json              # Grafana dashboard definitions
│   ├── grafana-values.yaml # Helm configuration
│   ├── alb-ingress.yaml    # Kubernetes Ingress config
│   ├── infrastructure/     # CloudFormation templates
│   ├── deploy-stack.sh     # Infrastructure deployment script
│   └── *.md                # Deployment documentation
├── utils/                   # Utility scripts
│   ├── postgres_track_commits.py  # Commit tracking and management
│   └── compare_benchmark_results.py  # Result comparison utilities
├── benchmark.py             # Main entry point
├── valkey_build.py          # Handles building Valkey from source
├── valkey_server.py         # Manages Valkey server instances
├── valkey_benchmark.py      # Runs benchmark tests
├── process_metrics.py       # Processes and formats benchmark results
└── requirements.txt         # Python dependencies
```

Each benchmark run clones a fresh copy of the Valkey repository for the target commit. When `--valkey-path` is omitted, the repository is cloned into `valkey_<commit>` and removed after the run to maintain build isolation and repeatability.

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

# Use a custom valkey-benchmark executable
python benchmark.py --valkey-benchmark-path /path/to/custom/valkey-benchmark

# Use a pre-running Valkey Server
python benchmark.py --valkey-path /path/to/valkey --use-running-server

### Comparison Mode

# Compare with baseline
python benchmark.py --commits HEAD --baseline unstable

# Run multiple benchmark runs for statistical reliability
python benchmark.py --commits HEAD --runs 5
```

## Benchmark Comparison and Analysis

The project includes a comparison tool for analyzing benchmark results with statistical analysis and graph generation.

### Compare Benchmark Results

```bash
# Basic comparison between two result files
python utils/compare_benchmark_results.py --baseline results/commit1/metrics.json --new results/commit2/metrics.json --output comparison.md

# Generate graphs along with comparison
python utils/compare_benchmark_results.py --baseline results/commit1/metrics.json --new results/commit2/metrics.json --output comparison.md --graphs --graph-dir graphs/

# Filter to show only RPS metrics
python utils/compare_benchmark_results.py --baseline results/commit1/metrics.json --new results/commit2/metrics.json --output comparison.md --metrics rps --graphs

# Filter to show only latency metrics
python utils/compare_benchmark_results.py --baseline results/commit1/metrics.json --new results/commit2/metrics.json --output comparison.md --metrics latency --graphs
```

### Comparison Tool Features

- **Automatic Run Averaging**: Groups and averages multiple benchmark runs with identical configurations
- **Statistical Analysis**: Calculates means, standard deviations, and Coefficient of Variation (CV) with sample standard deviation (n-1)
- **Coefficient of Variation**: Provides normalized variability metrics (CV = σ/μ × 100%) for scale-independent comparison across performance metrics
- **Graph Generation**: matplotlib-based visualization including:
  - Consolidated comparison graphs for all metrics
  - Variance line graphs showing individual run values with error bars
  - RPS-focused filtering for integration purposes
- **Metrics Filtering**: Supports filtering by metric type (all, rps, latency)
- **Standardized Output**: Generates markdown reports with statistical information including CV

### Statistical Display Format

When multiple runs are available, the comparison tool displays comprehensive statistical information:

```
Metric Value (n=X, σ=Y, CV=Z%)
```

Where:

- `n`: Number of runs
- `σ`: Standard deviation
- `CV`: Coefficient of Variation as a percentage

The Coefficient of Variation (CV) is useful for:

- **Scale-independent comparison**: Compares variability across metrics with different units (e.g., RPS vs latency)
- **Performance consistency assessment**: Lower CV indicates more consistent performance
- **Benchmark reliability evaluation**: High CV indicates unstable test conditions

### Graph Types

1. **Consolidated Comparison Graphs**: Single graphs showing all metrics with legend format `{commit}-P{pipeline}/IO{io_threads}`
2. **Variance Line Graphs**: Individual run values with standard deviation visualization and error bars

### Advanced Options

```bash
# Use an already running Valkey server (client mode only with `--valkey-path`)
python benchmark.py --mode client --valkey-path /path/to/valkey --use-running-server

# Specify custom results directory
python benchmark.py --results-dir ./my-results

# Set logging level
python benchmark.py --log-level DEBUG

# Use a custom valkey-benchmark executable (useful for testing modified versions)
python benchmark.py --valkey-benchmark-path /path/to/custom/valkey-benchmark
```

### Benchmarking Remote Servers and Pre-Running Servers

When using `--use-running-server` or benchmarking remote servers, **restarting the server between benchmark runs is the user's responsibility**. Failure to restart between runs affects test results.

### Custom Valkey-Benchmark Executable

The `--valkey-benchmark-path` option specifies a custom path to the `valkey-benchmark` executable. This is useful when:

- Testing a modified version of `valkey-benchmark`
- Using a pre-built binary from a different location
- Benchmarking with a specific version of the tool

When not specified, the tool uses the default `src/valkey-benchmark` relative to the Valkey source directory.

````bash
# Example: Use a custom benchmark tool
python benchmark.py --valkey-benchmark-path /usr/local/bin/valkey-benchmark

# Example: Use with custom Valkey path
python benchmark.py --valkey-path /custom/valkey --valkey-benchmark-path /custom/valkey/src/valkey-benchmark

## Configuration

Create benchmark configurations in JSON format. Each object represents a single set of options and configurations are **not** automatically cross-multiplied. Example:

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
    "warmup": 10,
    "io-threads": [1, 4, 8],
    "server_cpu_range": "0-1",
    "client_cpu_range": "2-3"
  }
]
````

### Configuration Parameters

| Parameter          | Description                                                    | Data Type           | Multiple Values |
| ------------------ | -------------------------------------------------------------- | ------------------- | --------------- |
| `requests`         | Number of requests to perform                                  | Integer             | Yes             |
| `keyspacelen`      | Key space size (number of distinct keys)                       | Integer             | Yes             |
| `data_sizes`       | Size of data in bytes                                          | Integer             | Yes             |
| `pipelines`        | Number of commands to pipeline                                 | Integer             | Yes             |
| `clients`          | Number of concurrent client connections                        | Integer             | Yes             |
| `commands`         | Valkey commands to benchmark                                   | String              | Yes             |
| `cluster_mode`     | Whether to enable cluster mode                                 | String ("yes"/"no") | No              |
| `tls_mode`         | Whether to enable TLS                                          | String ("yes"/"no") | No              |
| `warmup`           | Warmup time in seconds before benchmarking                     | Integer             | No              |
| `io-threads`       | Number of I/O threads for server                               | Integer             | Yes             |
| `server_cpu_range` | CPU cores for server (e.g. "0-3", "0,2,4", or "144-191,48-95") | String              | No              |
| `client_cpu_range` | CPU cores for client (e.g. "4-7", "1,3,5", or "0-3,8-11")      | String              | No              |

When `warmup` is provided for read commands, the benchmark performs three stages:

1. A data injection pass using the corresponding write command with `--sequential` to seed the keyspace.
2. A warmup run of the read command (without `--sequential`) for the specified duration.
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
  - Benchmarks new commits from the Valkey unstable branch
  - Manages commit tracking via PostgreSQL
  - Uploads results to S3 and pushes metrics to PostgreSQL
  - Supports manual triggering with configurable commit limits

- **`basic.yml`**: Basic validation and testing
- **`check_format.yml`**: Code formatting validation
- **`cluster_tls.yml`**: Tests for cluster and TLS configurations

### Commit Tracking

The system uses PostgreSQL to track benchmarking progress. Commits are stored in the `benchmark_commits` table with their status and configuration.

#### Status Flow

Commits progress through the following statuses during the benchmarking workflow:

1. **`in_progress`** - Marked by the workflow when commits are selected for benchmarking
2. **`complete`** - Marked by the workflow after metrics are successfully pushed to PostgreSQL

#### Status Descriptions

- **`in_progress`**: Workflow has selected the commit and is running the benchmark
- **`complete`**: Full workflow completed - metrics are in PostgreSQL and available in dashboards

#### Automatic Cleanup

The system cleans up `in_progress` entries on the next workflow run. This ensures:

- Failed benchmark runs are retried
- Commits stuck in progress do not block future runs
- The tracking reflects completed work accurately

#### Config Tracking

Each commit is tracked with the actual configuration content used for benchmarking. This provides:

- Comparison of results across different configurations by actual content
- Tracking of exact config parameters used for each benchmark
- Detection when config content changes even if file name stays the same
- Benchmarking the same commit with different configs
- Reproducibility by storing complete config data

#### Manual Commit Management

You can manually manage commit statuses using the PostgreSQL-based utility script:

```bash
# Mark commits with config tracking (loads config content from file)
python utils/postgres_track_commits.py mark \
  --repo /path/to/valkey \
  --status in_progress \
  --config-file ./configs/benchmark-config-arm.json \
  abc123 def456

# Mark commits as benchmark_complete
python utils/postgres_track_commits.py mark \
  --repo /path/to/valkey \
  --status benchmark_complete \
  --config-file ./configs/benchmark-config-arm.json \
  abc123

# Mark commits as complete
python utils/postgres_track_commits.py mark \
  --repo /path/to/valkey \
  --status complete \
  --config-file ./configs/benchmark-config-arm.json \
  abc123

# Determine which commits need benchmarking (any config)
python utils/postgres_track_commits.py determine \
  --repo /path/to/valkey \
  --branch unstable \
  --max-commits 5

# Determine which commits need benchmarking for specific config
# (allows same commit to be benchmarked with different configs)
python utils/postgres_track_commits.py determine \
  --repo /path/to/valkey \
  --branch unstable \
  --max-commits 5 \
  --config-file ./configs/benchmark-config-arm.json

# Disable subset detection (force exact config matching)
python utils/postgres_track_commits.py determine \
  --repo /path/to/valkey \
  --branch unstable \
  --max-commits 5 \
  --config-file ./configs/benchmark-config-arm.json \
  --disable-subset-detection

# Query commits by config file
python utils/postgres_track_commits.py query \
  --config-file ./configs/benchmark-config-arm.json

# List all config files used
python utils/postgres_track_commits.py query \
  --list-configs

# Show all commits with full config data (no filter)
python utils/postgres_track_commits.py query
```

### Grafana Dashboard

The `dashboards/` directory contains a Grafana dashboard solution:

**Features:**

- PostgreSQL backend for data persistence
- Deployment on AWS EKS with CloudFront CDN
- Public dashboard sharing capabilities
- Visualization and alerting
- Real-time performance metrics
- Historical trend analysis
- Commit-level performance tracking

**Contents:**

- Grafana dashboard JSON files (3 variants)
- AWS infrastructure as code (CloudFormation)
- Kubernetes manifests and Helm values
- Deployment scripts
- Documentation

**Quick Start:**

```bash
cd dashboards/
./deploy-stack.sh  # Deploy AWS infrastructure
# Follow GRAFANA_DEPLOYMENT_GUIDE.md for complete setup
```

See `dashboards/README.md` for detailed deployment instructions.

## Utilities

### Commit Management

- `utils/postgres_track_commits.py`: Manages commit tracking, status updates, and cleanup operations using PostgreSQL
- `utils/push_to_postgres.py`: Pushes benchmark metrics to PostgreSQL with **dynamic schema support** - creates database columns for new metrics added to `metrics.json` files
- `utils/compare_benchmark_results.py`: Utilities for comparing benchmark results across commits

### Configuration Files

- `configs/benchmark-configs.json`: Standard benchmark configurations
- `configs/benchmark-configs-cluster-tls.json`: Specialized configurations for cluster and TLS testing

## Development

### Local Development

For local development, simply run:

```bash
python benchmark.py --commits HEAD
```

### Adding New Configurations

Create new JSON configuration files in the `configs/` directory following the existing format. Each configuration object represents a benchmark scenario.

## License

Please see the [LICENSE.md](./LICENSE.md)
