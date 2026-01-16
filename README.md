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
- **FTS (Full-Text Search) performance testing** with valkey-search module
- **Performance profiling** with flamegraph generation via `perf`

## Prerequisites

- Git
- Python 3.6+
- Linux environment (for taskset CPU pinning)
- Build tools required by Valkey (gcc, make, etc.)
- Install python modules required for this project: `pip install -r requirements.txt`

### Additional Prerequisites for FTS Tests

- [valkey-search](https://github.com/valkey-io/valkey-search) module (fulltext branch)
- `perf` tool for profiling (optional: `sudo yum install perf` or `sudo apt-get install linux-tools-generic`)
- `bunzip2` for dataset extraction (`sudo yum install bzip2` or `sudo apt-get install bzip2`)

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
├── dashboards/              # Grafana dashboards and AWS infrastructure
│   ├── grafana/            # Grafana dashboard definitions and Helm config
│   ├── kubernetes/         # Kubernetes manifests (ALB Ingress)
│   ├── infrastructure/     # CloudFormation templates
│   ├── scripts/            # Phase-based deployment scripts (00-06)
│   ├── schema.sql          # PostgreSQL database schema
│   └── README.md           # Deployment documentation
├── utils/                   # Utility scripts
│   ├── postgres_track_commits.py  # Commit tracking and management
│   └── compare_benchmark_results.py  # Result comparison utilities
├── benchmark.py             # Main entry point (core and search modules)
├── valkey_build.py          # Handles building Valkey from source
├── valkey_server.py         # Manages Valkey server instances
├── valkey_benchmark.py      # Runs benchmark tests
├── search_benchmark.py      # Search module benchmark execution (FTS, vector, numeric, tag)
├── profiler.py              # Generic performance profiler (flamegraphs)
├── cpu_monitor.py           # Generic CPU monitoring
├── process_metrics.py       # Processes and formats benchmark results
├── scripts/                 # Helper scripts
│   ├── setup_datasets.py   # FTS dataset generator
│   ├── flamegraph.pl       # Flamegraph visualization
│   └── stackcollapse-perf.pl  # Stack trace processor
├── datasets/                # FTS test datasets (auto-generated)
│   ├── field_explosion_50k.xml
│   ├── search_terms.csv
│   └── proximity_phrases.csv
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

- **`fts_benchmark.yml`**: FTS continuous benchmarking workflow
  - Tests valkey-search module performance
  - Builds Valkey + valkey-search from specified branches
  - Generates/caches FTS datasets on runner
  - Runs configurable test groups with optional profiling
  - Uploads results to S3 (`fts-results/` prefix) and PostgreSQL
  - Scheduled weekly or manual trigger
  - Parameters: branch, test groups, runs, profiling enable/disable

- **`basic.yml`**: Basic validation and testing
- **`check_format.yml`**: Code formatting validation
- **`cluster_tls.yml`**: Tests for cluster and TLS configurations

### Commit Tracking

The system uses PostgreSQL to track benchmarking progress. Commits are stored in the `benchmark_commits` table with their status, configuration, and architecture.

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

### Performance Dashboard

The project includes a complete AWS infrastructure for visualizing benchmark results using Grafana:

- **AWS EKS Fargate** - Serverless Kubernetes (no EC2 nodes)
- **Amazon RDS PostgreSQL** - Stores benchmark metrics and Grafana configuration
- **CloudFront CDN** - Global content delivery with HTTPS
- **Application Load Balancer** - Secured for CloudFront-only access

See `dashboards/README.md` for complete deployment guide and architecture details.

## Utilities

### Commit Management

- `utils/postgres_track_commits.py`: Manages commit tracking, status updates, and cleanup operations using PostgreSQL
- `utils/push_to_postgres.py`: Pushes benchmark metrics to PostgreSQL with dynamic schema support
- `utils/compare_benchmark_results.py`: Compares benchmarparing benchmark results across commits

### Configuration Files

- `configs/benchmark-configs.json`: Standard benchmark configurations
- `configs/benchmark-configs-cluster-tls.json`: Specialized configurations for cluster and TLS testing

## Development

### Local Development

For local development, simply run:
```bash
python benchmark.py
```

### Adding New Configurations

Create new JSON configuration files in the `configs/` directory following the existing format. Each configuration object represents a benchmark scenario.

### Extending for New Modules

The framework supports module-specific testing through a unified entry point (`benchmark.py`) with `--module` flag:

**For new modules (e.g., JSON, TimeSeries):**

1. **Create module benchmark class** extending `ClientRunner`:
   ```python
   # my_module_benchmark.py
   from valkey_benchmark import ClientRunner
   
   class MyModuleBenchmarkRunner(ClientRunner):
       def run_module_specific_tests(self, config):
           # Module-specific test logic
           pass
   ```

2. **Add module dispatch** in `benchmark.py`:
   ```python
   # In benchmark.py main()
   elif args.module == "my_module":
       from my_module_benchmark import run_my_module_benchmarks
       run_my_module_benchmarks(...)
   ```

3. **Create convenience wrapper** (optional):
   ```python
   # run_my_module_tests.py
   subprocess.run(["python3", "benchmark.py", "--module", "my_module"] + sys.argv[1:])
   ```

4. **Reuse generic infrastructure**:
   - `profiler.py` - Performance profiling
   - `cpu_monitor.py` - CPU monitoring
   - `process_metrics.py` - Metrics processing
   - `push_to_postgres.py` - Database integration (use `--test-type my_module`)

**Example: Search module (FTS, vector, numeric, tag)**
- Module class: `search_benchmark.py::SearchBenchmarkRunner`
- Dispatch: `benchmark.py` detects `--module search`
- Usage: `python benchmark.py --module search --config configs/fts-benchmarks.json --groups 1`

## Module Testing (FTS, JSON, etc.)

The framework supports generic module testing through a unified `ClientRunner`.

### Module Config Structure

Module tests use structured `test_groups` with `scenarios`:

```json
{
  "test_groups": [{
    "group": 1,
    "scenarios": [
      {
        "type": "ingestion",
        "setup_commands": ["FT.CREATE idx ..."],
        "command": "HSET ...",
        "dataset": "data.xml",
        "clients": 1000,
        "maxdocs": 50000
      },
      {
        "type": "search",
        "command": "FT.SEARCH idx __field:term__",
        "dataset": "queries.csv",
        "clients": 1000,
        "duration": 60,
        "warmup": 20
      }
    ]
  }],
  "cluster_mode": false,
  "tls_mode": false
}
```

### Setting Up PR Benchmarks

Both valkey core and module repositories can set up automated PR benchmarking using our unified workflow template.

#### Quick Start

1. **Copy the template** from this repo:
   ```bash
   cp .github/workflow-templates/pr-benchmark-template.yml \
      .github/workflows/benchmark-on-label.yml
   ```

2. **Customize for your repository:**
   - **Runner label**: Update `runs-on` with your self-hosted runner label
   - **For module repos**: Update `MODULE_NAME`, `.so` path, and build commands
   - **For core repo**: Remove or comment out module-specific steps

3. **Trigger benchmarks** by adding the `run-benchmark` label to any PR

#### Template Customization Guide

The template includes clear `CUSTOMIZE` markers for:

**Module Repositories (valkey-search, valkey-json, etc.):**
- Module build command (build.sh, make, cmake)
- Path to .so file (e.g., `.build-release/libsearch.so`)
- Module name for `--module` parameter
- Benchmark config file path

**Core Repository (valkey/valkey):**
- Uses conditional steps based on `github.repository`
- Most sections work without modification

See `.github/workflow-templates/pr-benchmark-template.yml` for detailed inline documentation.

#### Workflow Features

- Triggered by `run-benchmark` label on PRs
- Compares PR branch against base branch
- Posts results as PR comment
- Uploads artifacts for detailed analysis
- Automatic cleanup and label removal

### Running Module Tests Locally

`--module-path` requires a pre-built .so file (not source directory) since modules use different build systems (make, cmake, build.sh) and may need specific compilers.

**Build module first:**

```bash
cd valkey-search
make BUILD_TLS=yes  # or ./build.sh, cmake, etc.
ls -lh .build-release/libsearch.so
```

**Run benchmarks:**

```bash
python benchmark.py \
  --module search \
  --module-path ../valkey-search/.build-release/libsearch.so \
  --valkey-path ../valkey \
  --config configs/fts-benchmarks.json \
  --groups 1
```

Framework manages server lifecycle automatically.

### Running FTS Tests

**Note:** Datasets are automatically generated on first run if missing. The initial run may take 30-60 minutes to download Wikipedia and generate datasets. Subsequent runs use cached datasets and start immediately.

#### Unified Entry Point (Recommended)

Use `benchmark.py` with `--module search`:

```bash
# Run FTS test Group 1 (datasets auto-generated if missing)
python benchmark.py \
  --module search \
  --valkey-path /path/to/valkey \
  --config configs/fts-benchmarks.json \
  --groups 1

# Run specific scenarios only
python benchmark.py \
  --module search \
  --valkey-path /path/to/valkey \
  --config configs/fts-benchmarks.json \
  --scenarios ingest,a,b

# Against remote server
python benchmark.py \
  --module search \
  --valkey-path /path/to/valkey \
  --target-ip 192.168.1.100 \
  --config configs/fts-benchmarks.json \
  --groups 1
```

Results saved to `results/search_tests/` with optional flamegraphs if profiling enabled in config.

#### CPU Pinning Configuration

CPU pinning is configured in the config file:

```json
{
  "server_cpu_range": "0-7",   // Pin server to cores 0-7 (when using --mode both)
  "client_cpu_range": "8-15"   // Pin benchmark client to cores 8-15
}
```

**With `--mode both` (recommended):**
Framework manages server automatically with CPU pinning from config.

```bash
python benchmark.py \
  --module search \
  --module-path ../valkey-search/.build-release/libsearch.so \
  --valkey-path ../valkey \
  --config configs/fts-benchmarks.json \
  --groups 1
```

**With `--use-running-server` (manual server management):**
Start server yourself with desired CPU pinning.

```bash
# Start server manually
taskset -c 0-7 /path/to/valkey-server --loadmodule libsearch.so ...

# Run benchmarks
python benchmark.py \
  --module search \
  --valkey-path /path/to/valkey \
  --use-running-server \
  --config configs/fts-benchmarks.json \
  --groups 1
```

### Dataset Generation System

The framework uses a **transform-based dataset generation system** that supports multiple testing strategies:

**Config Format:**
```json
"dataset_generation": {
  "dataset_name.xml": {
    "doc_count": 50000,
    "fields": [
      {
        "name": "field0",
        "size": 100,
        "transforms": [
          {"type": "wikipedia"},
          {"type": "inject", "term": "MARKER_TERM", "percentage": 0.5}
        ]
      }
    ]
  }
}
```

**Supported Transforms:**
- `wikipedia`: Extract Wikipedia content (base text)
- `inject`: Add marker terms at specified percentage
- `repeat`: Duplicate terms N times (for term_repetition tests)
- `prefix_gen`: Generate prefix variations (for prefix_explosion tests)
- `proximity_phrase`: Generate N-term phrases for proximity testing
  - Parameters: `term_count`, `combinations` (1=best case, 100=worst case), `repeats` (copies per pattern)
  - Supports CSV output (no Wikipedia needed)

**Compact Format (for field explosion):**
```json
"field_explosion_50k.xml": {
  "doc_count": 50000,
  "generate_fields": {
    "count": 50,
    "size": 1000,
    "transforms": [{"type": "wikipedia"}]
  }
}
```

**Query Generation:**
```json
"query_generation": {
  "proximity_5term_queries.csv": {
    "type": "proximity_phrase",
    "doc_count": 100,
    "term_count": 5
  }
}
```
- Auto-generates query CSVs matching ingestion datasets
- Supports type-based generation (extensible for future query types)

### FTS Test Groups

**Group 1: Multi-field comprehensive (NOSTEM)**
- 50-field index, 50K documents, 1000 chars per field
- Tests: Single term, 2-term composed AND, 2-term proximity phrase
- Scenarios: 1a-1g (with/without NOCONTENT variants)
  - 1a / 1a_nocontent: Single term all fields
  - 1b / 1b_nocontent: Single term @field1
  - 1c / 1c_nocontent: Composed AND all fields
  - 1d / 1d_nocontent: Composed AND @field1
  - 1e / 1e_nocontent: Mixed pattern @field1
  - 1f / 1f_nocontent: Proximity phrase all fields
  - 1g / 1g_nocontent: Proximity phrase @field1

**Group 2: Proximity queries - 5-term best case (1 combination)**
- 100K documents, 1 field, 100 queries × 1000 matches
- Adjacent terms → 1 position tuple check (best case)
- Tests: Default field and specific field queries with SLOP 0 INORDER

**Group 3: Proximity queries - 5-term worst case (100 combinations)**
- 100K documents, 1 field, 100 queries × 1000 matches
- Repeated terms with noise → ~100 position tuple checks before match
- Tests: SLOP 0 and SLOP 3 variations

**Group 4: Proximity queries - 25-term worst case (100 combinations)**
- 100K documents, 1 field, 100 queries × 1000 matches
- 25-term phrases with complexity testing
- Tests: SLOP 3 INORDER

### FTS Results

Results are saved to `results/search_tests/`:
- `metrics.json` - Performance metrics
- `flamegraphs/` - Profiling data (if enabled)

Sample metrics structure:
```json
{
  "test_id": "1a",
  "test_phase": "search",
  "rps": 6596.42,
  "avg_latency_ms": 7.535,
  "p50_latency_ms": 5.367,
  "p95_latency_ms": 20.975,
  "cpu_avg_percent": 692.84,
  "cpu_peak_percent": 751.10,
  "memory_mb": 4469.87
}
```

## Performance Profiling

The framework includes a generic profiler that works with both core and FTS tests.

### Using Profiler in Core Tests

The `PerformanceProfiler` class can be integrated into any benchmark script:

```python
from profiler import PerformanceProfiler

# Initialize profiler
profiler = PerformanceProfiler(results_dir, enabled=True)

# Example from search module:
profiler.start_profiling("search_1a", target_process="valkey-server")

# Run your benchmark
runner.run_benchmark_config()

# After benchmark completes
profiler.stop_profiling("search_1a")
# → Generates:
#    - flamegraph: results_dir/commit_id/flamegraphs/search_1a_20251218_080245.svg
#    - perf report: results_dir/commit_id/flamegraphs/search_1a_20251218_080245_report.txt
#    - raw data: results_dir/commit_id/flamegraphs/search_1a_20251218_080245.perf.data
```

### Profiler Features

- **Flamegraph generation**: Visual call stack analysis
- **Auto-downloads scripts**: Fetches flamegraph tools from GitHub on first use
- **Profiling modes**: cpu (cycles) or wall-time (all execution time)
- **Function hotspot analysis**: Identify CPU-intensive code paths
- **Kernel + user space profiling**: Complete stack traces with DWARF
- **Generic implementation**: Works with any process (valkey-server, redis-server, etc.)
- **Configurable sampling**: 999Hz default

### profiling_sets Configuration

Run tests with multiple profiling configurations:

```json
{
  "profiling_sets": [
    {"enabled": false},
    {"enabled": true, "mode": "wall-time", "sampling_freq": 999}
  ],
  "config_sets": [
    {"search.reader-threads": 1},
    {"search.reader-threads": 8}
  ]
}
```

**Behavior:**
- Iterates profiling_sets × config_sets
- Profiling OFF → Collects metrics in `metrics.json`
- Profiling ON → Generates flamegraphs, skips metrics
- Flamegraphs: `group{X}_{scenario}_{config_values}_{timestamp}.svg`

**Per-scenario override:**
```json
{"id": "a", "profiling": {"delays": {"search": {"delay": 0, "duration": 10}}}}
```

Scenario overrides any profiling_set values.

**Delay strategy pattern:**
```json
{
  "profiling_sets": [{
    "enabled": true,
    "delays": {
      "ingestion": {"delay": 0, "duration": 10},
      "search": {"delay": 30, "duration": 10}
    }
  }],
  "scenarios": [{
    "id": "a",
    "type": "ingestion",
    "profiling": {"delays": {"ingestion": {"delay": 10, "duration": 10}}}
  }]
}
```

Group 1 ingestion uses 10s delay (dataset loading), others use 0s (immediate).

## License

Please see the [LICENSE.md](./LICENSE.md)
