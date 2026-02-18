# Valkey Docker Deployment Guide

This guide explains how to run Valkey server with the search module in Docker for local testing and benchmarking.

## Overview

The Docker solution provides:
- **Guaranteed CPU isolation** via Docker's `--cpuset-cpus` (uses cgroups)
- **Memory limits** via `--memory` flag
- **Easy cleanup** - single command to stop/remove
- **Volume mounting** - uses locally built Valkey binaries (no rebuild needed)
- **Reproducible environment** - consistent setup across machines

## Prerequisites

1. Docker installed and running
2. Valkey built locally: `cd /path/to/valkey && make`
3. Search module built: `cd /path/to/valkey-search && make`

## Quick Start

### 1. Launch Valkey Server

```bash
# Use default configuration (auto-detects paths)
./scripts/run_valkey_docker.sh

# Custom configuration
./scripts/run_valkey_docker.sh \
  --valkey-path /home/ramvolet/Workspace/valkey \
  --search-module /home/ramvolet/Workspace/valkey-search/.build-release/libsearch.so \
  --cpus "0-7" \
  --memory 128g \
  --bind-ip 10.189.160.64 \
  --port 6379 \
  --io-threads 1
```

### 2. Verify CPU Pinning

```bash
# Check that all threads respect CPU constraints
./scripts/verify_docker_affinity.sh
```

### 3. Test Connection

```bash
# From host
docker exec valkey-search-test /valkey/bin/valkey-cli -h 10.189.160.64 -p 6379 ping

# Run FT.SEARCH query
docker exec valkey-search-test /valkey/bin/valkey-cli -h 10.189.160.64 -p 6379 \
  FT.SEARCH myindex "query"
```

### 4. Stop Server

```bash
./scripts/stop_valkey_docker.sh
```

## Configuration Options

### Default Values

The launch script uses these defaults (matching your original setup):

```bash
CPUS="0-7"              # Pin to CPUs 0-7
MEMORY="128g"           # 128GB memory limit
BIND_IP="10.189.160.64" # Your server IP
PORT="6379"             # Standard Valkey port
IO_THREADS="1"          # Single IO thread
```

### Override Any Parameter

```bash
# Different port
./scripts/run_valkey_docker.sh --port 6380

# Different CPU range
./scripts/run_valkey_docker.sh --cpus "0-15"

# More IO threads
./scripts/run_valkey_docker.sh --io-threads 4

# Different memory limit
./scripts/run_valkey_docker.sh --memory 64g
```

## Architecture

### Volume Mounts

The container mounts these from your host:

```
Host                                    → Container
/path/to/valkey/src/valkey-server      → /valkey/bin/valkey-server
/path/to/valkey/src/valkey-cli         → /valkey/bin/valkey-cli
/path/to/libsearch.so                  → /valkey/modules/libsearch.so
```

Benefits:
- No need to rebuild Docker image when you rebuild Valkey
- Test different Valkey versions by changing `--valkey-path`
- Direct access to your local build artifacts

### Network Mode

Uses `--network host` so the container shares the host's network stack:
- Valkey binds directly to host IP (10.189.160.64)
- No port mapping needed
- Benchmarks run from host can connect directly

## Comparison: taskset vs Docker

### Your Original Approach (taskset)

```bash
taskset -c 0-7 src/valkey-server \
  --bind 10.189.160.64 \
  --port 6379 \
  --io-threads 1 \
  --loadmodule /path/to/libsearch.so \
  --appendonly no \
  --save "" \
  --protected-mode no
```

**Pros:**
- Direct process execution (no container overhead)
- Your tests showed it works with search module threads

**Cons:**
- `taskset` is process-level (threads *can* override)
- Manual cleanup (kill process, check for stragglers)
- Less isolation from host environment

### Docker Approach (this solution)

```bash
./scripts/run_valkey_docker.sh --cpus "0-7" --io-threads 1
```

**Pros:**
- **Kernel-enforced** CPU isolation via cgroups (cannot be overridden)
- Memory limits enforced
- Easy cleanup (one command stops everything)
- Reproducible environment
- Better isolation for testing

**Cons:**
- Slight container overhead (usually negligible)
- Requires Docker installed

## Why Docker for CPU Pinning?

Even though your tests showed taskset working, Docker provides stronger guarantees:

1. **Cgroups vs taskset:**
   - `taskset`: Sets `sched_setaffinity()` on process - threads can call `sched_setaffinity(0, ...)` to reset
   - Docker `--cpuset-cpus`: Kernel cgroup restriction - **cannot be overridden** by any thread

2. **Module thread behavior:**
   - Modules may spawn threads dynamically during load
   - New threads might not inherit affinity
   - Cgroups enforce on ALL threads regardless of creation method

3. **Verification:**
   - Run `./scripts/verify_docker_affinity.sh` to confirm
   - Shows cgroup restrictions + per-thread affinity
   - Catches any threads escaping CPU constraints

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs valkey-search-test

# Common issues:
# 1. Port already in use
./scripts/stop_valkey_docker.sh  # Stop existing container
lsof -i :6379  # Check if another process using port

# 2. Path not found
./scripts/run_valkey_docker.sh --valkey-path /correct/path
```

### CPU Affinity Not Working

```bash
# Verify with the verification script
./scripts/verify_docker_affinity.sh

# Should show:
# ✓ All threads correctly pinned to CPUs: 0-7
# ✓ Docker cpuset enforcement is working

# If not, check Docker configuration
docker info | grep -i cpu
```

### Permission Issues

```bash
# If you get permission errors, ensure Docker daemon is running
sudo systemctl status docker

# Add user to docker group (logout/login required)
sudo usermod -aG docker $USER
```

## Integration with Benchmarks

Your existing benchmark tools work unchanged:

```bash
# Original command (against taskset server)
./src/valkey-benchmark -p 6379 --dataset search_terms.csv -n 50000 \
  FT.SEARCH rd0 "__field:term__"

# Same command (against Docker server)
./src/valkey-benchmark -h 10.189.160.64 -p 6379 --dataset search_terms.csv -n 50000 \
  FT.SEARCH rd0 "__field:term__"
```

The benchmark client runs on the host and connects to the Docker container via the host network.

## Monitoring

### View Container Stats

```bash
# Real-time stats
docker stats valkey-search-test

# Shows:
# - CPU usage %
# - Memory usage / limit
# - Network I/O
```

### Check Logs

```bash
# Follow logs
docker logs -f valkey-search-test

# Last 100 lines
docker logs --tail 100 valkey-search-test
```

### Execute Commands Inside Container

```bash
# Run valkey-cli
docker exec valkey-search-test /valkey/bin/valkey-cli -h 10.189.160.64 -p 6379 INFO

# Check process tree
docker exec valkey-search-test ps aux

# Check CPU affinity of specific thread
docker exec valkey-search-test taskset -cp <TID>
```

## Advanced Usage

### Custom Valkey Arguments

The run script passes common arguments, but you can modify `scripts/run_valkey_docker.sh` to add more:

```bash
# In the docker run command, add after --protected-mode no:
    --maxmemory 64gb \
    --maxmemory-policy allkeys-lru \
    --tcp-backlog 511
```

### Multiple Containers

Run multiple Valkey instances with different ports:

```bash
# Instance 1: Port 6379
./scripts/run_valkey_docker.sh --port 6379

# Instance 2: Port 6380 (requires modifying CONTAINER_NAME in script)
# Edit scripts/run_valkey_docker.sh: CONTAINER_NAME="valkey-search-test-2"
./scripts/run_valkey_docker.sh --port 6380
```

### Persist Data

To keep data between container restarts, add a volume for /valkey/data:

```bash
# Modify docker run command in scripts/run_valkey_docker.sh:
-v /host/path/to/data:/valkey/data \
```

Then change Valkey args to enable persistence:
```bash
--appendonly yes \
--dir /valkey/data
```

## Summary

This Docker solution provides the same functionality as your original `taskset` approach, but with:
- **Stronger CPU isolation guarantees** (kernel cgroups)
- **Easier lifecycle management** (start/stop/cleanup)
- **Better testing environment** (reproducible, isolated)

Use it as a drop-in replacement for local testing and benchmarking!
