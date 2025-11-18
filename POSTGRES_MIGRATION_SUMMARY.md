# PostgreSQL Migration Summary

## Overview

The system has been migrated from using `completed_commits.json` file-based tracking to PostgreSQL as the single source of truth for commit benchmarking status.

## Key Changes

### 1. Removed File-Based Tracking
- **Removed**: `completed_commits.json` file and S3 sync operations
- **Removed**: `--completed-file` argument from `benchmark.py`
- **Removed**: `mark_commits()` call from `benchmark.py` after benchmarking

### 2. PostgreSQL-Based Tracking
- **Added**: `utils/postgres_track_commits.py` - PostgreSQL commit tracking module
- **Database Table**: `benchmark_commits` with columns:
  - `sha`: Commit SHA
  - `timestamp`: Commit timestamp
  - `status`: One of `in_progress`, `benchmark_complete`, `complete`
  - `config`: JSONB field storing the benchmark configuration
  - Unique constraint on `(sha, config)` to allow same commit with different configs

### 3. Workflow Changes

The GitHub Actions workflow now:

1. **Initializes PostgreSQL tables** at the start
2. **Cleans up incomplete commits** (`in_progress` and `benchmark_complete` statuses)
3. **Determines commits** to benchmark from PostgreSQL
4. **Marks commits as `in_progress`** before benchmarking
5. **Runs benchmarks** (no status update from benchmark.py)
6. **Marks commits as `benchmark_complete`** after benchmark completes
7. **Pushes metrics to PostgreSQL**
8. **Marks commits as `complete`** after successful PostgreSQL push

### 4. Status Flow

```
[PostgreSQL determines unbenchmarked commits]
           ↓
    [in_progress] ← Workflow marks before benchmarking
           ↓
    [benchmark runs]
           ↓
 [push metrics to PostgreSQL]
           ↓
      [complete] ← Workflow marks after successful push
```

### 5. Automatic Cleanup

On each workflow run, any commits stuck in `in_progress` status are automatically cleaned up, ensuring failed runs are retried.

## Benefits

1. **Single Source of Truth**: PostgreSQL is the only place tracking commit status
2. **No File Sync Issues**: No need to sync `completed_commits.json` to/from S3
3. **Better Concurrency**: PostgreSQL handles concurrent access better than file-based tracking
4. **Automatic Cleanup**: Failed runs are automatically retried
5. **Query Flexibility**: Easy to query commit status and history
6. **Config Tracking**: Each commit tracks the exact config used for benchmarking

## Usage

### Command Line (postgres_track_commits.py)

```bash
# Initialize tables
python utils/postgres_track_commits.py init \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass>

# Cleanup incomplete commits
python utils/postgres_track_commits.py cleanup \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass>

# Determine commits to benchmark
python utils/postgres_track_commits.py determine \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass> \
  --repo ../valkey --branch unstable \
  --max-commits 5 --config-file ./configs/benchmark-config-arm.json

# Mark commits with status
python utils/postgres_track_commits.py mark \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass> \
  --repo ../valkey --status in_progress \
  --config-file ./configs/benchmark-config-arm.json \
  abc123 def456

# Query commits
python utils/postgres_track_commits.py query \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass> \
  --config-file ./configs/benchmark-config-arm.json

# Get statistics
python utils/postgres_track_commits.py stats \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass>

# Export to JSON (for backup/debugging)
python utils/postgres_track_commits.py export \
  --host <host> --port 5432 --database <db> \
  --username <user> --password <pass> \
  --output backup.json
```

### Benchmark Script

```bash
# benchmark.py no longer needs --completed-file
python benchmark.py \
  --commits HEAD \
  --config ./configs/benchmark-config-arm.json \
  --results-dir results
```

## Migration Notes

- The workflow now uses IAM authentication tokens for PostgreSQL connections
- All commit status updates happen in the workflow, not in benchmark.py
- The `completed_commits.json` file is no longer used or synced to S3
- Failed benchmarks are automatically retried on the next workflow run

## Files Modified

1. `benchmark.py` - Removed `--completed-file` arg and `mark_commits()` call
2. `.github/workflows/valkey_benchmark.yml` - Updated to use PostgreSQL
3. `utils/postgres_track_commits.py` - New PostgreSQL tracking module (already existed)

## Files No Longer Used

1. `completed_commits.json` - Replaced by PostgreSQL
2. `utils/workflow_commits.py` - Replaced by `utils/postgres_track_commits.py`
