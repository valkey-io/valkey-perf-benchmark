# FTS PostgreSQL Schema Migration

## Overview

FTS tests integrate with existing `benchmark_metrics` table using additional columns for filtering and tracking module-specific information.

## Schema Changes

### New Columns Added

The following columns are automatically added by `push_to_postgres.py` when processing FTS results:

```sql
-- Test type identifier for dashboard filtering
ALTER TABLE benchmark_metrics 
ADD COLUMN test_type VARCHAR(50) DEFAULT 'core';

-- Module name being tested (e.g., 'valkey-search')
ALTER TABLE benchmark_metrics 
ADD COLUMN module VARCHAR(100);

-- Module-specific commit SHA
ALTER TABLE benchmark_metrics 
ADD COLUMN module_commit VARCHAR(255);

-- FTS-specific fields (auto-created from metrics.json)
ALTER TABLE benchmark_metrics 
ADD COLUMN test_id VARCHAR(50);

ALTER TABLE benchmark_metrics 
ADD COLUMN test_phase VARCHAR(50);

ALTER TABLE benchmark_metrics 
ADD COLUMN dataset VARCHAR(255);

-- CPU monitoring fields
ALTER TABLE benchmark_metrics 
ADD COLUMN cpu_avg_percent DECIMAL(15,6);

ALTER TABLE benchmark_metrics 
ADD COLUMN cpu_peak_percent DECIMAL(15,6);

ALTER TABLE benchmark_metrics 
ADD COLUMN memory_mb DECIMAL(15,6);

-- Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_test_type 
ON benchmark_metrics(test_type);

CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_module 
ON benchmark_metrics(module, module_commit);
```

### Column Descriptions

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `test_type` | VARCHAR(50) | Test category for filtering | 'core', 'fts' |
| `module` | VARCHAR(100) | Module name (optional) | 'valkey-search' |
| `module_commit` | VARCHAR(255) | Module commit SHA | 'abc123...' |
| `test_id` | VARCHAR(50) | FTS test identifier | '1a', '1f', 'I1' |
| `test_phase` | VARCHAR(50) | Test phase | 'ingestion', 'search' |
| `dataset` | VARCHAR(255) | Dataset file used | 'datasets/search_terms.csv' |
| `cpu_avg_percent` | DECIMAL | Average CPU utilization | 692.84 |
| `cpu_peak_percent` | DECIMAL | Peak CPU utilization | 751.10 |
| `memory_mb` | DECIMAL | Memory usage in MB | 4469.87 |

## Migration Strategy

### No Manual Migration Required

The schema uses **dynamic evolution**:
1. `push_to_postgres.py` analyzes incoming metrics
2. Detects new columns not in existing table
3. Automatically adds missing columns
4. Inserts data

### Backward Compatibility

**Core tests continue working unchanged:**
- `test_type` defaults to 'core'
- New columns are optional (NULL allowed)
- Existing queries unaffected

## Grafana Integration

### Filtering FTS vs Core Tests

```sql
-- Core tests only
SELECT * FROM benchmark_metrics WHERE test_type = 'core';

-- FTS tests only
SELECT * FROM benchmark_metrics WHERE test_type = 'fts';

-- Specific FTS module/commit
SELECT * FROM benchmark_metrics 
WHERE module = 'valkey-search' 
  AND module_commit = 'abc123...';

-- Specific FTS scenario
SELECT * FROM benchmark_metrics 
WHERE test_type = 'fts' 
  AND test_id = '1a';
```

### Dashboard Modifications

**Minimal changes needed:**
1. Add `test_type` filter variable (default: 'core')
2. Update queries to include `WHERE test_type = $test_type`
3. Optionally add FTS-specific panels for new metrics

## Data Flow

### Core Tests
```
benchmark.py 
  → results/{commit}/metrics.json 
  → push_to_postgres.py (test_type='core')
  → benchmark_metrics table
```

### FTS Tests
```
run_fts_tests.py 
  → results/fts_tests/metrics.json 
  → push_to_postgres.py (test_type='fts', module='valkey-search')
  → benchmark_metrics table
```

## Rollback Plan

If needed, remove FTS-specific columns:

```sql
-- Remove FTS columns (will lose FTS data)
ALTER TABLE benchmark_metrics DROP COLUMN test_type;
ALTER TABLE benchmark_metrics DROP COLUMN module;
ALTER TABLE benchmark_metrics DROP COLUMN module_commit;
ALTER TABLE benchmark_metrics DROP COLUMN test_id;
ALTER TABLE benchmark_metrics DROP COLUMN test_phase;
ALTER TABLE benchmark_metrics DROP COLUMN dataset;
ALTER TABLE benchmark_metrics DROP COLUMN cpu_avg_percent;
ALTER TABLE benchmark_metrics DROP COLUMN cpu_peak_percent;
ALTER TABLE benchmark_metrics DROP COLUMN memory_mb;

-- Remove indexes
DROP INDEX IF EXISTS idx_benchmark_metrics_test_type;
DROP INDEX IF EXISTS idx_benchmark_metrics_module;
```

## Testing

### Verify Schema

```bash
# Dry run to see schema changes
python utils/push_to_postgres.py \
  --results-dir ./results/fts_tests \
  --test-type fts \
  --module valkey-search \
  --module-commit abc123 \
  --dry-run

# Actual insertion
python utils/push_to_postgres.py \
  --results-dir ./results/fts_tests \
  --host <host> \
  --database <db> \
  --username <user> \
  --password <pass> \
  --test-type fts \
  --module valkey-search \
  --module-commit abc123
```

### Verify Data

```sql
-- Check FTS records
SELECT test_type, module, test_id, command, rps, avg_latency_ms 
FROM benchmark_metrics 
WHERE test_type = 'fts' 
ORDER BY created_at DESC 
LIMIT 10;

-- Check column additions
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'benchmark_metrics' 
ORDER BY column_name;
