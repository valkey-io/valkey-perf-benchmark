-- ========================================
-- RDS Database Setup for Valkey Benchmark
-- ========================================
-- Creates two databases:
-- 1. grafana - for Grafana metrics and settings
-- 2. postgres - for valkey_benchmark_metrics
--
-- Creates two users:
-- 1. Admin (postgres) - full access to all databases
-- 2. github_actions - IAM-enabled user for GitHub Actions
-- ========================================

-- Create databases
SELECT 'CREATE DATABASE grafana' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'grafana')\gexec
SELECT 'CREATE DATABASE postgres' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'postgres')\gexec

-- Create IAM-enabled user for GitHub Actions
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'github_actions') THEN
    CREATE USER github_actions WITH LOGIN;
    RAISE NOTICE 'Created user: github_actions';
  ELSE
    RAISE NOTICE 'User github_actions already exists';
  END IF;
END
$$;

-- Grant rds_iam role for IAM authentication to github_actions
GRANT rds_iam TO github_actions;

-- Grant CREATE permission on public schema to github_actions
GRANT CREATE ON SCHEMA public TO github_actions;

-- Set github_actions as the owner for new objects in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO github_actions;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO github_actions;

-- Create benchmark_metrics table in postgres database (owned by github_actions)
CREATE TABLE IF NOT EXISTS benchmark_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    commit VARCHAR(40) NOT NULL,
    command VARCHAR(50) NOT NULL,
    data_size INTEGER,
    pipeline INTEGER,
    clients INTEGER,
    requests INTEGER,
    rps DECIMAL(12,2),
    avg_latency_ms DECIMAL(10,3),
    min_latency_ms DECIMAL(10,3),
    p50_latency_ms DECIMAL(10,3),
    p95_latency_ms DECIMAL(10,3),
    p99_latency_ms DECIMAL(10,3),
    max_latency_ms DECIMAL(10,3),
    cluster_mode BOOLEAN,
    tls BOOLEAN,
    io_threads INTEGER,
    benchmark_threads INTEGER,
    benchmark_mode VARCHAR(50),
    duration INTEGER,
    warmup INTEGER,
    architecture VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create benchmark_commits table for tracking commit benchmarking status
CREATE TABLE IF NOT EXISTS benchmark_commits (
    id SERIAL PRIMARY KEY,
    sha VARCHAR(40) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('in_progress', 'complete')),
    config JSONB NOT NULL,
    architecture VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint: same commit + config + architecture can only exist once
    CONSTRAINT unique_sha_config_arch UNIQUE(sha, config, architecture)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_unique 
    ON benchmark_metrics(timestamp, commit, command, data_size, pipeline, rps, cluster_mode, tls, io_threads, architecture);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_timestamp_command ON benchmark_metrics(timestamp, command);
CREATE INDEX IF NOT EXISTS idx_commits_sha_status ON benchmark_commits(sha, status);
CREATE INDEX IF NOT EXISTS idx_commits_status ON benchmark_commits(status);
CREATE INDEX IF NOT EXISTS idx_commits_config ON benchmark_commits USING GIN(config);

-- Change ownership of tables and sequences to github_actions
ALTER TABLE IF EXISTS benchmark_metrics OWNER TO github_actions;
ALTER TABLE IF EXISTS benchmark_commits OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_metrics_id_seq OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_commits_id_seq OWNER TO github_actions;

-- Grant permissions to github_actions user for postgres database
GRANT CONNECT ON DATABASE postgres TO github_actions;
GRANT USAGE, CREATE ON SCHEMA public TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_metrics TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_commits TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_metrics_id_seq TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_commits_id_seq TO github_actions;

-- ========================================
-- DEVELOPMENT PLACEHOLDER: benchmark_metrics_tiering_ts
-- ========================================
-- WARNING: THE TABLE NAME BELOW IS A DEVELOPMENT PLACEHOLDER AND IS NOT FINAL.
-- It is derived from the table identifier 'tiering_ts' passed as
-- 'push_to_postgres.py --table tiering_ts', which resolve_table_name() maps to
-- 'benchmark_metrics_tiering_ts'. Renaming it later means: this block, the
-- --table value in the workflow, and (once one exists) the Grafana dashboard JSON.
-- Nothing else references it, so the rename is intentionally cheap.
--
-- Grain: one row per (commit, scenario, elapsed_sec), one sample per second of a
-- data tiering benchmark run. elapsed_sec is the seconds offset from the start of
-- the measured phase and is the x-axis used to overlay several commits on one
-- chart. timestamp is the absolute wall clock time of the same sample.
--
-- The run identity columns (timestamp, commit, command, data_size, pipeline,
-- clients) are DENORMALIZED onto every per-second row on purpose. create_indexes()
-- in utils/push_to_postgres.py hardcodes exactly those column names, so carrying
-- them on each row lets that helper run unmodified.
--
-- Metric column names match the upstream per-second collector
-- (benchmark/tools/metrics-collector/metrics-collector.sh in the valkey-data-tiering
-- fork) so sampled CSV fields map straight onto columns.
--
-- Byte counters are BIGINT, not INTEGER: used_memory on a large host exceeds the
-- INTEGER range. Note that push_to_postgres.py's detect_field_type() would infer
-- INTEGER for these if it created the table itself, so create this table from this
-- file before the first push.
--
-- This block is kept self-contained (table, indexes, ownership, grants together)
-- rather than split across the sections above, so the eventual rename is one edit.
CREATE TABLE IF NOT EXISTS benchmark_metrics_tiering_ts (
    id SERIAL PRIMARY KEY,

    -- Denormalized run identity, repeated on every per-second row
    timestamp TIMESTAMPTZ NOT NULL,
    commit VARCHAR(40) NOT NULL,
    command TEXT NOT NULL,
    data_size INTEGER,
    pipeline INTEGER,
    clients INTEGER,
    architecture VARCHAR(50),
    test_type VARCHAR(50),
    scenario VARCHAR(50),

    -- config_set is the server config variant a row was sampled under, and is
    -- what tells apart rows that otherwise share (commit, scenario,
    -- elapsed_sec). convert_metrics_to_rows() in utils/push_to_postgres.py
    -- wraps it in psycopg2's Json adapter, so it arrives as serialized JSON
    -- text. TEXT, not VARCHAR(255): a config_set with several keys serializes
    -- past 255 characters, and TEXT is also what detect_field_type() infers for
    -- a dict, so a table created from this file and one created by that helper
    -- agree.
    config_set TEXT,

    -- Time series axis
    elapsed_sec INTEGER NOT NULL CHECK (elapsed_sec >= 0),

    -- Memory
    used_memory BIGINT,
    used_memory_rss BIGINT,
    maxmemory BIGINT,
    mem_frag_ratio DECIMAL(10,3),

    -- Keyspace and throughput
    keyspace_hits BIGINT,
    keyspace_misses BIGINT,
    ops_per_sec INTEGER,
    total_commands_delta BIGINT,
    blocked_clients INTEGER,

    -- Tiering counters
    total_num_items_spilled_to_ext_storage BIGINT,
    total_num_items_fetched_from_ext_storage BIGINT,
    num_items_spilling_to_ext_storage BIGINT,
    completion_read_ok BIGINT,
    dram_value_hits BIGINT,
    kbc_fetching_block BIGINT,
    disk_hit_pct DECIMAL(6,2),
    mem_hit_pct DECIMAL(6,2),
    disk_hit_pct_interval DECIMAL(6,2),
    mem_hit_pct_interval DECIMAL(6,2),

    -- Spill accounting
    spill_submitted_count BIGINT,
    spill_serialized_count BIGINT,
    mean_spill_ram DECIMAL(15,2),
    inflight_spill_ram_bytes BIGINT,

    -- Throttling
    throttle_total_throttled BIGINT,
    throttle_queued_clients INTEGER,
    throttle_current_rate DECIMAL(15,2),
    throttle_allowed_tps DECIMAL(15,2),

    -- CPU (percent)
    cpu_user DECIMAL(6,2),
    cpu_sys DECIMAL(6,2),
    valkey_cpu_user DECIMAL(8,2),
    valkey_cpu_sys DECIMAL(8,2),
    valkey_cpu_total DECIMAL(8,2),
    asio_cpu_pct DECIMAL(8,2),

    -- Disk
    disk_read_iops INTEGER,
    disk_write_iops INTEGER,
    disk_read_mb DECIMAL(12,2),
    disk_write_mb DECIMAL(12,2),
    disk_read_merges_ps DECIMAL(12,2),
    disk_write_merges_ps DECIMAL(12,2),
    disk_r_await_ms DECIMAL(10,3),
    disk_w_await_ms DECIMAL(10,3),
    disk_aqu_sz DECIMAL(10,3),
    disk_util_pct DECIMAL(6,2),
    disk_in_flight INTEGER,
    disk_req_sz_kb DECIMAL(10,2),

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Primary time series lookup: one commit and scenario ordered along the x-axis.
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_tiering_ts_commit_scenario_elapsed
    ON benchmark_metrics_tiering_ts(commit, scenario, elapsed_sec);

-- The four indexes below mirror create_indexes() in utils/push_to_postgres.py,
-- name for name, so a table created from this file and a table created by that
-- helper end up with the same indexes.
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_tiering_ts_commit
    ON benchmark_metrics_tiering_ts(commit);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_tiering_ts_timestamp
    ON benchmark_metrics_tiering_ts(timestamp);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_tiering_ts_command
    ON benchmark_metrics_tiering_ts(command);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_tiering_ts_config
    ON benchmark_metrics_tiering_ts(commit, command, data_size, pipeline, clients);

-- No UNIQUE constraint on (commit, scenario, elapsed_sec), deliberately.
-- push_to_postgres.py inserts with execute_values and no ON CONFLICT clause, so a
-- duplicate key would abort the whole push instead of skipping a row. Re-pushing a
-- commit is a normal event here: in_progress commits are cleaned up and retried,
-- and --runs N repeats the same (commit, scenario, elapsed_sec) triple on purpose.
-- Duplicates are therefore filtered at query time, matching how benchmark_metrics
-- already behaves (its idx_benchmark_metrics_unique index is not actually unique).

ALTER TABLE IF EXISTS benchmark_metrics_tiering_ts OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_metrics_tiering_ts_id_seq OWNER TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_metrics_tiering_ts TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_metrics_tiering_ts_id_seq TO github_actions;

-- Grant permissions to postgres (admin) user for postgres database
GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- Summary - show what was created
\dt
SELECT 'Database initialization complete!' as status;
SELECT 'Created databases: grafana (for Grafana), postgres (for benchmark data)' as summary;
SELECT 'Created tables: benchmark_metrics, benchmark_commits' as tables;
SELECT 'Created users: postgres (Admin with full access), github_actions (IAM-enabled)' as users;
