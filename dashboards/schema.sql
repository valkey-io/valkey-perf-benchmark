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

-- Create benchmark_timeseries_tiering table for per-second sampler rows.
-- Each source column holds that source's raw cumulative reading as the sampler
-- recorded it, NULL when the source was not read. Derived numbers come from the
-- benchmark_timeseries_tiering_rates view below.
CREATE TABLE IF NOT EXISTS benchmark_timeseries_tiering (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Identity of the sampled run
    config_name TEXT NOT NULL,
    commit VARCHAR(40) NOT NULL,
    timestamp TIMESTAMPTZ,
    repository TEXT,
    module_commit TEXT,
    test_id TEXT NOT NULL,
    scenario TEXT,
    command TEXT,
    data_size INTEGER,
    pipeline INTEGER,
    clients INTEGER,
    io_threads INTEGER,
    architecture TEXT,
    cluster_mode BOOLEAN,
    tls BOOLEAN,
    config_set JSONB,
    profiling_set JSONB,
    run INTEGER NOT NULL,
    run_started_at TIMESTAMPTZ NOT NULL,
    sample_time TIMESTAMPTZ NOT NULL,
    elapsed_sec INTEGER NOT NULL,

    -- Raw sampler sources
    valkey_info JSONB,
    latency_histogram JSONB,
    process_cpu JSONB,
    disk JSONB,

    -- A re-push of the same rows inserts nothing
    CONSTRAINT unique_timeseries_tiering_sample
        UNIQUE (commit, config_name, test_id, run_started_at, run, elapsed_sec)
);

-- Matches the run partition and order of benchmark_timeseries_tiering_rates
CREATE INDEX IF NOT EXISTS idx_benchmark_timeseries_tiering_run
    ON benchmark_timeseries_tiering(config_name, test_id, commit, run, run_started_at, elapsed_sec);

-- Linux USER_HZ, the unit of /proc clock tick counters
CREATE OR REPLACE FUNCTION tiering_clock_ticks_per_sec() RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
    SELECT 100.0::double precision
$$;

-- A raw INFO string as a number, NULL when it is not a plain number
CREATE OR REPLACE FUNCTION tiering_num(value text) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN value ~ '^[-+]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$'
        THEN value::double precision END
$$;

-- Per-second change of a cumulative counter, NULL when either reading is
-- missing or the counter went backwards
CREATE OR REPLACE FUNCTION tiering_rate(
    cur double precision, prev double precision, interval_sec double precision
) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN cur >= prev AND interval_sec > 0
        THEN (cur - prev) / interval_sec END
$$;

-- Sum of the listed elements of a JSON array of numbers, NULL when any is missing
CREATE OR REPLACE FUNCTION tiering_array_sum(arr jsonb, idx int[]) RETURNS double precision
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN count(arr ->> i) = cardinality(idx)
        THEN sum((arr ->> i)::double precision) END
    FROM unnest(idx) AS i
$$;

-- Clock ticks used between two process_cpu.threads readings by every server
-- thread except the main thread. The main thread is the lowest tid, which is
-- the server pid. A thread absent from prev counts from zero. NULL when any
-- thread's ticks went backwards.
CREATE OR REPLACE FUNCTION tiering_worker_thread_ticks(prev jsonb, cur jsonb)
RETURNS double precision
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN coalesce(bool_and(ticks >= 0), true)
        THEN coalesce(sum(ticks), 0) END
    FROM (
        SELECT (c.value ->> 'utime')::double precision
             + (c.value ->> 'stime')::double precision
             - coalesce((prev -> c.key ->> 'utime')::double precision
                      + (prev -> c.key ->> 'stime')::double precision, 0) AS ticks
        FROM jsonb_each(cur) c
        WHERE c.key::bigint > (SELECT min(k::bigint) FROM jsonb_object_keys(cur) k)
    ) threads
$$;

-- Latency in usec at quantile q of the calls made between two LATENCY HISTOGRAM
-- readings of one command. Each histogram_usec object maps a power-of-two
-- bucket's upper bound in usec to the count of calls at or below it since the
-- server started, so counts are cumulative across buckets and across time, and
-- a bucket with no new calls is left out. Each reading is carried forward to
-- every bucket of the other, the earlier one is subtracted, and the result is
-- the first bucket whose interval count reaches q of the interval total. NULL
-- when there were no calls or a count went backwards.
CREATE OR REPLACE FUNCTION tiering_histogram_percentile(
    prev jsonb, cur jsonb, q double precision
) RETURNS double precision
LANGUAGE sql IMMUTABLE STRICT AS $$
    WITH points AS (
        SELECT key::double precision AS usec,
               value::double precision AS cur_count,
               NULL::double precision AS prev_count
        FROM jsonb_each_text(cur)
        UNION ALL
        SELECT key::double precision, NULL, value::double precision
        FROM jsonb_each_text(prev)
    ),
    steps AS (
        SELECT usec,
               coalesce(max(cur_count) OVER w, 0) - coalesce(max(prev_count) OVER w, 0) AS calls
        FROM points
        WINDOW w AS (ORDER BY usec)
    ),
    total AS (
        SELECT calls FROM steps ORDER BY usec DESC LIMIT 1
    )
    SELECT CASE WHEN bool_and(s.calls >= 0) AND max(t.calls) > 0
        THEN min(s.usec) FILTER (WHERE s.calls >= q * t.calls) END
    FROM steps s CROSS JOIN total t
$$;

-- tiering_histogram_percentile of one command between two latency_histogram
-- readings. A command missing from a reading has made no calls.
CREATE OR REPLACE FUNCTION tiering_command_percentile(
    prev jsonb, cur jsonb, command text, q double precision
) RETURNS double precision
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT tiering_histogram_percentile(
        coalesce(prev -> command -> 'histogram_usec', '{}'),
        coalesce(cur -> command -> 'histogram_usec', '{}'),
        q)
$$;

-- Cumulative calls of one command in a latency_histogram reading
CREATE OR REPLACE FUNCTION tiering_command_calls(histogram jsonb, command text)
RETURNS double precision
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT coalesce((histogram -> command ->> 'calls')::double precision, 0)
$$;

-- Per-second numbers of benchmark_timeseries_tiering. Each row is compared with
-- the previous row of the same run, so the first row of a run has NULL rates.
-- CPU percentages count 100 per fully busy core, except host_cpu_*_pct which is
-- a share of every host CPU. Disk fields are the /sys/block/<dev>/stat counters
-- of kernel Documentation/block/stat.rst, with 512 byte sectors.
DROP VIEW IF EXISTS benchmark_timeseries_tiering_rates;
CREATE VIEW benchmark_timeseries_tiering_rates AS
SELECT
    s.id,
    s.config_name,
    s.commit,
    s.timestamp,
    s.repository,
    s.module_commit,
    s.test_id,
    s.scenario,
    s.command,
    s.data_size,
    s.pipeline,
    s.clients,
    s.io_threads,
    s.architecture,
    s.cluster_mode,
    s.tls,
    s.config_set,
    s.profiling_set,
    s.run,
    s.run_started_at,
    s.sample_time,
    s.elapsed_sec,
    s.interval_sec,

    -- valkey_info
    tiering_rate(tiering_num(s.valkey_info ->> 'total_commands_processed'),
                 tiering_num(s.prev_valkey_info ->> 'total_commands_processed'),
                 s.interval_sec) AS ops_per_sec,
    tiering_rate(tiering_num(s.valkey_info ->> 'keyspace_hits'),
                 tiering_num(s.prev_valkey_info ->> 'keyspace_hits'),
                 s.interval_sec) AS keyspace_hits_per_sec,
    tiering_rate(tiering_num(s.valkey_info ->> 'keyspace_misses'),
                 tiering_num(s.prev_valkey_info ->> 'keyspace_misses'),
                 s.interval_sec) AS keyspace_misses_per_sec,
    tiering_num(s.valkey_info ->> 'used_memory') AS used_memory,
    tiering_num(s.valkey_info ->> 'used_memory_rss') AS used_memory_rss,
    tiering_num(s.valkey_info ->> 'maxmemory') AS maxmemory,
    tiering_num(s.valkey_info ->> 'mem_fragmentation_ratio') AS mem_fragmentation_ratio,
    tiering_num(s.valkey_info ->> 'blocked_clients') AS blocked_clients,
    100 * tiering_rate(tiering_num(s.valkey_info ->> 'used_cpu_sys_main_thread')
                         + tiering_num(s.valkey_info ->> 'used_cpu_user_main_thread'),
                       tiering_num(s.prev_valkey_info ->> 'used_cpu_sys_main_thread')
                         + tiering_num(s.prev_valkey_info ->> 'used_cpu_user_main_thread'),
                       s.interval_sec) AS main_thread_cpu_pct,
    100 * tiering_rate(tiering_num(s.valkey_info ->> 'used_cpu_sys')
                         + tiering_num(s.valkey_info ->> 'used_cpu_user'),
                       tiering_num(s.prev_valkey_info ->> 'used_cpu_sys')
                         + tiering_num(s.prev_valkey_info ->> 'used_cpu_user'),
                       s.interval_sec) AS server_cpu_pct,
    tiering_num(s.valkey_info ->> 'ext_storage_capacity_bytes') AS ext_storage_capacity_bytes,
    tiering_num(s.valkey_info ->> 'ext_storage_total_num_items') AS ext_storage_total_num_items,
    tiering_num(s.valkey_info ->> 'ext_storage_total_num_bytes') AS ext_storage_total_num_bytes,
    tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_spilled_to_storage')
        AS ext_storage_total_num_items_spilled_to_storage,
    tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_fetched_from_storage')
        AS ext_storage_total_num_items_fetched_from_storage,
    tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_deleted_from_storage')
        AS ext_storage_total_num_items_deleted_from_storage,
    tiering_rate(tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_spilled_to_storage'),
                 tiering_num(s.prev_valkey_info ->> 'ext_storage_total_num_items_spilled_to_storage'),
                 s.interval_sec) AS ext_storage_spilled_per_sec,
    tiering_rate(tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_fetched_from_storage'),
                 tiering_num(s.prev_valkey_info ->> 'ext_storage_total_num_items_fetched_from_storage'),
                 s.interval_sec) AS ext_storage_fetched_per_sec,
    tiering_rate(tiering_num(s.valkey_info ->> 'ext_storage_total_num_items_deleted_from_storage'),
                 tiering_num(s.prev_valkey_info ->> 'ext_storage_total_num_items_deleted_from_storage'),
                 s.interval_sec) AS ext_storage_deleted_per_sec,

    -- latency_histogram
    tiering_rate(tiering_command_calls(s.latency_histogram, 'get'),
                 tiering_command_calls(s.prev_latency_histogram, 'get'),
                 s.interval_sec) AS get_calls_per_sec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'get', 0.5) AS get_p50_usec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'get', 0.99) AS get_p99_usec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'get', 0.999) AS get_p999_usec,
    tiering_rate(tiering_command_calls(s.latency_histogram, 'set'),
                 tiering_command_calls(s.prev_latency_histogram, 'set'),
                 s.interval_sec) AS set_calls_per_sec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'set', 0.5) AS set_p50_usec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'set', 0.99) AS set_p99_usec,
    tiering_command_percentile(s.prev_latency_histogram, s.latency_histogram, 'set', 0.999) AS set_p999_usec,

    -- process_cpu: proc_stat cpu is user nice system idle iowait irq softirq steal, per proc(5)
    100 * tiering_rate(tiering_array_sum(s.process_cpu #> '{proc_stat,cpu}', '{0,1}'),
                       tiering_array_sum(s.prev_process_cpu #> '{proc_stat,cpu}', '{0,1}'), 1)
        / NULLIF(tiering_rate(tiering_array_sum(s.process_cpu #> '{proc_stat,cpu}', '{0,1,2,3,4,5,6,7}'),
                              tiering_array_sum(s.prev_process_cpu #> '{proc_stat,cpu}', '{0,1,2,3,4,5,6,7}'), 1), 0)
        AS host_cpu_user_pct,
    100 * tiering_rate(tiering_array_sum(s.process_cpu #> '{proc_stat,cpu}', '{2,5,6}'),
                       tiering_array_sum(s.prev_process_cpu #> '{proc_stat,cpu}', '{2,5,6}'), 1)
        / NULLIF(tiering_rate(tiering_array_sum(s.process_cpu #> '{proc_stat,cpu}', '{0,1,2,3,4,5,6,7}'),
                              tiering_array_sum(s.prev_process_cpu #> '{proc_stat,cpu}', '{0,1,2,3,4,5,6,7}'), 1), 0)
        AS host_cpu_sys_pct,
    100 * tiering_worker_thread_ticks(s.prev_process_cpu -> 'threads', s.process_cpu -> 'threads')
        / tiering_clock_ticks_per_sec() / NULLIF(s.interval_sec, 0) AS worker_threads_cpu_pct,

    -- disk: stat is reads, read merges, sectors read, ms reading, writes, write
    -- merges, sectors written, ms writing, in flight, io_ticks, time_in_queue, ...
    tiering_rate((s.disk #>> '{stat,0}')::double precision,
                 (s.prev_disk #>> '{stat,0}')::double precision, s.interval_sec) AS disk_read_iops,
    tiering_rate((s.disk #>> '{stat,4}')::double precision,
                 (s.prev_disk #>> '{stat,4}')::double precision, s.interval_sec) AS disk_write_iops,
    tiering_rate((s.disk #>> '{stat,2}')::double precision,
                 (s.prev_disk #>> '{stat,2}')::double precision, s.interval_sec) * 512 / 1e6
        AS disk_read_mb_per_sec,
    tiering_rate((s.disk #>> '{stat,6}')::double precision,
                 (s.prev_disk #>> '{stat,6}')::double precision, s.interval_sec) * 512 / 1e6
        AS disk_write_mb_per_sec,
    tiering_rate((s.disk #>> '{stat,3}')::double precision,
                 (s.prev_disk #>> '{stat,3}')::double precision, 1)
        / NULLIF(tiering_rate((s.disk #>> '{stat,0}')::double precision,
                              (s.prev_disk #>> '{stat,0}')::double precision, 1), 0) AS disk_r_await_ms,
    tiering_rate((s.disk #>> '{stat,7}')::double precision,
                 (s.prev_disk #>> '{stat,7}')::double precision, 1)
        / NULLIF(tiering_rate((s.disk #>> '{stat,4}')::double precision,
                              (s.prev_disk #>> '{stat,4}')::double precision, 1), 0) AS disk_w_await_ms,
    tiering_rate((s.disk #>> '{stat,10}')::double precision,
                 (s.prev_disk #>> '{stat,10}')::double precision, s.interval_sec) / 1000 AS disk_aqu_sz,
    tiering_rate((s.disk #>> '{stat,9}')::double precision,
                 (s.prev_disk #>> '{stat,9}')::double precision, s.interval_sec) / 10 AS disk_util_pct,
    (s.disk #>> '{stat,8}')::double precision AS disk_in_flight
FROM (
    SELECT t.*,
           EXTRACT(EPOCH FROM t.sample_time - lag(t.sample_time) OVER w)::double precision AS interval_sec,
           lag(t.valkey_info) OVER w AS prev_valkey_info,
           lag(t.latency_histogram) OVER w AS prev_latency_histogram,
           lag(t.process_cpu) OVER w AS prev_process_cpu,
           lag(t.disk) OVER w AS prev_disk
    FROM benchmark_timeseries_tiering t
    WINDOW w AS (PARTITION BY t.config_name, t.test_id, t.commit, t.run, t.run_started_at
                 ORDER BY t.elapsed_sec)
) s;

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
ALTER TABLE IF EXISTS benchmark_timeseries_tiering OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_metrics_id_seq OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_commits_id_seq OWNER TO github_actions;
ALTER SEQUENCE IF EXISTS benchmark_timeseries_tiering_id_seq OWNER TO github_actions;
ALTER VIEW IF EXISTS benchmark_timeseries_tiering_rates OWNER TO github_actions;

-- Grant permissions to github_actions user for postgres database
GRANT CONNECT ON DATABASE postgres TO github_actions;
GRANT USAGE, CREATE ON SCHEMA public TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_metrics TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_commits TO github_actions;
GRANT ALL PRIVILEGES ON benchmark_timeseries_tiering TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_metrics_id_seq TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_commits_id_seq TO github_actions;
GRANT ALL PRIVILEGES ON SEQUENCE benchmark_timeseries_tiering_id_seq TO github_actions;

-- Grant permissions to postgres (admin) user for postgres database
GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- Summary - show what was created
\dt
SELECT 'Database initialization complete!' as status;
SELECT 'Created databases: grafana (for Grafana), postgres (for benchmark data)' as summary;
SELECT 'Created tables: benchmark_metrics, benchmark_commits, benchmark_timeseries_tiering' as tables;
SELECT 'Created users: postgres (Admin with full access), github_actions (IAM-enabled)' as users;
