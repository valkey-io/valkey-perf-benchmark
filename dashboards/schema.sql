-- Valkey Benchmark Metrics Database Schema
-- This schema matches the one in utils/push_to_postgres.py
-- NOTE: push_to_postgres.py automatically creates this table, so this file is optional
-- Use this if you want to pre-create the schema before running benchmarks

-- Connect to the grafana database first
\c grafana

-- Create benchmark_metrics table (matches push_to_postgres.py)
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_commit ON benchmark_metrics(commit);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_timestamp ON benchmark_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_benchmark_metrics_command ON benchmark_metrics(command);

-- Create unique index to prevent duplicate entries
CREATE UNIQUE INDEX IF NOT EXISTS idx_benchmark_metrics_unique 
    ON benchmark_metrics(timestamp, commit, command, data_size, pipeline);

-- Create a view for latest metrics per commit
CREATE OR REPLACE VIEW latest_commit_metrics AS
SELECT DISTINCT ON (commit, command)
    commit,
    timestamp,
    command,
    rps,
    p50_latency_ms,
    p95_latency_ms,
    p99_latency_ms,
    clients,
    pipeline,
    data_size,
    cluster_mode,
    tls,
    created_at
FROM benchmark_metrics
ORDER BY commit, command, timestamp DESC;

-- Create a view for performance trends over time
CREATE OR REPLACE VIEW performance_trends AS
SELECT 
    DATE_TRUNC('day', timestamp) as day,
    commit,
    command,
    AVG(rps) as avg_rps,
    AVG(p50_latency_ms) as avg_p50,
    AVG(p95_latency_ms) as avg_p95,
    AVG(p99_latency_ms) as avg_p99,
    COUNT(*) as test_count
FROM benchmark_metrics
GROUP BY DATE_TRUNC('day', timestamp), commit, command
ORDER BY day DESC;

-- Create a view for command comparison
CREATE OR REPLACE VIEW command_comparison AS
SELECT 
    command,
    COUNT(DISTINCT commit) as commit_count,
    AVG(rps) as avg_rps,
    MAX(rps) as max_rps,
    MIN(rps) as min_rps,
    AVG(p99_latency_ms) as avg_p99_latency,
    COUNT(*) as total_tests
FROM benchmark_metrics
GROUP BY command
ORDER BY avg_rps DESC;

-- Grant permissions to the IAM-enabled database user
GRANT SELECT, INSERT, UPDATE ON benchmark_metrics TO CURRENT_USER;
GRANT USAGE, SELECT ON SEQUENCE benchmark_metrics_id_seq TO CURRENT_USER;
GRANT SELECT ON latest_commit_metrics TO CURRENT_USER;
GRANT SELECT ON performance_trends TO CURRENT_USER;
GRANT SELECT ON command_comparison TO CURRENT_USER;

-- Grant rds_iam role for IAM authentication (required for IAM database auth)
GRANT rds_iam TO CURRENT_USER;

-- Display table info
\d benchmark_metrics

-- Show sample query
SELECT 
    commit,
    command,
    rps,
    p99_latency_ms,
    timestamp
FROM benchmark_metrics
ORDER BY timestamp DESC
LIMIT 10;

-- Show summary statistics
SELECT 
    command,
    COUNT(*) as test_count,
    AVG(rps)::NUMERIC(10,2) as avg_rps,
    AVG(p99_latency_ms)::NUMERIC(10,3) as avg_p99_ms
FROM benchmark_metrics
GROUP BY command
ORDER BY avg_rps DESC;

COMMENT ON TABLE benchmark_metrics IS 'Stores Valkey performance benchmark results';
COMMENT ON COLUMN benchmark_metrics.commit IS 'Git commit SHA from valkey repository';
COMMENT ON COLUMN benchmark_metrics.command IS 'Benchmark command (GET, SET, LPUSH, etc.)';
COMMENT ON COLUMN benchmark_metrics.rps IS 'Requests per second (throughput)';
COMMENT ON COLUMN benchmark_metrics.p50_latency_ms IS 'P50 latency in milliseconds';
COMMENT ON COLUMN benchmark_metrics.p99_latency_ms IS 'P99 latency in milliseconds';
COMMENT ON COLUMN benchmark_metrics.cluster_mode IS 'Whether cluster mode was enabled';
COMMENT ON COLUMN benchmark_metrics.tls IS 'Whether TLS was enabled';
