-- ============================================================
-- SQL Server Monitoring Platform
-- PostgreSQL monitoring schema and tables
-- ============================================================

CREATE SCHEMA IF NOT EXISTS monitoring
AUTHORIZATION monitoring_user;


-- ============================================================
-- 1. General SQL Server / Database Metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.sqlserver_metrics (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    server_name TEXT NOT NULL,
    database_name TEXT,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    unit TEXT
);

CREATE INDEX IF NOT EXISTS idx_sqlserver_metrics_time
ON monitoring.sqlserver_metrics (collected_at);

CREATE INDEX IF NOT EXISTS idx_sqlserver_metrics_name_time
ON monitoring.sqlserver_metrics (metric_name, collected_at);

CREATE INDEX IF NOT EXISTS idx_sqlserver_metrics_db_metric_time
ON monitoring.sqlserver_metrics
(database_name, metric_name, collected_at DESC);


-- ============================================================
-- 2. Index Usage
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.sqlserver_index_usage (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    server_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    index_name TEXT,
    index_id INTEGER NOT NULL,
    user_seeks BIGINT NOT NULL DEFAULT 0,
    user_scans BIGINT NOT NULL DEFAULT 0,
    user_lookups BIGINT NOT NULL DEFAULT 0,
    user_updates BIGINT NOT NULL DEFAULT 0,
    last_user_seek TIMESTAMPTZ,
    last_user_scan TIMESTAMPTZ,
    last_user_lookup TIMESTAMPTZ,
    last_user_update TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_index_usage_lookup
ON monitoring.sqlserver_index_usage
(database_name, table_name, index_name, collected_at DESC);


-- ============================================================
-- 3. Missing Index Suggestions
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.sqlserver_missing_indexes (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    server_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    schema_name TEXT,
    table_name TEXT NOT NULL,
    equality_columns TEXT,
    inequality_columns TEXT,
    included_columns TEXT,
    user_seeks BIGINT,
    user_scans BIGINT,
    avg_total_user_cost DOUBLE PRECISION,
    avg_user_impact DOUBLE PRECISION,
    improvement_score DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_missing_indexes_time
ON monitoring.sqlserver_missing_indexes
(database_name, collected_at DESC);


-- ============================================================
-- 4. Index Fragmentation
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.sqlserver_index_fragmentation (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    server_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    index_name TEXT,
    index_id INTEGER NOT NULL,
    index_type TEXT,
    avg_fragmentation_percent DOUBLE PRECISION,
    page_count BIGINT,
    fragment_count BIGINT
);

CREATE INDEX IF NOT EXISTS idx_fragmentation_lookup
ON monitoring.sqlserver_index_fragmentation
(database_name, table_name, collected_at DESC);


-- ============================================================
-- 5. SQL Server Wait Statistics
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.sqlserver_wait_stats (
    id BIGSERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    server_name TEXT NOT NULL,
    wait_type TEXT NOT NULL,
    waiting_tasks_count BIGINT NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    max_wait_time_ms BIGINT NOT NULL,
    signal_wait_time_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wait_stats_type_time
ON monitoring.sqlserver_wait_stats
(wait_type, collected_at DESC);
