-- ============================================================
-- PostgreSQL permissions for the monitoring platform
--
-- Assumptions:
--   monitoring_db   already exists
--   monitoring_user already exists
--   grafana_reader  already exists
--
-- Passwords are intentionally NOT stored in this repository.
-- ============================================================


-- monitoring_user owns and writes monitoring data
GRANT CONNECT ON DATABASE monitoring_db TO monitoring_user;

GRANT USAGE, CREATE
ON SCHEMA monitoring
TO monitoring_user;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA monitoring
TO monitoring_user;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA monitoring
TO monitoring_user;


-- Grafana receives read-only access
GRANT CONNECT ON DATABASE monitoring_db TO grafana_reader;

GRANT USAGE
ON SCHEMA monitoring
TO grafana_reader;

GRANT SELECT
ON ALL TABLES IN SCHEMA monitoring
TO grafana_reader;


-- Future tables created by monitoring_user
-- automatically become readable by grafana_reader
ALTER DEFAULT PRIVILEGES
FOR ROLE monitoring_user
IN SCHEMA monitoring
GRANT SELECT ON TABLES TO grafana_reader;
