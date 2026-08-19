from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MSSQL_CONN_ID = "sqlserver_monitoring"
POSTGRES_CONN_ID = "postgres_monitoring"


SERVER_METRICS_SQL = """
SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)) AS server_name,
    'user_sessions' AS metric_name,
    CAST(COUNT(*) AS FLOAT) AS metric_value,
    'sessions' AS unit
FROM sys.dm_exec_sessions
WHERE is_user_process = 1

UNION ALL

SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)),
    'running_requests',
    CAST(COUNT(*) AS FLOAT),
    'requests'
FROM sys.dm_exec_requests
WHERE session_id <> @@SPID
  AND status = 'running'

UNION ALL

SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)),
    'blocked_requests',
    CAST(COUNT(*) AS FLOAT),
    'requests'
FROM sys.dm_exec_requests
WHERE blocking_session_id > 0

UNION ALL

SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)),
    'sqlserver_memory_mb',
    CAST(physical_memory_in_use_kb / 1024.0 AS FLOAT),
    'MB'
FROM sys.dm_os_process_memory

UNION ALL

SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)),
    'available_os_memory_mb',
    CAST(available_physical_memory_kb / 1024.0 AS FLOAT),
    'MB'
FROM sys.dm_os_sys_memory;
"""


def collect_server_metrics():

    # 1) اتصال به SQL Server
    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # 2) اجرای Query روی SQL Server
    rows = mssql.get_records(SERVER_METRICS_SQL)

    if not rows:
        raise ValueError("No metrics returned from SQL Server")

    # 3) آماده‌سازی داده برای PostgreSQL
    postgres_rows = []

    for row in rows:
        server_name = row[0]
        metric_name = row[1]
        metric_value = row[2]
        unit = row[3]

        postgres_rows.append(
            (
                server_name,
                None,
                metric_name,
                metric_value,
                unit,
            )
        )

    # 4) اتصال به PostgreSQL
    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # 5) ذخیره Metricها در PostgreSQL
    postgres.insert_rows(
        table="monitoring.sqlserver_metrics",
        rows=postgres_rows,
        target_fields=[
            "server_name",
            "database_name",
            "metric_name",
            "metric_value",
            "unit",
        ],
    )

    print(f"Inserted {len(postgres_rows)} SQL Server metrics into PostgreSQL")


with DAG(
    dag_id="sqlserver_server_metrics",
    description="Collect SQL Server instance metrics and store them in PostgreSQL",
    start_date=datetime(2026, 8, 14),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["sqlserver", "monitoring", "postgres"],
) as dag:

    collect_metrics_task = PythonOperator(
        task_id="collect_server_metrics",
        python_callable=collect_server_metrics,
    )
