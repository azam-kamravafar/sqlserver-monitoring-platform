from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MSSQL_CONN_ID = "sqlserver_monitoring"
POSTGRES_CONN_ID = "postgres_monitoring"


DATABASE_METRICS_SQL = """
WITH db_list AS
(
    SELECT
        database_id,
        name AS database_name,
        state_desc
    FROM sys.databases
    WHERE database_id > 4
      AND source_database_id IS NULL
),
file_sizes AS
(
    SELECT
        database_id,

        SUM(
            CASE
                WHEN type_desc = 'ROWS'
                THEN CAST(size AS BIGINT) * 8.0 / 1024
                ELSE 0
            END
        ) AS data_size_mb,

        SUM(
            CASE
                WHEN type_desc = 'LOG'
                THEN CAST(size AS BIGINT) * 8.0 / 1024
                ELSE 0
            END
        ) AS log_size_mb

    FROM sys.master_files
    WHERE database_id > 4
    GROUP BY database_id
),
connections AS
(
    SELECT
        database_id,
        COUNT(*) AS user_connections
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    GROUP BY database_id
),
io_stats AS
(
    SELECT
        database_id,

        SUM(num_of_reads) AS reads_total,
        SUM(num_of_writes) AS writes_total,

        SUM(num_of_bytes_read) / 1048576.0 AS read_mb_total,
        SUM(num_of_bytes_written) / 1048576.0 AS write_mb_total,

        CASE
            WHEN SUM(num_of_reads) = 0 THEN 0
            ELSE
                CAST(SUM(io_stall_read_ms) AS FLOAT)
                / SUM(num_of_reads)
        END AS avg_read_latency_ms,

        CASE
            WHEN SUM(num_of_writes) = 0 THEN 0
            ELSE
                CAST(SUM(io_stall_write_ms) AS FLOAT)
                / SUM(num_of_writes)
        END AS avg_write_latency_ms

    FROM sys.dm_io_virtual_file_stats(NULL, NULL)
    WHERE database_id > 4
    GROUP BY database_id
)

SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
        AS server_name,

    d.database_name,

    metrics.metric_name,

    CAST(metrics.metric_value AS FLOAT)
        AS metric_value,

    metrics.unit

FROM db_list d

LEFT JOIN file_sizes fs
    ON d.database_id = fs.database_id

LEFT JOIN connections c
    ON d.database_id = c.database_id

LEFT JOIN io_stats io
    ON d.database_id = io.database_id

CROSS APPLY
(
    VALUES

    (
        'database_online',
        CASE
            WHEN d.state_desc = 'ONLINE' THEN 1.0
            ELSE 0.0
        END,
        'status'
    ),

    (
        'data_size_mb',
        COALESCE(fs.data_size_mb, 0),
        'MB'
    ),

    (
        'log_size_mb',
        COALESCE(fs.log_size_mb, 0),
        'MB'
    ),

    (
        'user_connections',
        COALESCE(c.user_connections, 0),
        'connections'
    ),

    (
        'reads_total',
        COALESCE(io.reads_total, 0),
        'operations'
    ),

    (
        'writes_total',
        COALESCE(io.writes_total, 0),
        'operations'
    ),

    (
        'read_mb_total',
        COALESCE(io.read_mb_total, 0),
        'MB'
    ),

    (
        'write_mb_total',
        COALESCE(io.write_mb_total, 0),
        'MB'
    ),

    (
        'avg_read_latency_ms',
        COALESCE(io.avg_read_latency_ms, 0),
        'ms'
    ),

    (
        'avg_write_latency_ms',
        COALESCE(io.avg_write_latency_ms, 0),
        'ms'
    )

) metrics
(
    metric_name,
    metric_value,
    unit
)

ORDER BY
    d.database_name,
    metrics.metric_name;
"""


def collect_database_metrics():

    # -----------------------------------------
    # 1. Connect to SQL Server
    # -----------------------------------------

    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # -----------------------------------------
    # 2. Run monitoring query
    # -----------------------------------------

    rows = mssql.get_records(
        DATABASE_METRICS_SQL
    )

    if not rows:
        raise ValueError(
            "No database metrics returned from SQL Server"
        )

    # -----------------------------------------
    # 3. Prepare rows for PostgreSQL
    # -----------------------------------------

    postgres_rows = []

    for row in rows:

        server_name = row[0]
        database_name = row[1]
        metric_name = row[2]
        metric_value = row[3]
        unit = row[4]

        postgres_rows.append(
            (
                server_name,
                database_name,
                metric_name,
                metric_value,
                unit,
            )
        )

    # -----------------------------------------
    # 4. Connect to PostgreSQL
    # -----------------------------------------

    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # -----------------------------------------
    # 5. Insert metrics
    # -----------------------------------------

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

    print(
        f"Inserted {len(postgres_rows)} "
        f"database metrics into PostgreSQL"
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="sqlserver_database_metrics",
    description=(
        "Collect SQL Server database metrics "
        "and store them in PostgreSQL"
    ),
    start_date=datetime(2026, 8, 14),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "sqlserver",
        "database",
        "monitoring",
        "postgres",
    ],
) as dag:

    collect_database_metrics_task = PythonOperator(
        task_id="collect_database_metrics",
        python_callable=collect_database_metrics,
        execution_timeout=timedelta(minutes=2),
    )
