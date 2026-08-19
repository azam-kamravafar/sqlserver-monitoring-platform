from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MSSQL_CONN_ID = "sqlserver_monitoring"
POSTGRES_CONN_ID = "postgres_monitoring"


WAIT_STATS_SQL = """
SELECT
    CAST(
        SERVERPROPERTY('ServerName')
        AS NVARCHAR(128)
    ) AS server_name,

    wait_type,

    waiting_tasks_count,

    wait_time_ms,

    max_wait_time_ms,

    signal_wait_time_ms

FROM sys.dm_os_wait_stats

WHERE waiting_tasks_count > 0

ORDER BY wait_time_ms DESC;
"""


def collect_wait_stats():

    # -----------------------------------------
    # 1. Connect to SQL Server
    # -----------------------------------------
    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # -----------------------------------------
    # 2. Read wait statistics
    # -----------------------------------------
    rows = mssql.get_records(
        WAIT_STATS_SQL
    )

    if not rows:
        print("No SQL Server wait statistics found.")
        return

    postgres_rows = []

    # -----------------------------------------
    # 3. Prepare PostgreSQL rows
    # -----------------------------------------
    for row in rows:

        postgres_rows.append(
            (
                row[0],  # server_name
                row[1],  # wait_type
                row[2],  # waiting_tasks_count
                row[3],  # wait_time_ms
                row[4],  # max_wait_time_ms
                row[5],  # signal_wait_time_ms
            )
        )

    # -----------------------------------------
    # 4. Connect to PostgreSQL
    # -----------------------------------------
    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # -----------------------------------------
    # 5. Store snapshot
    # -----------------------------------------
    postgres.insert_rows(
        table="monitoring.sqlserver_wait_stats",
        rows=postgres_rows,
        target_fields=[
            "server_name",
            "wait_type",
            "waiting_tasks_count",
            "wait_time_ms",
            "max_wait_time_ms",
            "signal_wait_time_ms",
        ],
        commit_every=1000,
    )

    print(
        f"Inserted {len(postgres_rows)} "
        f"wait statistics rows into PostgreSQL"
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="sqlserver_wait_stats",
    description=(
        "Collect SQL Server instance wait statistics "
        "and store snapshots in PostgreSQL"
    ),
    start_date=datetime(2026, 8, 14),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "sqlserver",
        "waits",
        "monitoring",
        "postgres",
    ],
) as dag:

    collect_wait_stats_task = PythonOperator(
        task_id="collect_wait_stats",
        python_callable=collect_wait_stats,
        execution_timeout=timedelta(minutes=3),
    )
