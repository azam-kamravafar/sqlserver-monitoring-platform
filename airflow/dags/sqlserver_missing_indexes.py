from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MSSQL_CONN_ID = "sqlserver_monitoring"
POSTGRES_CONN_ID = "postgres_monitoring"


MISSING_INDEX_SQL = """
SELECT
    CAST(
        SERVERPROPERTY('ServerName')
        AS NVARCHAR(128)
    ) AS server_name,

    DB_NAME(mid.database_id)
        AS database_name,

    OBJECT_SCHEMA_NAME(
        mid.object_id,
        mid.database_id
    ) AS schema_name,

    OBJECT_NAME(
        mid.object_id,
        mid.database_id
    ) AS table_name,

    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,

    COALESCE(migs.user_seeks, 0)
        AS user_seeks,

    COALESCE(migs.user_scans, 0)
        AS user_scans,

    CAST(
        migs.avg_total_user_cost
        AS FLOAT
    ) AS avg_total_user_cost,

    CAST(
        migs.avg_user_impact
        AS FLOAT
    ) AS avg_user_impact,

    CAST(
        migs.avg_total_user_cost
        * migs.avg_user_impact
        * (
            migs.user_seeks
            + migs.user_scans
        )
        AS FLOAT
    ) AS improvement_score

FROM sys.dm_db_missing_index_group_stats AS migs

INNER JOIN sys.dm_db_missing_index_groups AS mig
    ON migs.group_handle =
       mig.index_group_handle

INNER JOIN sys.dm_db_missing_index_details AS mid
    ON mig.index_handle =
       mid.index_handle

WHERE mid.database_id > 4

ORDER BY improvement_score DESC;
"""


def collect_missing_indexes():

    # ---------------------------------
    # 1. Connect to SQL Server
    # ---------------------------------

    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # ---------------------------------
    # 2. Read missing-index suggestions
    # ---------------------------------

    rows = mssql.get_records(
        MISSING_INDEX_SQL
    )

    # No missing indexes is valid.
    if not rows:
        print(
            "No missing index suggestions "
            "found in SQL Server."
        )
        return

    postgres_rows = []

    for row in rows:

        postgres_rows.append(
            (
                row[0],   # server_name
                row[1],   # database_name
                row[2],   # schema_name
                row[3],   # table_name
                row[4],   # equality_columns
                row[5],   # inequality_columns
                row[6],   # included_columns
                row[7],   # user_seeks
                row[8],   # user_scans
                row[9],   # avg_total_user_cost
                row[10],  # avg_user_impact
                row[11],  # improvement_score
            )
        )

    # ---------------------------------
    # 3. Connect to PostgreSQL
    # ---------------------------------

    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # ---------------------------------
    # 4. Store snapshot
    # ---------------------------------

    postgres.insert_rows(
        table="monitoring.sqlserver_missing_indexes",
        rows=postgres_rows,
        target_fields=[
            "server_name",
            "database_name",
            "schema_name",
            "table_name",
            "equality_columns",
            "inequality_columns",
            "included_columns",
            "user_seeks",
            "user_scans",
            "avg_total_user_cost",
            "avg_user_impact",
            "improvement_score",
        ],
        commit_every=500,
    )

    print(
        f"Inserted {len(postgres_rows)} "
        "missing index suggestions "
        "into PostgreSQL"
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="sqlserver_missing_indexes",
    description=(
        "Collect SQL Server missing index "
        "recommendations into PostgreSQL"
    ),
    start_date=datetime(2026, 8, 14),
    schedule="7 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "sqlserver",
        "index",
        "missing-index",
        "monitoring",
        "postgres",
    ],
) as dag:

    collect_missing_indexes_task = PythonOperator(
        task_id="collect_missing_indexes",
        python_callable=collect_missing_indexes,
        execution_timeout=timedelta(minutes=5),
    )
