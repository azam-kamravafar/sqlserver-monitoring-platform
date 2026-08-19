from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MSSQL_CONN_ID = "sqlserver_monitoring"
POSTGRES_CONN_ID = "postgres_monitoring"


DATABASE_LIST_SQL = """
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND source_database_id IS NULL
  AND state_desc = 'ONLINE'
ORDER BY name;
"""


def collect_index_usage():

    # -------------------------------------------------
    # 1. Connect to SQL Server
    # -------------------------------------------------
    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # -------------------------------------------------
    # 2. Discover all online user databases
    # -------------------------------------------------
    databases = mssql.get_records(
        DATABASE_LIST_SQL
    )

    if not databases:
        raise ValueError(
            "No online user databases found"
        )

    postgres_rows = []

    # -------------------------------------------------
    # 3. Collect index usage from every database
    # -------------------------------------------------
    for db_row in databases:

        database_name = db_row[0]

        # Safe database identifier:
        # ] becomes ]] inside [database_name]
        safe_db_identifier = database_name.replace(
            "]",
            "]]"
        )

        # Safe SQL string literal:
        # ' becomes ''
        safe_db_literal = database_name.replace(
            "'",
            "''"
        )

        index_usage_sql = f"""
        SELECT
            CAST(
                SERVERPROPERTY('ServerName')
                AS NVARCHAR(128)
            ) AS server_name,

            N'{safe_db_literal}' AS database_name,

            s.name AS schema_name,

            t.name AS table_name,

            i.name AS index_name,

            i.index_id,

            COALESCE(us.user_seeks, 0)
                AS user_seeks,

            COALESCE(us.user_scans, 0)
                AS user_scans,

            COALESCE(us.user_lookups, 0)
                AS user_lookups,

            COALESCE(us.user_updates, 0)
                AS user_updates,

            us.last_user_seek,

            us.last_user_scan,

            us.last_user_lookup,

            us.last_user_update

        FROM [{safe_db_identifier}].sys.indexes AS i

        INNER JOIN [{safe_db_identifier}].sys.tables AS t
            ON i.object_id = t.object_id

        INNER JOIN [{safe_db_identifier}].sys.schemas AS s
            ON t.schema_id = s.schema_id

        LEFT JOIN sys.dm_db_index_usage_stats AS us
            ON us.database_id =
                DB_ID(N'{safe_db_literal}')
           AND us.object_id = i.object_id
           AND us.index_id = i.index_id

        WHERE
            i.index_id > 0
            AND i.is_hypothetical = 0
            AND t.is_ms_shipped = 0

        ORDER BY
            s.name,
            t.name,
            i.index_id;
        """

        rows = mssql.get_records(
            index_usage_sql
        )

        for row in rows:

            postgres_rows.append(
                (
                    row[0],   # server_name
                    row[1],   # database_name
                    row[2],   # schema_name
                    row[3],   # table_name
                    row[4],   # index_name
                    row[5],   # index_id
                    row[6],   # user_seeks
                    row[7],   # user_scans
                    row[8],   # user_lookups
                    row[9],   # user_updates
                    row[10],  # last_user_seek
                    row[11],  # last_user_scan
                    row[12],  # last_user_lookup
                    row[13],  # last_user_update
                )
            )

        print(
            f"{database_name}: "
            f"{len(rows)} indexes collected"
        )

    if not postgres_rows:
        raise ValueError(
            "No index usage data collected"
        )

    # -------------------------------------------------
    # 4. Connect to PostgreSQL
    # -------------------------------------------------
    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # -------------------------------------------------
    # 5. Store snapshot in PostgreSQL
    # -------------------------------------------------
    postgres.insert_rows(
        table="monitoring.sqlserver_index_usage",
        rows=postgres_rows,
        target_fields=[
            "server_name",
            "database_name",
            "schema_name",
            "table_name",
            "index_name",
            "index_id",
            "user_seeks",
            "user_scans",
            "user_lookups",
            "user_updates",
            "last_user_seek",
            "last_user_scan",
            "last_user_lookup",
            "last_user_update",
        ],
        commit_every=1000,
    )

    print(
        f"Inserted {len(postgres_rows)} "
        f"index usage rows into PostgreSQL"
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="sqlserver_index_usage",
    description=(
        "Collect SQL Server index usage statistics "
        "for all user databases"
    ),
    start_date=datetime(2026, 8, 14),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "sqlserver",
        "index",
        "monitoring",
        "postgres",
    ],
) as dag:

    collect_index_usage_task = PythonOperator(
        task_id="collect_index_usage",
        python_callable=collect_index_usage,
        execution_timeout=timedelta(minutes=5),
    )
