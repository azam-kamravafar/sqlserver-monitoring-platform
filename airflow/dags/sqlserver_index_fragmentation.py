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


def collect_index_fragmentation():

    # -------------------------------------------------
    # 1. Connect to SQL Server
    # -------------------------------------------------
    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # -------------------------------------------------
    # 2. Discover online user databases
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
    # 3. Collect fragmentation from every database
    # -------------------------------------------------
    for db_row in databases:

        database_name = db_row[0]

        # Protect SQL identifier
        safe_db_identifier = database_name.replace(
            "]",
            "]]"
        )

        # Protect string literal
        safe_db_literal = database_name.replace(
            "'",
            "''"
        )

        fragmentation_sql = f"""
        SELECT
            CAST(
                SERVERPROPERTY('ServerName')
                AS NVARCHAR(128)
            ) AS server_name,

            N'{safe_db_literal}'
                AS database_name,

            s.name
                AS schema_name,

            t.name
                AS table_name,

            i.name
                AS index_name,

            ps.index_id,

            MAX(ps.index_type_desc)
                AS index_type,

            CASE
                WHEN SUM(ps.page_count) = 0
                    THEN 0
                ELSE
                    SUM(
                        ps.avg_fragmentation_in_percent
                        * ps.page_count
                    )
                    / SUM(ps.page_count)
            END AS avg_fragmentation_percent,

            SUM(ps.page_count)
                AS page_count,

            SUM(ps.fragment_count)
                AS fragment_count

        FROM sys.dm_db_index_physical_stats(
            DB_ID(N'{safe_db_literal}'),
            NULL,
            NULL,
            NULL,
            'LIMITED'
        ) AS ps

        INNER JOIN [{safe_db_identifier}].sys.indexes AS i
            ON ps.object_id = i.object_id
           AND ps.index_id = i.index_id

        INNER JOIN [{safe_db_identifier}].sys.tables AS t
            ON i.object_id = t.object_id

        INNER JOIN [{safe_db_identifier}].sys.schemas AS s
            ON t.schema_id = s.schema_id

        WHERE
            ps.index_id > 0
            AND ps.index_level = 0
            AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
            AND i.is_hypothetical = 0
            AND t.is_ms_shipped = 0

        GROUP BY
            s.name,
            t.name,
            i.name,
            ps.index_id

        ORDER BY
            avg_fragmentation_percent DESC;
        """

        rows = mssql.get_records(
            fragmentation_sql
        )

        for row in rows:
            postgres_rows.append(
                (
                    row[0],  # server_name
                    row[1],  # database_name
                    row[2],  # schema_name
                    row[3],  # table_name
                    row[4],  # index_name
                    row[5],  # index_id
                    row[6],  # index_type
                    row[7],  # fragmentation
                    row[8],  # page_count
                    row[9],  # fragment_count
                )
            )

        print(
            f"{database_name}: "
            f"{len(rows)} indexes checked"
        )

    if not postgres_rows:
        print(
            "No fragmentation data found."
        )
        return

    # -------------------------------------------------
    # 4. Connect to PostgreSQL
    # -------------------------------------------------
    postgres = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    # -------------------------------------------------
    # 5. Store fragmentation snapshot
    # -------------------------------------------------
    postgres.insert_rows(
        table="monitoring.sqlserver_index_fragmentation",
        rows=postgres_rows,
        target_fields=[
            "server_name",
            "database_name",
            "schema_name",
            "table_name",
            "index_name",
            "index_id",
            "index_type",
            "avg_fragmentation_percent",
            "page_count",
            "fragment_count",
        ],
        commit_every=1000,
    )

    print(
        f"Inserted {len(postgres_rows)} "
        f"fragmentation rows into PostgreSQL"
    )


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sqlserver_index_fragmentation",
    description=(
        "Daily SQL Server index fragmentation "
        "monitoring for all user databases"
    ),
    start_date=datetime(2026, 8, 14),
    schedule="17 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "sqlserver",
        "index",
        "fragmentation",
        "monitoring",
        "postgres",
    ],
) as dag:

    collect_index_fragmentation_task = PythonOperator(
        task_id="collect_index_fragmentation",
        python_callable=collect_index_fragmentation,
        execution_timeout=timedelta(minutes=20),
    )
