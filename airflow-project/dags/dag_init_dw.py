from __future__ import annotations
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

SQL_FILE_PATH = "sql/create_dw_schema.sql"
POSTGRES_DW_CONN_ID = "postgres_dw"

with DAG(
    dag_id="00_dw_schema_initialization",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dw", "setup"],
    doc_md=__doc__,
) as dag:
    create_dw_schema = PostgresOperator(
        task_id="create_dw_schema",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql=SQL_FILE_PATH,
        database="adventureworks_dw",
        autocommit=True,
    )
