from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="05_main_orchestration_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["orchestration", "main", "etl"],
    doc_md="""
    ## DAG de Orquestração Principal

    Este DAG coordena a execução de todos os fluxos de ETL:
    1.  Carga de Dimensões (Date, Product, Customer).
    2.  Carga da Tabela Fato (Sales).
    """,
) as dag:
    trigger_init = TriggerDagRunOperator(
        task_id="trigger_00_initialization_dag",
        trigger_dag_id="00_initialization_dag",
        conf={},
        execution_date="{{ ds }}",
    )

    trigger_dim_date = TriggerDagRunOperator(
        task_id="trigger_01_etl_dim_date",
        trigger_dag_id="01_etl_dim_date",
        conf={},
    )

    trigger_dim_product = TriggerDagRunOperator(
        task_id="trigger_03_etl_dim_product",
        trigger_dag_id="03_etl_dim_product",
        conf={},
    )

    trigger_dim_customer = TriggerDagRunOperator(
        task_id="trigger_04_etl_dim_customer",
        trigger_dag_id="04_etl_dim_customer",
        conf={},
    )

    trigger_fact_sales = TriggerDagRunOperator(
        task_id="trigger_02_etl_fact_sales",
        trigger_dag_id="02_etl_fact_sales",
        conf={},
    )

    trigger_init >> trigger_dim_date
    trigger_dim_date >> [trigger_dim_product, trigger_dim_customer]
    [trigger_dim_product, trigger_dim_customer] >> trigger_fact_sales
