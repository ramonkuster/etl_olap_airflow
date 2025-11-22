from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# -------------------------------------------------------------
# 1. Definição do DAG de Orquestração
# -------------------------------------------------------------
# Este DAG é responsável por chamar todos os sub-DAGs na ordem correta
# para garantir que as dimensões sejam carregadas antes da tabela Fato.
# -------------------------------------------------------------

with DAG(
    dag_id="05_main_orchestration_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 0 * * *",  # Roda uma vez por dia à meia-noite (Horário UTC)
    catchup=False,
    tags=["orchestration", "main", "etl"],
    doc_md="""
    ## DAG de Orquestração Principal

    Este DAG coordena a execução de todos os fluxos de ETL:
    1.  Carga de Dimensões (Date, Product, Customer).
    2.  Carga da Tabela Fato (Sales).
    """,
) as dag:
    
    # 1. Inicialização do Esquema (Rodar apenas se necessário, geralmente uma vez)
    # Recomendado: Desativar este operador após a primeira execução bem-sucedida do "00_initialization_dag"
    trigger_init = TriggerDagRunOperator(
        task_id="trigger_00_initialization_dag",
        trigger_dag_id="00_initialization_dag",
        conf={},
        execution_date="{{ ds }}", # Passa a data de execução atual
    )

    # 2. Carregamento da Dimensão de Data (DimDate)
    trigger_dim_date = TriggerDagRunOperator(
        task_id="trigger_01_etl_dim_date",
        trigger_dag_id="01_etl_dim_date",
        conf={},
    )
    
    # 3. Carregamento da Dimensão de Produto (DimProduct) - NOVO PASSO
    trigger_dim_product = TriggerDagRunOperator(
        task_id="trigger_03_etl_dim_product",
        trigger_dag_id="03_etl_dim_product",
        conf={},
    )
    
    # 4. Carregamento da Dimensão de Cliente (DimCustomer)
    trigger_dim_customer = TriggerDagRunOperator(
        task_id="trigger_04_etl_dim_customer",
        trigger_dag_id="04_etl_dim_customer",
        conf={},
    )

    # 5. Carregamento da Tabela Fato de Vendas (FactSales)
    # A FactSales depende das dimensões (Date, Product e Customer) estarem carregadas.
    trigger_fact_sales = TriggerDagRunOperator(
        task_id="trigger_02_etl_fact_sales",
        trigger_dag_id="02_etl_fact_sales",
        conf={},
    )
    
    # ---------------------------------------------------------
    # 2. Definição da Ordem de Execução (Dependencies)
    # ---------------------------------------------------------
    
    # Ordem: Inicialização -> Dimensões em Paralelo -> Fato
    
    # 1. A Inicialização deve ocorrer primeiro
    trigger_init >> trigger_dim_date

    # 2. A Dimensão de Produto pode ser executada em paralelo com as outras dimensões
    trigger_dim_date >> [trigger_dim_product, trigger_dim_customer]

    # 3. A Fato de Vendas só pode rodar depois que TODAS as dimensões estiverem prontas
    [trigger_dim_product, trigger_dim_customer] >> trigger_fact_sales