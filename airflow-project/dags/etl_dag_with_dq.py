from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

POSTGRES_CONN_ID = "postgres_dw"

def run_quality_checks():
    """
    Executa verificações críticas de qualidade de dados (DQ) no Data Warehouse.
    Se qualquer verificação falhar, lança uma exceção para falhar a tarefa do Airflow.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    data_quality_tests = [
        ("SELECT COUNT(*) FROM dw.FactOrder WHERE OrderSK IS NULL", 0),
        ("SELECT COUNT(*) FROM dw.FactOrder WHERE CustomerSK IS NULL OR ProductSK IS NULL OR DateSK IS NULL", 0),
        ("SELECT COUNT(*) FROM dw.FactOrder WHERE SalesAmount <= 0", 0),
        ("""
         SELECT COUNT(DISTINCT fo.CustomerSK) 
         FROM dw.FactOrder fo
         LEFT JOIN dw.DimCustomer dc ON fo.CustomerSK = dc.CustomerSK
         WHERE dc.CustomerSK IS NULL;
         """, 0)
    ]

    error_count = 0
    print("--- INICIANDO VERIFICAÇÕES DE QUALIDADE DE DADOS (DQ) ---")

    for i, (sql_query, expected_result) in enumerate(data_quality_tests):
        try:
            print(f"\nVerificação {i+1}: Executando query: {sql_query}")
            records = hook.get_records(sql_query)
            actual_result = records[0][0] if records and records[0] else None
            
            if actual_result != expected_result:
                print(f"** FALHA NA VERIFICAÇÃO {i+1}! **")
                print(f"Resultado Esperado: {expected_result}, Resultado Obtido: {actual_result}")
                error_count += 1
            else:
                print(f"Verificação {i+1} CONCLUÍDA com sucesso. Resultado: {actual_result}")
                
        except Exception as e:
            print(f"ERRO CRÍTICO ao executar a verificação {i+1}: {e}")
            error_count += 1
    
    if error_count > 0:
        raise ValueError(f"O pipeline ETL falhou nas verificações de Data Quality. Total de {error_count} falhas.")
    
    print("\n--- TODAS AS VERIFICAÇÕES DE QUALIDADE DE DADOS PASSARAM COM SUCESSO! ---")

def load_dimension(dim_name: str):
    """Função simulada para carregar tabelas Dimensão.""" 
    print(f"Iniciando a lógica de SCD-1 para a dimensão: dw.Dim{dim_name}...")
    print(f"dw.Dim{dim_name} carregada com sucesso.")

def load_fact_table(fact_name: str):
    """Função simulada para carregar a tabela Fato."""
    print(f"Iniciando a carga da tabela Fato: dw.{fact_name}...")
    print(f"dw.{fact_name} carregada com sucesso.")

with DAG(
    dag_id="etl_data_warehouse_v2",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dw", "etl", "data_quality"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "postgres_conn_id": POSTGRES_CONN_ID,
    },
) as dag:
    
    create_schema_and_tables = BashOperator(
        task_id="create_schema_and_tables",
        bash_command="echo 'Simulando a execução do script dw_schema.sql para criar o Schema dw e as tabelas...'"
    )

    load_dim_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_dimension,
        op_kwargs={"dim_name": "Customer"},
    )

    load_dim_product = PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dimension,
        op_kwargs={"dim_name": "Product"},
    )

    load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dimension,
        op_kwargs={"dim_name": "Date"},
    )

    load_fact_order = PythonOperator(
        task_id="load_fact_order",
        python_callable=load_fact_table,
        op_kwargs={"fact_name": "FactOrder"},
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_quality_checks,
    )

    create_schema_and_tables >> [load_dim_customer, load_dim_product, load_dim_date]
    [load_dim_customer, load_dim_product, load_dim_date] >> load_fact_order
    load_fact_order >> data_quality_check
