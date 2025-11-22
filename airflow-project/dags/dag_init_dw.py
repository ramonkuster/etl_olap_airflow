from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define o caminho do arquivo SQL a ser executado
SQL_FILE_PATH = "sql/create_dw_schema.sql"

# Define o ID da conexão que criamos no Airflow Webserver
POSTGRES_DW_CONN_ID = "postgres_dw"

with DAG(
    dag_id="00_dw_schema_initialization",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # Rodamos este DAG apenas uma vez manualmente
    catchup=False,
    tags=["dw", "setup"],
    doc_md=__doc__,
) as dag:
    
    # Tarefa para executar o script SQL de criação de tabelas
    create_dw_schema = PostgresOperator(
        task_id="create_dw_schema",
        postgres_conn_id=POSTGRES_DW_CONN_ID, # Usa a conexão com o DW
        sql=SQL_FILE_PATH, # O Airflow lerá o conteúdo do arquivo
        # Define o banco de dados específico, embora já esteja na conexão
        database="adventureworks_dw", 
        # Garante que os comandos CREATE TABLE e CREATE INDEX funcionem
        autocommit=True, 
    )