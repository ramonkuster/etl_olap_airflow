from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define o ID da conexão com o DW
POSTGRES_DW_CONN_ID = "postgres_dw"

# Período de tempo para gerar as datas
START_DATE = '2023-01-01'
END_DATE = '2025-12-31'

with DAG(
    dag_id="10_load_dim_date",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dw", "dimension", "initial_load"],
    doc_md="""
    ## DAG 10: Carregamento da Dimensão de Data (DimDate)
    
    Este DAG resolve o problema persistente de UniqueViolation garantindo que **todas**
    as chaves de unicidade (PK e Unique Constraints na coluna `full_date`) sejam
    removidas antes da inserção e recriadas ao final.
    """,
) as dag:
    
    # 1. Tarefa para PREPARAR E LIMPAR a DimDate (Dropa todas as Constraints, Garante Colunas, Limpa Dados)
    prepare_dim_date = PostgresOperator(
        task_id="prepare_dim_date",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql=f"""
        -- PASSO 1: Dropa TODAS as chaves de unicidade/primária conhecidas para evitar conflitos
        DO $$ 
        DECLARE 
            r record;
        BEGIN
            -- Tenta dropar a chave primária
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.dimdate'::regclass AND conname = 'dimdate_pkey') THEN
                EXECUTE 'ALTER TABLE public.dimdate DROP CONSTRAINT dimdate_pkey CASCADE';
            END IF;
            
            -- Tenta dropar a chave de unicidade na coluna full_date
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.dimdate'::regclass AND conname = 'dimdate_full_date_key') THEN
                EXECUTE 'ALTER TABLE public.dimdate DROP CONSTRAINT dimdate_full_date_key CASCADE';
            END IF;
        END $$;

        -- PASSO 2: Garante que todas as colunas necessárias existam
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS date_key INT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS full_date DATE;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS year SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS day_of_week SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS day_of_month SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS day_of_year SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS week_of_year SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS month_number SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS quarter_number SMALLINT;
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS day_name VARCHAR(20);
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS month_name VARCHAR(20);
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS quarter_name VARCHAR(10);
        ALTER TABLE public.dimdate ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN;
        
        -- PASSO 3: Limpa o conteúdo da tabela (o DELETE deve ser eficaz agora que não há constraints)
        DELETE FROM public.dimdate;
        """,
        database="adventureworks_dw", 
        autocommit=True, 
    )
    
    # 2. Tarefa para executar a lógica de INSERT (carregamento de dados)
    insert_dim_date_data = PostgresOperator(
        task_id="insert_dim_date_data",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql=f"""
        DO $$
        DECLARE
            start_date DATE := '{START_DATE}';
            end_date DATE := '{END_DATE}';
            current_date DATE;
        BEGIN
            -- Nenhuma chave de unicidade está ativa, permitindo a inserção limpa
            current_date := start_date;
            
            WHILE current_date <= end_date LOOP
                INSERT INTO DimDate (
                    date_key,
                    full_date,
                    day_of_week,
                    day_name,
                    day_of_month,
                    day_of_year,
                    week_of_year,
                    month_number,
                    month_name,
                    quarter_number,
                    quarter_name,
                    year,
                    is_weekend
                ) VALUES (
                    CAST(TO_CHAR(current_date, 'YYYYMMDD') AS INT),  -- date_key
                    current_date,                                   -- full_date
                    EXTRACT(DOW FROM current_date),                 -- day_of_week (0=Sunday, 6=Saturday)
                    TO_CHAR(current_date, 'Day'),                   -- day_name
                    EXTRACT(DAY FROM current_date),                 -- day_of_month
                    EXTRACT(DOY FROM current_date),                 -- day_of_year
                    EXTRACT(WEEK FROM current_date),                -- week_of_year
                    EXTRACT(MONTH FROM current_date),               -- month_number
                    TO_CHAR(current_date, 'Month'),                 -- month_name
                    EXTRACT(QUARTER FROM current_date),             -- quarter_number
                    'Q' || EXTRACT(QUARTER FROM current_date),      -- quarter_name
                    EXTRACT(YEAR FROM current_date),                -- year
                    CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END -- is_weekend
                );
                
                current_date := current_date + INTERVAL '1 day';
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """,
        database="adventureworks_dw", 
        autocommit=True, 
    )

    # 3. Tarefa para FINALIZAR o Schema (Recria as Chaves de Unicidade)
    finalize_dim_date_schema = PostgresOperator(
        task_id="finalize_dim_date_schema",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql="""
        -- Recria a Chave Primária
        ALTER TABLE public.dimdate ADD CONSTRAINT dimdate_pkey PRIMARY KEY (date_key);
        
        -- Recria a Chave de Unicidade em full_date
        ALTER TABLE public.dimdate ADD CONSTRAINT dimdate_full_date_key UNIQUE (full_date);
        """,
        database="adventureworks_dw", 
        autocommit=True, 
    )


    # Define a ordem de execução
    prepare_dim_date >> insert_dim_date_data >> finalize_dim_date_schema