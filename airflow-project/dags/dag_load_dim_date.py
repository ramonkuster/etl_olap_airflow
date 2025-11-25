from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_DW_CONN_ID = "postgres_dw"
START_DATE = '2023-01-01'
END_DATE = '2025-12-31'

with DAG(
    dag_id="10_load_dim_date",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dw", "dimension", "initial_load"],
) as dag:

    prepare_dim_date = PostgresOperator(
        task_id="prepare_dim_date",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql=f"""
        DO $$
        DECLARE
            r record;
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.dimdate'::regclass AND conname = 'dimdate_pkey') THEN
                EXECUTE 'ALTER TABLE public.dimdate DROP CONSTRAINT dimdate_pkey CASCADE';
            END IF;
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.dimdate'::regclass AND conname = 'dimdate_full_date_key') THEN
                EXECUTE 'ALTER TABLE public.dimdate DROP CONSTRAINT dimdate_full_date_key CASCADE';
            END IF;
        END $$;
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
        DELETE FROM public.dimdate;
        """,
        database="adventureworks_dw",
        autocommit=True,
    )

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
                    CAST(TO_CHAR(current_date, 'YYYYMMDD') AS INT),
                    current_date,
                    EXTRACT(DOW FROM current_date),
                    TO_CHAR(current_date, 'Day'),
                    EXTRACT(DAY FROM current_date),
                    EXTRACT(DOY FROM current_date),
                    EXTRACT(WEEK FROM current_date),
                    EXTRACT(MONTH FROM current_date),
                    TO_CHAR(current_date, 'Month'),
                    EXTRACT(QUARTER FROM current_date),
                    'Q' || EXTRACT(QUARTER FROM current_date),
                    EXTRACT(YEAR FROM current_date),
                    CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END
                );
                current_date := current_date + INTERVAL '1 day';
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """,
        database="adventureworks_dw",
        autocommit=True,
    )

    finalize_dim_date_schema = PostgresOperator(
        task_id="finalize_dim_date_schema",
        postgres_conn_id=POSTGRES_DW_CONN_ID,
        sql="""
        ALTER TABLE public.dimdate ADD CONSTRAINT dimdate_pkey PRIMARY KEY (date_key);
        ALTER TABLE public.dimdate ADD CONSTRAINT dimdate_full_date_key UNIQUE (full_date);
        """,
        database="adventureworks_dw",
        autocommit=True,
    )

    prepare_dim_date >> insert_dim_date_data >> finalize_dim_date_schema
