from __future__ import annotations

import pendulum
from sqlalchemy import create_engine, text

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

OLTP_CONN_ID = "postgres_oltp"
DW_CONN_ID = "postgres_dw"

def initialize_oltp():
    """Cria tabelas no banco de dados OLTP e insere dados iniciais.
    
    ATENÇÃO: Usamos 'engine.begin()' para gerenciar a transação
    corretamente com o SQLAlchemy, resolvendo o erro 'commit'.
    """
    oltp_hook = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    oltp_conn_uri = oltp_hook.get_uri().replace("psycopg2", "postgresql")
    engine = create_engine(oltp_conn_uri)
    
    print("Inicializando esquema OLTP (Customer, Sales, Product)...")

    sql_customer = """
    DROP TABLE IF EXISTS Customer CASCADE;
    
    CREATE TABLE Customer (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        city VARCHAR(50),
        country VARCHAR(50),
        segment VARCHAR(20)
    );
    TRUNCATE TABLE Customer RESTART IDENTITY CASCADE;
    INSERT INTO Customer (first_name, last_name, email, city, country, segment) VALUES
    ('Alice', 'Smith', 'alice@example.com', 'New York', 'USA', 'Consumer'),
    ('Bob', 'Johnson', 'bob@example.com', 'London', 'UK', 'Corporate'),
    ('Charlie', 'Brown', 'charlie@example.com', 'Paris', 'France', 'Home Office'),
    ('David', 'Lee', 'david@example.com', 'Tokyo', 'Japan', 'Consumer'),
    ('Eve', 'Davis', 'eve@example.com', 'Berlin', 'Germany', 'Corporate');
    """

    sql_product = """
    DROP TABLE IF EXISTS Product CASCADE;
    
    CREATE TABLE Product (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        list_price NUMERIC(10, 2),
        standard_cost NUMERIC(10, 2)
    );
    TRUNCATE TABLE Product RESTART IDENTITY CASCADE;
    INSERT INTO Product (product_id, product_name, category, list_price, standard_cost) VALUES
    (1, 'Laptop Pro X1', 'Electronics', 1200.00, 800.00),
    (2, 'Monitor Ultra HD', 'Electronics', 450.00, 300.00),
    (3, 'Office Chair Ergonomic', 'Furniture', 250.00, 150.00),
    (4, 'Premium Coffee Maker', 'Home Goods', 150.00, 90.00),
    (5, 'Wireless Keyboard', 'Accessories', 75.00, 40.00);
    """

    sql_sales = """
    DROP TABLE IF EXISTS Sales CASCADE;
    
    CREATE TABLE Sales (
        sale_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES Customer(customer_id),
        sale_date DATE,
        product_id INTEGER REFERENCES Product(product_id),
        quantity INTEGER,
        amount NUMERIC(10, 2)
    );
    TRUNCATE TABLE Sales RESTART IDENTITY CASCADE;
    INSERT INTO Sales (customer_id, sale_date, product_id, quantity, amount) VALUES
    (1, '2024-01-05', 1, 1, 1200.00),
    (2, '2024-01-05', 3, 2, 500.00),
    (3, '2024-01-06', 4, 1, 150.00),
    (1, '2024-01-07', 2, 1, 450.00),
    (4, '2024-01-08', 5, 3, 225.00),
    (5, '2024-01-08', 1, 1, 1200.00),
    (5, '2024-01-09', 2, 1, 450.00),
    (4, '2024-01-09', 4, 2, 300.00);
    """
    
    with engine.begin() as connection:
        connection.execute(text(sql_customer))
        connection.execute(text(sql_product)) 
        connection.execute(text(sql_sales))
    print("Esquema OLTP inicializado com dados de 5 clientes, 5 produtos e 8 vendas.")

def initialize_dw():
    """Cria tabelas de Dimensão e Fato no Data Warehouse."""
    dw_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    dw_conn_uri = dw_hook.get_uri().replace("psycopg2", "postgresql")
    engine = create_engine(dw_conn_uri)
    
    print("Inicializando esquema DW (DimCustomer, DimDate, DimProduct, FactSales)...")
    
    sql_dw = """
    DROP TABLE IF EXISTS DimCustomer CASCADE;
    CREATE TABLE DimCustomer (
        customer_key SERIAL PRIMARY KEY,
        customer_id INTEGER UNIQUE,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        full_name VARCHAR(100),
        city VARCHAR(50),
        country VARCHAR(50),
        segment VARCHAR(20)
    );
    TRUNCATE TABLE DimCustomer RESTART IDENTITY CASCADE;

    DROP TABLE IF EXISTS DimDate CASCADE;
    CREATE TABLE DimDate (
        date_key INTEGER PRIMARY KEY,
        full_date DATE UNIQUE,
        day_of_month INTEGER,
        month INTEGER,
        year INTEGER,
        day_of_week VARCHAR(10),
        is_weekend BOOLEAN
    );

    DROP TABLE IF EXISTS DimProduct CASCADE;
    CREATE TABLE DimProduct (
        product_key SERIAL PRIMARY KEY,
        product_id INTEGER UNIQUE,
        product_name VARCHAR(100),
        category VARCHAR(50),
        list_price NUMERIC(10, 2),
        standard_cost NUMERIC(10, 2)
    );
    TRUNCATE TABLE DimProduct RESTART IDENTITY CASCADE;

    DROP TABLE IF EXISTS FactSales CASCADE;
    CREATE TABLE FactSales (
        sales_key SERIAL PRIMARY KEY,
        sales_order_id INTEGER UNIQUE,
        customer_key INTEGER REFERENCES DimCustomer(customer_key),
        order_date_key INTEGER REFERENCES DimDate(date_key),
        product_key INTEGER REFERENCES DimProduct(product_key),
        order_qty INTEGER,
        line_total NUMERIC(10, 2)
    );
    TRUNCATE TABLE FactSales RESTART IDENTITY CASCADE;
    """
    
    with engine.begin() as connection:
        connection.execute(text(sql_dw))
    print("Esquema DW inicializado com dimensões e fato.")

with DAG(
    dag_id="00_initialization_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["setup", "oltp", "dw"],
    doc_md=__doc__,
) as dag:
    
    oltp_init_task = PythonOperator(
        task_id="initialize_oltp_schema_and_data",
        python_callable=initialize_oltp,
    )
    
    dw_init_task = PythonOperator(
        task_id="initialize_dw_schema",
        python_callable=initialize_dw,
    )
    
    oltp_init_task >> dw_init_task
