import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Conexões
OLTP_CONN_ID = "postgres_oltp"
DW_CONN_ID = "postgres_dw"

# --- FUNÇÃO DE CRIAÇÃO DO SCHEMA E TABELAS DW ---
def create_dw_schema_and_tables():
    """Cria o schema DW e as tabelas DimCustomer, DimProduct e FactOrder."""
    dw_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    
    # 1. Criação do Schema DW
    print("Verificando e criando o schema DW...")
    dw_hook.run("CREATE SCHEMA IF NOT EXISTS dw;")

    # 2. Criação da DimCustomer
    print("Criando a tabela dw.DimCustomer...")
    dw_hook.run("""
    CREATE TABLE IF NOT EXISTS dw.DimCustomer (
        CustomerSK SERIAL PRIMARY KEY,
        CustomerBK VARCHAR(255) NOT NULL UNIQUE,
        FullName VARCHAR(510),
        Email VARCHAR(255),
        City VARCHAR(255),
        Country VARCHAR(255),
        Segment VARCHAR(255)
    );
    """)

    # 3. Criação da DimProduct
    print("Criando a tabela dw.DimProduct...")
    dw_hook.run("""
    CREATE TABLE IF NOT EXISTS dw.DimProduct (
        ProductSK SERIAL PRIMARY KEY,
        ProductBK VARCHAR(255) NOT NULL UNIQUE,
        ProductName VARCHAR(510),
        Category VARCHAR(255),
        Price NUMERIC(10, 2)
    );
    """)

    # 4. Criação da FactOrder
    print("Criando a tabela dw.FactOrder...")
    dw_hook.run("""
    CREATE TABLE IF NOT EXISTS dw.FactOrder (
        OrderSK SERIAL PRIMARY KEY,
        OrderBK VARCHAR(255) NOT NULL UNIQUE,
        CustomerSK INT REFERENCES dw.DimCustomer(CustomerSK),
        ProductSK INT REFERENCES dw.DimProduct(ProductSK),
        OrderDate DATE,
        Quantity INT,
        SalesAmount NUMERIC(10, 2)
    );
    """)
    print("Estrutura do DW criada com sucesso!")
# --- FIM FUNÇÃO DE CRIAÇÃO ---


# --- FUNÇÕES DE ETL ---

def etl_dim_customer(table_name="dw.dimcustomer"):
    """Extrai, Transforma e Carrega dados de clientes para a DimCustomer."""
    
    oltp_hook = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dw_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    
    print(f"Iniciando ETL para DimCustomer com schema OLTP: public...")

    # Extração: Ler os dados da tabela OLTP 'customer'
    sql_query = "SELECT customer_id, first_name, last_name, email, city, country, segment FROM customer;"
    df = oltp_hook.get_pandas_df(sql=sql_query)
    
    # Transformação: Concatena nome e sobrenome
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df = df.rename(columns={'customer_id': 'customerbk'})
    df = df[['customerbk', 'full_name', 'email', 'city', 'country', 'segment']]
    
    print(f"Colunas do DataFrame de Customer: {list(df.columns)}")

    # Carregamento: Limpeza (TRUNCATE) e Inserção
    print(f"Limpando a tabela {table_name} antes de carregar...")
    
    # Adicionado CASCADE para limpar FactOrder, que a referencia.
    dw_hook.run(f"TRUNCATE TABLE {table_name} CASCADE;")
    
    print(f"Carregando {len(df)} registros na tabela {table_name}...")
    dw_hook.insert_rows(
        table=table_name, 
        rows=df.values.tolist(), 
        target_fields=['customerbk', 'fullname', 'email', 'city', 'country', 'segment']
    )
    print("ETL DimCustomer concluído com sucesso.")

def etl_dim_product(table_name="dw.dimproduct"):
    """Extrai, Transforma e Carrega dados de produtos para a DimProduct."""
    
    oltp_hook = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dw_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    
    print(f"Iniciando ETL para DimProduct com schema OLTP: public...")

    # Extração: Ler os dados da tabela OLTP 'product'
    sql_query = "SELECT product_id, product_name, category, list_price FROM product;"
    df = oltp_hook.get_pandas_df(sql=sql_query)
    
    # Transformação: Renomear colunas para o padrão DW
    df = df.rename(columns={'product_id': 'productbk', 'list_price': 'price'})
    
    print(f"Colunas do DataFrame de Product: {list(df.columns)}")

    # Carregamento: Limpeza (TRUNCATE) e Inserção
    print(f"Limpando a tabela {table_name} antes de carregar...")
    # Adicionado CASCADE para limpar FactOrder, que a referencia.
    dw_hook.run(f"TRUNCATE TABLE {table_name} CASCADE;")
    
    print(f"Carregando {len(df)} registros na tabela {table_name}...")
    dw_hook.insert_rows(
        table=table_name, 
        rows=df.values.tolist(), 
        target_fields=['productbk', 'productname', 'category', 'price']
    )
    print("ETL DimProduct concluído com sucesso.")


def etl_fact_order(table_name="dw.factorder"):
    """Extrai, Transforma e Carrega dados de pedidos para a FactOrder."""
    
    oltp_hook = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dw_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    
    print(f"Iniciando ETL para FactOrder com schema OLTP: public...")

    # Extração: Ler os dados da tabela OLTP 'sales'. 
    sql_query = "SELECT sale_id, customer_id, product_id, sale_date, quantity, amount FROM public.sales;"
    df_order = oltp_hook.get_pandas_df(sql=sql_query)
    
    # CORREÇÃO CRÍTICA DE TIPAGEM: 
    # Garante que as chaves de OLTP (customer_id e product_id) sejam strings (object)
    # para que o pd.merge funcione corretamente com as chaves BK (customerbk, productbk)
    # que são VARCHAR no DW.
    df_order['customer_id'] = df_order['customer_id'].astype(str)
    df_order['product_id'] = df_order['product_id'].astype(str)
    
    # 1. Obter SKs (Surrogate Keys)
    # Obter DimCustomer para mapeamento
    df_customer_sk = dw_hook.get_pandas_df(sql="SELECT customersk, customerbk FROM dw.DimCustomer;")
    # Obter DimProduct para mapeamento
    df_product_sk = dw_hook.get_pandas_df(sql="SELECT productsk, productbk FROM dw.DimProduct;")

    # 2. Mapeamento
    # Mapear CustomerSK
    df_order = pd.merge(
        df_order, 
        df_customer_sk, 
        left_on='customer_id', 
        right_on='customerbk', 
        how='left'
    )
    # Mapear ProductSK
    df_order = pd.merge(
        df_order, 
        df_product_sk, 
        left_on='product_id', 
        right_on='productbk', 
        how='left'
    )
    
    # 3. Seleção final das colunas para a Fact Table
    # Renomeia chaves OLTP para o padrão DW
    df_order = df_order.rename(columns={
        'sale_id': 'orderbk', 
        'sale_date': 'order_date',
        'amount': 'sales_amount' 
    })
    
    df_order = df_order[[
        'orderbk', 'customersk', 'productsk', 
        'order_date', 'quantity', 'sales_amount'
    ]]
    
    print(f"Colunas do DataFrame de FactOrder: {list(df_order.columns)}")

    # Carregamento: Limpeza (TRUNCATE) e Inserção
    print(f"Limpando a tabela {table_name} antes de carregar...")
    # Não precisa de CASCADE aqui, FactOrder é a 'folha' na árvore de dependências.
    dw_hook.run(f"TRUNCATE TABLE {table_name};") 
    
    print(f"Carregando {len(df_order)} registros na tabela {table_name}...")
    dw_hook.insert_rows(
        table=table_name, 
        rows=df_order.values.tolist(), 
        target_fields=['orderbk', 'customersk', 'productsk', 'orderdate', 'quantity', 'salesamount']
    )
    print("ETL FactOrder concluído com sucesso.")

# --- DEFINIÇÃO DO DAG ---

@dag(
    dag_id="04_etl_pipeline_completo",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "datawarehouse"],
)
def etl_pipeline_completo_dag():
    
    # Nova tarefa: Cria o schema e as tabelas DW
    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_dw_schema_and_tables,
    )

    # Tarefas ETL de Dimensão
    etl_customer = PythonOperator(
        task_id='etl_dim_customer',
        python_callable=etl_dim_customer,
    )

    etl_product = PythonOperator(
        task_id='etl_dim_product',
        python_callable=etl_dim_product,
    )

    # Tarefa ETL de Fato
    etl_order = PythonOperator(
        task_id='etl_fact_order',
        python_callable=etl_fact_order,
    )

    # Definição das dependências
    
    # 1. Criação de tabelas deve vir antes de tudo
    create_tables >> [etl_customer, etl_product] 
    
    # 2. Fato depende das Dimensões estarem carregadas
    [etl_customer, etl_product] >> etl_order

etl_pipeline_completo_dag()